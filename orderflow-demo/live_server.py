#!/usr/bin/env python3
"""DNSE market-only collector + persisted replay API + WebSocket UI. No trading calls."""
import argparse
import asyncio
import base64
import binascii
from datetime import datetime
from functools import partial
import hashlib
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import json
import math
import os
import re
from pathlib import Path
import select
import signal
import socket
import ssl
import struct
import threading
import time
from urllib.parse import parse_qs, urlsplit

from dotenv import load_dotenv
from live_store import LiveStore, normalize, normalize_influence, normalize_market_index
from replay_server import ROOT, TZ


def number(q, name, default, integer=False):
    value=q.get(name,[default])[0]
    n=int(value) if integer else float(value)
    if not math.isfinite(n):
        raise ValueError('Non-finite '+name)
    return n


class Runtime:
    def __init__(self, root, symbol, day=None):
        if not re.fullmatch(r'[A-Z0-9_-]{1,32}',symbol):
            raise ValueError('Invalid symbol')
        self.root,self.symbol,self.day=root,symbol,day
        self.stores={}
        self.writers={}
        self.lock=threading.RLock()
        self.status='offline'
        self.stop=threading.Event()

    def store(self, day=None):
        day=day or self.day or datetime.now(TZ).date().isoformat()
        with self.lock:
            if day not in self.stores:
                # Drain and close old databases before deleting their WAL sidecars.
                for old_day in list(self.stores):
                    if old_day<day:
                        old=self.stores.pop(old_day)
                        old.stop.set()
                        self.writers.pop(old_day).join()
                        old.close()
                self.prune(day)
                store=LiveStore(self.root,self.symbol,day)
                store.connection(self.status)
                writer=threading.Thread(target=store.writer,daemon=True)
                writer.start()
                self.stores[day]=store;self.writers[day]=writer
            return self.stores[day]

    def prune(self, day):
        """Only remove this collector's older daily files for the selected symbol."""
        datetime.strptime(day,'%Y-%m-%d')
        for path in (Path(self.root)/self.symbol).glob('*'):
            match=re.fullmatch(r'(\d{4}-\d{2}-\d{2})\.sqlite(?:-wal|-shm|-journal)?',path.name)
            if match and match[1]<day and path.is_file() and not path.is_symlink():
                try:
                    datetime.strptime(match[1],'%Y-%m-%d')
                except ValueError:
                    continue
                path.unlink()

    def connection(self,status,error=None):
        with self.lock:
            self.status=status
            self.store().connection(status,error)

    def close(self):
        self.stop.set()
        for store in self.stores.values():
            store.stop.set()
        for writer in self.writers.values():
            writer.join()  # drain every accepted record before closing SQLite
        for store in self.stores.values():
            if not store.failed:
                store.close()


async def collect(runtime, key, secret, url):
    # SDK timestamp parser uses local time; never depend on the host timezone.
    os.environ['TZ']='Asia/Bangkok'
    if hasattr(time,'tzset'):
        time.tzset()
    from dnse import TradingClient
    class MarketClient(TradingClient):
        async def _dispatch_message(self,data):
            # SDK 2.4.1 PriceLevel expects qtty; docs also use quantity.
            if data.get('T')=='q':
                for side in ('bid','offer'):
                    for level in data.get(side) or []:
                        if 'qtty' not in level and 'quantity' in level:
                            level['qtty']=level['quantity']
            await super()._dispatch_message(data)
    delay=1
    while not runtime.stop.is_set():
        client=None
        try:
            runtime.connection('connecting')
            client=MarketClient(api_key=key,api_secret=secret,base_url=url,
                                 encoding='msgpack',auto_reconnect=True,max_retries=3,timeout=15)
            client.on('reconnecting',lambda _:runtime.connection('reconnecting'))
            client.on('reconnected',lambda _:runtime.connection('connected'))
            # Expose error type, never arbitrary exception text containing credentials.
            client.on('error',lambda e:runtime.connection('error',type(e).__name__))
            def receive(kind,obj):
                store=runtime.store()
                if store.failed:
                    return
                try:
                    event=normalize(obj,kind,runtime.symbol,store.day)
                    if store.status=='invalid':
                        store.connection('connected')
                    store.enqueue(event)
                except (ValueError,TypeError,AttributeError,OverflowError):
                    store.invalid+=1
                    store.connection('invalid','Invalid market payload; see invalid count')
            def receive_influence(obj):
                store=runtime.store()
                if store.failed:
                    return
                try:
                    store.enqueue_influence(normalize_influence(obj))
                except (ValueError,TypeError,AttributeError,OverflowError):
                    with store.lock:
                        store.influence_invalid+=1
                        store.changed.notify_all()
            def receive_index(kind,obj):
                store=runtime.store()
                if store.failed:
                    return
                try:
                    store.enqueue_index(normalize_market_index(obj,kind,store.day))
                except (ValueError,TypeError,AttributeError,OverflowError):
                    with store.lock:
                        store.index_invalid+=1
                        store.changed.notify_all()
            # Register before SUBSCRIBE so the first pushed records cannot race registration.
            client.on('trade_extra',partial(receive,1))
            client.on('quote',partial(receive,0))
            client.on('market_index_influence',receive_influence)
            client.on('market_index',partial(receive_index,'market'))
            client.on('estimated_market_index',partial(receive_index,'estimated'))
            await asyncio.wait_for(client.connect(),20)
            await client.subscribe_trade_extra([runtime.symbol],encoding='msgpack',board_id='G1')
            await client.subscribe_quotes([runtime.symbol],encoding='msgpack',board_id='G1')
            await client.subscribe_market_index('VN30',encoding='msgpack')
            await client.subscribe_estimated_market_index('VN30',encoding='msgpack')
            await client.subscribe_market_index_influence('VN30',resolution=1,encoding='msgpack')
            runtime.connection('connected')
            delay=1
            while not runtime.stop.is_set():
                await asyncio.sleep(.5)
                store=runtime.store()  # rotate storage automatically at local midnight
                if store.failed:
                    raise RuntimeError('Storage unavailable')
                # SDK retries internally; supervise task death as well as SDK events.
                task=client._message_handler_task
                if task is None or task.done():
                    raise RuntimeError('SDK receiver stopped')
                if not client._connection.is_connected and runtime.status=='connected':
                    raise RuntimeError('SDK socket closed')
        except asyncio.CancelledError:
            break
        except Exception as exc:
            runtime.connection('error',type(exc).__name__)
            print('DNSE connection unavailable: '+type(exc).__name__,flush=True)
        finally:
            if client:
                try:
                    await asyncio.wait_for(client.disconnect(),10)
                except Exception:
                    pass
        if runtime.store().failed:
            break
        for _ in range(delay*2):
            if runtime.stop.is_set():
                break
            await asyncio.sleep(.5)
        delay=min(60,delay*2)
    if not runtime.store().failed:
        runtime.connection('stopped')


class Handler(SimpleHTTPRequestHandler):
    protocol_version='HTTP/1.1'

    def __init__(self,*args,runtime,**kwargs):
        self.runtime=runtime
        super().__init__(*args,directory=str(ROOT/'dist'),**kwargs)

    def log_message(self, fmt, *args):
        # Keep noisy market polling out of console; no secrets in query strings.
        if args and str(args[1] if len(args)>1 else '') not in ('200','304'):
            super().log_message(fmt,*args)

    def json(self,data):
        body=json.dumps(data,separators=(',',':'),allow_nan=False).encode()
        self.send_response(200)
        self.send_header('Content-Type','application/json; charset=utf-8')
        self.send_header('Cache-Control','no-store')
        self.send_header('Content-Length',str(len(body)))
        self.end_headers();self.wfile.write(body)

    def do_GET(self):
        url=urlsplit(self.path)
        if not url.path.startswith('/api/'):
            if url.path=='/':
                self.path='/orderflow-live.html'
            # Serve only actual public UI assets, never data/config through filesystem.
            if not self.path.split('?',1)[0] in ('/orderflow-live.html','/orderflow-replay.html',
                    '/orderflow-dom.html','/dom-latency.html','/replay-static.js'):
                self.send_error(404);return
            super().do_GET();return
        try:
            q=parse_qs(url.query)
            store=self.runtime.store()
            if url.path=='/api/meta':
                self.json(store.info());return
            if url.path=='/api/status':
                self.json(store.info());return
            if url.path=='/api/influence-history':
                self.json(store.influence_history(number(q,'bucket',60,True)));return
            if url.path=='/api/ws/influence':
                self.websocket_influence();return
            if url.path=='/api/ws/index':
                self.websocket_index();return
            tf=number(q,'tf',60,True);window=number(q,'window',600,True)
            minimum=number(q,'large_min',50,True)
            if tf not in (30,60,180,300,900) or not 60<=window<=3600 or minimum<0:
                raise ValueError('Invalid timeframe/window/threshold')
            if url.path=='/api/large-trades':
                t=number(q,'t',0) if 't' in q else None
                if t is not None and t<0:
                    raise ValueError('Invalid timestamp')
                self.json(store.large_trades(t,minimum,q.get('side',['ALL'])[0],
                          number(q,'limit',100,True),q.get('before',[None])[0],number(q,'from',0)))
            elif url.path=='/api/state':
                t=number(q,'t',0) if 't' in q else None
                if t is not None and t<0:
                    raise ValueError('Invalid timestamp')
                self.json(store.snapshot(t,tf,window,minimum))
            elif url.path=='/api/ws':
                self.websocket(tf,window,minimum)
            elif url.path=='/api/ws/raw':
                self.websocket_raw()
            else:
                self.send_error(404)
        except (ValueError,TypeError) as exc:
            self.send_error(400,str(exc))
        except (BrokenPipeError,ConnectionResetError,TimeoutError):
            pass

    def websocket(self,tf,window,minimum):
        if not self.websocket_upgrade():
            return
        previous=None;previous_persisted=None;last_sent=0
        while not self.runtime.stop.is_set():
            if self.websocket_peer_closed():
                break
            store=self.runtime.store()
            version=(store.day,store.version,store.status,store.invalid)
            if version!=previous or time.monotonic()-last_sent>=2:
                try:
                    data=store.snapshot(None,tf,window,minimum)
                except ValueError:
                    if store.retired:
                        continue  # midnight rollover closed this session
                    raise
                self.websocket_send({'type':'state','data':data})
                previous=version;last_sent=time.monotonic()
            persisted=(store.day,store.persisted_meta['trades'])
            if persisted!=previous_persisted:
                self.websocket_send({'type':'persisted','day':persisted[0],'trades':persisted[1]})
                previous_persisted=persisted
            with store.changed:
                store.changed.wait_for(
                    lambda: (store.day,store.version,store.status,store.invalid)!=previous
                            or (store.day,store.persisted_meta['trades'])!=previous_persisted
                            or self.runtime.stop.is_set(),timeout=.1)

    def websocket_raw(self):
        if not self.websocket_upgrade():
            return
        store=None;cursor=0;status=None;last_heartbeat=0
        while not self.runtime.stop.is_set():
            if self.websocket_peer_closed():
                break
            current=self.runtime.store()
            if current is not store:
                store=current
                initial=store.raw_initial()
                cursor=initial['cursor'];status=initial['status']
                self.websocket_send({'type':'hello',**initial,'sent_ms':time.time_ns()//1000000})
                last_heartbeat=time.monotonic()
            events,next_cursor,lost=store.raw_since(cursor)
            if events:
                self.websocket_send({'type':'events','events':events,'lost':lost,
                                     'sent_ms':time.time_ns()//1000000})
                cursor=next_cursor
                continue
            if store.status!=status or time.monotonic()-last_heartbeat>=2:
                status=store.status
                self.websocket_send({'type':'heartbeat','status':status,
                                     'sent_ms':time.time_ns()//1000000})
                last_heartbeat=time.monotonic()
            with store.changed:
                store.changed.wait_for(lambda:store.raw_sequence!=cursor or store.status!=status
                                       or self.runtime.stop.is_set(),timeout=.1)

    def websocket_influence(self):
        if not self.websocket_upgrade():
            return
        store=None;cursor=0;status=None;last_heartbeat=0
        while not self.runtime.stop.is_set():
            if self.websocket_peer_closed():
                break
            current=self.runtime.store()
            if current is not store:
                store=current
                initial=store.influence_initial()
                cursor=initial['cursor'];status=initial['status']
                self.websocket_send({'type':'hello',**initial,'sent_ms':time.time_ns()//1000000})
                last_heartbeat=time.monotonic()
            events,next_cursor,lost=store.influence_since(cursor)
            if events:
                self.websocket_send({'type':'events','events':events,'lost':lost,
                                     'sent_ms':time.time_ns()//1000000})
                cursor=next_cursor
                continue
            if store.status!=status or time.monotonic()-last_heartbeat>=2:
                status=store.status
                self.websocket_send({'type':'heartbeat','status':status,
                                     'sent_ms':time.time_ns()//1000000})
                last_heartbeat=time.monotonic()
            with store.changed:
                store.changed.wait_for(lambda:store.influence_sequence!=cursor or store.status!=status
                                       or self.runtime.stop.is_set(),timeout=.1)

    def websocket_index(self):
        if not self.websocket_upgrade():
            return
        store=None;cursor=0;status=None;last_heartbeat=0
        while not self.runtime.stop.is_set():
            if self.websocket_peer_closed():
                break
            current=self.runtime.store()
            if current is not store:
                store=current
                initial=store.index_initial()
                cursor=initial['cursor'];status=initial['status']
                self.websocket_send({'type':'hello',**initial,'sent_ms':time.time_ns()//1000000})
                last_heartbeat=time.monotonic()
            events,next_cursor,lost=store.index_since(cursor)
            if events:
                self.websocket_send({'type':'events','events':events,'lost':lost,
                                     'sent_ms':time.time_ns()//1000000})
                cursor=next_cursor
                continue
            if store.status!=status or time.monotonic()-last_heartbeat>=2:
                status=store.status
                self.websocket_send({'type':'heartbeat','status':status,
                                     'sent_ms':time.time_ns()//1000000})
                last_heartbeat=time.monotonic()
            with store.changed:
                store.changed.wait_for(lambda:store.index_sequence!=cursor or store.status!=status
                                       or self.runtime.stop.is_set(),timeout=.1)

    def websocket_upgrade(self):
        connection={part.strip().lower() for part in self.headers.get('Connection','').split(',')}
        key=self.headers.get('Sec-WebSocket-Key','')
        try:
            valid_key=len(base64.b64decode(key,validate=True))==16
        except (ValueError,binascii.Error):
            valid_key=False
        origin=self.headers.get('Origin')
        if origin and urlsplit(origin).netloc.lower()!=self.headers.get('Host','').lower():
            self.send_error(403,'WebSocket origin mismatch');return False
        if (self.headers.get('Upgrade','').lower()!='websocket' or 'upgrade' not in connection
                or self.headers.get('Sec-WebSocket-Version')!='13' or not valid_key):
            self.send_error(426,'WebSocket upgrade required');return False
        accept=base64.b64encode(hashlib.sha1((key+
            '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest()).decode()
        self.send_response(101,'Switching Protocols')
        self.send_header('Upgrade','websocket')
        self.send_header('Connection','Upgrade')
        self.send_header('Sec-WebSocket-Accept',accept)
        self.end_headers()
        self.close_connection=True
        self.connection.settimeout(1)
        try:
            self.connection.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1)
        except (AttributeError,OSError):
            pass  # in-process socket doubles do not expose TCP options
        return True

    def websocket_frame(self, opcode, payload=b''):
        size=len(payload)
        if size<126:
            header=bytes((0x80|opcode,size))
        elif size<65536:
            header=bytes((0x80|opcode,126))+struct.pack('!H',size)
        else:
            header=bytes((0x80|opcode,127))+struct.pack('!Q',size)
        return header+payload

    def websocket_send(self, message):
        payload=json.dumps(message,separators=(',',':'),allow_nan=False).encode()
        self.connection.sendall(self.websocket_frame(1,payload))

    def websocket_peer_closed(self):
        if not hasattr(self.connection,'fileno'):
            return False  # in-process HTTP tests use a minimal socket double
        def read_exact(size):
            data=b''
            while len(data)<size:
                part=self.connection.recv(size-len(data))
                if not part:
                    raise ConnectionError('WebSocket closed')
                data+=part
            return data
        try:
            pending=getattr(self.connection,'pending',lambda:0)()
            if not pending and not select.select([self.connection],[],[],0)[0]:
                return False
            head=read_exact(2)
            opcode=head[0]&0x0f
            if not head[1]&0x80:
                return True  # client-to-server frames must be masked
            size=head[1]&0x7f
            if size==126:size=struct.unpack('!H',read_exact(2))[0]
            elif size==127:size=struct.unpack('!Q',read_exact(8))[0]
            if size>1024:
                return True  # this channel only expects close/control frames
            mask=read_exact(4);payload=read_exact(size)
            payload=bytes(value^mask[i%4] for i,value in enumerate(payload))
            if opcode==8:
                self.connection.sendall(self.websocket_frame(8,payload[:125]))
                return True
            if opcode==9:
                self.connection.sendall(self.websocket_frame(10,payload[:125]))
        except (OSError,ConnectionError):
            return True
        return False


def main():
    load_dotenv(ROOT.parent/'.env')
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--symbol',default=os.getenv('DNSE_SYMBOL','41I1GB000'))
    parser.add_argument('--host',default='127.0.0.1')
    parser.add_argument('--port',type=int,default=8097)
    parser.add_argument('--data-dir',type=Path,default=ROOT.parent/'data'/'live')
    parser.add_argument('--offline',action='store_true',help='Serve persisted data without DNSE connection')
    parser.add_argument('--day',help='Offline day; only the current UTC+7 day is retained')
    parser.add_argument('--tls-cert',type=Path,default=os.getenv('ORDERFLOW_TLS_CERT'),
                        help='TLS certificate for HTTPS/WSS; requires --tls-key')
    parser.add_argument('--tls-key',type=Path,default=os.getenv('ORDERFLOW_TLS_KEY'),
                        help='TLS private key for HTTPS/WSS; requires --tls-cert')
    args=parser.parse_args()
    if args.day and not args.offline:
        parser.error('--day requires --offline')
    if args.day and args.day!=datetime.now(TZ).date().isoformat():
        parser.error('Only the current UTC+7 session is retained; omit --day')
    if bool(args.tls_cert)!=bool(args.tls_key):
        parser.error('--tls-cert and --tls-key must be provided together')
    key,secret=os.getenv('DNSE_API_KEY'),os.getenv('DNSE_API_SECRET')
    if not args.offline and not (key and secret):
        parser.error('Set DNSE_API_KEY and DNSE_API_SECRET in project .env')
    runtime=Runtime(args.data_dir,args.symbol.upper(),args.day)
    runtime.store()
    try:
        server=ThreadingHTTPServer((args.host,args.port),partial(Handler,runtime=runtime))
        if args.tls_cert:
            context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(args.tls_cert,args.tls_key)
            server.socket=context.wrap_socket(server.socket,server_side=True)
    except OSError as exc:
        if 'server' in locals():
            server.server_close()
        runtime.close()
        parser.exit(1,'Cannot start HTTP server: '+str(exc)+'\n')
    collector=None
    if not args.offline:
        collector=threading.Thread(target=lambda:asyncio.run(collect(runtime,key,secret,
                   os.getenv('DNSE_WSS_URL','wss://ws-openapi.dnse.com.vn'))),daemon=True)
        collector.start()
    def shutdown(*_):
        runtime.stop.set()
        threading.Thread(target=server.shutdown,daemon=True).start()
    signal.signal(signal.SIGTERM,shutdown);signal.signal(signal.SIGINT,shutdown)
    scheme='https' if args.tls_cert else 'http'
    print(f'LIVE: {scheme}://localhost:{args.port}/orderflow-live.html ({runtime.symbol})',flush=True)
    print('Storage: '+str(runtime.store().path),flush=True)
    try:
        server.serve_forever(poll_interval=.25)
    finally:
        runtime.stop.set()
        server.server_close()
        if collector:
            collector.join()
        runtime.close()


if __name__=='__main__':
    main()
