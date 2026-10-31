#!/usr/bin/env python3
"""Provider-independent market capture and DOM dashboard API."""
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
import sqlite3
import ssl
import struct
import sys
import threading
import time
from urllib.parse import parse_qs, urlsplit

from dotenv import load_dotenv
from live_store import LiveStore, backup_non_market, basis_history_from_connection
from replay_server import ROOT, TZ
from vn30_stocks import (VN30_PERIOD, VN30_SOURCE, VN30_SYMBOLS,
                         load_vn30_symbols)


def number(q, name, default, integer=False):
    value=q.get(name,[default])[0]
    n=int(value) if integer else float(value)
    if not math.isfinite(n):
        raise ValueError('Non-finite '+name)
    return n


class Runtime:
    def __init__(self, root, symbol, day=None, vn30_symbols=VN30_SYMBOLS,
                 vn30_source=VN30_SOURCE, vn30_period=VN30_PERIOD):
        if not re.fullmatch(r'[A-Z0-9_-]{1,32}',symbol):
            raise ValueError('Invalid symbol')
        self.root,self.symbol,self.day=root,symbol,day
        self.vn30_symbols=tuple(vn30_symbols)
        self.vn30_set=frozenset(self.vn30_symbols)
        self.vn30_source,self.vn30_period=vn30_source,vn30_period
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
                store=LiveStore(self.root,self.symbol,day,self.vn30_symbols,
                                self.vn30_source,self.vn30_period)
                store.connection(self.status)
                writer=threading.Thread(target=store.writer,daemon=True)
                writer.start()
                self.stores[day]=store;self.writers[day]=writer
            return self.stores[day]

    def prune(self, day):
        """Archive non-market data before removing older files for this symbol."""
        datetime.strptime(day,'%Y-%m-%d')
        by_day={}
        for path in (Path(self.root)/self.symbol).glob('*'):
            match=re.fullmatch(r'(\d{4}-\d{2}-\d{2})\.sqlite(?:-wal|-shm|-journal)?',path.name)
            if match and match[1]<day and path.is_file() and not path.is_symlink():
                try:
                    datetime.strptime(match[1],'%Y-%m-%d')
                except ValueError:
                    continue
                by_day.setdefault(match[1],[]).append(path)
        for old_day, paths in sorted(by_day.items()):
            database=Path(self.root)/self.symbol/(old_day+'.sqlite')
            if database in paths:
                archive=Path(self.root).parent/(Path(self.root).name+'-backup')/self.symbol/database.name
                backup_non_market(database,archive)
            for path in paths:
                path.unlink(missing_ok=True)

    def connection(self,status,error=None):
        with self.lock:
            self.status=status
            self.store().connection(status,error)

    def basis_history(self, day, bucket):
        current=self.store()
        if day is None or day==current.day:
            return current.basis_history(bucket)
        datetime.strptime(day,'%Y-%m-%d')
        if day>current.day:
            raise ValueError('Future day')
        archive=Path(self.root).parent/(Path(self.root).name+'-backup')/self.symbol/(day+'.sqlite')
        if not archive.is_file():
            return {'day':day,'symbol':self.symbol,'bucket_seconds':bucket,'points':[]}
        with sqlite3.connect(archive.as_uri()+'?mode=ro',uri=True) as conn:
            return basis_history_from_connection(conn,day,self.symbol,bucket)

    def close(self):
        self.stop.set()
        for store in self.stores.values():
            store.stop.set()
        for writer in self.writers.values():
            writer.join()  # drain every accepted record before closing SQLite
        for store in self.stores.values():
            if not store.failed:
                store.close()


from providers import select_provider

load_dotenv(ROOT.parent/'.env')
PROVIDER_FILE=ROOT/'data'/'provider.json'
try:
    saved_provider=json.loads(PROVIDER_FILE.read_text()).get('provider','ssi-sdk')
except (OSError,ValueError,AttributeError):
    saved_provider='ssi-sdk'
ACTIVE_PROVIDER=os.getenv('ORDERFLOW_SDK',saved_provider)
PROVIDER=select_provider(ACTIVE_PROVIDER)
DEFAULT_COLLECTOR=PROVIDER.collect
DOM_LATENCY_ONLY=True


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
        if url.path=='/api/provider':
            self.json({'provider':ACTIVE_PROVIDER,'options':['hybrid','ssi-sdk','dnse'],
                       'capabilities':getattr(self.runtime,'capabilities',PROVIDER.capabilities),
                       'sources':getattr(self.runtime,'source_status',{}),
                       'status':self.runtime.status,'error':self.runtime.store().error});return
        if not url.path.startswith('/api/'):
            if url.path=='/':
                self.path='/dom-latency.html' if DOM_LATENCY_ONLY else '/orderflow-live.html'
            # Serve only actual public UI assets, never data/config through filesystem.
            allowed=('/dom-latency.html',) if DOM_LATENCY_ONLY else (
                '/orderflow-live.html','/orderflow-replay.html','/orderflow-dom.html',
                '/dom-latency.html','/replay-static.js')
            if not self.path.split('?',1)[0] in allowed:
                self.send_error(404);return
            super().do_GET();return
        try:
            q=parse_qs(url.query)
            if DOM_LATENCY_ONLY and url.path not in (
                    '/api/influence-history','/api/ohlcv-1m','/api/vn30-stocks',
                    '/api/trade-speed','/api/basis-history','/api/large-trades',
                    '/api/ws/influence','/api/ws/index','/api/ws/raw'):
                self.send_error(404);return
            store=self.runtime.store()
            if url.path=='/api/meta':
                self.json(store.info());return
            if url.path=='/api/status':
                self.json(store.info());return
            if url.path=='/api/influence-history':
                self.json(store.influence_history(number(q,'bucket',60,True)));return
            if url.path=='/api/ohlcv-1m':
                self.json(store.ohlcv_history());return
            if url.path=='/api/vn30-stocks':
                self.json(store.vn30_stock_snapshot());return
            if url.path=='/api/trade-speed':
                self.json(store.trade_speed_history());return
            if url.path=='/api/basis-history':
                self.json(self.runtime.basis_history(q.get('day',[None])[0],
                        number(q,'bucket',5,True)));return
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

    def do_POST(self):
        if urlsplit(self.path).path!='/api/provider':
            self.send_error(404);return
        origin=self.headers.get('Origin')
        if origin and urlsplit(origin).netloc!=self.headers.get('Host'):
            self.send_error(403);return
        try:
            size=int(self.headers.get('Content-Length','0'))
            if not 0<size<=256:
                raise ValueError('Invalid request size')
            provider=json.loads(self.rfile.read(size)).get('provider')
            if provider not in ('hybrid','ssi-sdk','dnse'):
                raise ValueError('Invalid provider')
            prefixes=('SSI','DNSE') if provider=='hybrid' else (select_provider(provider).credential_prefix,)
            for prefix in prefixes:
                if not all(os.getenv(prefix+suffix) for suffix in ('_API_KEY','_API_SECRET')):
                    self.send_error(400,'Missing '+prefix+' credentials');return
            with self.runtime.lock:
                PROVIDER_FILE.parent.mkdir(parents=True,exist_ok=True)
                temp=PROVIDER_FILE.with_suffix('.tmp')
                temp.write_text(json.dumps({'provider':provider}))
                temp.replace(PROVIDER_FILE)
                self.runtime.requested_provider=provider
            self.json({'provider':provider,'restarting':True})
            self.runtime.stop.set()
            threading.Thread(target=self.server.shutdown,daemon=True).start()
        except (ValueError,TypeError,AttributeError,OSError):
            self.send_error(400,'Cannot change provider')
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
    parser.add_argument('--symbol',default=os.getenv('ORDERFLOW_SYMBOL',os.getenv('DNSE_SYMBOL','41I1GB000')))
    parser.add_argument('--host',default='127.0.0.1')
    parser.add_argument('--port',type=int,default=8097)
    parser.add_argument('--data-dir',type=Path,default=ROOT/'data'/ACTIVE_PROVIDER)
    parser.add_argument('--ohlc-symbol-type',default=os.getenv('DNSE_OHLC_SYMBOL_TYPE','VN30F1M'),
                        help='DNSE derivative OHLC symbolType (default: VN30F1M)')
    parser.add_argument('--offline',action='store_true',help='Serve persisted data without a source connection')
    parser.add_argument('--day',help='Offline day; only the current UTC+7 day is retained')
    parser.add_argument('--tls-cert',type=Path,default=os.getenv('ORDERFLOW_TLS_CERT'),
                        help='TLS certificate for HTTPS/WSS; requires --tls-key')
    parser.add_argument('--tls-key',type=Path,default=os.getenv('ORDERFLOW_TLS_KEY'),
                        help='TLS private key for HTTPS/WSS; requires --tls-cert')
    args=parser.parse_args()
    args.ohlc_symbol_type=args.ohlc_symbol_type.upper()
    if not re.fullmatch(r'[A-Z0-9_-]{1,32}',args.ohlc_symbol_type):
        parser.error('Invalid --ohlc-symbol-type')
    if args.day and not args.offline:
        parser.error('--day requires --offline')
    if args.day and args.day!=datetime.now(TZ).date().isoformat():
        parser.error('Only the current UTC+7 session is retained; omit --day')
    if bool(args.tls_cert)!=bool(args.tls_key):
        parser.error('--tls-cert and --tls-key must be provided together')
    sdk=ACTIVE_PROVIDER
    prefixes=('SSI','DNSE') if sdk=='hybrid' else (PROVIDER.credential_prefix,)
    for prefix in prefixes:
        if not args.offline and not all(os.getenv(prefix+suffix) for suffix in ('_API_KEY','_API_SECRET')):
            parser.error(f'Set {prefix}_API_KEY and {prefix}_API_SECRET in project .env')
    prefix=PROVIDER.credential_prefix
    key,secret=os.getenv(prefix+'_API_KEY'),os.getenv(prefix+'_API_SECRET')
    vn30_symbols,vn30_source,vn30_period=load_vn30_symbols(
        args.data_dir/'vn30_symbols.json',online=not args.offline)
    print(f'VN30 constituents: {len(vn30_symbols)} from {vn30_source} ({vn30_period})',flush=True)
    runtime=Runtime(args.data_dir,args.symbol.upper(),args.day,
                    vn30_symbols,vn30_source,vn30_period)
    runtime.capabilities=PROVIDER.capabilities.copy()
    if sdk=='hybrid':
        runtime.source_status={source:{'status':'offline','error':None} for source in ('ssi-sdk','dnse')}
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
        collector=threading.Thread(target=lambda:asyncio.run(DEFAULT_COLLECTOR(runtime,key,secret,
                   os.getenv(PROVIDER.stream_url_env,PROVIDER.default_stream_url),
                   args.ohlc_symbol_type)),daemon=True)
        collector.start()
        if PROVIDER.bootstrap:
            threading.Thread(target=PROVIDER.bootstrap,args=(runtime,key,secret),daemon=True).start()
    def shutdown(*_):
        runtime.stop.set()
        threading.Thread(target=server.shutdown,daemon=True).start()
    signal.signal(signal.SIGTERM,shutdown);signal.signal(signal.SIGINT,shutdown)
    scheme='https' if args.tls_cert else 'http'
    if DOM_LATENCY_ONLY:
        print(f'DOM latency: {scheme}://localhost:{args.port}/dom-latency.html',flush=True)
    else:
        print(f'LIVE: {scheme}://localhost:{args.port}/orderflow-live.html ({runtime.symbol})',flush=True)
        print(f'DOM latency: {scheme}://localhost:{args.port}/dom-latency.html',flush=True)
    print('Storage: '+str(runtime.store().path),flush=True)
    try:
        server.serve_forever(poll_interval=.25)
    finally:
        runtime.stop.set()
        server.server_close()
        if collector:
            collector.join()
        runtime.close()

    requested=getattr(runtime,'requested_provider',None)
    if requested:
        os.environ['ORDERFLOW_SDK']=requested
        os.execv(sys.executable,[sys.executable,*sys.argv])


if __name__=='__main__':
    main()
