"""Compact, durable, per-contract/day market capture. No credentials are persisted.
SDK callbacks update the LIVE view in memory and enqueue SQLite writes separately.
The writer commits <=50ms batches (WAL/FULL).
Quote payloads are MessagePack tick/quantity arrays, optionally zlib compressed.
"""
import hashlib
import bisect
import json
import math
import os
import queue
import re
import sqlite3
import threading
import tempfile
import time
import zlib
from datetime import datetime
from pathlib import Path

import msgpack
from replay_server import Replay, TZ, ticks, record_book_history
from vn30_stocks import VN30_PERIOD, VN30_SOURCE, VN30_SYMBOLS


BACKUP_TABLES = ('metadata', 'gaps', 'index_influence', 'index_influence_extrema',
                 'index_influence_total_extrema',
                 'market_index_latest', 'market_index_history', 'basis_history',
                 'vn30_stock_latest')


def backup_non_market(source_path, backup_path):
    """Atomically archive non-Quote/Trade tables before deleting a daily store."""
    source_path, backup_path = Path(source_path), Path(backup_path)
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=backup_path.name+'.', suffix='.tmp',
                                     dir=backup_path.parent)
    os.close(fd)
    try:
        # A stale WAL may need SQLite recovery before the old database can be read.
        with sqlite3.connect(source_path) as source:
            with sqlite3.connect(temp_name) as backup:
                for table in BACKUP_TABLES:
                    schema = source.execute(
                        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
                        (table,)).fetchone()
                    if schema is None:  # Older sessions have no index history table.
                        continue
                    backup.execute(schema[0])
                    rows = source.execute('SELECT * FROM '+table)
                    columns = len(rows.description)
                    insert = 'INSERT INTO '+table+' VALUES ('+','.join('?'*columns)+')'
                    while chunk := rows.fetchmany(1000):
                        backup.executemany(insert, chunk)
                backup.commit()
        os.replace(temp_name, backup_path)
    finally:
        if os.path.exists(temp_name):
            os.unlink(temp_name)


def basis_history_from_connection(conn, day, symbol, bucket_seconds=5):
    if bucket_seconds not in (1,5,15,60):
        raise ValueError('Invalid basis interval')
    if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='basis_history'").fetchone() is None:
        return {'day':day,'symbol':symbol,'bucket_seconds':bucket_seconds,'points':[]}
    rows=conn.execute('''SELECT b.received_ms,b.kind,b.basis FROM basis_history b
        JOIN (SELECT kind,MAX(second) AS second FROM basis_history
              GROUP BY kind,second / ?) picked
          ON picked.kind=b.kind AND picked.second=b.second
        ORDER BY b.received_ms,b.kind''',(bucket_seconds,)).fetchall()
    return {'day':day,'symbol':symbol,'bucket_seconds':bucket_seconds,
            'points':[[received,kind,basis] for received,kind,basis in rows]}


def pack(data):
    raw = msgpack.packb(data)
    compressed = zlib.compress(raw, 1)
    return b'Z'+compressed if len(compressed)<len(raw) else b'M'+raw


def unpack(data):
    if data[:1] == b'Z':
        return msgpack.unpackb(zlib.decompress(data[1:]))
    if data[:1] != b'M':
        raise ValueError('Unknown payload format')
    return msgpack.unpackb(data[1:])


def integer(value, positive=False):
    n = float(value)
    if not math.isfinite(n) or n != int(n) or n < (1 if positive else 0):
        raise ValueError('Invalid quantity')
    return int(n)


def optional_number(value, positive=False):
    try:
        n = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return n if math.isfinite(n) and (n > 0 if positive else n >= 0) else None


class LiveReplay(Replay):
    decode_payload = staticmethod(unpack)


class LiveAccumulator(LiveReplay):
    """Incremental latest-state cache; late sources never trigger a full-day rebuild."""
    def events_between(self, position, target):
        # LIVE receives normalized records before the SQLite writer commits them.
        return ()

    def reset(self):
        super().reset()
        self.trade_keys=[]
        self.book_key=(-1,-1)
        self.last_trade_key=(-1,-1)
        self.history_needs_rebuild=False
        self.assessment_revision=-1
        self.assessment_last_rebuild=0
        self.assessment_data_end=0
        # Per-price session peaks are rebuilt from persisted Quote events on
        # startup, then extended by the same live ingestion path.
        self.max_bids={}
        self.max_asks={}
        self.last_bids={}
        self.last_asks={}
        self.hit_bids=set()
        self.hit_asks=set()

    def rebuild_history(self):
        """Repair visit boundaries after a rare out-of-order Quote, without rebuilding trades."""
        self.history={}
        self.visible_prices={'bid':set(),'ask':set()}
        for ts,payload in self.conn.execute(
                'SELECT ts,payload FROM events WHERE kind=0 ORDER BY ts,source_order'):
            record_book_history(self.history,self.visible_prices,unpack(payload),
                                (ts-self.meta['base_ms'])/1000)
        self.history_needs_rebuild=False

    def ingest(self, ts, kind, payload, sequence):
        relative=(ts-self.meta['base_ms'])/1000
        key=(ts,sequence)
        data=unpack(payload)
        self.last_data_ts=max(ts,self.last_data_ts or ts)
        if kind==0:
            self.quote_count+=1
            for price,qty in data[0]:
                previous=self.max_bids.get(price)
                if previous is None or qty>previous[0]:
                    self.max_bids[price]=(qty,ts)
                previous=self.last_bids.get(price)
                if previous is None or key>(previous[1],previous[2]):
                    self.last_bids[price]=(qty,ts,sequence)
            for price,qty in data[1]:
                previous=self.max_asks.get(price)
                if previous is None or qty>previous[0]:
                    self.max_asks[price]=(qty,ts)
                previous=self.last_asks.get(price)
                if previous is None or key>(previous[1],previous[2]):
                    self.last_asks[price]=(qty,ts,sequence)
            if key>self.book_key:
                self.book_key=key;self.book=data;self.book_ts=ts
                record_book_history(self.history,self.visible_prices,data,relative)
            else:
                self.history_needs_rebuild=True
        else:
            p,q,side,cumulative=data
            index=bisect.bisect_right(self.trade_keys,key)
            self.trade_keys.insert(index,key)
            self.trade_seconds.insert(index,relative)
            self.trades.insert(index,(relative,p,q,side))
            self.trade_prices.add(p)
            if side=='SELL':
                self.hit_bids.add(p)
            elif side=='BUY':
                self.hit_asks.add(p)
            self.volume[side]+=q
            if key>self.last_trade_key:
                self.last_price=p;self.source_volume=cumulative;self.last_trade_key=key
        return data


class LiveStore:
    def __init__(self, root, symbol, day, vn30_symbols=VN30_SYMBOLS,
                 vn30_source=VN30_SOURCE, vn30_period=VN30_PERIOD):
        if not re.fullmatch(r'[A-Z0-9_-]{1,32}', symbol):
            raise ValueError('Invalid symbol')
        datetime.strptime(day,'%Y-%m-%d')
        folder = Path(root)/symbol
        folder.mkdir(parents=True,exist_ok=True)
        self.path = folder/(day+'.sqlite')
        self.symbol, self.day = symbol, day
        self.vn30_symbols = tuple(vn30_symbols)
        self.vn30_set = frozenset(self.vn30_symbols)
        self.vn30_source, self.vn30_period = vn30_source, vn30_period
        self.lock = threading.RLock()
        self.changed = threading.Condition(self.lock)
        self.db_lock = threading.Lock()
        self.conn = sqlite3.connect(self.path,check_same_thread=False)
        self.conn.execute('PRAGMA journal_mode=WAL')
        self.conn.execute('PRAGMA synchronous=FULL')
        self.conn.executescript('''
        CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY,value TEXT);
        CREATE TABLE IF NOT EXISTS events(
          source_order INTEGER PRIMARY KEY, ts INTEGER NOT NULL, kind INTEGER NOT NULL,
          received_ms INTEGER NOT NULL,payload BLOB NOT NULL,
          price INTEGER,qty INTEGER,side TEXT,source_volume INTEGER,dedup BLOB);
        CREATE UNIQUE INDEX IF NOT EXISTS trade_dedup ON events(dedup) WHERE dedup IS NOT NULL;
        CREATE INDEX IF NOT EXISTS events_time ON events(ts,kind,source_order);
        CREATE INDEX IF NOT EXISTS trades_time ON events(ts DESC,source_order DESC) WHERE kind=1;
        CREATE TABLE IF NOT EXISTS gaps(id INTEGER PRIMARY KEY,start_ms INTEGER,end_ms INTEGER,reason TEXT);
        CREATE TABLE IF NOT EXISTS index_influence(
          second INTEGER PRIMARY KEY,received_ms INTEGER NOT NULL,payload BLOB NOT NULL);
        CREATE TABLE IF NOT EXISTS index_influence_extrema(
          symbol TEXT PRIMARY KEY,min_value REAL NOT NULL,max_value REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS index_influence_total_extrema(
          id INTEGER PRIMARY KEY CHECK(id=1),up_value REAL NOT NULL,down_value REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS market_index_latest(
          kind TEXT PRIMARY KEY,source_ms INTEGER,received_ms INTEGER NOT NULL,payload BLOB NOT NULL);
        CREATE TABLE IF NOT EXISTS market_index_history(
          id INTEGER PRIMARY KEY,kind TEXT NOT NULL,source_ms INTEGER,
          received_ms INTEGER NOT NULL,payload BLOB NOT NULL);
        CREATE INDEX IF NOT EXISTS market_index_history_time
          ON market_index_history(kind,source_ms,received_ms);
        CREATE TABLE IF NOT EXISTS basis_history(
          second INTEGER NOT NULL,kind TEXT NOT NULL,received_ms INTEGER NOT NULL,
          basis REAL NOT NULL,
          PRIMARY KEY(second,kind));
        CREATE TABLE IF NOT EXISTS ohlcv_1m(
          minute_ms INTEGER PRIMARY KEY,updated_ms INTEGER NOT NULL,received_ms INTEGER NOT NULL,
          open REAL NOT NULL,high REAL NOT NULL,low REAL NOT NULL,close REAL NOT NULL,
          volume INTEGER NOT NULL,closed INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS vn30_stock_latest(
          symbol TEXT PRIMARY KEY,payload BLOB NOT NULL);
        ''')
        meta_row = self.conn.execute("SELECT value FROM metadata WHERE key='stats'").fetchone()
        reference_row=self.conn.execute(
            "SELECT value FROM metadata WHERE key='reference_price'").fetchone()
        self.reference_price=json.loads(reference_row[0]) if reference_row else None
        self.ohlcv_1m={}
        for row in self.conn.execute('SELECT minute_ms,updated_ms,received_ms,open,high,low,close,volume,closed FROM ohlcv_1m'):
            candle=dict(zip(('minute_ms','updated_ms','received_ms','open','high','low','close','volume','closed'),row))
            candle['closed']=bool(candle['closed'])
            self.ohlcv_1m[row[0]]=candle
        self.ohlcv_invalid=0
        base = round(datetime.fromisoformat(day).replace(tzinfo=TZ).timestamp()*1000)
        self.persisted_meta = json.loads(meta_row[0]) if meta_row else dict(symbol=symbol,day=day,base_ms=base,
                    start_ms=base,end_ms=base,duration=0,quotes=0,trades=0,tape_volume=0,
                    duplicates=0,ordering='SOURCE_TIME; equal timestamp: Quote then arrival order',
                    source='Market data provider', storage='SQLite WAL / MessagePack ticks',live=True)
        self.meta = dict(self.persisted_meta)
        self._save_meta(self.persisted_meta)
        self.conn.commit()
        self.version = 0
        # Small, memory-only feed for the DOM latency probe. SQLite persistence
        # remains independent of this path.
        self.raw_events = [None]*8192
        self.raw_sequence = 0
        self.influence_events = [None]*4096
        self.influence_sequence = 0
        self.influence_invalid = 0
        # Seed an older database from its retained per-second snapshots once.
        if self.conn.execute('SELECT 1 FROM index_influence_extrema LIMIT 1').fetchone() is None:
            historical = {}
            for (payload,) in self.conn.execute('SELECT payload FROM index_influence'):
                for ticker,value in unpack(payload):
                    previous=historical.get(ticker)
                    historical[ticker]=[min(previous[0],value),max(previous[1],value)] if previous else [min(0,value),max(0,value)]
            self.conn.executemany('INSERT INTO index_influence_extrema VALUES (?,?,?)',
                                  ((ticker,*values) for ticker,values in historical.items()))
            self.conn.commit()
        # Older rows used the smallest/largest observation even if both had the
        # same sign. Zero means that side has not been seen in this session.
        self.conn.execute('''UPDATE index_influence_extrema SET
            min_value=MIN(0,min_value),max_value=MAX(0,max_value)
            WHERE min_value>0 OR max_value<0''')
        self.conn.commit()
        self.influence_extrema={ticker:[minimum,maximum] for ticker,minimum,maximum in
            self.conn.execute('SELECT symbol,min_value,max_value FROM index_influence_extrema')}
        total_row=self.conn.execute(
            'SELECT up_value,down_value FROM index_influence_total_extrema WHERE id=1').fetchone()
        if total_row is None:
            for (payload,) in self.conn.execute('SELECT payload FROM index_influence'):
                items=unpack(payload)
                up=sum(value for _,value in items if value>0)
                down=sum(value for _,value in items if value<0)
                total_row=(max(total_row[0],up),min(total_row[1],down)) if total_row else (up,down)
            if total_row is not None:
                self.conn.execute('INSERT INTO index_influence_total_extrema VALUES (1,?,?)',total_row)
                self.conn.commit()
        self.influence_total_extrema=(dict(zip(('up','down'),total_row))
                                      if total_row else None)
        influence_row = self.conn.execute(
            'SELECT received_ms,payload FROM index_influence ORDER BY second DESC LIMIT 1').fetchone()
        self.influence_latest = ({'received_ms':influence_row[0],
                                  'items':unpack(influence_row[1])} if influence_row else None)
        self.index_events = [None]*1024
        self.index_sequence = 0
        self.index_invalid = 0
        self.index_latest = {kind:unpack(payload) for kind,payload in self.conn.execute(
            'SELECT kind,payload FROM market_index_latest')}
        self.vn30_stock_latest={symbol:unpack(payload) for symbol,payload in self.conn.execute(
            'SELECT symbol,payload FROM vn30_stock_latest') if symbol in self.vn30_set}
        self.vn30_stock_invalid=0
        self.queue = queue.Queue(maxsize=100000)
        self.stop = threading.Event()
        self.status = 'offline'
        self.error = None
        self.invalid = 0
        self.engine = None
        self.live_engine = LiveAccumulator(self.path)
        self.sequence = self.conn.execute('SELECT coalesce(max(source_order),0) FROM events').fetchone()[0]
        self.seen_trade_keys = {key for (key,) in self.conn.execute('SELECT dedup FROM events WHERE dedup IS NOT NULL')}
        for event in self.conn.execute('SELECT ts,kind,payload,source_order FROM events ORDER BY ts,kind,source_order'):
            self.live_engine.ingest(*event)
        saved_stats=self.persisted_meta.get('latest_trade_stats')
        saved_key=self.persisted_meta.get('latest_trade_stats_key')
        if saved_stats is not None and saved_key is not None:
            self.latest_trade_stats=saved_stats
            self.latest_trade_stats_key=tuple(saved_key)
        elif self.live_engine.last_price is not None:
            self.latest_trade_stats={'totalVolumeTraded':self.live_engine.source_volume}
            self.latest_trade_stats_key=self.live_engine.last_trade_key
        else:
            self.latest_trade_stats=None
            self.latest_trade_stats_key=(-1,-1)
        self.assessment_quotes = list(self.conn.execute(
            'SELECT ts,source_order,payload FROM events WHERE kind=0 AND ts>=? ORDER BY ts,source_order',
            (self.meta['end_ms']-185000,)))
        prior = self.conn.execute(
            'SELECT ts,source_order,payload FROM events WHERE kind=0 AND ts<? ORDER BY ts DESC,source_order DESC LIMIT 1',
            (self.meta['end_ms']-185000,)).fetchone()
        if prior:
            self.assessment_quotes.insert(0,prior)
        self.gaps = [list(row) for row in self.conn.execute('SELECT start_ms,end_ms FROM gaps ORDER BY id')]
        self.last_quote_trim = self.meta['end_ms']
        self.active_gap = None
        self.history_rebuild_ready = True
        self.failed = False
        self.retired = False

    def _save_meta(self, meta):
        self.conn.execute("INSERT OR REPLACE INTO metadata VALUES ('stats',?)",(json.dumps(meta),))

    def connection(self, status, reason=None):
        with self.lock:
            if self.retired:
                return
            self.status = status
            self.error = reason
            if status != 'connected':
                if self.active_gap is None:
                    start = self.meta['end_ms'] if self.meta['quotes']+self.meta['trades'] else round(time.time()*1000)
                    with self.db_lock:
                        self.conn.execute('INSERT INTO gaps(start_ms,reason) VALUES (?,?)',(start,status))
                        self.active_gap = self.conn.execute('SELECT last_insert_rowid()').fetchone()[0]
                        self.conn.commit()
                    self.gaps.append([start,None])
                self.version += 1
            # Close gap on first data after connect, not on socket handshake.
            self.changed.notify_all()

    def enqueue(self, event):
        with self.lock:
            if self.stop.is_set():
                return  # the previous day's queue is draining during rollover
            try:
                self.queue.put_nowait(event)
            except queue.Full:
                self.failed = True
                self.connection('error','Capture queue full; recording stopped')
                raise RuntimeError('Capture queue full')
            prior_trade=self.live_engine.last_trade_key
            self.sequence += 1
            data=self._project(event,self.sequence)
            if data is not None:
                ts,kind,received=event[:3]
                if kind==1 and self.live_engine.last_trade_key!=prior_trade:
                    for index_kind in ('market','estimated'):
                        self._queue_basis(index_kind,received)
                self.raw_sequence += 1
                raw={'seq':self.raw_sequence,'kind':'quote' if kind==0 else 'trade',
                     'source_ms':ts,'received_ms':received,
                     'projected_ms':time.time_ns()//1000000}
                if kind==0:
                    raw['bids'],raw['asks']=data
                else:
                    raw['price'],raw['qty'],raw['side']=data[:3]
                    raw['trade_stats']=event[9]
                self.raw_events[self.raw_sequence % len(self.raw_events)]=raw
            self.changed.notify_all()

    def enqueue_influence(self, snapshot):
        with self.lock:
            if self.stop.is_set():
                return
            try:
                self.queue.put_nowait(('influence', snapshot))
            except queue.Full:
                self.failed = True
                self.connection('error','Capture queue full; recording stopped')
                raise RuntimeError('Capture queue full')
            self.influence_sequence += 1
            event = {'seq':self.influence_sequence, **snapshot}
            self.influence_events[self.influence_sequence % len(self.influence_events)] = event
            if (self.influence_latest is None or
                    snapshot['received_ms'] >= self.influence_latest['received_ms']):
                self.influence_latest = snapshot
            for ticker,value in snapshot['items']:
                extrema=self.influence_extrema.get(ticker)
                if extrema is None:
                    self.influence_extrema[ticker]=[min(0,value),max(0,value)]
                else:
                    extrema[0]=min(extrema[0],value)
                    extrema[1]=max(extrema[1],value)
            up=sum(value for _,value in snapshot['items'] if value>0)
            down=sum(value for _,value in snapshot['items'] if value<0)
            if self.influence_total_extrema is None:
                self.influence_total_extrema={'up':up,'down':down}
            else:
                self.influence_total_extrema['up']=max(self.influence_total_extrema['up'],up)
                self.influence_total_extrema['down']=min(self.influence_total_extrema['down'],down)
            self.changed.notify_all()

    def influence_initial(self):
        with self.lock:
            return {'index':'VN30','day':self.day,'status':self.status,
                    'cursor':self.influence_sequence,'latest':self.influence_latest,
                    'extrema':{ticker:values[:] for ticker,values in self.influence_extrema.items()},
                    'total_extrema':self.influence_total_extrema.copy() if self.influence_total_extrema else None,
                    'invalid':self.influence_invalid}

    def influence_since(self, cursor, limit=128):
        with self.lock:
            if not self.influence_sequence:
                return [],cursor,0
            oldest=max(1,self.influence_sequence-len(self.influence_events)+1)
            lost=max(0,oldest-cursor-1)
            first=max(cursor+1,oldest)
            end=min(self.influence_sequence+1,first+limit)
            events=[self.influence_events[seq % len(self.influence_events)] for seq in range(first,end)]
            return events,(events[-1]['seq'] if events else cursor),lost

    @staticmethod
    def index_key(snapshot):
        return (snapshot.get('source_ms') or snapshot['received_ms'],snapshot['received_ms'])

    def enqueue_index(self, snapshot):
        with self.lock:
            if self.stop.is_set():
                return
            kind=snapshot['kind']
            old=self.index_latest.get(kind)
            if old and self.index_key(old)>self.index_key(snapshot):
                return
            try:
                self.queue.put_nowait(('index',snapshot))
            except queue.Full:
                self.failed=True
                self.connection('error','Capture queue full; recording stopped')
                raise RuntimeError('Capture queue full')
            self.index_latest[kind]=snapshot
            self._queue_basis(kind,snapshot['received_ms'])
            self.index_sequence+=1
            self.index_events[self.index_sequence % len(self.index_events)] = {
                'seq':self.index_sequence,**snapshot}
            self.changed.notify_all()

    def enqueue_vn30_stock(self, snapshot):
        with self.lock:
            if self.stop.is_set():
                return
            symbol,kind=snapshot['symbol'],snapshot['kind']
            if symbol not in self.vn30_set or kind not in ('reference','trade'):
                raise ValueError('Invalid VN30 stock update')
            old=self.vn30_stock_latest.get(symbol,{'symbol':symbol})
            old_key=(old.get(kind+'_source_ms',-1),old.get(kind+'_received_ms',-1))
            new_key=(snapshot['source_ms'],snapshot['received_ms'])
            if new_key<old_key:
                return
            updated=dict(old)
            updated[kind+'_source_ms']=snapshot['source_ms']
            updated[kind+'_received_ms']=snapshot['received_ms']
            for field in ('reference',) if kind=='reference' else ('last','high','low'):
                if snapshot.get(field) is not None:
                    updated[field]=snapshot[field]
            try:
                self.queue.put_nowait(('vn30_stock',updated))
            except queue.Full:
                self.failed=True
                self.connection('error','Capture queue full; recording stopped')
                raise RuntimeError('Capture queue full')
            self.vn30_stock_latest[symbol]=updated
            self.changed.notify_all()

    def vn30_stock_snapshot(self):
        with self.lock:
            return {'index':'VN30','day':self.day,'period':self.vn30_period,
                    'source':self.vn30_source,'status':self.status,
                    'stocks':[dict(self.vn30_stock_latest.get(symbol,{'symbol':symbol}))
                              for symbol in self.vn30_symbols],
                    'invalid':self.vn30_stock_invalid}

    def write_vn30_stocks(self, snapshots):
        latest={snapshot['symbol']:snapshot for snapshot in snapshots}
        with self.db_lock:
            try:
                self.conn.executemany('''INSERT INTO vn30_stock_latest(symbol,payload)
                    VALUES (?,?) ON CONFLICT(symbol) DO UPDATE SET payload=excluded.payload''',
                    [(symbol,pack(snapshot)) for symbol,snapshot in latest.items()])
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

    def set_reference_price(self, price, received):
        if not math.isfinite(price) or price<=0:
            raise ValueError('Invalid reference price')
        with self.db_lock:
            self.conn.execute("INSERT OR REPLACE INTO metadata VALUES ('reference_price',?)",
                              (json.dumps(price),))
            self.conn.commit()
        with self.lock:
            if self.stop.is_set():
                return
            self.reference_price=price
            self.raw_sequence+=1
            raw={'seq':self.raw_sequence,'kind':'reference','source_ms':received,
                 'received_ms':received,'price':price}
            self.raw_events[self.raw_sequence % len(self.raw_events)]=raw
            self.changed.notify_all()

    @staticmethod
    def ohlcv_key(candle):
        return (bool(candle['closed']),candle['updated_ms'],candle['received_ms'])

    def enqueue_ohlcv(self, candle):
        with self.lock:
            if self.stop.is_set():
                return
            minute=candle['minute_ms']
            old=self.ohlcv_1m.get(minute)
            if old and self.ohlcv_key(old)>self.ohlcv_key(candle):
                return
            try:
                self.queue.put_nowait(('ohlcv',candle))
            except queue.Full:
                self.failed=True
                self.connection('error','Capture queue full; recording stopped')
                raise RuntimeError('Capture queue full')
            self.ohlcv_1m[minute]=candle
            self.raw_sequence+=1
            raw={'seq':self.raw_sequence,'kind':'ohlcv',
                 'source_ms':minute,'received_ms':candle['received_ms'],
                 'candle':dict(candle)}
            self.raw_events[self.raw_sequence % len(self.raw_events)]=raw
            self.changed.notify_all()

    def write_ohlcv(self, candles):
        with self.db_lock:
            try:
                self.conn.executemany('''INSERT INTO ohlcv_1m
                    (minute_ms,updated_ms,received_ms,open,high,low,close,volume,closed)
                    VALUES (:minute_ms,:updated_ms,:received_ms,:open,:high,:low,:close,:volume,:closed)
                    ON CONFLICT(minute_ms) DO UPDATE SET
                    updated_ms=excluded.updated_ms,received_ms=excluded.received_ms,
                    open=excluded.open,high=excluded.high,low=excluded.low,
                    close=excluded.close,volume=excluded.volume,closed=excluded.closed
                    WHERE excluded.closed>ohlcv_1m.closed OR
                    (excluded.closed=ohlcv_1m.closed AND
                     (excluded.updated_ms>ohlcv_1m.updated_ms OR
                      (excluded.updated_ms=ohlcv_1m.updated_ms AND
                       excluded.received_ms>=ohlcv_1m.received_ms)))''',candles)
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

    def ohlcv_history(self):
        with self.lock:
            return {'day':self.day,'symbol':self.symbol,'resolution':'1m',
                    'candles':[dict(self.ohlcv_1m[minute]) for minute in sorted(self.ohlcv_1m)],
                    'invalid':self.ohlcv_invalid}

    def _queue_basis(self, kind, received):
        """Sample the latest futures/index pair at most once per second and kind."""
        index=self.index_latest.get(kind)
        future=self.live_engine.last_price
        if index is None or future is None:
            return
        local=datetime.fromtimestamp(received/1000,TZ)
        if local.date().isoformat()!=self.day or local.hour<9:
            return
        sample=(received//1000,kind,received,future/10-index['value'])
        try:
            self.queue.put_nowait(('basis',sample))
        except queue.Full:
            self.failed=True
            self.connection('error','Capture queue full; recording stopped')
            raise RuntimeError('Capture queue full')

    def write_basis(self, samples):
        with self.db_lock:
            try:
                self.conn.executemany('''INSERT INTO basis_history
                    (second,kind,received_ms,basis)
                    VALUES (?,?,?,?) ON CONFLICT(second,kind) DO UPDATE SET
                    received_ms=excluded.received_ms,basis=excluded.basis
                    WHERE excluded.received_ms>=basis_history.received_ms''',samples)
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

    def basis_history(self, bucket_seconds=5):
        with self.db_lock:
            return basis_history_from_connection(self.conn,self.day,self.symbol,bucket_seconds)

    def index_initial(self):
        with self.lock:
            return {'day':self.day,'status':self.status,'cursor':self.index_sequence,
                    'market':self.index_latest.get('market'),
                    'estimated':self.index_latest.get('estimated'),
                    'invalid':self.index_invalid}

    def index_since(self,cursor,limit=128):
        with self.lock:
            if not self.index_sequence:
                return [],cursor,0
            oldest=max(1,self.index_sequence-len(self.index_events)+1)
            lost=max(0,oldest-cursor-1)
            first=max(cursor+1,oldest)
            end=min(self.index_sequence+1,first+limit)
            events=[self.index_events[seq % len(self.index_events)] for seq in range(first,end)]
            return events,(events[-1]['seq'] if events else cursor),lost

    def write_index(self,snapshots):
        with self.db_lock:
            try:
                for snapshot in snapshots:
                    old=self.conn.execute('SELECT payload FROM market_index_latest WHERE kind=?',
                                          (snapshot['kind'],)).fetchone()
                    if old and self.index_key(unpack(old[0]))>self.index_key(snapshot):
                        continue
                    self.conn.execute('''INSERT INTO market_index_history
                        (kind,source_ms,received_ms,payload) VALUES (?,?,?,?)''',
                        (snapshot['kind'],snapshot['source_ms'],snapshot['received_ms'],pack(snapshot)))
                    self.conn.execute('''INSERT INTO market_index_latest(kind,source_ms,received_ms,payload)
                        VALUES (?,?,?,?) ON CONFLICT(kind) DO UPDATE SET
                        source_ms=excluded.source_ms,received_ms=excluded.received_ms,
                        payload=excluded.payload''',
                        (snapshot['kind'],snapshot['source_ms'],snapshot['received_ms'],pack(snapshot)))
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

    def influence_history(self, bucket_seconds=60):
        if bucket_seconds not in (1,5,60,300):
            raise ValueError('Invalid influence interval')
        with self.db_lock:
            rows=list(self.conn.execute(
                '''SELECT received_ms,payload FROM index_influence WHERE second IN
                   (SELECT MAX(second) FROM index_influence GROUP BY second / ?)
                   ORDER BY second''',(bucket_seconds,)))
        return {'index':'VN30','day':self.day,'bucket_seconds':bucket_seconds,
                'snapshots':[{'received_ms':received,'items':unpack(payload)}
                             for received,payload in rows]}

    def write_influence(self, snapshots):
        with self.db_lock:
            try:
                for snapshot in snapshots:
                    received=snapshot['received_ms']
                    up=sum(value for _,value in snapshot['items'] if value>0)
                    down=sum(value for _,value in snapshot['items'] if value<0)
                    self.conn.execute('''INSERT INTO index_influence_total_extrema(id,up_value,down_value)
                        VALUES (1,?,?) ON CONFLICT(id) DO UPDATE SET
                        up_value=MAX(up_value,excluded.up_value),
                        down_value=MIN(down_value,excluded.down_value)''',(up,down))
                    self.conn.executemany('''INSERT INTO index_influence_extrema(symbol,min_value,max_value)
                        VALUES (?,?,?) ON CONFLICT(symbol) DO UPDATE SET
                        min_value=MIN(min_value,excluded.min_value),
                        max_value=MAX(max_value,excluded.max_value)''',
                        ((ticker,min(0,value),max(0,value)) for ticker,value in snapshot['items']))
                    self.conn.execute('''INSERT INTO index_influence(second,received_ms,payload)
                        VALUES (?,?,?) ON CONFLICT(second) DO UPDATE SET
                        received_ms=excluded.received_ms,payload=excluded.payload
                        WHERE excluded.received_ms>=index_influence.received_ms''',
                        (received//1000,received,pack(snapshot['items'])))
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

    def _project(self, event, sequence):
        ts,kind,received,payload,p,qty,side,cum,key,stats=event
        if key is not None:
            if key in self.seen_trade_keys:
                self.meta['duplicates']+=1
                self.version+=1
                return None
            self.seen_trade_keys.add(key)
        if not self.meta['quotes']+self.meta['trades']:
            self.meta['start_ms']=ts
        self.meta['start_ms']=min(ts,self.meta['start_ms'])
        self.meta['end_ms']=max(ts,self.meta['end_ms'])
        self.meta['duration']=(self.meta['end_ms']-self.meta['base_ms'])/1000
        self.meta['quotes' if kind==0 else 'trades']+=1
        if kind==1:
            self.meta['tape_volume']+=qty
        data=self.live_engine.ingest(ts,kind,payload,sequence)
        if kind==1 and stats is not None and (ts,sequence)>self.latest_trade_stats_key:
            self.latest_trade_stats=stats
            self.latest_trade_stats_key=(ts,sequence)
        if kind==0:
            bisect.insort_right(self.assessment_quotes,(ts,sequence,payload))
            if self.meta['end_ms']-self.last_quote_trim>=1000:
                lower=self.meta['end_ms']-185000
                cut=bisect.bisect_left(self.assessment_quotes,(lower,-1,b''))
                del self.assessment_quotes[:max(0,cut-1)]
                self.last_quote_trim=self.meta['end_ms']
            if self.live_engine.history_needs_rebuild:
                self.history_rebuild_ready=False
        if self.active_gap is not None and self.status=='connected' and self.gaps[-1][1] is None:
            self.gaps[-1][1]=received
        self.version+=1
        return data

    def raw_initial(self):
        with self.lock:
            return {'day':self.day,'symbol':self.symbol,'status':self.status,
                    'reference_price':self.reference_price,
                    'ohlcv_latest':dict(self.ohlcv_1m[max(self.ohlcv_1m)]) if self.ohlcv_1m else None,
                    'cursor':self.raw_sequence,'quotes':self.meta['quotes'],
                    'trades':self.meta['trades'],'book':self.live_engine.book,
                    'book_source_ms':self.live_engine.book_ts,
                    'last_price':self.live_engine.last_price,
                    'last_price_source_ms':self.live_engine.last_trade_key[0]
                        if self.live_engine.last_price is not None else None,
                    'trade_stats':self.latest_trade_stats,
                    'trade_stats_source_ms':self.latest_trade_stats_key[0] if self.latest_trade_stats else None,
                    'max_bids':[[price,qty,ts] for price,(qty,ts) in self.live_engine.max_bids.items()],
                    'max_asks':[[price,qty,ts] for price,(qty,ts) in self.live_engine.max_asks.items()],
                    'last_bids':[[price,qty,ts] for price,(qty,ts,_) in self.live_engine.last_bids.items()],
                    'last_asks':[[price,qty,ts] for price,(qty,ts,_) in self.live_engine.last_asks.items()],
                    'hit_bids':sorted(self.live_engine.hit_bids),
                    'hit_asks':sorted(self.live_engine.hit_asks)}

    def trade_speed_history(self):
        """Trade count and contract volume for each source-time second in this session."""
        with self.lock:
            seconds=[]
            for (source_ms,_),(_,_,qty,_) in zip(
                    self.live_engine.trade_keys,self.live_engine.trades):
                second_ms=source_ms//1000*1000
                if seconds and seconds[-1][0]==second_ms:
                    seconds[-1][1]+=1
                    seconds[-1][2]+=qty
                else:
                    seconds.append([second_ms,1,qty])
            return {'day':self.day,'symbol':self.symbol,'cursor':self.raw_sequence,
                    'seconds':seconds}

    def raw_since(self, cursor, limit=512):
        with self.lock:
            if not self.raw_sequence:
                return [],cursor,0
            oldest=max(1,self.raw_sequence-len(self.raw_events)+1)
            lost=max(0,oldest-cursor-1)
            first=max(cursor+1,oldest)
            end=min(self.raw_sequence+1,first+limit)
            events=[self.raw_events[seq % len(self.raw_events)] for seq in range(first,end)]
            return events,(events[-1]['seq'] if events else cursor),lost

    def write(self, batch, queued=False):
        with self.lock:
            gap_id=self.active_gap if self.status=='connected' else None
        accepted=[]
        count=0
        duplicates=0
        earliest=None
        closed_gap=False
        with self.db_lock:
            disk_meta=dict(self.persisted_meta)
            try:
                for event in batch:
                    ts,kind,received,payload,p,qty,side,cum,key,stats = event
                    cur = self.conn.execute('''INSERT OR IGNORE INTO events
                    (ts,kind,received_ms,payload,price,qty,side,source_volume,dedup)
                    VALUES (?,?,?,?,?,?,?,?,?)''',event[:9])
                    if not cur.rowcount:
                        disk_meta['duplicates'] += 1
                        duplicates += 1
                        continue
                    accepted.append(event)
                    if not disk_meta['quotes']+disk_meta['trades']:
                        disk_meta['start_ms']=ts
                    disk_meta['start_ms']=min(ts,disk_meta['start_ms'])
                    disk_meta['end_ms']=max(ts,disk_meta['end_ms'])
                    disk_meta['duration']=(disk_meta['end_ms']-disk_meta['base_ms'])/1000
                    disk_meta['quotes' if kind==0 else 'trades']+=1
                    if kind==1:
                        disk_meta['tape_volume']+=qty
                        if stats is not None and (ts,cur.lastrowid)>tuple(disk_meta.get('latest_trade_stats_key',(-1,-1))):
                            disk_meta['latest_trade_stats']=stats
                            disk_meta['latest_trade_stats_key']=[ts,cur.lastrowid]
                    earliest = ts if earliest is None else min(earliest,ts)
                    count+=1
                if count and gap_id is not None:
                    self.conn.execute('UPDATE gaps SET end_ms=? WHERE id=? AND end_ms IS NULL',
                                      (max(e[2] for e in batch),gap_id))
                    closed_gap=True
                self._save_meta(disk_meta)
                self.conn.commit()
                self.persisted_meta=disk_meta
            except Exception:
                self.conn.rollback()
                raise
        with self.lock:
            if closed_gap and self.active_gap==gap_id:
                self.active_gap=None
            if self.engine and earliest is not None and earliest<=self.engine.position:
                self.engine.reset()  # late source events must appear in history
            if queued:
                if self.queue.empty():
                    self.history_rebuild_ready=True
            else:
                self.meta['duplicates']+=duplicates
                for event in accepted:
                    self.sequence+=1
                    self._project(event,self.sequence)
                self.history_rebuild_ready=True
                if duplicates and not accepted:
                    self.version+=1
                self.changed.notify_all()
            if queued and any(event[1]==1 for event in accepted):
                self.changed.notify_all()
            return count

    def writer(self):
        try:
            while not self.stop.is_set() or not self.queue.empty():
                batch=[]
                try:
                    batch.append(self.queue.get(timeout=.05))
                except queue.Empty:
                    continue
                deadline=time.monotonic()+.05
                while len(batch)<2000:
                    try:
                        batch.append(self.queue.get(timeout=max(0,deadline-time.monotonic())))
                    except queue.Empty:
                        break
                market=[event for event in batch if event[0] not in ('influence','index','basis','ohlcv','vn30_stock')]
                influence=[event[1] for event in batch if event[0]=='influence']
                indexes=[event[1] for event in batch if event[0]=='index']
                basis=[event[1] for event in batch if event[0]=='basis']
                candles=[event[1] for event in batch if event[0]=='ohlcv']
                vn30_stocks=[event[1] for event in batch if event[0]=='vn30_stock']
                if market:
                    self.write(market,queued=True)
                if influence:
                    self.write_influence(influence)
                if indexes:
                    self.write_index(indexes)
                if basis:
                    self.write_basis(basis)
                if candles:
                    self.write_ohlcv(candles)
                if vn30_stocks:
                    self.write_vn30_stocks(vn30_stocks)
                for _ in batch:
                    self.queue.task_done()
        except Exception as exc:
            self.failed=True
            self.connection('error','Storage failed: '+type(exc).__name__)
            self.stop.set()

    def info(self):
        with self.lock:
            return {**self.meta,'capture':self.capture(),'revision':self.version}

    def capture(self):
        return dict(status=self.status,error=self.error,invalid=self.invalid,
                    queued=self.queue.qsize(),duplicates=self.meta['duplicates'],
                    partial=True,note='Chỉ gồm dữ liệu đã thu; khoảng mất kết nối không tự backfill.')

    def snapshot(self, seconds=None, tf=60, window=600, large_min=50):
        with self.lock:
            if self.retired:
                raise ValueError('Session changed; reconnect to the current session')
            live = seconds is None
            snapshot_meta=self.meta if live else self.persisted_meta
            target=snapshot_meta['duration'] if live else max(0,min(seconds,snapshot_meta['duration']))
            if live:
                engine=self.live_engine
                if engine.history_needs_rebuild and self.history_rebuild_ready:
                    engine.rebuild_history()
                if (engine.assessment_revision!=self.version and
                        (engine.assessment_revision<0 or
                         time.monotonic()-engine.assessment_last_rebuild>=.5)):
                    from flow_assessment import FlowAssessment
                    engine.assessor=FlowAssessment()
                    lower=self.meta['end_ms']-185000
                    quote_start=bisect.bisect_left(self.assessment_quotes,(lower,-1,b''))
                    for ts,_,payload in self.assessment_quotes[max(0,quote_start-1):]:
                        engine.assessor.push_quote((ts-self.meta['base_ms'])/1000,unpack(payload))
                    trade_start=bisect.bisect_left(engine.trade_seconds,(lower-self.meta['base_ms'])/1000)
                    for t,p,q,side in engine.trades[trade_start:]:
                        engine.assessor.push_trade(t,p,q,side)
                    engine.assessment_revision=self.version
                    engine.assessment_last_rebuild=time.monotonic()
                    engine.assessment_data_end=self.meta['duration']
                # All committed rows have already been ingested, including late ones.
                engine.position=self.meta['end_ms']
            else:
                if self.engine is None:
                    self.engine=LiveReplay(self.path)
                engine=self.engine
            engine.meta=dict(snapshot_meta)
            data=engine.snapshot(target,tf,window,large_min,include_large=False,
                                 include_assessment=not live)
            timestamp=snapshot_meta['base_ms']+round(target*1000)
            if live:
                # Use wall time for freshness/assessment, source time for plotted history.
                age=None if engine.book_ts is None else max(0,(time.time()*1000-engine.book_ts)/1000)
                data['quoteAge']=age
                from replay_session import display_policy
                policy=display_policy(round(time.time()*1000),engine.book_ts,
                    bool(data['frame']['bids'] and data['frame']['asks'] and
                         data['frame']['bids'][0]['price']<data['frame']['asks'][0]['price']),
                    engine.last_data_ts)
                data['sessionBreak']=policy['sessionBreak'];data['bookHeld']=policy['bookHeld']
                assessment_time=(engine.assessment_data_end if engine.assessment_revision!=self.version
                                 else max(target,(time.time()*1000-self.meta['base_ms'])/1000))
                data['assessment']=engine.assessor.assess(assessment_time)
                if (age is None or age>30) and not data['bookHeld']:
                    data['frame']['gap']=True
            if live:
                gap=any(start<=timestamp and (end if end is not None else timestamp)>=timestamp-180000
                        for start,end in self.gaps)
            else:
                with self.db_lock:
                    gap=self.conn.execute('SELECT 1 FROM gaps WHERE start_ms<=? AND coalesce(end_ms,?)>=? LIMIT 1',
                                          (timestamp,timestamp,timestamp-180000)).fetchone()
            if gap or (live and self.status!='connected'):
                data['assessment']=None
            speed_as_of=max(target,(time.time()*1000-self.meta['base_ms'])/1000) if live else target
            speed_end=bisect.bisect_right(engine.trade_seconds,speed_as_of)
            speed_windows=[]
            for span in (10,60):
                start_ms=self.meta['base_ms']+round((speed_as_of-span)*1000)
                if live:
                    now_ms=self.meta['base_ms']+round(speed_as_of*1000)
                    interrupted=any(gap_start<=now_ms and
                                    (gap_end if gap_end is not None else now_ms)>start_ms
                                    for gap_start,gap_end in self.gaps)
                else:
                    with self.db_lock:
                        interrupted=self.conn.execute(
                            'SELECT 1 FROM gaps WHERE start_ms<=? AND coalesce(end_ms,?)>? LIMIT 1',
                            (snapshot_meta['base_ms']+round(speed_as_of*1000),
                             snapshot_meta['base_ms']+round(speed_as_of*1000),start_ms)).fetchone()
                complete=start_ms>=snapshot_meta['start_ms']
                connected=not live or self.status=='connected'
                fresh=not live or (engine.last_data_ts is not None and
                                   speed_as_of-(engine.last_data_ts-self.meta['base_ms'])/1000<=30)
                available=complete and not interrupted and connected and fresh and not data['sessionBreak']
                left=bisect.bisect_right(engine.trade_seconds,speed_as_of-span)
                trades=engine.trades[left:speed_end]
                buy=sum(q for _,_,q,side in trades if side=='BUY')
                sell=sum(q for _,_,q,side in trades if side=='SELL')
                unknown=sum(q for _,_,q,side in trades if side not in ('BUY','SELL'))
                speed_windows.append(dict(seconds=span,buy=buy,sell=sell,unknown=unknown,
                                          trades=len(trades),buy_per_sec=buy/span,
                                          sell_per_sec=sell/span,delta_per_sec=(buy-sell)/span,
                                          available=available))
            data['flowSpeed']=speed_windows
            return {**data,'finished':False if live else data['finished'],'live':live,
                    'capture':self.capture(),'revision':self.version,
                    'persistedTrades':self.persisted_meta['trades'],'meta':dict(snapshot_meta)}

    def large_trades(self, seconds=None, minimum=50, side='ALL', limit=100, before=None, start=0):
        if minimum<0 or side not in ('ALL','BUY','SELL') or not 1<=limit<=500:
            raise ValueError('Invalid large trade filter')
        with self.lock:
            if self.retired:
                raise ValueError('Session changed; reconnect to the current session')
            at=self.persisted_meta['duration'] if seconds is None else min(seconds,self.persisted_meta['duration'])
            end_ms=self.persisted_meta['base_ms']+round(at*1000)
            start_ms=self.persisted_meta['base_ms']+round(start*1000)
            where="kind=1 AND qty>? AND side IN ('BUY','SELL') AND ts>=? AND ts<=?"
            args=[minimum,start_ms,end_ms]
            if side!='ALL':
                where+=' AND side=?';args.append(side)
            with self.db_lock:
                stats=self.conn.execute(f'''SELECT count(*),coalesce(sum(CASE WHEN side='BUY' THEN qty ELSE 0 END),0),
                  coalesce(sum(CASE WHEN side='SELL' THEN qty ELSE 0 END),0) FROM events WHERE {where}''',args).fetchone()
                if before is not None:
                    ts,seq=map(int,before.split(':'))
                    where+=' AND (ts<? OR (ts=? AND source_order<?))';args.extend([ts,ts,seq])
                rows=self.conn.execute(f'SELECT ts,source_order,price,qty,side FROM events WHERE {where} ORDER BY ts DESC,source_order DESC LIMIT ?',[*args,limit+1]).fetchall()
            more=len(rows)>limit;rows=rows[:limit]
            return dict(base_ms=self.persisted_meta['base_ms'],
                        trades=[[(t-self.persisted_meta['base_ms'])/1000,p,q,s] for t,i,p,q,s in rows],
                        count=stats[0],buy=stats[1],sell=stats[2],as_of=at,minimum=minimum,side=side,
                        next=(f'{rows[-1][0]}:{rows[-1][1]}' if more else None),revision=self.version)

    def close(self):
        with self.lock:
            if self.retired:
                return
            if self.engine:
                self.engine.conn.close()
            if self.live_engine:
                self.live_engine.conn.close()
            with self.db_lock:
                self.conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')
                self.conn.close()
            self.retired=True
