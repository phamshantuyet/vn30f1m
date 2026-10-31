#!/usr/bin/env python3
"""Local recorded-market replay. Uses source time; files contain no receive clock.
Raw files stay unchanged. Equal timestamps retain reverse file order within source.
Quote rows are treated as recorded book images, not exchange order-by-order events.
"""
import argparse
import bisect
import copy
import json
import sqlite3
import threading
import time
from collections import Counter
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from flow_assessment import FlowAssessment
from replay_session import display_policy

ROOT = Path(__file__).resolve().parent
DATA = ROOT.parent / 'data'
TZ = timezone(timedelta(hours=7))

def ticks(value):
    return int(Decimal(str(value)) * 10)

def latest_pair(symbol):
    sources = []
    for folder in ('quotes', 'trade_tape'):
        sources.append({p.name: p for p in (DATA / folder / symbol).glob('*_G1.jsonl')})
    common = sources[0].keys() & sources[1].keys()
    if not common:
        raise ValueError(f'No matching Quote/Tape day for {symbol}')
    name = max(common)
    return name[:10].replace('_', '-'), sources[0][name], sources[1][name]

def build_cache(symbol):
    day, quotes, tape = latest_pair(symbol)
    cache_dir = ROOT / '.replay-cache'
    cache_dir.mkdir(exist_ok=True)
    db = cache_dir / f'{symbol}-{day}.sqlite'
    signature = [(str(p), p.stat().st_size, p.stat().st_mtime_ns) for p in (quotes, tape)]
    signature = json.dumps(signature)
    conn = sqlite3.connect(db)
    conn.execute('CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT)')
    saved = conn.execute("SELECT value FROM metadata WHERE key='signature'").fetchone()
    if saved and saved[0] == signature:
        conn.close()
        return db
    conn.execute('DROP TABLE IF EXISTS events')
    conn.execute('CREATE TABLE events (ts INTEGER, kind INTEGER, source_order INTEGER, payload TEXT)')
    conn.execute('PRAGMA journal_mode=OFF')
    base = int(datetime.fromisoformat(day).replace(tzinfo=TZ).timestamp()*1000)
    counts = Counter()
    stats = {'symbol': symbol, 'day': day, 'files': [str(p.relative_to(ROOT.parent)) for p in (quotes, tape)],
             'ordering': 'SOURCE_TIME; equal timestamps: reverse file order within source; cross-source order unverified',
             'quote_semantics': 'Recorded bid/offer arrays displayed as book images; delivery completeness unverified',
             'trade_dedup': 'All recorded rows retained; no exchange trade ID available'}
    min_ts, max_ts = None, None
    sides = Counter()
    for kind, path in enumerate((quotes, tape)):
        batch = []
        for line_no, line in enumerate(path.open()):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
                if row.get('symbol') != symbol or row.get('boardId') != 'G1':
                    raise ValueError('Wrong contract or board')
                if not row['time'].startswith(day):
                    raise ValueError('Wrong day')
                clock = row['time'].split(' ')[1]
                h, m, sec = clock.split(':')
                ts = base + int(h)*3600000 + int(m)*60000 + int(Decimal(sec)*1000)
                if kind == 0:
                    payload = [[[ticks(l['price']), l['quantity']] for l in (row.get(side) or [])] for side in ('bid','offer')]
                    if any(q < 0 for levels in payload for _, q in levels):
                        raise ValueError('Negative book quantity')
                else:
                    side = row.get('side')
                    side = side if side in ('BUY','SELL') else 'UNKNOWN'
                    if row['matchQtty'] <= 0:
                        raise ValueError('Invalid match quantity')
                    payload = [ticks(row['matchPrice']), row['matchQtty'], side, row.get('totalVolumeTraded')]
                    sides[side] += row['matchQtty']
                batch.append((ts,kind,-line_no,json.dumps(payload,separators=(',',':'))))
                counts['quotes' if kind==0 else 'trades'] += 1
                min_ts = ts if min_ts is None else min(min_ts,ts)
                max_ts = ts if max_ts is None else max(max_ts,ts)
            except (ValueError, KeyError, TypeError) as exc:
                raise ValueError(f'{path}:{line_no+1}: {exc}') from exc
            if len(batch) >= 10000:
                conn.executemany('INSERT INTO events VALUES (?,?,?,?)',batch)
                batch.clear()
        conn.executemany('INSERT INTO events VALUES (?,?,?,?)',batch)
        conn.commit()
        print(f'Imported {path.name}: {dict(counts)}',flush=True)
    conn.execute('CREATE INDEX events_time ON events(ts,kind,source_order)')
    stats.update(counts)
    stats.update({'start_ms': min_ts, 'end_ms': max_ts, 'base_ms': (min_ts//1000)*1000,
                  'duration': (max_ts-(min_ts//1000)*1000)/1000, 'tape_volume':sum(sides.values()), 'side_volume':dict(sides)})
    conn.execute('INSERT OR REPLACE INTO metadata VALUES (?,?)',('stats',json.dumps(stats)))
    conn.execute('INSERT OR REPLACE INTO metadata VALUES (?,?)',('signature',signature))
    conn.commit()
    conn.close()
    print(json.dumps(stats,ensure_ascii=False),flush=True)
    return db

def record_book_history(history, visible_prices, book, relative):
    """Track observed depth and the first quote when a price returns to the book.

    A missing price is outside the observed levels, never an inferred zero order.
    Empty sides do not prove that any previously visible price disappeared.
    """
    for levels,side in zip(book,('bid','ask')):
        if not levels:
            continue
        current={price for price,_ in levels}
        for price in visible_prices[side]-current:
            history[price][side]['away']=True
        for price,qty in levels:
            entry=history.setdefault(price,{'price':price})
            hist=entry.setdefault(side,{'max':qty,'maxAt':relative})
            if hist.get('away') and 'last' in hist:
                hist.update(returnFrom=hist['last'],returnTo=qty,returnAt=relative,
                            returnDelta=qty-hist['last'],returnCount=hist.get('returnCount',0)+1)
            hist['away']=False
            if qty>hist['max']:
                hist.update(max=qty,maxAt=relative)
            hist.update(last=qty,lastAt=relative)
        visible_prices[side]=current


class Replay:
    decode_payload = staticmethod(json.loads)
    def __init__(self, db):
        self.conn = sqlite3.connect(db,check_same_thread=False)
        self.meta = json.loads(self.conn.execute("SELECT value FROM metadata WHERE key='stats'").fetchone()[0])
        self.lock = threading.Lock()
        self.reset()

    def events_between(self, position, target):
        return self.conn.execute('SELECT ts,kind,payload FROM events WHERE ts>? AND ts<=? ORDER BY ts,kind,source_order',(position,target))

    def reset(self):
        self.assessor = FlowAssessment()
        self.position = -1
        self.book = [[],[]]
        self.book_ts = None
        self.last_data_ts = None
        self.history = {}
        self.visible_prices = {'bid':set(),'ask':set()}
        self.trades = []
        self.trade_seconds = []
        self.trade_prices = set()
        self.volume = Counter()
        self.last_price = None
        self.source_volume = None
        self.quote_count = 0

    def snapshot(self, seconds, tf, window, large_min=50, include_large=True, include_assessment=True):
        if not isinstance(large_min,int) or large_min<0:
            raise ValueError('Invalid large-trade threshold')
        with self.lock:
            seconds = max(0,min(seconds,self.meta['duration']))
            target = self.meta['base_ms'] + round(seconds*1000)
            if target < self.position:
                self.reset()
            rows = self.events_between(self.position,target)
            for ts,kind,payload in rows:
                data = self.decode_payload(payload)
                self.last_data_ts=ts
                relative = (ts-self.meta['base_ms'])/1000
                if kind == 0:
                    self.assessor.push_quote(relative,data)
                    self.book, self.book_ts = data, ts
                    self.quote_count += 1
                    record_book_history(self.history,self.visible_prices,data,relative)
                else:
                    price,qty,side,source_volume = data
                    self.assessor.push_trade(relative,price,qty,side)
                    self.trades.append((relative,price,qty,side))
                    self.trade_seconds.append(relative)
                    self.trade_prices.add(price)
                    self.volume[side] += qty
                    self.last_price = price
                    self.source_volume = source_volume
            self.position = target
            raw_bids,raw_asks=self.book
            valid_book=bool(raw_bids and raw_asks and raw_bids[0][0]<raw_asks[0][0])
            display=display_policy(target,self.book_ts,valid_book,self.last_data_ts)
            view_second=(display.pop('view_ts')-self.meta['base_ms'])/1000
            # Bar boundaries align to local clock, including 15m and 30s.
            base_second = self.meta['base_ms']//1000
            start = max(0, ((base_second+max(0,view_second-window+1))//tf)*tf-base_second)
            bars = {}
            for second,price,qty,side in self.trades[bisect.bisect_left(self.trade_seconds,start):]:
                bt = ((base_second+second)//tf)*tf-base_second
                b = bars.setdefault(bt,{'time':bt,'buy':0,'sell':0,'unknown':0,'total':0,'levels':{},'open':price,'close':price,'high':price,'low':price})
                b['close'] = price
                b['high'], b['low'] = max(b['high'],price),min(b['low'],price)
                b['total'] += qty
                field = {'BUY':'buy','SELL':'sell'}.get(side,'unknown')
                b[field] += qty
                level = b['levels'].setdefault(price,{'buy':0,'sell':0,'unknown':0})
                level[field] += qty
            for b in bars.values():
                b['delta'] = b['buy']-b['sell']
                b['poc'] = max(sorted(b['levels']),key=lambda p:sum(b['levels'][p].values()))
                b['levels'] = list(b['levels'].items())
            bids,asks = [[{'price':p,'qty':q} for p,q in levels] for levels in self.book]
            age = None if self.book_ts is None else (target-self.book_ts)/1000
            gap = not valid_book or age is None or (age>30 and not display['bookHeld'])
            axis_price = self.last_price
            if axis_price is None:
                axis_price = bids[0]['price'] if bids else asks[0]['price'] if asks else 18520
            return {'cursor':seconds,'start':start,'end':view_second,**display,'bars':list(bars.values()),
                    'history':copy.deepcopy(list(self.history.items())),'quoteAge':age,'quotesProcessed':self.quote_count,
                    'frame':{'price':axis_price,'bids':bids,'asks':asks,'gap':gap,
                    'volume':sum(self.volume.values()),'buy':self.volume['BUY'],'sell':self.volume['SELL'],
                    'unknown':self.volume['UNKNOWN'],'cvd':self.volume['BUY']-self.volume['SELL'],
                    'tradeCount':len(self.trades),'tradePriceLevels':len(self.trade_prices),
                    'sourceVolume':self.source_volume},
                    'large_trades':[list(t) for t in self.trades if t[2]>large_min and t[3] in ('BUY','SELL')] if include_large else [],'large_trades_as_of':seconds,'large_threshold':large_min,
                    'assessment':self.assessor.assess(seconds) if include_assessment else None,
                    'finished':seconds>=self.meta['duration']}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--symbol',default='41I1GB000')
    parser.add_argument('--port',type=int,default=8096)
    parser.add_argument('--build-only',action='store_true')
    args = parser.parse_args()
    db = build_cache(args.symbol)
    if args.build_only:
        return
    replay = Replay(db)
    class Handler(SimpleHTTPRequestHandler):
        def __init__(self,*a,**kw):
            super().__init__(*a,directory=str(ROOT/'dist'),**kw)
        def do_GET(self):
            url = urlsplit(self.path)
            if url.path in ('/api/meta','/api/state'):
                try:
                    if url.path == '/api/meta':
                        data = replay.meta
                    else:
                        q = parse_qs(url.query)
                        seconds = float(q.get('t',['0'])[0])
                        tf = int(q.get('tf',['60'])[0])
                        window = int(q.get('window',['600'])[0])
                        if tf not in (30,60,180,300,900) or not 60<=window<=3600:
                            raise ValueError('Invalid timeframe/window')
                        large_min=int(q.get('large_min',['50'])[0])
                        data = replay.snapshot(seconds,tf,window,large_min)
                    body = json.dumps(data,separators=(',',':')).encode()
                    self.send_response(200)
                    self.send_header('Content-Type','application/json')
                    self.send_header('Cache-Control','no-store')
                    self.send_header('Content-Length',str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                except (ValueError,OverflowError) as e:
                    self.send_error(400,str(e))
                return
            if url.path=='/':
                self.path='/orderflow-replay.html'
            super().do_GET()
    server=ThreadingHTTPServer(('0.0.0.0',args.port),Handler)
    print(f'Replay: http://localhost:{args.port}/orderflow-replay.html',flush=True)
    server.serve_forever()

if __name__ == '__main__':
    main()
