"""Persistence, timestamp cutoffs, SDK normalization, cursor pages and HTTP/WebSocket."""
import io
import asyncio
import json
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace as Obj
import unittest
from unittest.mock import patch

from live_store import LiveStore, normalize, normalize_influence, normalize_market_index, unpack
from live_server import Handler, Runtime, collect

DAY='2026-10-19'
SYMBOL='41I1GB000'


def event(kind,sec=0,qty=60,side='BUY',cumulative=60,price=1800.1,stats=None):
    obj=Obj(symbol=SYMBOL,boardId='G1',time=f'{DAY} 09:{sec//60:02d}:{sec%60:02d}.123',receivedAt=1792375200+sec,
            bid=[Obj(price=1800,quantity=20)],offer=[Obj(price=1800.1,quantity=30)],
            price=price,quantity=qty,side=side,totalVolumeTraded=cumulative,**(stats or {}))
    return normalize(obj,kind,SYMBOL,DAY)


def quote(sec,bid,qty):
    obj=Obj(symbol=SYMBOL,boardId='G1',time=f'{DAY} 09:{sec//60:02d}:{sec%60:02d}.123',
            receivedAt=1792375200+sec,bid=[Obj(price=bid,quantity=qty)],
            offer=[Obj(price=bid+.1,quantity=1)])
    return normalize(obj,0,SYMBOL,DAY)


class StoreTest(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.store=LiveStore(self.tmp.name,SYMBOL,DAY)
        self.t=9*3600+.123

    def tearDown(self):
        self.store.close();self.tmp.cleanup()

    def test_restart_preserves_all_history_and_exact_retransmit(self):
        self.store.write([event(0),event(1),event(1),event(1,1,70,'SELL',130)])
        self.assertEqual(self.store.info()['duplicates'],1)
        self.store.close();self.store=LiveStore(self.tmp.name,SYMBOL,DAY)
        snap=self.store.snapshot(self.t+1)
        self.assertEqual(snap['frame']['volume'],130)
        self.assertEqual(snap['frame']['tradeCount'],2)
        self.assertEqual(snap['frame']['tradePriceLevels'],1)
        self.assertEqual(snap['frame']['cvd'],-10)
        self.assertEqual(snap['history'][0][1]['bid']['max'],20)
        self.assertEqual(self.store.large_trades()['count'],2)

    def test_raw_hello_restores_last_seen_book_quantity_after_price_leaves_top_levels(self):
        self.store.write([quote(0,1800,500),quote(1,1800,400),quote(2,1801,600)])
        self.store.close();self.store=LiveStore(self.tmp.name,SYMBOL,DAY)
        initial=self.store.raw_initial()
        self.assertEqual(initial['book'][0],[[18010,600]])
        self.assertEqual({p:q for p,q,_ in initial['max_bids']}[18000],500)
        self.assertEqual({p:q for p,q,_ in initial['last_bids']}[18000],400)
        self.assertEqual({p:q for p,q,_ in initial['last_asks']}[18001],1)
        self.store.write([quote(0,1800,300)])  # older source time must not replace newer observation
        self.assertEqual({p:q for p,q,_ in self.store.raw_initial()['last_bids']}[18000],400)

    def test_trade_extra_stats_restore_latest_source_after_restart(self):
        extra={'grossTradeAmount':252000.5,'avgPrice':1800.4,
               'highestPrice':1801.2,'lowestPrice':1799.8,'openPrice':1800.0}
        latest={'totalVolumeTraded':130,**extra}
        self.store.write([event(1,1,70,'SELL',130,stats=extra),
                          event(1,0,60,'BUY',60,stats={**extra,'avgPrice':1800.1})])
        self.assertEqual(self.store.raw_initial()['trade_stats'],latest)
        self.assertEqual(self.store.raw_initial()['trade_stats_source_ms'],event(1,1)[0])
        self.store.close();self.store=LiveStore(self.tmp.name,SYMBOL,DAY)
        self.assertEqual(self.store.raw_initial()['trade_stats'],latest)
        self.assertEqual(self.store.raw_initial()['trade_stats_source_ms'],event(1,1)[0])

    def test_vn30_influence_realtime_and_persisted_history(self):
        received=1792375200.1
        source=Obj(index_name='VN30',receivedAt=received,data=[
            Obj(symbol='VCB',influence=1.25),Obj(symbol='HPG',influence=-.4)])
        snapshot=normalize_influence(source)
        self.assertEqual(snapshot['items'],[['HPG',-.4],['VCB',1.25]])
        self.store.enqueue_influence(snapshot)
        self.assertEqual(self.store.influence_initial()['latest'],snapshot)
        self.assertEqual(self.store.influence_since(0)[0][0]['items'],snapshot['items'])
        self.assertEqual(self.store.influence_history(300)['snapshots'],[])
        source.receivedAt=received+.4
        source.data[0].influence=1.5
        latest=normalize_influence(source)
        self.store.enqueue_influence(latest)
        source.receivedAt=received+30
        source.data[1].influence=-.6
        within_bucket=normalize_influence(source)
        self.store.enqueue_influence(within_bucket)
        source.receivedAt=received+70
        source.data[0].influence=1.1
        next_minute=normalize_influence(source)
        self.store.enqueue_influence(next_minute)
        source.receivedAt=received+300
        source.data[0].influence=.9
        next_bucket=normalize_influence(source)
        self.store.enqueue_influence(next_bucket)
        self.store.stop.set()
        worker=threading.Thread(target=self.store.writer);worker.start();worker.join(2)
        self.assertFalse(worker.is_alive())
        self.assertEqual(self.store.influence_history()['snapshots'],[within_bucket,next_minute,next_bucket])
        self.assertEqual(self.store.influence_history(300)['snapshots'],[next_minute,next_bucket])
        self.store.close();self.store=LiveStore(self.tmp.name,SYMBOL,DAY)
        self.assertEqual(self.store.influence_initial()['latest'],next_bucket)
        self.assertEqual(self.store.influence_history(60)['snapshots'],[within_bucket,next_minute,next_bucket])
        self.assertEqual(self.store.influence_history(1)['snapshots'],[latest,within_bucket,next_minute,next_bucket])
        with self.assertRaises(ValueError):
            normalize_influence(Obj(index_name='VN30',receivedAt=received,data=[
                Obj(symbol='VCB',influence=float('nan'))]))

    def test_vn30_index_values_persist_and_ignore_older_source(self):
        received=1792375200.1
        market=Obj(indexName='VN30',receivedAt=received,valueIndexes=1795.25,
                   changedValue=5.25,changedRatio=.29,priorValueIndexes=1790,
                   highestValueIndexes=1800,lowestValueIndexes=1780,
                   fluctuationUpIssueCount=12,fluctuationDownIssueCount=15,
                   fluctuationSteadinessIssueCount=3,totalVolumeTraded=1000000,
                   grossTradeAmount=100.5,transactTime=DAY+' 09:00:00.000')
        estimated=Obj(indexName='VN30',receivedAt=received,valueIndexes=1796,
                      changedValue=6,changedRatio=.34,fluctuationUpIssueCount=13,
                      fluctuationDownIssueCount=14,fluctuationSteadinessIssueCount=3,
                      totalVolumeTraded=1005000,grossTradeAmount=101.2,
                      time=DAY+' 09:00:00.100')
        official=normalize_market_index(market,'market',DAY)
        estimate=normalize_market_index(estimated,'estimated',DAY)
        self.assertEqual((official['value'],official['prior'],estimate['value']),
                         (1795.25,1790,1796))
        self.store.enqueue_index(official)
        self.store.enqueue_index(estimate)
        self.assertEqual([e['kind'] for e in self.store.index_since(0)[0]],
                         ['market','estimated'])
        market.receivedAt=received+2
        market.transactTime=DAY+' 08:59:59.000'
        market.valueIndexes=1700
        self.store.enqueue_index(normalize_market_index(market,'market',DAY))
        self.assertEqual(self.store.index_initial()['market'],official)
        self.assertEqual(self.store.index_sequence,2)
        self.store.stop.set()
        worker=threading.Thread(target=self.store.writer);worker.start();worker.join(2)
        self.assertFalse(worker.is_alive())
        self.store.close();self.store=LiveStore(self.tmp.name,SYMBOL,DAY)
        self.assertEqual(self.store.index_initial()['market'],official)
        self.assertEqual(self.store.index_initial()['estimated'],estimate)
        with self.assertRaises(ValueError):
            normalize_market_index(Obj(indexName='VNINDEX',receivedAt=received,
                                       valueIndexes=1800),'market',DAY)

    def test_rewind_and_late_event_no_future_peaks_or_trades(self):
        self.store.write([event(0),event(1),event(1,2,80,'SELL',140,1800.2)])
        self.store.snapshot(self.t+2)
        earlier=self.store.snapshot(self.t)
        self.assertEqual(earlier['frame']['volume'],60)
        self.assertEqual(earlier['frame']['tradePriceLevels'],1)
        self.assertEqual(self.store.large_trades(self.t)['count'],1)
        self.store.snapshot(self.t+2)
        self.store.write([event(1,1,20,'BUY',80)])
        self.assertEqual(self.store.snapshot(self.t+2)['frame']['volume'],160)
        self.assertEqual(self.store.snapshot(self.t+2)['frame']['tradePriceLevels'],2)
        self.assertEqual(self.store.snapshot(self.t+.5)['frame']['volume'],60)
        self.assertEqual(self.store.snapshot(self.t+.5)['frame']['tradePriceLevels'],1)

    def test_keyset_same_timestamp_stable_new_arrival(self):
        self.store.write([event(1,0,60,'BUY',60),event(1,0,70,'SELL',130),event(1,0,80,'BUY',210)])
        page=self.store.large_trades(limit=1)
        self.assertEqual(page['trades'][0][2],80)
        self.store.write([event(1,1,90,'SELL',300)])
        next_page=self.store.large_trades(limit=2,before=page['next'])
        self.assertEqual([t[2] for t in next_page['trades']],[70,60])
        self.assertIsNone(next_page['next'])
        self.assertEqual(self.store.large_trades(minimum=70)['count'],2)
        self.assertEqual(self.store.large_trades(side='BUY')['buy'],140)

    def test_unknown_not_delta_and_missing_volume_retained(self):
        self.store.write([event(1,side='?',cumulative=None)]*2)
        s=self.store.snapshot(self.t)
        self.assertEqual(s['frame']['unknown'],120)
        self.assertEqual(s['frame']['cvd'],0)
        self.assertEqual(self.store.large_trades(minimum=0)['count'],0)

    def test_flow_speed_uses_only_rolling_trades_and_marks_gaps(self):
        self.store.write([event(0),event(1,0,100),event(1,10,30,'SELL',130),
                          event(1,51,20,'BUY',150),event(1,60,10,'SELL',160)])
        early=self.store.snapshot(self.t+20)['flowSpeed']
        self.assertTrue(early[0]['available'])
        self.assertFalse(early[1]['available'])
        latest=self.store.snapshot(self.t+60)['flowSpeed']
        self.assertEqual((latest[0]['buy'],latest[0]['sell'],latest[0]['delta_per_sec']),(20,10,1))
        self.assertEqual((latest[1]['buy'],latest[1]['sell'],latest[1]['trades']),(20,40,3))
        self.assertTrue(all(w['available'] for w in latest))
        self.store.conn.execute('INSERT INTO gaps(start_ms,end_ms,reason) VALUES (?,?,?)',
                                (event(0,55)[0],event(0,56)[0],'test outage'))
        self.store.conn.commit()
        self.assertFalse(any(w['available'] for w in self.store.snapshot(self.t+60)['flowSpeed']))

    def test_reject_wrong_symbol_negative_fractional(self):
        with self.assertRaises(ValueError):
            event(1,qty=-1)
        with self.assertRaises(ValueError):
            event(1,qty=1.5)
        with self.assertRaises(ValueError):
            normalize(Obj(symbol='OTHER',boardId='G1'),0,SYMBOL,DAY)
        self.assertEqual(unpack(event(0)[3]),[[[18000,20]],[[18001,30]]])

    def test_outage_suppresses_assessment_and_stale_book(self):
        self.store.write([event(0),event(1)])
        self.store.connection('reconnecting')
        with patch('live_store.time.time',return_value=(event(0)[0]+60000)/1000):
            s=self.store.snapshot()
        self.assertIsNone(s['assessment'])
        self.assertTrue(s['frame']['gap'])
        self.assertEqual(s['capture']['status'],'reconnecting')

    def test_writer_flushes_on_shutdown(self):
        self.store.enqueue(event(1))
        self.store.stop.set()
        worker=threading.Thread(target=self.store.writer);worker.start();worker.join(2)
        self.assertFalse(worker.is_alive())
        self.assertEqual(self.store.info()['trades'],1)

    def test_live_dom_and_tape_update_before_sqlite_commit(self):
        self.store.enqueue(event(0))
        self.store.enqueue(event(1))
        self.store.enqueue(event(1))  # exact retransmit must not double the LIVE tape
        sql=[]
        self.store.conn.set_trace_callback(sql.append)
        self.store.live_engine.conn.set_trace_callback(sql.append)
        live=self.store.snapshot()
        self.store.conn.set_trace_callback(None)
        self.store.live_engine.conn.set_trace_callback(None)
        self.assertEqual(sql,[])
        self.assertEqual(live['frame']['tradeCount'],1)
        self.assertEqual(live['frame']['volume'],60)
        self.assertEqual(live['frame']['bids'][0]['qty'],20)
        self.assertEqual(live['frame']['asks'][0]['qty'],30)
        self.assertEqual(live['capture']['duplicates'],1)
        self.assertEqual(self.store.persisted_meta['trades'],0)
        self.assertEqual(self.store.snapshot(self.t)['frame']['volume'],0)
        self.store.stop.set()
        worker=threading.Thread(target=self.store.writer);worker.start();worker.join(2)
        self.assertFalse(worker.is_alive())
        self.assertEqual(self.store.persisted_meta['trades'],1)
        self.assertEqual(self.store.snapshot(self.t)['frame']['volume'],60)
        self.assertEqual(self.store.snapshot()['frame']['volume'],60)

    def test_late_quote_history_repairs_after_async_commit(self):
        for item in (quote(0,1800,500),quote(3,1800.2,50),quote(4,1800,400),
                     quote(2,1800,550)):
            self.store.enqueue(item)
        self.assertEqual(self.store.snapshot()['frame']['bids'][0]['qty'],400)
        self.store.stop.set()
        worker=threading.Thread(target=self.store.writer);worker.start();worker.join(2)
        self.assertFalse(worker.is_alive())
        live=dict(self.store.snapshot()['history'])[18000]['bid']
        replay=dict(self.store.snapshot(self.t+4)['history'])[18000]['bid']
        self.assertEqual(live,replay)
        self.assertEqual((live['max'],live['returnFrom'],live['returnTo']),
                         (550,550,400))

    def test_incremental_live_matches_full_replay_with_late_events(self):
        self.store.write([event(0),event(1),event(0,3),event(1,3,80,'SELL',140)])
        self.store.snapshot()  # initialize the incremental cache
        self.store.write([event(1,1,10,'BUY',70),event(0,2)])
        latest=self.store.snapshot()
        historical=self.store.snapshot(self.t+3)
        for field in ('volume','buy','sell','unknown','cvd','tradeCount','tradePriceLevels','price','bids','asks'):
            self.assertEqual(latest['frame'][field],historical['frame'][field],field)
        self.assertEqual(latest['bars'],historical['bars'])
        self.assertEqual(dict(latest['history']),dict(historical['history']))
        self.assertEqual(latest['quotesProcessed'],historical['quotesProcessed'])

    def test_live_revisit_depth_and_late_quote_repair(self):
        self.store.write([quote(0,1800,500),quote(3,1800.2,50),quote(4,1800,400)])
        before=dict(self.store.snapshot()['history'])[18000]['bid']
        self.assertEqual((before['max'],before['returnFrom'],before['returnTo']),
                         (500,500,400))
        self.store.write([quote(2,1800,550)])
        live=dict(self.store.snapshot()['history'])[18000]['bid']
        replay=dict(self.store.snapshot(self.t+4)['history'])[18000]['bid']
        self.assertEqual(live,replay)
        self.assertEqual((live['max'],live['returnFrom'],live['returnTo']),
                         (550,550,400))

    def test_live_assessment_cadence_keeps_pending_timestamp_honest(self):
        self.store.write([event(0),event(1)])
        self.store.connection('connected')
        with patch('live_store.time.monotonic',return_value=1000),\
             patch('live_store.time.time',return_value=event(0)[0]/1000):
            first=self.store.snapshot()
        self.store.write([event(1,1,20,'SELL',80)])
        with patch('live_store.time.monotonic',return_value=1000.1),\
             patch('live_store.time.time',return_value=event(0)[0]/1000+1):
            pending=self.store.snapshot()
        self.assertEqual(pending['assessment']['as_of'],first['assessment']['as_of'])
        self.assertEqual(pending['frame']['volume'],80)
        with patch('live_store.time.monotonic',return_value=1000.6),\
             patch('live_store.time.time',return_value=event(0)[0]/1000+1):
            refreshed=self.store.snapshot()
        self.assertGreater(refreshed['assessment']['as_of'],pending['assessment']['as_of'])


class FakeSocket:
    def __init__(self,path,on_send=None,headers=None):
        lines=[f'GET {path} HTTP/1.1','Host: localhost']
        if not headers or 'Connection' not in headers:
            lines.append('Connection: close')
        lines.extend(f'{key}: {value}' for key,value in (headers or {}).items())
        self.input=io.BytesIO(('\r\n'.join(lines)+'\r\n\r\n').encode())
        self.output=bytearray();self.on_send=on_send
    def makefile(self,*_):return self.input
    def sendall(self,data):
        self.output.extend(data)
        if self.on_send:self.on_send(data)
    def settimeout(self,*_):pass


WS_HEADERS={'Connection':'Upgrade','Upgrade':'websocket','Sec-WebSocket-Version':'13',
            'Sec-WebSocket-Key':'dGhlIHNhbXBsZSBub25jZQ==','Origin':'http://localhost'}


def websocket_messages(response):
    data=response.split(b'\r\n\r\n',1)[1]
    messages=[]
    while data:
        assert data[0]==0x81 and not data[1]&0x80
        size=data[1]&0x7f;offset=2
        if size==126:
            size=int.from_bytes(data[offset:offset+2],'big');offset+=2
        elif size==127:
            size=int.from_bytes(data[offset:offset+8],'big');offset+=8
        messages.append(json.loads(data[offset:offset+size]))
        data=data[offset+size:]
    return messages


class RetentionTest(unittest.TestCase):
    def test_startup_removes_old_live_files_preserves_current_and_other_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)/'live'
            old=LiveStore(root,SYMBOL,'2026-10-18');old.close()
            current=LiveStore(root,SYMBOL,DAY);current.write([event(1)]);current.close()
            folder=root/SYMBOL
            for suffix in ('-wal','-shm','-journal'):
                (folder/('2026-10-18.sqlite'+suffix)).write_bytes(b'old sidecar')
            keep=folder/'notes.txt';keep.write_text('keep')
            raw=Path(tmp)/'raw.jsonl';raw.write_text('raw data')
            runtime=Runtime(root,SYMBOL,DAY)
            try:
                self.assertEqual(runtime.store().info()['trades'],1)
                self.assertFalse(list(folder.glob('2026-10-18.sqlite*')))
                self.assertTrue((folder/(DAY+'.sqlite')).exists())
                self.assertEqual(keep.read_text(),'keep')
                self.assertEqual(raw.read_text(),'raw data')
            finally:
                runtime.close()

    def test_rollover_closes_old_store_and_removes_database_and_sidecars(self):
        with tempfile.TemporaryDirectory() as tmp:
            runtime=Runtime(tmp,SYMBOL,DAY)
            try:
                old=runtime.store();old.enqueue(event(1))
                runtime.day='2026-10-20'
                current=runtime.store()
                self.assertTrue(old.retired)
                self.assertTrue(old.queue.empty())
                self.assertFalse(list((Path(tmp)/SYMBOL).glob(DAY+'.sqlite*')))
                self.assertEqual(current.info()['trades'],0)
                self.assertEqual(list(runtime.stores),['2026-10-20'])
                with self.assertRaises(ValueError):old.snapshot()
            finally:
                runtime.close()


class HttpTest(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.runtime=Runtime(self.tmp.name,SYMBOL,DAY)
        self.runtime.store().write([event(0),event(1)])
    def tearDown(self):
        self.runtime.close();self.tmp.cleanup()
    def request(self,path,on_send=None,headers=None):
        sock=FakeSocket(path,on_send,headers)
        Handler(sock,('127.0.0.1',123),None,runtime=self.runtime)
        return bytes(sock.output)
    def test_json_and_private_files_and_filters(self):
        r=self.request('/api/large-trades?large_min=50&side=BUY&limit=1')
        head,body=r.split(b'\r\n\r\n',1)
        self.assertIn(b'200 OK',head)
        self.assertEqual(json.loads(body)['buy'],60)
        self.assertIn(b'400',self.request('/api/state?t=nan').split(b'\r\n',1)[0])
        self.assertIn(b'400',self.request('/api/large-trades?limit=501').split(b'\r\n',1)[0])
        self.assertIn(b'404',self.request('/.env').split(b'\r\n',1)[0])
        self.assertIn(b'404',self.request('/../.env').split(b'\r\n',1)[0])
        self.assertIn(b'200',self.request('/orderflow-live.html').split(b'\r\n',1)[0])
        self.assertIn(b'200',self.request('/dom-latency.html').split(b'\r\n',1)[0])
    def test_websocket_upgrade_pushes_snapshot(self):
        def sent(data):
            if data.startswith(b'\x81'):self.runtime.stop.set()
        self.assertIn(b'426',self.request('/api/ws').split(b'\r\n',1)[0])
        self.assertIn(b'403',self.request('/api/ws',headers={**WS_HEADERS,'Origin':'https://other.example'}).split(b'\r\n',1)[0])
        r=self.request('/api/ws',sent,WS_HEADERS)
        self.assertIn(b'101 Switching Protocols',r)
        self.assertIn(b'Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=',r)
        messages=websocket_messages(r)
        self.assertEqual(messages[0]['type'],'state')
        self.assertEqual(messages[0]['data']['frame']['volume'],60)
        self.assertEqual(messages[1],{'type':'persisted','day':DAY,'trades':1})

    def test_websocket_uses_live_projection_while_sqlite_writer_is_blocked(self):
        store=self.runtime.store()
        store.db_lock.acquire()
        try:
            store.enqueue(event(1,1,70,'SELL',130))
            self.assertEqual(store.persisted_meta['tape_volume'],60)
            def sent(data):
                if data.startswith(b'\x81'):self.runtime.stop.set()
            r=self.request('/api/ws',sent,WS_HEADERS)
            self.assertEqual(websocket_messages(r)[0]['data']['frame']['volume'],130)
            self.assertEqual(store.persisted_meta['tape_volume'],60)
        finally:
            store.db_lock.release()

    def test_raw_websocket_reports_each_received_event_before_sqlite_commit(self):
        store=self.runtime.store()
        frames=0
        def sent(data):
            nonlocal frames
            if not data.startswith(b'\x81'):
                return
            frames+=1
            if frames==1:
                store.enqueue(quote(2,1800,450))
                store.enqueue(event(1,2,70,'SELL',130,stats={
                    'grossTradeAmount':234000,'avgPrice':1800.2,
                    'highestPrice':1800.5,'lowestPrice':1799.9,'openPrice':1800.0}))
            elif frames>=2:
                self.runtime.stop.set()
        store.db_lock.acquire()
        try:
            response=self.request('/api/ws/raw',sent,WS_HEADERS)
            messages=websocket_messages(response)
            self.assertEqual([m['type'] for m in messages],['hello','events'])
            self.assertEqual([e['kind'] for e in messages[1]['events']],['quote','trade'])
            self.assertEqual(messages[1]['events'][0]['bids'][0],[18000,450])
            self.assertEqual(messages[1]['events'][1]['qty'],70)
            self.assertEqual(messages[1]['events'][1]['trade_stats'],{
                'totalVolumeTraded':130,'grossTradeAmount':234000,'avgPrice':1800.2,
                'highestPrice':1800.5,'lowestPrice':1799.9,'openPrice':1800.0})
            self.assertEqual(store.raw_initial()['trade_stats']['avgPrice'],1800.2)
            self.assertEqual(store.persisted_meta['trades'],1)
            self.assertIn(b'403',self.request('/api/ws/raw',headers={**WS_HEADERS,
                'Origin':'https://other.example'}).split(b'\r\n',1)[0])
        finally:
            store.db_lock.release()

    def test_influence_api_and_websocket_push_before_commit(self):
        store=self.runtime.store()
        snapshot={'received_ms':1792375200123,'items':[['HPG',-.5],['VCB',1.2]]}
        frames=0
        def sent(data):
            nonlocal frames
            if not data.startswith(b'\x81'):
                return
            frames+=1
            if frames==1:
                store.enqueue_influence(snapshot)
            elif frames>=2:
                self.runtime.stop.set()
        store.db_lock.acquire()
        try:
            response=self.request('/api/ws/influence',sent,WS_HEADERS)
            messages=websocket_messages(response)
            self.assertEqual([message['type'] for message in messages],['hello','events'])
            self.assertEqual(messages[1]['events'][0]['items'],snapshot['items'])
            self.assertEqual(store.influence_initial()['latest'],snapshot)
            self.assertEqual(store.conn.execute('SELECT count(*) FROM index_influence').fetchone()[0],0)
        finally:
            store.db_lock.release()
        store.queue.join()
        history=json.loads(self.request('/api/influence-history').split(b'\r\n\r\n',1)[1])
        self.assertEqual(history['bucket_seconds'],60)
        self.assertEqual(history['snapshots'],[snapshot])

    def test_index_websocket_pushes_before_sqlite_commit(self):
        store=self.runtime.store()
        snapshot={'kind':'market','received_ms':1792375200123,'source_ms':1792375200000,
                  'value':1795.25,'change':5.25,'change_pct':.29,'prior':1790,
                  'high':1800,'low':1780,'up':12,'down':15,'flat':3,
                  'volume':1000000,'amount':100.5}
        frames=0
        def sent(data):
            nonlocal frames
            if not data.startswith(b'\x81'):
                return
            frames+=1
            if frames==1:
                store.enqueue_index(snapshot)
            elif frames>=2:
                self.runtime.stop.set()
        store.db_lock.acquire()
        try:
            messages=websocket_messages(self.request('/api/ws/index',sent,WS_HEADERS))
            self.assertEqual([m['type'] for m in messages],['hello','events'])
            self.assertEqual(messages[1]['events'][0]['value'],1795.25)
            self.assertEqual(store.conn.execute(
                'SELECT count(*) FROM market_index_latest').fetchone()[0],0)
        finally:
            store.db_lock.release()
        store.queue.join()
        self.assertEqual(store.index_initial()['market'],snapshot)
        self.assertEqual(unpack(store.conn.execute(
            "SELECT payload FROM market_index_latest WHERE kind='market'").fetchone()[0]),snapshot)

    def test_raw_cursor_reads_ring_after_overflow(self):
        store=self.runtime.store()
        capacity=len(store.raw_events)
        for seq in range(1,capacity+4):
            store.raw_sequence=seq
            store.raw_events[seq % capacity]={'seq':seq}
        events,cursor,lost=store.raw_since(1,limit=2)
        self.assertEqual([event['seq'] for event in events],[4,5])
        self.assertEqual((cursor,lost),(5,2))
        events,cursor,lost=store.raw_since(cursor,limit=2)
        self.assertEqual([event['seq'] for event in events],[6,7])
        self.assertEqual(lost,0)

    def test_websocket_answers_masked_ping_and_close(self):
        class FrameSocket:
            def __init__(self,opcode):
                self.input=io.BytesIO(bytes((0x80|opcode,0x80))+b'abcd')
                self.output=bytearray()
            def fileno(self):return 0
            def recv(self,size):return self.input.read(size)
            def sendall(self,data):self.output.extend(data)
        handler=Handler.__new__(Handler)
        for opcode,reply,closed in ((9,b'\x8a\x00',False),(8,b'\x88\x00',True)):
            handler.connection=FrameSocket(opcode)
            with patch('live_server.select.select',return_value=([handler.connection],[],[])):
                self.assertEqual(handler.websocket_peer_closed(),closed)
            self.assertEqual(bytes(handler.connection.output),reply)
        frame=handler.websocket_frame(1,b'x'*70000)
        self.assertEqual(frame[:2],b'\x81\x7f')
        self.assertEqual(int.from_bytes(frame[2:10],'big'),70000)


class CollectorTest(unittest.TestCase):
    def test_real_sdk_auth_subscribe_decode_to_durable_store(self):
        """Run actual SDK auth/dispatch/model code with an in-process DNSE transport."""
        import msgpack
        from datetime import datetime
        from replay_server import TZ
        sent=[]
        timestamp=int(datetime.fromisoformat(DAY+'T09:00:00').replace(tzinfo=TZ).timestamp())
        class Transport:
            def __init__(self,**kwargs):
                self.queue=asyncio.Queue();self.is_connected=False
            async def connect(self):
                self.is_connected=True
                await self.queue.put(msgpack.packb({'sid':'test-session'}))
            async def send(self,raw):
                data=msgpack.unpackb(raw);sent.append(data)
                if data['action']=='auth':
                    await self.queue.put(msgpack.packb({'action':'auth_success'}))
                if data['action']=='subscribe':
                    channel=data['channels'][0]['name']
                    base=dict(symbol=SYMBOL,boardId='G1',time={'Seconds':timestamp,'Nanos':123000000})
                    if channel.startswith('tick_extra'):
                        base.update(T='te',matchPrice=1800.1,matchQtty=60,side='BUY',
                                    totalVolumeTraded=60,grossTradeAmount=108006,
                                    avgPrice=1800.1,highestPrice=1800.2,
                                    lowestPrice=1799.9,openPrice=1800.0)
                    elif channel.startswith('market_index_influence'):
                        base.update(T='ii',index_name='VN30',Data=[
                            dict(symbol='VCB',influence=1.25),dict(symbol='HPG',influence=-.4)])
                    elif channel.startswith('estimated_market_index'):
                        base={'T':'emi','marketIndex':dict(indexName='VN30',valueIndexes=1796,
                            changedValue=6,changedRatio=.34,fluctuationUpIssueCount=13,
                            fluctuationDownIssueCount=14,fluctuationSteadinessIssueCount=3,
                            time=DAY+' 09:00:00.123')}
                    elif channel.startswith('market_index'):
                        base={'T':'mi','indexName':'VN30','valueIndexes':1795.25,
                              'changedValue':5.25,'changedRatio':.29,'priorValueIndexes':1790,
                              'transactTime':{'Seconds':timestamp,'Nanos':123000000}}
                    else:
                        base.update(T='q',bid=[dict(price=1800,quantity=20)],offer=[dict(price=1800.1,qtty=30)])
                    await self.queue.put(msgpack.packb(base))
            async def receive(self):return await self.queue.get()
            def __aiter__(self):return self
            async def __anext__(self):return await self.receive()
            async def close(self):self.is_connected=False
        async def exercise(runtime):
            task=asyncio.create_task(collect(runtime,'test-key','test-secret','wss://example.invalid'))
            try:
                for _ in range(80):
                    await asyncio.sleep(.05)
                    if (runtime.store().info()['quotes']==1 and runtime.store().info()['trades']==1
                            and runtime.store().index_initial()['market']
                            and runtime.store().index_initial()['estimated']):
                        break
                self.assertEqual(runtime.store().info()['trades'],1)
                self.assertEqual(runtime.store().info()['quotes'],1)
                self.assertEqual(runtime.store().influence_initial()['latest']['items'],
                                 [['HPG',-.4],['VCB',1.25]])
                self.assertEqual(runtime.store().index_initial()['market']['value'],1795.25)
                self.assertEqual(runtime.store().index_initial()['estimated']['value'],1796)
                self.assertEqual(runtime.store().snapshot()['frame']['volume'],60)
                self.assertEqual(runtime.store().raw_initial()['trade_stats'],{
                    'totalVolumeTraded':60,'grossTradeAmount':108006,
                    'avgPrice':1800.1,'highestPrice':1800.2,
                    'lowestPrice':1799.9,'openPrice':1800.0})
                for _ in range(80):
                    if runtime.store().persisted_meta['quotes']==1 and runtime.store().persisted_meta['trades']==1:
                        break
                    await asyncio.sleep(.05)
                snapshot=runtime.store().snapshot(9*3600+.123)
                self.assertEqual(snapshot['frame']['volume'],60)
                self.assertEqual(snapshot['frame']['bids'][0]['qty'],20)
                self.assertEqual(snapshot['frame']['asks'][0]['qty'],30)
            finally:
                runtime.stop.set()
                await asyncio.wait_for(task,5)
        with tempfile.TemporaryDirectory() as tmp:
            runtime=Runtime(tmp,SYMBOL,DAY)
            try:
                with patch('dnse.websocket.client.WebSocketConnection',Transport):
                    asyncio.run(exercise(runtime))
                self.assertEqual(sent[0]['action'],'auth')
                channels=[c['name'] for s in sent if s['action']=='subscribe' for c in s['channels']]
                self.assertEqual(channels,['tick_extra.G1.msgpack','top_price.G1.msgpack',
                                           'market_index.VN30.msgpack',
                                           'estimated_market_index.VN30.msgpack',
                                           'market_index_influence.VN30.1.msgpack'])
            finally:
                runtime.close()


if __name__=='__main__':unittest.main()
