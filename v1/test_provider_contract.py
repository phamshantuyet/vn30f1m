"""Contract tests across real SDK models and the common persisted store."""
import tempfile
import unittest
from types import SimpleNamespace as Obj
from datetime import datetime

from dnse_normalize import normalize
from events import tick_event
from live_store import unpack
from providers import select_provider
from replay_server import TZ
from server import Runtime
from ssi_provider import SSIEvents, timestamp, subscribed
from ssi_sdk.models.streaming import (TradeMessage, QuoteMessage, IntervalMessage,
                                      MarketDataMessage, IndexTickMessage, IndexSummaryMessage)

DAY='2026-10-30'
AT=round(datetime(2026,10,30,9,0,tzinfo=TZ).timestamp()*1000)
SYMBOL='41I1GB000'


class ContractTest(unittest.TestCase):
    def test_common_trade_matches_dnse_encoding_and_dedup(self):
        old=normalize(Obj(symbol=SYMBOL,boardId='G1',time=DAY+'T09:00:00',
            receivedAt=AT/1000,price=1800.1,quantity=50,side='BUY',totalVolumeTraded=50),1,SYMBOL,DAY)
        new=tick_event(symbol=SYMBOL,expected_symbol=SYMBOL,day=DAY,source_ms=AT,
            received_ms=AT,price=1800.1,quantity=50,side='BUY',total_volume=50)
        self.assertEqual(old[:9],new[:9])

    def test_common_rejects_other_day_and_fractional_quantity(self):
        for changes in ({'day':'2026-10-31'},{'quantity':1.5}):
            values=dict(symbol=SYMBOL,expected_symbol=SYMBOL,day=DAY,source_ms=AT,
                received_ms=AT,price=1800.1,quantity=50)
            with self.assertRaises(ValueError):tick_event(**{**values,**changes})

    def test_ssi_models_feed_same_store_and_restore(self):
        with tempfile.TemporaryDirectory() as root:
            runtime=Runtime(root,SYMBOL,DAY)
            events=SSIEvents(runtime)
            events.receive(QuoteMessage(symbol=SYMBOL,trading_time='09:00:00',
                bid_prices=[1800],bid_volumes=[10],ask_prices=[1800.1],ask_volumes=[20]),AT)
            events.receive(TradeMessage(symbol=SYMBOL,trading_time='09:00:00',
                price=1800.1,quantity=50,side='B',total_volume=50),AT)
            events.receive(MarketDataMessage(symbol=SYMBOL,ref_price=1799),AT)
            events.receive(MarketDataMessage(symbol='HPG',trading_time='2026/10/30',ref_price=21.2),AT)
            events.receive(TradeMessage(symbol='HPG',trading_time='09:00:00',price=21300,
                quantity=100,high=21500,low=21000),AT)
            events.receive(IndexSummaryMessage(index='VN30',data={
                't':'2026/10/30 09:00:00','v':'1900.25','c':'+2.25','cr':'0.12',
                'adv':'18','dec':'10','nc':'2','tmq':'1000','tmv':'2000000'}),AT)
            events.receive(IndexTickMessage(symbol='VN30',trading_time='2026/10/30 09:00:01',
                value=1900.5,change=2.5,change_percent=.13),AT+1000)
            for minute,source in ((0,'09:00:10'),(1,'09:01:01')):
                events.receive(IntervalMessage(symbol=SYMBOL,interval_time=f'09:0{minute}:00',
                    trading_time=source,open=1800,high=1801,low=1799,close=1800.1,volume=1600),AT+minute*60000)
            store=runtime.store();store.queue.join()
            stocks=store.vn30_stock_snapshot()['stocks']
            hpg=next(s for s in stocks if s['symbol']=='HPG')
            self.assertEqual((hpg['reference'],hpg['last'],hpg['high'],hpg['low']),(21.2,21.3,21.5,21.0))
            rows=store.conn.execute('SELECT kind,payload FROM events ORDER BY kind').fetchall()
            self.assertEqual(unpack(rows[1][1]),[18001,50,'BUY',50])
            self.assertEqual(store.reference_price,1799)
            self.assertEqual(store.index_latest['market']['value'],1900.5)
            self.assertEqual(store.index_latest['market']['up'],18)
            self.assertTrue(store.ohlcv_history()['candles'][0]['closed'])
            runtime.close()
            runtime=Runtime(root,SYMBOL,DAY)
            self.assertEqual(runtime.store().reference_price,1799)
            self.assertEqual(runtime.store().conn.execute('SELECT count(*) FROM events').fetchone()[0],2)
            runtime.close()

    def test_time_and_capabilities(self):
        self.assertEqual(timestamp('09:00:00',DAY),AT)
        self.assertEqual(timestamp(DAY+'T09:00:00+07:00',DAY),AT)
        self.assertEqual(timestamp('2026/10/30 09:00:00',DAY),AT)
        self.assertEqual(timestamp('2026/10/30',DAY),AT-9*3600*1000)
        self.assertTrue(select_provider('ssi-sdk').capabilities['index'])
        self.assertFalse(select_provider('ssi-sdk').capabilities['contribution'])
        self.assertTrue(select_provider('dnse').capabilities['contribution'])
        topics=['trade.41I1GB000-HPG-SSI','quote.41I1GB000',
                'trade.41I1GB000@1m','trade.index.VN30','indexsummary.VN30']
        self.assertTrue(subscribed(topics,'trade','HPG'))
        self.assertTrue(subscribed(topics,'trade.index','VN30'))
        self.assertFalse(subscribed(topics,'market','HPG'))

if __name__=='__main__':unittest.main()
