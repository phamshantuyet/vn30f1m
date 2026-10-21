"""Replay invariants: timestamp cutoffs, source ordering, peaks, and tape volume."""
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from replay_server import Replay

class ReplayTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        path=Path(self.tmp.name)/'fixture.sqlite'
        db=sqlite3.connect(path)
        db.execute('CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT)')
        db.execute('INSERT INTO metadata VALUES (?,?)',('stats',json.dumps({'base_ms':0,'duration':100})))
        db.execute('CREATE TABLE events(ts INTEGER,kind INTEGER,source_order INTEGER,payload TEXT)')
        rows=[(0,0,-10,[[[100,5]],[]]),
              (1000,0,-9,[[[100,10]],[[101,20]]]),
              (1000,0,-8,[[[100,7]],[[101,3]]]),
              (1000,1,-2,[101,2,'BUY',2]),
              (1000,1,-1,[100,3,'UNKNOWN',5]),
              (2000,1,0,[100,4,'SELL',9]),
              (3000,0,-7,[[[99,50]],[[100,40]]])]
        db.executemany('INSERT INTO events VALUES (?,?,?,?)',[(a,b,c,json.dumps(d)) for a,b,c,d in rows])
        db.commit();db.close();self.replay=Replay(path)
    def tearDown(self):
        self.replay.conn.close();self.tmp.cleanup()
    def test_tape_and_peak_cutoffs(self):
        initial=self.replay.snapshot(0,30,600)
        state=self.replay.snapshot(1,30,600)
        self.assertEqual(dict(initial['history'])[100]['bid']['max'],5)
        self.assertEqual(state['frame']['volume'],5)
        self.assertEqual(initial['frame']['tradePriceLevels'],0)
        self.assertEqual(state['frame']['tradePriceLevels'],2)
        self.assertEqual(state['frame']['cvd'],2)
        self.assertEqual(state['frame']['sourceVolume'],5)
        self.assertEqual(state['frame']['bids'][0]['qty'],7)
        self.assertEqual(dict(state['history'])[100]['bid']['max'],10)
        self.assertNotIn(99,dict(state['history']))
        self.assertEqual(sum(b['total'] for b in state['bars']),5)
        earlier=state
        state=self.replay.snapshot(100,30,600)
        self.assertEqual(dict(earlier['history'])[100]['bid']['lastAt'],1)
        self.assertTrue(state['finished']);self.assertTrue(state['frame']['gap'])
        self.assertEqual(state['frame']['volume'],9)
        self.assertEqual(state['frame']['tradePriceLevels'],2)
        self.assertEqual(state['frame']['cvd'],-2)
        self.assertEqual(dict(state['history'])[99]['bid']['max'],50)
        state=self.replay.snapshot(0,30,600)
        self.assertEqual(state['frame']['volume'],0)
        self.assertEqual(state['frame']['tradePriceLevels'],0)
        self.assertEqual(dict(state['history'])[100]['bid']['max'],5)
        self.assertNotIn(99,dict(state['history']))

    def test_price_reappears_with_less_then_more_depth(self):
        rows=[(4000,0,1,[[[100,4]],[[101,20]]]),
              (5000,0,2,[[[99,9]],[[101,20]]]),
              (6000,0,3,[[[100,12]],[[101,20]]])]
        self.replay.conn.executemany('INSERT INTO events VALUES (?,?,?,?)',
                                     [(ts,kind,seq,json.dumps(book)) for ts,kind,seq,book in rows])
        self.replay.conn.commit()
        before=dict(self.replay.snapshot(3,30,600)['history'])[100]['bid']
        self.assertEqual(before['max'],10)
        self.assertNotIn('returnFrom',before)
        down=dict(self.replay.snapshot(4,30,600)['history'])[100]['bid']
        self.assertEqual((down['max'],down['returnFrom'],down['returnTo'],down['returnDelta']),
                         (10,7,4,-3))
        up=dict(self.replay.snapshot(6,30,600)['history'])[100]['bid']
        self.assertEqual((up['max'],up['returnFrom'],up['returnTo'],up['returnDelta'],up['returnCount']),
                         (12,4,12,8,2))
        rewound=dict(self.replay.snapshot(4,30,600)['history'])[100]['bid']
        self.assertEqual((rewound['max'],rewound['returnTo'],rewound['returnCount']),(10,4,1))
    def test_large_matches_are_strictly_above_50_and_causal(self):
        extra=[(4000,1,0,[101,50,'BUY',59]),(4100,1,0,[101,51,'BUY',110]),
               (4200,1,0,[100,75,'SELL',185]),(4300,1,0,[100,80,'UNKNOWN',265])]
        self.replay.conn.executemany('INSERT INTO events VALUES (?,?,?,?)',
                                     [(a,b,c,json.dumps(d)) for a,b,c,d in extra])
        self.replay.conn.commit()
        self.assertEqual(self.replay.snapshot(4.05,60,600)['large_trades'],[])
        self.assertEqual(self.replay.snapshot(4.15,60,600)['large_trades'],[[4.1,101,51,'BUY']])
        expected=[[4.1,101,51,'BUY'],[4.2,100,75,'SELL']]
        self.assertEqual(self.replay.snapshot(4.4,60,600)['large_trades'],expected)
        self.assertEqual(self.replay.snapshot(4.4,30,600)['large_trades'],expected)
        self.assertEqual(self.replay.snapshot(4.4,60,600,51)['large_trades'],[expected[1]])
        self.assertEqual(self.replay.snapshot(4.4,60,600,75)['large_trades'],[])
        all_matches=self.replay.snapshot(4.4,60,600,0)['large_trades']
        self.assertEqual([t[2] for t in all_matches],[2,4,50,51,75])
        with self.assertRaises(ValueError):self.replay.snapshot(4.4,60,600,-1)
        self.assertEqual(self.replay.snapshot(0,60,600)['large_trades'],[])

    def test_break_keeps_last_book_and_chart_without_synthesizing_data(self):
        base=(11*3600+28*60-7*3600)*1000  # 11:28 UTC+7
        self.replay.conn.execute('UPDATE events SET ts=ts+?',(base,))
        self.replay.conn.commit()
        self.replay.meta.update(base_ms=base,duration=6000)
        before=self.replay.snapshot(3,60,600)
        stale=self.replay.snapshot(40,60,600)
        self.assertFalse(stale['sessionBreak']);self.assertTrue(stale['frame']['gap'])
        self.assertFalse(self.replay.snapshot(119.999,60,600)['sessionBreak'])
        self.assertTrue(self.replay.snapshot(120,60,600)['sessionBreak'])
        held=self.replay.snapshot(1200,60,600)
        self.assertTrue(held['sessionBreak']);self.assertTrue(held['bookHeld'])
        self.assertFalse(held['frame']['gap'])
        self.assertEqual(held['frame']['bids'],before['frame']['bids'])
        self.assertEqual(held['frame']['tradeCount'],before['frame']['tradeCount'])
        self.assertEqual(held['end'],3)
        self.assertEqual(sum(b['total'] for b in held['bars']),9)
        self.replay.conn.execute('INSERT INTO events VALUES (?,?,?,?)',
            (base+140000,0,0,json.dumps([[[99,100]],[[100,40]]])))
        self.replay.conn.commit()
        fresh=self.replay.snapshot(140,60,600)
        self.assertTrue(fresh['sessionBreak']);self.assertFalse(fresh['bookHeld'])
        self.assertEqual(fresh['frame']['bids'][0]['qty'],100)
        reopened=self.replay.snapshot(5520,60,600)  # 13:00
        self.assertFalse(reopened['sessionBreak']);self.assertFalse(reopened['bookHeld'])
        self.assertTrue(reopened['frame']['gap'])

    def test_repeat_and_timeframe_do_not_double_count(self):
        a=self.replay.snapshot(3,30,600)
        for tf in (30,60,180,300,900):
            b=self.replay.snapshot(3,tf,600)
            self.assertEqual(b['frame'],a['frame'])
            self.assertEqual(sum(x['total'] for x in b['bars']),9)
            self.assertEqual(sum(x['delta'] for x in b['bars']),-2)

if __name__=='__main__':
    unittest.main()
