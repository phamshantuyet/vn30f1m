import copy
import unittest
from flow_assessment import FlowAssessment

class FlowAssessmentTest(unittest.TestCase):
    def make(self,direction=1,side='BUY',flat=False,end=200,quote_gap=None):
        a=FlowAssessment()
        for t in range(end+1):
            p=10000 if flat else 10000+direction*(t//5)
            if quote_gap is None or not quote_gap[0]<=t<=quote_gap[1]:
                a.push_quote(t,[[[p-1,30],[p-2,40],[p-3,50]],[[p,30],[p+1,40],[p+2,50]]])
            a.push_trade(t,p,5,side)
        return a
    def test_confirmed_up_and_down(self):
        for direction,side in [(1,'BUY'),(-1,'SELL')]:
            a=self.make(direction,side)
            result=a.assess(200)
            self.assertEqual(result['direction'],direction)
            self.assertEqual(result['usable_windows'],3)
            for w in result['windows']:
                self.assertEqual(w['structure'],direction)
                self.assertEqual(w['direction'],direction)
                self.assertEqual(w['quote_coverage'],1)
                self.assertEqual(w['trade_count'],w['seconds'])
                self.assertEqual(w['delta'],direction*5*w['seconds'])
    def test_absorption_candidate_is_not_trend(self):
        for side in ('BUY','SELL'):
            result=self.make(side=side,flat=True).assess(200)
            self.assertEqual(result['direction'],0)
            self.assertTrue(all(w['absorption'] for w in result['windows']))
    def test_unknown_sparse_and_stale_data_suppress_direction(self):
        for engine,t in [(self.make(side='UNKNOWN'),200),(self.make(end=10),10),
                         (self.make(),230),(self.make(quote_gap=(30,180)),200)]:
            result=engine.assess(t)
            self.assertEqual(result['direction'],0)
            self.assertLess(result['usable_windows'],3)
    def test_assessment_excludes_all_future_events(self):
        a=self.make()
        before=copy.deepcopy(a.assess(200))
        for t in range(201,301):
            a.push_quote(t,[[[5000,900]],[[5001,1]]])
            a.push_trade(t,5000,100,'SELL')
        self.assertEqual(a.assess(200),before)
    def test_ofi_at_fixed_price_and_quote_timeout(self):
        a=FlowAssessment()
        a.push_quote(0,[[[100,10]],[[101,20]]])
        a.push_quote(1,[[[100,15]],[[101,17]]])
        self.assertEqual(a._integrals(1)[2],8)
        self.assertEqual(a._integrals(100)[0],6)
        self.assertIsNone(a._quote_at(100))

if __name__=='__main__':unittest.main()
