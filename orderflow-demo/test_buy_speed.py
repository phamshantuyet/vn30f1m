import unittest
from buy_speed import BuySpeed, slowdown


class BuySpeedTests(unittest.TestCase):
    def test_strict_half_threshold_and_zero_baseline(self):
        self.assertFalse(slowdown(10,60,2,12)['volume_slow'])
        self.assertFalse(slowdown(10,60,2,12)['frequency_slow'])
        self.assertTrue(slowdown(0,60,0,12)['volume_slow'])
        self.assertFalse(slowdown(0,0,0,0)['volume_slow'])
        self.assertIsNone(slowdown(0,0,0,0)['volume_ratio'])

    def test_volume_frequency_and_size_are_distinct(self):
        # Same match frequency, smaller prints: volume falls without frequency falling.
        d=slowdown(20,300,10,30)
        self.assertTrue(d['volume_slow']);self.assertFalse(d['frequency_slow'])
        self.assertAlmostEqual(d['volume_ratio'],d['frequency_ratio']*d['average_size']/d['baseline_average_size'])
        # Fewer prints, larger size: frequency falls without volume falling.
        d=slowdown(100,300,2,30)
        self.assertFalse(d['volume_slow']);self.assertTrue(d['frequency_slow'])

    def test_disjoint_windows_and_future_invariance(self):
        buys=[(0,1000),(1,60),(29999,30),(30000,10),(30001,5),(40000,5)]
        d=BuySpeed(buys).at(40000,10)
        self.assertEqual((d['buy_volume'],d['baseline_volume'],d['buy_count'],d['baseline_count']),(10,100,2,3))
        self.assertEqual(d,BuySpeed(buys+[(40001,999)]).at(40000,10))


if __name__=='__main__':unittest.main()
