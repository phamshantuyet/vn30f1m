"""Prove that independent-output comparison detects deliberately corrupted results."""
import copy
import unittest
from compare_buy_exhaustion import NUMERIC_FIELDS,TEXT_FIELDS,compare_prediction_rows

class IndependentComparisonTest(unittest.TestCase):
    def row(self):
        r={field:'1' for field in NUMERIC_FIELDS}
        r.update({field:'' for field in TEXT_FIELDS});r['status']='BUY_WEAKENING'
        return r
    def test_identical_outputs_pass(self):
        a=[self.row()];self.assertEqual(compare_prediction_rows(a,copy.deepcopy(a)),[])
    def test_wrong_signal_wrong_volume_and_missing_row_are_detected(self):
        expected=[self.row()]
        for field in ('alert','buy_10s','covered_ms','ts'):
            actual=copy.deepcopy(expected);actual[0][field]='0' if field=='alert' else '999'
            self.assertTrue(any(e['field']==field for e in compare_prediction_rows(actual,expected)))
        self.assertTrue(compare_prediction_rows([],expected))
    def test_wrong_quote_direction_is_detected(self):
        expected=[self.row()];actual=copy.deepcopy(expected);actual[0]['both_quotes_fall']='0'
        self.assertTrue(compare_prediction_rows(actual,expected))

if __name__=='__main__':unittest.main()
