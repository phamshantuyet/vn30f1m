"""Hybrid routing keeps each channel on one live source."""
import tempfile
import unittest

from events import EventSink
from hybrid_provider import SourceRuntime
from server import Runtime


class HybridRoutingTest(unittest.TestCase):
    def test_source_selection_and_failover(self):
        with tempfile.TemporaryDirectory() as root:
            runtime = Runtime(root, '41I1GB000', '2026-10-30')
            ssi = SourceRuntime(runtime, 'ssi-sdk')
            dnse = SourceRuntime(runtime, 'dnse')
            runtime.hybrid_sources = {'ssi-sdk': ssi, 'dnse': dnse}
            runtime.source_status = {
                source: {'status': 'offline', 'error': None}
                for source in runtime.hybrid_sources
            }
            self.assertTrue(dnse.accepts('tick', (0, 0)))
            self.assertFalse(ssi.accepts('tick', (0, 0)))
            ssi.connection('connected')
            dnse.connection('connected')
            self.assertTrue(ssi.accepts('tick', (0, 0)))
            self.assertFalse(dnse.accepts('tick', (0, 0)))
            self.assertTrue(dnse.accepts('contribution', {}))
            self.assertTrue(dnse.accepts('index', {'kind': 'estimated'}))
            self.assertFalse(dnse.accepts('index', {'kind': 'market'}))

            ssi.capabilities['candle'] = False
            ssi.capabilities['stocks'] = False
            self.assertTrue(dnse.accepts('candle', {}))
            self.assertTrue(dnse.accepts('stock', {}))
            self.assertFalse(dnse.accepts('reference', {}))

            ssi.capabilities['breadth'] = False
            self.assertFalse(ssi.accepts('index', {'kind': 'market'}))
            self.assertTrue(dnse.accepts('index', {'kind': 'market'}))
            ssi.connection('error', 'subscription denied')
            self.assertEqual(runtime.status, 'connected')
            self.assertTrue(dnse.accepts('tick', (0, 0)))
            runtime.close()

    def test_rejected_source_does_not_write(self):
        with tempfile.TemporaryDirectory() as root:
            runtime = Runtime(root, '41I1GB000', '2026-10-30')
            ssi = SourceRuntime(runtime, 'ssi-sdk')
            dnse = SourceRuntime(runtime, 'dnse')
            runtime.hybrid_sources = {'ssi-sdk': ssi, 'dnse': dnse}
            runtime.source_status = {
                source: {'status': 'offline', 'error': None}
                for source in runtime.hybrid_sources
            }
            ssi.connection('connected')
            dnse.connection('connected')
            event = {'reference': 1800, 'received_ms': 1790000000000}
            EventSink(dnse).emit('reference', event)
            self.assertIsNone(runtime.store().reference_price)
            EventSink(ssi).emit('reference', event)
            self.assertEqual(runtime.store().reference_price, 1800)
            runtime.close()


if __name__ == '__main__':
    unittest.main()
