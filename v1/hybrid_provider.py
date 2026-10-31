"""Run both feeds and route each canonical event to one active source."""
import asyncio
import os

from dnse_provider import bootstrap_vn30_stocks, collect as collect_dnse
from ssi_provider import collect as collect_ssi


class SourceRuntime:
    def __init__(self, root, source):
        self.root = root
        self.source = source
        self.status = 'offline'
        self.capabilities = {
            'quote': True, 'trade': True, 'reference': True, 'candle': True,
            'stocks': True, 'index': True, 'breadth': True,
            'estimated_index': source == 'dnse',
            'contribution': source == 'dnse',
        }

    def __getattr__(self, name):
        return getattr(self.root, name)

    def store(self, day=None):
        return self.root.store(day)

    def connection(self, status, error=None):
        with self.root.lock:
            self.status = status
            self.root.source_status[self.source] = {'status': status, 'error': error}
            states = [item['status'] for item in self.root.source_status.values()]
            overall = ('connected' if 'connected' in states else
                       'reconnecting' if 'reconnecting' in states else
                       'connecting' if 'connecting' in states else
                       'error' if 'error' in states else 'offline')
            self.root.connection(overall, error if overall == 'error' else None)

    def accepts(self, channel, event):
        capability = (
            ('quote' if event[1] == 0 else 'trade') if channel == 'tick' else
            ('estimated_index' if event['kind'] == 'estimated' else 'index')
            if channel == 'index' else
            {'stock': 'stocks', 'contribution': 'contribution'}.get(channel, channel)
        )
        if self.source == 'ssi-sdk':
            if capability == 'index':
                return (self.status == 'connected' and self.capabilities['index']
                        and self.capabilities['breadth'])
            return self.status == 'connected' and self.capabilities.get(capability, False)
        if capability in ('estimated_index', 'contribution'):
            return True
        ssi = self.root.hybrid_sources['ssi-sdk']
        if capability == 'index':
            # DNSE market-index events also carry breadth. Keep both fields from one feed.
            return ssi.status != 'connected' or not (
                ssi.capabilities['index'] and ssi.capabilities['breadth'])
        return ssi.status != 'connected' or not ssi.capabilities.get(capability, False)


async def collect(runtime, key, secret, url=None, ohlc_symbol_type='VN30F1M'):
    dnse_key = os.getenv('DNSE_API_KEY')
    dnse_secret = os.getenv('DNSE_API_SECRET')
    ssi = SourceRuntime(runtime, 'ssi-sdk')
    dnse = SourceRuntime(runtime, 'dnse')
    runtime.hybrid_sources = {'ssi-sdk': ssi, 'dnse': dnse}
    runtime.source_status = {
        source: {'status': 'offline', 'error': None} for source in runtime.hybrid_sources
    }
    async def bootstrap():
        try:
            await asyncio.to_thread(bootstrap_vn30_stocks, dnse, dnse_key, dnse_secret)
        except Exception as exc:
            print('DNSE stock snapshot unavailable: '+type(exc).__name__, flush=True)
    await asyncio.gather(
        collect_ssi(ssi, key, secret, url, ohlc_symbol_type),
        collect_dnse(dnse, dnse_key, dnse_secret,
                     os.getenv('DNSE_WSS_URL', 'wss://ws-openapi.dnse.com.vn'),
                     ohlc_symbol_type),
        bootstrap(),
    )
