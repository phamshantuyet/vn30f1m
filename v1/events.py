"""SDK-neutral event contract consumed by storage and every dashboard box.

Prices: futures/index in points, stock snapshots in thousand VND.
Times: UTC epoch milliseconds. Volume: individual shares/contracts, never lots.
Tick payload encoding is private to storage; adapters use tick_event().
Other channels use dictionaries matching the public store snapshots.
"""
import hashlib
import math
from datetime import datetime
import msgpack
from live_store import pack, integer, optional_number
from replay_server import TZ, ticks


def tick_event(*, symbol, expected_symbol, day, source_ms, received_ms,
               bids=None, asks=None, price=None, quantity=None, side='UNKNOWN',
               total_volume=None, stats=None):
    if symbol!=expected_symbol or datetime.fromtimestamp(source_ms/1000,TZ).date().isoformat()!=day:
        raise ValueError('Unexpected market symbol/date')
    if not all(math.isfinite(v) and v>0 for v in (source_ms,received_ms)):
        raise ValueError('Invalid market time')
    def tick(value):
        p=ticks(value)
        if p<=0 or abs(p/10-float(value))>1e-8:
            raise ValueError('Invalid tick price')
        return p
    if bids is not None or asks is not None:
        payload=[sorted([[tick(p),integer(q)] for p,q in levels],reverse=i==0)
                 for i,levels in enumerate((bids or [],asks or []))]
        if any(len({p for p,q in levels})!=len(levels) for levels in payload):
            raise ValueError('Duplicate price levels')
        return (round(source_ms),0,round(received_ms),pack(payload),None,None,None,None,None,None)
    p,q=tick(price),integer(quantity,True)
    side=side if side in ('BUY','SELL') else 'UNKNOWN'
    cumulative=None if total_volume is None else integer(total_volume)
    payload=[p,q,side,cumulative]
    key=hashlib.blake2b(msgpack.packb([round(source_ms),*payload]),digest_size=16).digest() if cumulative is not None else None
    stats={**(stats or {}),'totalVolumeTraded':cumulative}
    return (round(source_ms),1,round(received_ms),pack(payload),p,q,side,cumulative,key,stats)


class EventSink:
    """Single delivery boundary: SDK objects never enter the store or UI."""
    def __init__(self,runtime):
        self.runtime=runtime

    def emit(self,channel,event):
        if hasattr(self.runtime,'accepts') and not self.runtime.accepts(channel,event):
            return
        store=self.runtime.store()
        if store.failed or self.runtime.stop.is_set():
            return
        if channel=='tick':
            if datetime.fromtimestamp(event[0]/1000,TZ).date().isoformat()!=store.day:
                raise ValueError('Unexpected tick day')
            store.enqueue(event)
        elif channel=='reference':
            reference=optional_number(event.get('reference'),positive=True)
            if reference is None:
                raise ValueError('Invalid reference')
            store.set_reference_price(reference,event['received_ms'])
        elif channel=='stock':
            if datetime.fromtimestamp(event['source_ms']/1000,TZ).date().isoformat()!=store.day:
                raise ValueError('Unexpected stock day')
            required='reference' if event['kind']=='reference' else 'last'
            if optional_number(event.get(required),positive=True) is None:
                raise ValueError('Invalid stock price')
            for field in ('reference','last','high','low'):
                if event.get(field) is not None and optional_number(event[field],positive=True) is None:
                    raise ValueError('Invalid stock price')
            if event.get('high') is not None and event.get('low') is not None and event['high']<event['low']:
                raise ValueError('Invalid stock range')
            store.enqueue_vn30_stock(event)
        elif channel=='index':
            store.enqueue_index(event)
        elif channel=='contribution':
            store.enqueue_influence(event)
        elif channel=='candle':
            if datetime.fromtimestamp(event['minute_ms']/1000,TZ).date().isoformat()!=store.day:
                raise ValueError('Unexpected candle day')
            prices=[float(event[k]) for k in ('open','high','low','close')]
            if not all(math.isfinite(p) and p>0 for p in prices) or prices[1]<max(prices) or prices[2]>min(prices):
                raise ValueError('Invalid candle prices')
            integer(event['volume'])
            store.enqueue_ohlcv(event)
        else:
            raise ValueError('Unknown market channel')
