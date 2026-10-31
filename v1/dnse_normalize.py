"""DNSE payload conversion at the provider boundary."""
import hashlib, math, time
from datetime import datetime
import msgpack
from replay_server import TZ, ticks
from live_store import pack, integer, optional_number

def normalize(obj, kind, symbol, day):
    from events import tick_event
    if obj.symbol!=symbol or obj.boardId!='G1':
        raise ValueError('Unexpected symbol/board')
    dt=datetime.fromisoformat(obj.time)
    if dt.tzinfo is None:
        dt=dt.replace(tzinfo=TZ)
    common=dict(symbol=obj.symbol,expected_symbol=symbol,day=day,
                source_ms=round(dt.timestamp()*1000),
                received_ms=round((obj.receivedAt or time.time())*1000))
    if kind==0:
        return tick_event(**common,bids=[(level.price,level.quantity) for level in obj.bid],
                          asks=[(level.price,level.quantity) for level in obj.offer])
    return tick_event(**common,price=obj.price,quantity=obj.quantity,side=obj.side,
        total_volume=obj.totalVolumeTraded,
        stats={field:optional_number(getattr(obj,source,None),positive=field!='grossTradeAmount')
               for field,source in (('grossTradeAmount','grossTradeAmount'),('avgPrice','avgPrice'),
                                    ('highestPrice','highestPrice'),('lowestPrice','lowestPrice'),('openPrice','openPrice'))})


def normalize_influence(obj):
    """Keep DNSE's own VN30 point contributions, not inferred weights or prices."""
    if obj.index_name != 'VN30':
        raise ValueError('Unexpected index')
    received = round((obj.receivedAt or time.time())*1000)
    if not 0 < received < 10**15:
        raise ValueError('Invalid influence timestamp')
    items = {}
    if len(obj.data) > 200:
        raise ValueError('Too many influence symbols')
    for item in obj.data:
        symbol = item.symbol
        if not isinstance(symbol, str) or not re.fullmatch(r'[A-Z0-9]{1,12}', symbol):
            raise ValueError('Invalid influence symbol')
        try:
            value = float(item.influence)
        except (TypeError, ValueError, OverflowError):
            raise ValueError('Invalid influence value') from None
        if not math.isfinite(value):
            raise ValueError('Invalid influence value')
        items[symbol] = value
    return {'received_ms':received,'items':[[symbol,value] for symbol,value in sorted(items.items())]}


def normalize_market_index(obj, kind, day):
    """Keep DNSE's official/estimated VN30 values for a same-day basis display."""
    if kind not in ('market','estimated') or obj.indexName != 'VN30':
        raise ValueError('Unexpected market index')
    received = round((obj.receivedAt or time.time())*1000)
    if not 0 < received < 10**15:
        raise ValueError('Invalid market index timestamp')
    source_time = getattr(obj,'transactTime' if kind=='market' else 'time',None)
    source_ms = None
    if source_time:
        dt = datetime.fromisoformat(source_time.replace('Z','+00:00'))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=TZ)
        dt = dt.astimezone(TZ)
        if dt.date().isoformat()!=day:
            raise ValueError('Unexpected market index source date')
        source_ms = round(dt.timestamp()*1000)
    elif datetime.fromtimestamp(received/1000,TZ).date().isoformat()!=day:
        raise ValueError('Unexpected market index receive date')
    def finite(value,positive=False):
        if value is None:
            return None
        number=float(value)
        if not math.isfinite(number) or positive and number<=0:
            raise ValueError('Invalid market index value')
        return number
    def count(name):
        value=getattr(obj,name,None)
        return None if value is None else integer(value)
    value=finite(obj.valueIndexes,positive=True)
    if value is None:
        raise ValueError('Missing market index value')
    return {'kind':kind,'received_ms':received,'source_ms':source_ms,'value':value,
            'change':finite(getattr(obj,'changedValue',None)),
            'change_pct':finite(getattr(obj,'changedRatio',None)),
            'prior':finite(getattr(obj,'priorValueIndexes',None),positive=True),
            'high':finite(getattr(obj,'highestValueIndexes',None),positive=True),
            'low':finite(getattr(obj,'lowestValueIndexes',None),positive=True),
            'up':count('fluctuationUpIssueCount'),
            'down':count('fluctuationDownIssueCount'),
            'flat':count('fluctuationSteadinessIssueCount'),
            'volume':count('totalVolumeTraded'),
            'amount':finite(getattr(obj,'grossTradeAmount',None))}


def normalize_reference_price(obj, symbol):
    if obj.symbol != symbol or obj.boardId != 'G1':
        raise ValueError('Unexpected security definition')
    price=optional_number(getattr(obj,'basicPrice',None),positive=True)
    received=round((obj.receivedAt or time.time())*1000)
    if price is None:
        raise ValueError('Invalid reference price')
    return price,received


def normalize_ohlcv_1m(obj, symbol, day, closed=False, ohlc_symbol_type=None):
    """Normalize DNSE's live/closed one-minute candle without inferring missing bars."""
    if obj.symbol!=(ohlc_symbol_type or symbol) or str(obj.resolution)!='1':
        raise ValueError('Unexpected OHLC symbol or resolution')
    def epoch_ms(value):
        number=float(value)
        if not math.isfinite(number):
            raise ValueError('Invalid OHLC timestamp')
        if 10**9<=number<10**11:
            return round(number*1000)
        if 10**12<=number<10**14:
            return round(number)
        raise ValueError('Invalid OHLC timestamp')
    source_ms=epoch_ms(obj.time)
    minute_ms=source_ms//60000*60000
    if datetime.fromtimestamp(minute_ms/1000,TZ).date().isoformat()!=day:
        raise ValueError('Unexpected OHLC day')
    received_ms=round((obj.receivedAt or time.time())*1000)
    updated_ms=epoch_ms(obj.lastUpdated) if obj.lastUpdated is not None else received_ms
    values=[float(getattr(obj,name)) for name in ('open','high','low','close')]
    if not all(math.isfinite(value) and value>0 for value in values):
        raise ValueError('Invalid OHLC price')
    opening,high,low,closing=values
    if low>min(opening,closing) or high<max(opening,closing) or high<low:
        raise ValueError('Inconsistent OHLC prices')
    volume=integer(obj.volume)
    return {'minute_ms':minute_ms,'updated_ms':updated_ms,'received_ms':received_ms,
            'open':opening,'high':high,'low':low,'close':closing,'volume':volume,
            'closed':bool(closed)}



from vn30_stocks import VN30_SET

def field(obj, name):
    return obj.get(name) if isinstance(obj, dict) else getattr(obj, name, None)


def positive_price(value):
    try:
        price = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return price if math.isfinite(price) and price > 0 else None


def source_time(obj, day):
    received_ms = round((field(obj, 'receivedAt') or time.time()) * 1000)
    value = field(obj, 'time')
    if value:
        dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=TZ)
        dt = dt.astimezone(TZ)
    else:
        dt = datetime.fromtimestamp(received_ms / 1000, TZ)
    if dt.date().isoformat() != day:
        raise ValueError('Unexpected VN30 stock date')
    return round(dt.timestamp() * 1000), received_ms


def normalize_vn30_stock(obj, kind, day, allowed_symbols=VN30_SET):
    symbol = field(obj, 'symbol')
    if symbol not in allowed_symbols or field(obj, 'boardId') != 'G1' or field(obj, 'marketId') != 'STO':
        raise ValueError('Unexpected VN30 stock symbol/board/market')
    source_ms, received_ms = source_time(obj, day)
    snapshot = {'symbol': symbol, 'kind': kind, 'source_ms': source_ms,
                'received_ms': received_ms}
    if kind == 'reference':
        reference = positive_price(field(obj, 'basicPrice'))
        if reference is None:
            raise ValueError('Invalid VN30 reference')
        snapshot['reference'] = reference
    elif kind == 'trade':
        last = positive_price(field(obj, 'price') or field(obj, 'matchPrice'))
        high = positive_price(field(obj, 'highestPrice'))
        low = positive_price(field(obj, 'lowestPrice'))
        if last is None or high is not None and low is not None and high < low:
            raise ValueError('Invalid VN30 trade price')
        snapshot.update(last=last, high=high, low=low)
    else:
        raise ValueError('Invalid VN30 stock kind')
    return snapshot
