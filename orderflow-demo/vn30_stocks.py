"""Current VN30 constituents and DNSE stock quote normalization."""
from datetime import datetime
import json
import math
from pathlib import Path
import re
import time
from urllib.request import Request, urlopen

from replay_server import TZ


# HOSE constituent disclosure, kỳ 7/2026, page 1 (the 30 VN30 rows only).
VN30_SSI_SOURCE = 'https://iboard-query.ssi.com.vn/stock/group/VN30'
VN30_SOURCE = ('https://staticfile.hsx.vn/Uploads/UploadDocuments/2479382/'
               '15072026%20CBTT%20-%20Danh%20muc%20thanh%20phan%20HOSE-Index%20thang%207.2026.pdf')
VN30_PERIOD = '7/2026'
VN30_SYMBOLS = (
    'ACB','BID','BSR','CTG','FPT','GAS','GVR','HDB','HPG','LPB',
    'MBB','MCH','MSN','MWG','SAB','SHB','SSB','SSI','STB','TCB',
    'TCX','VCB','VHM','VIB','VIC','VJC','VNM','VPB','VPL','VRE',
)
VN30_SET = frozenset(VN30_SYMBOLS)


def parse_vn30_symbols(payload):
    """Accept only a complete, unambiguous SSI VN30 constituent response."""
    if not isinstance(payload, dict) or payload.get('code') != 'SUCCESS':
        raise ValueError('SSI VN30 response failed')
    rows = payload.get('data')
    if not isinstance(rows, list) or len(rows) != 30:
        raise ValueError('SSI VN30 response must contain 30 stocks')
    symbols = []
    for row in rows:
        if not isinstance(row, dict) or row.get('exchange') != 'hose' or row.get('market') != 'STO':
            raise ValueError('Invalid SSI VN30 stock')
        symbol = row.get('stockSymbol')
        if not isinstance(symbol, str) or not re.fullmatch(r'[A-Z0-9]{1,12}', symbol):
            raise ValueError('Invalid SSI VN30 symbol')
        symbols.append(symbol)
    if len(set(symbols)) != 30:
        raise ValueError('Duplicate SSI VN30 symbol')
    return tuple(symbols)


def load_vn30_symbols(cache_path, online=True):
    """Fetch SSI constituents; retain the last valid response for offline startup."""
    cache_path = Path(cache_path)
    if online:
        try:
            request = Request(VN30_SSI_SOURCE, headers={
                'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'})
            with urlopen(request, timeout=8) as response:
                symbols = parse_vn30_symbols(json.load(response))
        except (OSError, ValueError, TypeError, KeyError) as exc:
            print(f'SSI VN30 list unavailable: {exc}', flush=True)
        else:
            try:
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                temp_path = cache_path.with_suffix('.tmp')
                temp_path.write_text(json.dumps({'symbols': symbols}), encoding='utf-8')
                temp_path.replace(cache_path)
            except OSError as exc:
                print(f'Cannot cache SSI VN30 list: {exc}', flush=True)
            return symbols, VN30_SSI_SOURCE, 'SSI VN30'
    try:
        cached = json.loads(cache_path.read_text(encoding='utf-8'))['symbols']
        if not isinstance(cached, list) or len(cached) != 30 or len(set(cached)) != 30 or any(
                not isinstance(symbol, str) or not re.fullmatch(r'[A-Z0-9]{1,12}', symbol)
                for symbol in cached):
            raise ValueError('Invalid cached VN30 symbols')
        return tuple(cached), VN30_SSI_SOURCE, 'SSI VN30 (lưu gần nhất)'
    except (OSError, ValueError, TypeError, KeyError):
        return VN30_SYMBOLS, VN30_SOURCE, VN30_PERIOD


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
