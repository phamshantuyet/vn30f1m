"""Causal BUY slowdown features; quantity rate and match frequency are distinct.
Windows are open-left / closed-right. Baseline is the preceding 3 windows.
No prices, future labels or SELL features enter the slowdown conditions.
"""
from bisect import bisect_right

WINDOWS = (5, 10, 30)


def slowdown(buy_volume, baseline_volume, buy_count, baseline_count):
    if min(buy_volume, baseline_volume, buy_count, baseline_count) < 0:
        raise ValueError('Negative volume/count')
    volume_ratio = 3 * buy_volume / baseline_volume if baseline_volume else None
    frequency_ratio = 3 * buy_count / baseline_count if baseline_count else None
    current_size = buy_volume / buy_count if buy_count else None
    baseline_size = baseline_volume / baseline_count if baseline_count else None
    return {
        'buy_volume': buy_volume, 'baseline_volume': baseline_volume,
        'buy_count': buy_count, 'baseline_count': baseline_count,
        'volume_ratio': volume_ratio, 'frequency_ratio': frequency_ratio,
        'average_size': current_size, 'baseline_average_size': baseline_size,
        'volume_slow': baseline_volume > 0 and 6 * buy_volume < baseline_volume,
        'frequency_slow': baseline_count > 0 and 6 * buy_count < baseline_count,
    }


class BuySpeed:
    def __init__(self, buys):
        self.times = []
        self.volumes = [0]
        for ts, qty in sorted(buys):
            if qty < 0:
                raise ValueError('Negative quantity')
            self.times.append(ts)
            self.volumes.append(self.volumes[-1] + qty)

    def at(self, ts, seconds):
        a = bisect_right(self.times, ts - 4 * seconds * 1000)
        b = bisect_right(self.times, ts - seconds * 1000)
        c = bisect_right(self.times, ts)
        result = slowdown(self.volumes[c] - self.volumes[b],
                          self.volumes[b] - self.volumes[a], c-b, b-a)
        result.update(buy_contracts_per_second=result['buy_volume']/seconds,
                      baseline_contracts_per_second=result['baseline_volume']/(3*seconds),
                      buy_matches_per_second=result['buy_count']/seconds,
                      baseline_matches_per_second=result['baseline_count']/(3*seconds))
        return result
