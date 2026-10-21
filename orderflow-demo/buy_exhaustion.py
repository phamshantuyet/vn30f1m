"""Fixed buy-weakening rule v1. No future prices, UI state or tuned afternoon values.
All timestamps are integer source milliseconds; prices are integer 0.1-point ticks.
"""
from bisect import bisect_right
from dataclasses import dataclass

@dataclass(frozen=True)
class BuyExhaustionConfig:
    current_ms: int = 10_000
    baseline_ms: int = 30_000
    buy_ratio_numerator: int = 1
    buy_ratio_denominator: int = 2
    sell_share_percent: int = 65
    midpoint_drop_half_ticks: int = 2  # -0.1 point
    min_trades: int = 20
    min_classified_volume: int = 50
    min_classified_percent: int = 90
    min_quote_coverage_percent: int = 80
    quote_hold_ms: int = 5_000
    max_trade_age_ms: int = 5_000
    version: str = 'buy-exhaustion-v1'

@dataclass(frozen=True)
class Quote:
    ts: int
    bid: int | None
    ask: int | None

    @property
    def valid(self):
        return self.bid is not None and self.ask is not None and self.bid < self.ask

@dataclass(frozen=True)
class Observation:
    ts: int
    first_event_ts: int | None
    buy: int
    sell: int
    unknown: int
    baseline_buy: int
    trade_count: int
    last_trade_ts: int | None
    quote_now: Quote | None
    quote_before: Quote | None
    covered_ms: int


def evaluate_buy_exhaustion(o: Observation, config=None):
    """Pure decision. Strict thresholds use integer comparisons, not rounded ratios."""
    c=config or BuyExhaustionConfig()
    classified=o.buy+o.sell
    total=classified+o.unknown
    issues=[]
    if o.first_event_ts is None or o.ts-o.first_event_ts<c.current_ms+c.baseline_ms:
        issues.append('warmup')
    if o.trade_count<c.min_trades:issues.append('few_trades')
    if classified<c.min_classified_volume:issues.append('low_classified_volume')
    if not total or classified*100<total*c.min_classified_percent:issues.append('unknown_side')
    if not o.baseline_buy:issues.append('zero_buy_baseline')
    if o.last_trade_ts is None or not 0<=o.ts-o.last_trade_ts<=c.max_trade_age_ms:
        issues.append('stale_trade')
    for name,q,at in [('quote_now',o.quote_now,o.ts),('quote_before',o.quote_before,o.ts-c.current_ms)]:
        if q is None or not q.valid or not 0<=at-q.ts<=c.quote_hold_ms:issues.append(name)
    if o.covered_ms*100<c.current_ms*c.min_quote_coverage_percent:issues.append('quote_coverage')
    qn,qp=o.quote_now,o.quote_before
    comparable=qn is not None and qp is not None and qn.valid and qp.valid
    mid_half=(qn.bid+qn.ask)-(qp.bid+qp.ask) if comparable else None
    bid_ticks=qn.bid-qp.bid if comparable else None
    ask_ticks=qn.ask-qp.ask if comparable else None
    conditions={
        'buy_rate_falls':bool(o.baseline_buy and o.buy*c.baseline_ms*c.buy_ratio_denominator < o.baseline_buy*c.current_ms*c.buy_ratio_numerator),
        'sell_dominates':bool(classified and o.sell*100>classified*c.sell_share_percent),
        'midpoint_falls':mid_half is not None and mid_half<=-c.midpoint_drop_half_ticks,
        'both_quotes_fall':comparable and bid_ticks<0 and ask_ticks<0,
    }
    alert=not issues and all(conditions.values())
    return {'ts':o.ts,'version':c.version,'eligible':not issues,'alert':alert,
      'status':'INSUFFICIENT_DATA' if issues else 'BUY_WEAKENING' if alert else 'NO_SIGNAL',
      'issues':issues,'conditions':conditions,
      'buy_10s':o.buy,'sell_10s':o.sell,'unknown_10s':o.unknown,'baseline_buy_30s':o.baseline_buy,
      'trade_count_10s':o.trade_count,'covered_ms':o.covered_ms,
      'buy_rate':o.buy/(c.current_ms/1000),'baseline_buy_rate':o.baseline_buy/(c.baseline_ms/1000),
      'buy_ratio':o.buy*c.baseline_ms/(o.baseline_buy*c.current_ms) if o.baseline_buy else None,
      'sell_share':o.sell/classified if classified else None,
      'midpoint_change_points':mid_half/20 if mid_half is not None else None,
      'bid_change_points':bid_ticks/10 if bid_ticks is not None else None,
      'ask_change_points':ask_ticks/10 if ask_ticks is not None else None,
      'midpoint_points':(qn.bid+qn.ask)/20 if qn is not None and qn.valid else None,
      'midpoint_half_ticks':qn.bid+qn.ask if qn is not None and qn.valid else None}


class BuyExhaustionEngine:
    """Causal feature extraction. Earlier evaluations remain independent of future appends."""
    def __init__(self,config=None):
        self.config=config or BuyExhaustionConfig()
        self.first_ts=None
        self.trade_ts=[];self.volumes=[(0,0,0)]
        self.quote_ts=[];self.quotes=[];self.coverage=[]

    def _observe_first(self,ts):
        self.first_ts=ts if self.first_ts is None else min(self.first_ts,ts)

    def push_trade(self,ts,quantity,side):
        if quantity<=0:raise ValueError('Trade quantity must be positive')
        if self.trade_ts and ts<self.trade_ts[-1]:raise ValueError('Trade timestamps must be nondecreasing')
        self._observe_first(ts)
        v=list(self.volumes[-1]);v[{'BUY':0,'SELL':1}.get(side,2)]+=quantity
        self.trade_ts.append(ts);self.volumes.append(tuple(v))

    def push_quote(self,ts,bid,ask):
        if self.quote_ts and ts<self.quote_ts[-1]:raise ValueError('Quote timestamps must be nondecreasing')
        self._observe_first(ts)
        area=0
        if self.quotes:
            previous=self.quotes[-1]
            area=self.coverage[-1]+(min(ts-previous.ts,self.config.quote_hold_ms) if previous.valid else 0)
        self.quote_ts.append(ts);self.quotes.append(Quote(ts,bid,ask));self.coverage.append(area)

    def _at(self,ts):
        i=bisect_right(self.quote_ts,ts)-1
        return self.quotes[i] if i>=0 else None

    def _area(self,ts):
        i=bisect_right(self.quote_ts,ts)-1
        if i<0:return 0
        q=self.quotes[i]
        return self.coverage[i]+(min(ts-q.ts,self.config.quote_hold_ms) if q.valid else 0)

    def evaluate(self,ts):
        c=self.config
        lo=bisect_right(self.trade_ts,ts-c.current_ms)
        hi=bisect_right(self.trade_ts,ts)
        base=bisect_right(self.trade_ts,ts-c.current_ms-c.baseline_ms)
        v=[self.volumes[hi][i]-self.volumes[lo][i] for i in range(3)]
        baseline=self.volumes[lo][0]-self.volumes[base][0]
        o=Observation(ts,self.first_ts,*v,baseline,hi-lo,
          self.trade_ts[hi-1] if hi else None,self._at(ts),self._at(ts-c.current_ms),
          self._area(ts)-self._area(ts-c.current_ms))
        return evaluate_buy_exhaustion(o,c)
