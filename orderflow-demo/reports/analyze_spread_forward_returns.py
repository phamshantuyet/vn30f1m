"""Descriptive source-time event study, without selecting on subsequent direction.
Run from the workspace root. Same-day recorded 41I1GB000 data; prices in integer ticks.
"""
import bisect
import csv
import json
import math
import sqlite3
import statistics
from collections import Counter
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]
DAY='2026-10-16';SYMBOL='41I1GB000'
DB=ROOT/'orderflow-demo'/'.replay-cache'/f'{SYMBOL}-{DAY}.sqlite'
OUT=Path(__file__).resolve().parent
TZ=timezone(timedelta(hours=7))
HORIZONS=(60,180,300)
BUY_LOOKBACK_MS=100
PREVIOUS_QUOTE_MAX_AGE_MS=500
ENDPOINT_MAX_AGE_MS=5000
BREAK_START=int(datetime.fromisoformat(DAY+'T11:30:00').replace(tzinfo=TZ).timestamp()*1000)
BREAK_END=int(datetime.fromisoformat(DAY+'T13:00:00').replace(tzinfo=TZ).timestamp()*1000)
clock=lambda ts:datetime.fromtimestamp(ts/1000,TZ).strftime('%H:%M:%S.%f')[:-3]
valid=lambda q:q[1] is not None and q[3] is not None and q[1]<q[3]

def quantile(values,p):
    values=sorted(values)
    if not values:return None
    pos=(len(values)-1)*p;lo=math.floor(pos);hi=math.ceil(pos)
    return values[lo]+(values[hi]-values[lo])*(pos-lo)

def summarize(events,horizon):
    key=f'delta_{horizon}s_ticks'
    values=[e[key] for e in events if e[key] is not None]
    n=len(values)
    if not n:return {'n':0}
    down=sum(v<0 for v in values);up=sum(v>0 for v in values);same=n-up-down
    return {'n':n,'higher':up,'same':same,'lower':down,
      'higher_pct':up/n*100,'same_pct':same/n*100,'lower_pct':down/n*100,
      'mean_points':statistics.mean(values)/10,'median_points':statistics.median(values)/10,
      'p25_points':quantile(values,.25)/10,'p75_points':quantile(values,.75)/10,
      'min_points':min(values)/10,'max_points':max(values)/10}

def run():
    conn=sqlite3.connect(DB)
    q=[];t=[]
    for ts,kind,payload in conn.execute('SELECT ts,kind,payload FROM events ORDER BY ts,kind,source_order'):
        data=json.loads(payload)
        if kind==0:
            bids,asks=data
            row=(ts,bids[0][0] if bids else None,bids[0][1] if bids else 0,
                 asks[0][0] if asks else None,asks[0][1] if asks else 0)
            # Only the final recorded image at each millisecond has positive duration.
            if q and q[-1][0]==ts:q[-1]=row
            else:q.append(row)
        else:t.append((ts,*data[:3]))
    qt=[r[0] for r in q];tt=[r[0] for r in t]
    # Prefix count of recording intervals longer than 30 seconds, not proof of feed loss.
    large_gaps=[0]
    for a,b in zip(q,q[1:]):large_gaps.append(large_gaps[-1]+int(b[0]-a[0]>30000))
    events=[];counts=Counter();rejections={h:Counter() for h in HORIZONS}
    last_record=max(q[-1][0],t[-1][0])
    for i in range(1,len(q)):
        old,now=q[i-1],q[i];ts,bb,bq,aa,aq=now
        if not valid(old) or not valid(now) or ts-old[0]>PREVIOUS_QUOTE_MAX_AGE_MS:continue
        if not(old[3]-old[1]<3<=aa-bb):continue
        counts['all_spread_crossings']+=1
        if aa<=old[3] or bb<old[1]:continue
        counts['ask_up_bid_not_down']+=1
        if BREAK_START<=ts<BREAK_END:continue
        burst=t[bisect.bisect_left(tt,ts-BUY_LOOKBACK_MS):bisect.bisect_right(tt,ts)]
        buys=[r for r in burst if r[3]=='BUY' and r[1]>=old[3]]
        if not buys:continue
        anchor=buys[-1]
        qty=sum(r[2] for r in buys)
        event={'event_ms':ts,'event_time':clock(ts),'spread_ticks':aa-bb,'spread_points':(aa-bb)/10,
          'old_bid':old[1]/10,'old_ask':old[3]/10,'new_bid':bb/10,'new_ask':aa/10,
          'buy_time':clock(anchor[0]),'buy_age_ms':ts-anchor[0],'buy_price':anchor[1]/10,
          'buy_price_ticks':anchor[1],'buy_burst_qty':qty,
          'buy_burst_vwap':sum(r[1]*r[2] for r in buys)/qty/10,
          'buy_reaches_new_ask':max(r[1] for r in buys)>=aa}
        for h in HORIZONS:
            target=ts+h*1000;reason=None
            if target>last_record:reason='after_file_end'
            elif ts<BREAK_START<=target or BREAK_START<=ts<BREAK_END:reason='crosses_lunch_pause'
            ti=bisect.bisect_right(tt,target)-1;qi=bisect.bisect_right(qt,target)-1
            if reason is None and (ti<0 or target-t[ti][0]>ENDPOINT_MAX_AGE_MS):reason='stale_last_trade'
            if reason is None and (qi<0 or target-q[qi][0]>ENDPOINT_MAX_AGE_MS or not valid(q[qi])):reason='stale_or_invalid_endpoint_quote'
            if reason is None and large_gaps[qi]-large_gaps[i]>0:reason='quote_recording_interval_over_30s'
            event[f'target_{h}s']=clock(target)
            event[f'delta_{h}s_ticks']=None
            event[f'excluded_{h}s']=reason
            if reason:
                rejections[h][reason]+=1
            else:
                price=t[ti][1]
                event.update({f'price_{h}s':price/10,f'trade_time_{h}s':clock(t[ti][0]),
                  f'trade_age_{h}s_ms':target-t[ti][0],f'delta_{h}s_ticks':price-anchor[1],
                  f'delta_{h}s_points':(price-anchor[1])/10,
                  f'bid_{h}s':q[qi][1]/10,f'ask_{h}s':q[qi][3]/10,
                  f'midpoint_delta_{h}s_points':((q[qi][1]+q[qi][3])/2-anchor[1])/10,
                  f'bid_delta_{h}s_points':(q[qi][1]-anchor[1])/10,
                  f'vwap_delta_{h}s_points':price/10-event['buy_burst_vwap']})
        events.append(event)
    complete=[e for e in events if all(e[f'delta_{h}s_ticks'] is not None for h in HORIZONS)]
    # Choose the first event after a fixed cooldown using timestamps alone, BEFORE checking outcomes.
    spaced=[];last_event=None
    for e in events:
        if last_event is None or e['event_ms']-last_event>=300000:
            spaced.append(e);last_event=e['event_ms']
    spaced_complete=[e for e in spaced if all(e[f'delta_{h}s_ticks'] is not None for h in HORIZONS)]
    report={'symbol':SYMBOL,'day':DAY,
      'rules':{'event':'Spread crosses from <0.3 to >=0.3; Ask rises, Bid does not fall; previous Quote <=500ms old',
       'anchor':'Latest provider-classified BUY at/above previous Ask, in [event-100ms,event]; no future BUY used',
       'horizons':'60,180,300 elapsed wall-clock seconds after Quote event',
       'comparison':'Last recorded trade at/before target, <=5s old; valid Quote <=5s old',
       'exclusions':'No lunch crossing (11:30–13:00 UTC+7), no target after file end, no Quote recording gap >30s',
       'ties':'Equal-ms images collapsed to last source record; cross-source order within the same millisecond unverified',
       'selection':'No condition on future SELL, lower Ask, or future price direction',
       'limits':'One day; overlapping event windows are correlated; descriptive statistics, not out-of-sample performance or net trading PnL'},
      'counts':dict(counts)|{'buy_associated_events':len(events),'common_cohort_all_horizons':len(complete),
       'spaced_5min_events_before_quality_filter':len(spaced),'spaced_5min_complete':len(spaced_complete)},
      'exclusions':{str(h):dict(rejections[h]) for h in HORIZONS},
      'all_available_by_horizon':{str(h):summarize(events,h) for h in HORIZONS},
      'common_cohort':{str(h):summarize(complete,h) for h in HORIZONS},
      'spaced_5min_common_cohort':{str(h):summarize(spaced_complete,h) for h in HORIZONS},
      'by_opening_spread':{name:{str(h):summarize([e for e in complete if (e['spread_ticks']==ticks if ticks else e['spread_ticks']>=5)],h) for h in HORIZONS}
         for name,ticks in [('0.3',3),('0.4',4),('>=0.5',None)]},
      'new_ask_reached_subgroup':{str(h):summarize([e for e in complete if e['buy_reaches_new_ask']],h) for h in HORIZONS},
      'common_cohort_vwap_sensitivity':{str(h):{'n':len(complete),
        'mean_points':statistics.mean(e[f'vwap_delta_{h}s_points'] for e in complete),
        'lower_pct':sum(e[f'vwap_delta_{h}s_points']<0 for e in complete)/len(complete)*100} for h in HORIZONS}}
    stem=f'spread_buy_forward_returns_{DAY.replace("-","")}'
    (OUT/(stem+'.json')).write_text(json.dumps(report,ensure_ascii=False,indent=2))
    fields=list(dict.fromkeys(k for e in events for k in e))
    with (OUT/(stem+'.csv')).open('w',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=fields);writer.writeheader();writer.writerows(events)
    print(json.dumps(report,ensure_ascii=False,indent=2),flush=True)
    print('EXAMPLES',json.dumps([e for e in events if e['event_time'] in ('09:08:09.778','10:01:20.124','13:00:19.446','09:54:52.183')],ensure_ascii=False),flush=True)

if __name__=='__main__':run()
