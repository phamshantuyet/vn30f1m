"""Frozen v1 afternoon evaluation. Source-cache adapter; no UI or FlowAssessment imports.
Predictions are fully recorded before any forward-price label is calculated.
"""
import bisect
import csv
import json
import sqlite3
import statistics
from collections import Counter
from dataclasses import asdict
from datetime import datetime,timezone,timedelta
from pathlib import Path
from buy_exhaustion import BuyExhaustionConfig,BuyExhaustionEngine

ROOT=Path(__file__).resolve().parent
DAY='2026-10-16';SYMBOL='41I1GB000';TZ=timezone(timedelta(hours=7))
HORIZONS=(60,180,300)

def millis(clock):
    return int(datetime.fromisoformat(DAY+'T'+clock).replace(tzinfo=TZ).timestamp()*1000)

def clock(ts):
    return datetime.fromtimestamp(ts/1000,TZ).strftime('%H:%M:%S')

def flatten(decision,onset):
    out={k:v for k,v in decision.items() if k not in ('issues','conditions')}
    out['time']=clock(decision['ts']);out['onset']=int(onset)
    for key in ('alert','eligible'):out[key]=int(out[key])
    out['issues']='|'.join(sorted(decision['issues']))
    out.update({k:int(v) for k,v in decision['conditions'].items()})
    return out

def summary(rows,h):
    values=[r[f'delta_mid_{h}s'] for r in rows if r[f'delta_mid_{h}s'] is not None]
    if not values:return {'n':0}
    n=len(values)
    return {'n':n,'lower':sum(x<0 for x in values),'flat':sum(x==0 for x in values),'higher':sum(x>0 for x in values),
      'lower_pct':sum(x<0 for x in values)/n*100,'mean_points':statistics.mean(values),
      'median_points':statistics.median(values),'min_points':min(values),'max_points':max(values)}

def run():
    db=sqlite3.connect(ROOT/'.replay-cache'/f'{SYMBOL}-{DAY}.sqlite')
    meta=json.loads(db.execute("SELECT value FROM metadata WHERE key='stats'").fetchone()[0])
    start=millis('13:00:00');end=meta['end_ms']
    config=BuyExhaustionConfig()
    engine=BuyExhaustionEngine(config)
    raw=list(db.execute('SELECT ts,kind,payload FROM events WHERE ts>=? AND ts<=? ORDER BY ts,kind,source_order',(start,end)))
    quotes=[];trades=[]
    for ts,kind,payload in raw:
        data=json.loads(payload)
        if kind==0:
            b,a=data;quotes.append((ts,b[0][0] if b else None,a[0][0] if a else None))
        else:trades.append((ts,data[0]))
    predictions=[];i=0;previous_alert=False
    for at in range(start,end+1,1000):
        while i<len(raw) and raw[i][0]<=at:
            ts,kind,payload=raw[i];data=json.loads(payload)
            if kind==0:
                b,a=data;engine.push_quote(ts,b[0][0] if b else None,a[0][0] if a else None)
            else:engine.push_trade(ts,data[1],data[2])
            i+=1
        d=engine.evaluate(at)
        predictions.append(flatten(d,d['alert'] and not previous_alert));previous_alert=d['alert']
    out=ROOT/'reports';out.mkdir(exist_ok=True)
    stem='buy_exhaustion_afternoon_20261016'
    with (out/(stem+'_predictions.csv')).open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=list(predictions[0]));w.writeheader();w.writerows(predictions)
    # Forward data is consulted only after the above causal prediction pass.
    qt=[q[0] for q in quotes];tt=[t[0] for t in trades]
    onsets=[r for r in predictions if r['onset']]
    spaced=[];last=None
    for row in onsets:
        if last is None or row['ts']-last>=300000:spaced.append(row);last=row['ts']
    baseline=[r for r in predictions if r['eligible'] and not r['alert'] and (r['ts']-start)%300000==60000]
    def label(row):
        r=dict(row)
        for h in HORIZONS:
            target=r['ts']+h*1000;reason=None
            qi=bisect.bisect_right(qt,target)-1;ti=bisect.bisect_right(tt,target)-1
            if target>end:reason='after_file_end'
            elif qi<0 or target-quotes[qi][0]>5000 or quotes[qi][1] is None or quotes[qi][2] is None or quotes[qi][1]>=quotes[qi][2]:reason='invalid_or_stale_quote'
            elif ti<0 or target-trades[ti][0]>5000:reason='stale_trade'
            r[f'delta_mid_{h}s']=None
            r[f'outcome_issue_{h}s']=reason
            if reason is None:r[f'delta_mid_{h}s']=((quotes[qi][1]+quotes[qi][2])-r['midpoint_half_ticks'])/20
        return r
    labelled=[label(r) for r in onsets];spaced_labels=[label(r) for r in spaced];baseline_labels=[label(r) for r in baseline]
    complete=[r for r in spaced_labels if all(r[f'delta_mid_{h}s'] is not None for h in HORIZONS)]
    for suffix,rows in [('alerts',labelled),('spaced_5min_alerts',spaced_labels),('baseline',baseline_labels)]:
        fields=list(rows[0]) if rows else list(predictions[0])+[f'delta_mid_{h}s' for h in HORIZONS]+[f'outcome_issue_{h}s' for h in HORIZONS]
        with (out/(stem+'_'+suffix+'.csv')).open('w',newline='') as f:
            w=csv.DictWriter(f,fieldnames=fields);w.writeheader();w.writerows(rows)
    report={'day':DAY,'symbol':SYMBOL,'session_start':'13:00:00','session_end':clock(end),
      'config':asdict(config),'sampling_ms':1000,
      'method':{'windows':'BUY current (t-10s,t]; BUY baseline (t-40s,t-10s]; quantities exclude UNKNOWN',
       'signal':'R_buy <0.5; SellShare >65%; midpoint decrease <=-0.1 points; both Bid and Ask decrease vs t-10s',
       'outcome':'Change in BBO midpoint from alert time to last valid Quote at/before t+60/180/300s; endpoint Quote and Tape age <=5s',
       'onsets':'First second of each alert run; no future-based selection',
       'spaced':'First onset, then next onset >=300s later, selected before endpoint-quality checks',
       'baseline':'Eligible non-alert seconds on a fixed 5min grid starting 13:01; descriptive comparison only',
       'scope':'Afternoon only; no morning input carried across lunch; no tuning after afternoon results',
       'limits':'Single-session diagnostic. Earlier morning example motivated the thresholds. Source-time replay, missing receive clock/sequence. Midpoint changes are not executable PnL; no fees, latency or slippage model.'},
      'counts':{'sampled_seconds':len(predictions),'eligible_seconds':sum(r['eligible'] for r in predictions),
        'alert_seconds':sum(r['alert'] for r in predictions),'onsets':len(onsets),'spaced_5min_onsets':len(spaced),
        'spaced_complete_all_horizons':len(complete),'baseline_grid_samples':len(baseline)},
      'quality_issue_counts':dict(Counter(issue for r in predictions for issue in r['issues'].split('|') if issue)),
      'onset_outcomes':{str(h):summary(labelled,h) for h in HORIZONS},
      'spaced_5min_outcomes':{str(h):summary(spaced_labels,h) for h in HORIZONS},
      'spaced_common_cohort':{str(h):summary(complete,h) for h in HORIZONS},
      'baseline_outcomes':{str(h):summary(baseline_labels,h) for h in HORIZONS}}
    (out/(stem+'_summary.json')).write_text(json.dumps(report,ensure_ascii=False,indent=2))
    print(json.dumps(report,ensure_ascii=False,indent=2),flush=True)
    print('FIRST_ALERTS',json.dumps(labelled[:6],ensure_ascii=False),flush=True)

if __name__=='__main__':run()
