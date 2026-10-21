"""Exploratory ablation after the frozen v1 afternoon experiment.
Uses causal cache-based BUY features and the previously verified raw oracle's
quality flags and outcomes. All variants share v1 quality + 120s warmup.
"""
import csv
import hashlib
import json
import sqlite3
import statistics
from pathlib import Path
from buy_speed import BuySpeed, WINDOWS

ROOT = Path(__file__).resolve().parent
STEM = 'buy_speed_afternoon_20261016'
OLD = ROOT/'reports/buy_exhaustion_afternoon_20261016_oracle.csv'
HORIZONS = (60,180,300)


def write_csv(path, rows):
    with path.open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader(); writer.writerows(rows)


def stats(rows, horizon):
    values = [float(r[f'delta_mid_{horizon}s']) for r in rows if r[f'delta_mid_{horizon}s'] != '']
    return {'n':len(values), 'lower_pct':100*sum(v<0 for v in values)/len(values) if values else None,
            'mean_points':statistics.mean(values) if values else None,
            'median_points':statistics.median(values) if values else None}


def run():
    with OLD.open() as f: oracle = list(csv.DictReader(f))
    start = int(oracle[0]['ts'])
    db = sqlite3.connect(ROOT/'.replay-cache/41I1GB000-2026-10-16.sqlite')
    buys = []
    for ts, payload in db.execute('SELECT ts,payload FROM events WHERE kind=1 AND ts>=? ORDER BY ts,source_order', (start,)):
        trade = json.loads(payload)
        if trade[2] == 'BUY': buys.append((ts, trade[1]))
    engine = BuySpeed(buys)
    predictions=[]
    for old in oracle:
        ts=int(old['ts']); eligible=old['eligible']=='1' and ts-start>=120000
        row={'ts':ts,'time':old['time'],'eligible':int(eligible)}
        for w in WINDOWS:
            features=engine.at(ts,w)
            row.update({f'w{w}_{key}':int(v) if isinstance(v,bool) else v for key,v in features.items()})
            row[f'volume_{w}']=int(eligible and features['volume_slow'])
            row[f'volume_frequency_{w}']=int(eligible and features['volume_slow'] and features['frequency_slow'])
        row['full_v1']=int(eligible and old['alert']=='1')
        predictions.append(row)
    out=ROOT/'reports';write_csv(out/(STEM+'_predictions.csv'),predictions)
    # Only now attach forward outcomes; no outcome-dependent event selection.
    variants=[f'{v}_{w}' for w in WINDOWS for v in ('volume','volume_frequency')]+['full_v1']
    alerts=[];spaced=[];report={}
    for name in variants:
        prev=False;selected=[];onsets=[];last=None
        for p,old in zip(predictions,oracle):
            flag=bool(p[name])
            if flag and not prev:
                row={'variant':name,**p,**{f'delta_mid_{h}s':old[f'delta_mid_{h}s'] for h in HORIZONS},
                     'midpoint_change_10s':old['midpoint_change_points'], 'sell_share_10s':old['sell_share']}
                onsets.append(row)
                if last is None or p['ts']-last>=300000:
                    selected.append(row);last=p['ts']
            prev=flag
        alerts.extend(onsets);spaced.extend(selected)
        complete=[r for r in selected if all(r[f'delta_mid_{h}s']!='' for h in HORIZONS)]
        report[name]={'alert_seconds':sum(p[name] for p in predictions),'onsets':len(onsets),
                      'spaced_onsets':len(selected),'complete_n':len(complete),
                      'onset_outcomes':{str(h):stats(onsets,h) for h in HORIZONS},
                      'spaced_outcomes':{str(h):stats(selected,h) for h in HORIZONS},
                      'complete_outcomes':{str(h):stats(complete,h) for h in HORIZONS},
                      'concurrent_mid_down_pct':100*sum(float(r['midpoint_change_10s'])<0 for r in onsets)/len(onsets) if onsets else None}
    write_csv(out/(STEM+'_alerts.csv'),alerts);write_csv(out/(STEM+'_spaced.csv'),spaced)
    summary={'day':'2026-10-16','symbol':'41I1GB000','sampled_seconds':len(predictions),
             'common_eligible_seconds':sum(r['eligible'] for r in predictions),
             'oracle_sha256':hashlib.sha256(OLD.read_bytes()).hexdigest(),
             'method':{
                'volume_ratio':'(BUY volume in (t-W,t]/W) / (BUY volume in (t-4W,t-W]/(3W))',
                'frequency_ratio':'Same calculation using number of BUY tape matches, not quantity',
                'threshold':'strictly <0.5; volume_frequency requires both ratios below 0.5',
                'quality':'Frozen v1 eligible AND >=120s since 13:00 for ALL variants, including full_v1. Full v1 therefore has a different start from previous report.',
                'zero_baseline':'No signal when baseline BUY quantity/count is zero; zero current BUY may signal.',
                'selection':'First signal after false; then first onset >=300 seconds after last selected onset, independently per variant and before forward labels. Tables use selected events with all 3 valid labels.',
                'outcome':'Previous independently verified raw oracle: midpoint(t+h)-midpoint(t); valid quote/trade <=5s at endpoint. Negative = lower. Not executable PnL.',
                'limitations':'Exploratory variants after viewing this afternoon; no blind holdout. Single day, few spaced samples, different entry times by variant, persistent slowdown may re-trigger. No optimized thresholds; no claim of best variant. Match count is feed prints, not original orders.'},
             'variants':report}
    (out/(STEM+'_summary.json')).write_text(json.dumps(summary,ensure_ascii=False,indent=2))
    lines=['# BUY volume and speed: afternoon 16/10/2026','',
           'Exploratory ablation on 41I1GB000. Every second from 13:00 through 14:45; shared v1 quality gates and 120s warmup.',
           '', 'For W = 5, 10, 30s: Rv = 3 × BUY quantity(t-W,t] / BUY quantity(t-4W,t-W]. Rn uses BUY match count instead. Volume signal: Rv < 0.5. Combined signal: Rv < 0.5 AND Rn < 0.5. Baseline must be positive.',
           '', 'Contracts/second is volume divided by time. Match frequency measures a separate component. Rv = Rn × (current mean match size / baseline mean match size) when counts are positive.',
           '', '| Variant | Complete spaced events | Lower 1m | Lower 3m | Lower 5m | Mean Δmid 5m |',
           '|---|---:|---:|---:|---:|---:|']
    for name,v in report.items():
        s=v['complete_outcomes']
        lines.append(f"| {name} | {v['complete_n']} | {s['60']['lower_pct']:.1f}% | {s['180']['lower_pct']:.1f}% | {s['300']['lower_pct']:.1f}% | {s['300']['mean_points']:+.3f} |")
    lines+=['', 'All onsets (overlapping future horizons; sensitivity check):', '', '| Variant | N at 5m | Lower 1m | Lower 3m | Lower 5m | Mean Δmid 5m |', '|---|---:|---:|---:|---:|---:|']
    for name,v in report.items():
        a=v['onset_outcomes']
        lines.append(f"| {name} | {a['300']['n']} | {a['60']['lower_pct']:.1f}% | {a['180']['lower_pct']:.1f}% | {a['300']['lower_pct']:.1f}% | {a['300']['mean_points']:+.3f} |")
    lines+=['', 'The apparent strength of volume_10 in the spaced sample does not persist across all onsets: 5m lower rate is 46.9% across 145 available onsets versus 66.7% across 15 spaced complete events. Event selection materially changes the result.', '', 'Events are first onsets spaced at least 5 minutes per variant, selected before checking future label availability. Different variants select different times, so this is descriptive, not a matched experiment.',
            '', 'The full v1 reference is recalculated with the common 120s warmup; it differs from the earlier 40s-warmup report. The existing full-v1 report remains unchanged.',
            '', 'These variants were proposed after seeing the afternoon. This session is not a new independent holdout. A fall in BUY volume or frequency is an observed slowdown, not proof of a future price decline. One session and few events cannot establish predictive reliability.',
            '', 'Features and decisions are saved before forward labels. Independent verification rereads raw Tape and uses SQL sums/counts, without importing buy_speed or using the cache. Quote quality and future labels reuse the raw oracle whose 6,301 samples were already independently checked.',
            '', 'Run: `python3 orderflow-demo/backtest_buy_speed.py`, then `python3 orderflow-demo/verify_buy_speed_raw.py`. See `_predictions.csv`, `_alerts.csv`, `_spaced.csv`, `_summary.json` and `_verification.json`.']
    (out/(STEM+'.md')).write_text('\n'.join(lines)+'\n')
    print(json.dumps({'common_eligible_seconds':summary['common_eligible_seconds'],'variants':report},indent=2))

if __name__=='__main__':run()
