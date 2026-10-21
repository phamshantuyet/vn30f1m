"""Independent raw-Tape SQL oracle, intentionally no formula/cache imports.
Hardcoded experiment contract, independent timestamp parser and SQL range sums.
Reuses the previous raw Quote oracle only for eligibility and forward prices.
"""
import csv
import hashlib
import json
import math
import sqlite3
import statistics
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT=Path(__file__).resolve().parent
STEM='buy_speed_afternoon_20261016'


def verify():
    raw=ROOT.parent/'data/trade_tape/41I1GB000/2026_10_16_G1.jsonl'
    manifest=json.loads((ROOT/'reports/buy_exhaustion_afternoon_20261016_oracle_manifest.json').read_text())
    db=sqlite3.connect(':memory:')
    db.execute('CREATE TABLE buys(ts INTEGER, quantity INTEGER)')
    digest=hashlib.sha256();buys=[]
    start=int(datetime(2026,10,16,13,tzinfo=timezone(timedelta(hours=7))).timestamp()*1000)
    with raw.open('rb') as f:
        for line in f:
            digest.update(line);record=json.loads(line)
            if record['side']!='BUY':continue
            dt=datetime.strptime(record['time'],'%Y-%m-%d %H:%M:%S.%f').replace(tzinfo=timezone(timedelta(hours=7)))
            ts=int(dt.timestamp()*1000)
            if ts>=start:buys.append((ts,int(record['matchQtty'])))
    db.executemany('INSERT INTO buys VALUES (?,?)',buys)
    db.execute('CREATE INDEX buys_ts ON buys(ts)')
    with (ROOT/'reports'/f'{STEM}_predictions.csv').open() as f:pred=list(csv.DictReader(f))
    oracle_path=ROOT/'reports/buy_exhaustion_afternoon_20261016_oracle.csv'
    with oracle_path.open() as f:oracle=list(csv.DictReader(f))
    summary=json.loads((ROOT/'reports'/f'{STEM}_summary.json').read_text())
    errors=[];checks=0
    def check(actual,expected,label):
        nonlocal checks
        checks+=1
        if expected is None:ok=actual==''
        elif isinstance(expected,bool):ok=(actual if isinstance(actual,bool) else bool(int(actual)))==expected
        elif isinstance(expected,float):ok=actual!='' and math.isclose(float(actual),expected,abs_tol=1e-10,rel_tol=1e-10)
        else:ok=str(actual)==str(int(expected))
        if not ok and len(errors)<30:errors.append({'field':label,'actual':actual,'expected':expected})
    if len(pred)!=len(oracle):errors.append({'row_counts':[len(pred),len(oracle)]})
    independent=[]
    for row,old in zip(pred,oracle):
        ts=int(old['ts']);check(row['ts'],ts,'timestamp')
        gate=old['eligible']=='1' and ts>=start+120000
        check(row['eligible'],gate,f'{ts}:eligible')
        check(row['full_v1'],gate and old['alert']=='1',f'{ts}:full_v1')
        signals={'ts':ts,'full_v1':gate and old['alert']=='1'}
        for w in (5,10,30):
            boundary=ts-w*1000
            q,base,n,nbase=db.execute('''SELECT
              COALESCE(SUM(CASE WHEN ts>? THEN quantity ELSE 0 END),0),
              COALESCE(SUM(CASE WHEN ts<=? THEN quantity ELSE 0 END),0),
              COALESCE(SUM(CASE WHEN ts>? THEN 1 ELSE 0 END),0),
              COALESCE(SUM(CASE WHEN ts<=? THEN 1 ELSE 0 END),0)
              FROM buys WHERE ts>? AND ts<=?''',
              (boundary,boundary,boundary,boundary,ts-4000*w,ts)).fetchone()
            slow=base>0 and 6*q<base;freq=nbase>0 and 6*n<nbase
            expected={'buy_volume':q,'baseline_volume':base,'buy_count':n,'baseline_count':nbase,
                      'volume_ratio':3*q/base if base else None,'frequency_ratio':3*n/nbase if nbase else None,
                      'average_size':q/n if n else None,'baseline_average_size':base/nbase if nbase else None,
                      'volume_slow':slow,'frequency_slow':freq,'buy_contracts_per_second':q/w,
                      'baseline_contracts_per_second':base/(3*w),'buy_matches_per_second':n/w,
                      'baseline_matches_per_second':nbase/(3*w)}
            for key,value in expected.items():check(row[f'w{w}_{key}'],value,f'{ts}:w{w}_{key}')
            signals[f'volume_{w}']=gate and slow
            signals[f'volume_frequency_{w}']=gate and slow and freq
            check(row[f'volume_{w}'],gate and slow,f'{ts}:volume_{w}')
            check(row[f'volume_frequency_{w}'],gate and slow and freq,f'{ts}:volume_frequency_{w}')
        independent.append(signals)
    # Verify event selection and summary directly from independently derived signals.
    for name, reported in summary['variants'].items():
        prev=False;onsets=[];selected=[];last=None
        for row in independent:
            if row[name] and not prev:
                onsets.append(row['ts'])
                if last is None or row['ts']-last>=300000:
                    selected.append(row['ts']);last=row['ts']
            prev=row[name]
        by_ts={int(r['ts']):r for r in oracle}
        complete=[t for t in selected if all(by_ts[t][f'delta_mid_{h}s']!='' for h in (60,180,300))]
        for field,value in {'onsets':len(onsets),'spaced_onsets':len(selected),'complete_n':len(complete),'alert_seconds':sum(r[name] for r in independent)}.items():
            check(reported[field],value,name+':'+field)
        for suffix,expected_times in (('alerts',onsets),('spaced',selected)):
            with (ROOT/'reports'/f'{STEM}_{suffix}.csv').open() as f:
                actual_times=[int(r['ts']) for r in csv.DictReader(f) if r['variant']==name]
            check(actual_times==expected_times,True,name+':'+suffix+'_selection')
        for label,times in (('onset_outcomes',onsets),('spaced_outcomes',selected),('complete_outcomes',complete)):
            for h in (60,180,300):
                values=[float(by_ts[t][f'delta_mid_{h}s']) for t in times if by_ts[t][f'delta_mid_{h}s']!='']
                expected={'n':len(values),'lower_pct':100*sum(v<0 for v in values)/len(values),'mean_points':statistics.mean(values),'median_points':statistics.median(values)}
                for key,value in expected.items():check(reported[label][str(h)][key],value,name+':'+label+':'+str(h)+':'+key)
    # All output labels must match the independent existing raw Quote oracle.
    old_by_ts={r['ts']:r for r in oracle}
    for suffix in ('alerts','spaced'):
        with (ROOT/'reports'/f'{STEM}_{suffix}.csv').open() as f:
            for row in csv.DictReader(f):
                old=old_by_ts[row['ts']]
                for h in (60,180,300):
                    checks+=1
                    key=f'delta_mid_{h}s'
                    if row[key]!=old[key]:errors.append({'label_mismatch':row['ts'],'horizon':h})
    check(summary['oracle_sha256']==hashlib.sha256(oracle_path.read_bytes()).hexdigest(),True,'oracle hash')
    # Manifest layout can differ; record source hash explicitly for review.
    result={'rows':len(pred),'checks':checks,'mismatches':len(errors),'first_errors':errors,
            'raw_buy_matches':len(buys),'raw_tape_sha256':digest.hexdigest(),
            'method':'Raw Tape JSONL -> independent datetime parser -> in-memory SQL SUM/COUNT. No new formula/cache imports. Previous raw Quote oracle supplies quality flags and forward midpoint labels.',
            'raw_manifest_tape_hash_present':digest.hexdigest() in json.dumps(manifest)}
    if not result['raw_manifest_tape_hash_present']:raise AssertionError('Tape hash differs from prior independent raw oracle')
    (ROOT/'reports'/f'{STEM}_verification.json').write_text(json.dumps(result,indent=2))
    print(json.dumps(result,indent=2))
    if errors:raise AssertionError('Independent verification mismatch')

if __name__=='__main__':verify()
