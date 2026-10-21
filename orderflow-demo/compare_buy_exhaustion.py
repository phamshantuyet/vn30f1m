"""Compare independent saved outputs; imports neither formula nor oracle implementation."""
import csv
import json
import math
from pathlib import Path

ROOT=Path(__file__).resolve().parent
PREFIX='buy_exhaustion_afternoon_20261016'
NUMERIC_FIELDS=('ts','eligible','alert','onset','buy_10s','sell_10s','unknown_10s',
 'baseline_buy_30s','trade_count_10s','covered_ms','buy_rate','baseline_buy_rate',
 'buy_ratio','sell_share','midpoint_change_points','bid_change_points','ask_change_points',
 'midpoint_half_ticks','buy_rate_falls','sell_dominates','midpoint_falls','both_quotes_fall')
TEXT_FIELDS=('time','status','issues')

def same_number(a,b):
    if a in ('',None) or b in ('',None):return a in ('',None) and b in ('',None)
    return math.isclose(float(a),float(b),rel_tol=0,abs_tol=1e-12)

def compare_prediction_rows(actual,expected):
    errors=[]
    if len(actual)!=len(expected):errors.append({'field':'row_count','actual':len(actual),'expected':len(expected)})
    if len({r['ts'] for r in actual})!=len(actual):errors.append({'field':'duplicate_timestamp'})
    for a,e in zip(actual,expected):
        for field in NUMERIC_FIELDS:
            if field not in a or field not in e or not same_number(a.get(field),e.get(field)):
                errors.append({'ts':e['ts'],'field':field,'actual':a.get(field),'expected':e.get(field)})
        for field in TEXT_FIELDS:
            if a.get(field)!=e.get(field):errors.append({'ts':e['ts'],'field':field,'actual':a.get(field),'expected':e.get(field)})
    return errors

def run():
    directory=ROOT/'reports'
    def read(suffix):
        with (directory/(PREFIX+suffix)).open() as f:return list(csv.DictReader(f))
    actual=read('_predictions.csv');oracle=read('_oracle.csv')
    errors=compare_prediction_rows(actual,oracle)
    lookup={r['ts']:r for r in oracle};alerts=read('_alerts.csv')
    expected_alerts={r['ts'] for r in oracle if r['onset']=='1'}
    if {r['ts'] for r in alerts}!=expected_alerts:errors.append({'field':'onset_timestamps'})
    outcome_comparisons=0
    for a in alerts:
        e=lookup[a['ts']]
        for h in (60,180,300):
            key=f'delta_mid_{h}s';outcome_comparisons+=1
            if not same_number(a[key],e[key]):errors.append({'ts':a['ts'],'field':key,'actual':a[key],'expected':e[key]})
    summary=json.loads((directory/(PREFIX+'_summary.json')).read_text())
    fixed={'current_ms':10000,'baseline_ms':30000,'buy_ratio_numerator':1,'buy_ratio_denominator':2,
      'sell_share_percent':65,'midpoint_drop_half_ticks':2,'min_trades':20,'min_classified_volume':50,
      'min_classified_percent':90,'min_quote_coverage_percent':80,'quote_hold_ms':5000,'max_trade_age_ms':5000,
      'version':'buy-exhaustion-v1'}
    if summary['config']!=fixed:errors.append({'field':'frozen_config','actual':summary['config'],'expected':fixed})
    oracle_onsets=[r for r in oracle if r['onset']=='1']
    spaced=[];previous=None
    for row in oracle_onsets:
        ts=int(row['ts'])
        if previous is None or ts-previous>=300000:spaced.append(row);previous=ts
    for key,rows in [('onset_outcomes',oracle_onsets),('spaced_5min_outcomes',spaced)]:
        for h in (60,180,300):
            vals=[float(r[f'delta_mid_{h}s']) for r in rows if r[f'delta_mid_{h}s']!='']
            saved=summary[key][str(h)]
            counts={'n':len(vals),'lower':sum(v<0 for v in vals),'flat':sum(v==0 for v in vals),'higher':sum(v>0 for v in vals)}
            for k,v in counts.items():
                if saved[k]!=v:errors.append({'field':f'{key}.{h}.{k}','actual':saved[k],'expected':v})
            if vals and not math.isclose(saved['mean_points'],sum(vals)/len(vals),rel_tol=0,abs_tol=1e-12):errors.append({'field':f'{key}.{h}.mean_points'})
    result={'prediction_rows':len(actual),'independent_oracle_rows':len(oracle),
      'fields_compared_per_row':len(NUMERIC_FIELDS)+len(TEXT_FIELDS),'alert_onsets':len(alerts),
      'forward_labels_compared':outcome_comparisons,'config_matches_frozen_v1':summary['config']==fixed,
      'mismatch_count':len(errors),'mismatches':errors[:20],'passed':not errors}
    (directory/(PREFIX+'_verification.json')).write_text(json.dumps(result,ensure_ascii=False,indent=2))
    print(json.dumps(result,ensure_ascii=False,indent=2))
    if errors:raise SystemExit(1)

if __name__=='__main__':run()
