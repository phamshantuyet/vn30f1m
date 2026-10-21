"""Independent oracle. Reads raw JSONL and uses SQL/Fraction, not the formula module.
Does NOT import buy_exhaustion, FlowAssessment, replay_server, cache adapters, or their config.
The literals below implement the documented frozen v1 contract independently.
"""
import csv
import hashlib
import json
import sqlite3
import tempfile
from datetime import datetime,timezone,timedelta
from decimal import Decimal
from fractions import Fraction
from pathlib import Path

ROOT=Path(__file__).resolve().parent
DAY='2026-10-16';SYMBOL='41I1GB000'
Z=timezone(timedelta(hours=7));EPOCH=datetime(1970,1,1,tzinfo=timezone.utc)

def timestamp(value):
    delta=datetime.fromisoformat(value).replace(tzinfo=Z)-EPOCH
    return delta.days*86400000+delta.seconds*1000+delta.microseconds//1000

def price_tick(value):
    scaled=Decimal(str(value))*Decimal('10')
    if scaled!=scaled.to_integral_value():raise ValueError('Off-tick price')
    return int(scaled)

def run():
    start=timestamp(DAY+'T13:00:00');end=0
    hashes={};source_counts={}
    with tempfile.TemporaryDirectory(prefix='independent_buy_oracle_') as temp:
        db=sqlite3.connect(Path(temp)/'raw.sqlite')
        db.execute('CREATE TABLE quote_raw(ts INTEGER,file_line INTEGER,bid INTEGER,ask INTEGER)')
        db.execute('CREATE TABLE tape(ts INTEGER,file_line INTEGER,price INTEGER,quantity INTEGER,side TEXT)')
        for kind,folder in [('quote','quotes'),('trade','trade_tape')]:
            file=ROOT.parent/'data'/folder/SYMBOL/(DAY.replace('-','_')+'_G1.jsonl')
            digest=hashlib.sha256();batch=[];count=0
            with file.open('rb') as raw:
                for line_number,line in enumerate(raw):
                    digest.update(line)
                    row=json.loads(line)
                    if row.get('symbol')!=SYMBOL or row.get('boardId')!='G1':raise ValueError('Unexpected contract/board')
                    # Date strings are checked before timestamps to avoid morning conversion work.
                    if row['time']<DAY+' 13:00:00':continue
                    ts=timestamp(row['time']);end=max(end,ts)
                    if kind=='quote':
                        bids=row.get('bid') or [];asks=row.get('offer') or []
                        batch.append((ts,line_number,price_tick(bids[0]['price']) if bids else None,
                                      price_tick(asks[0]['price']) if asks else None))
                    else:
                        side=row.get('side');side=side if side in ('BUY','SELL') else 'UNKNOWN'
                        if row['matchQtty']<=0:raise ValueError('Invalid quantity')
                        batch.append((ts,line_number,price_tick(row['matchPrice']),row['matchQtty'],side))
                    count+=1
                    if len(batch)>=10000:
                        db.executemany('INSERT INTO quote_raw VALUES (?,?,?,?)' if kind=='quote' else 'INSERT INTO tape VALUES (?,?,?,?,?)',batch);batch=[]
            db.executemany('INSERT INTO quote_raw VALUES (?,?,?,?)' if kind=='quote' else 'INSERT INTO tape VALUES (?,?,?,?,?)',batch)
            db.commit();hashes[str(file.relative_to(ROOT.parent))]=digest.hexdigest();source_counts[kind]=count
            print(f'Independent raw import {kind}: {count} afternoon rows',flush=True)
        db.execute('CREATE INDEX tape_time ON tape(ts)')
        db.execute('''CREATE TABLE quote_intervals AS
          SELECT ts,bid,ask,(bid IS NOT NULL AND ask IS NOT NULL AND bid<ask) AS valid,
                 LEAD(ts,1,?) OVER(ORDER BY ts) AS stop
          FROM (SELECT ts,bid,ask,ROW_NUMBER() OVER(PARTITION BY ts ORDER BY file_line ASC) AS pick FROM quote_raw)
          WHERE pick=1''',(end+1,))
        db.execute('CREATE UNIQUE INDEX quote_time ON quote_intervals(ts)')
        db.commit()
        first=db.execute('SELECT MIN(ts) FROM (SELECT MIN(ts) AS ts FROM tape UNION ALL SELECT MIN(ts) FROM quote_intervals)').fetchone()[0]
        def quote(at):
            return db.execute('SELECT ts,bid,ask,valid FROM quote_intervals WHERE ts<=? ORDER BY ts DESC LIMIT 1',(at,)).fetchone()
        def good(q,at):return q is not None and bool(q[3]) and 0<=at-q[0]<=5000
        results=[];previous=False
        for now in range(start,end+1,1000):
            cut=now-10000
            b,s,u,baseline,count=db.execute('''SELECT
                COALESCE(SUM(CASE WHEN ts>? AND side='BUY' THEN quantity ELSE 0 END),0),
                COALESCE(SUM(CASE WHEN ts>? AND side='SELL' THEN quantity ELSE 0 END),0),
                COALESCE(SUM(CASE WHEN ts>? AND side='UNKNOWN' THEN quantity ELSE 0 END),0),
                COALESCE(SUM(CASE WHEN ts<=? AND side='BUY' THEN quantity ELSE 0 END),0),
                COUNT(CASE WHEN ts>? THEN 1 END)
                FROM tape WHERE ts>? AND ts<=?''',(cut,cut,cut,cut,cut,now-40000,now)).fetchone()
            last=db.execute('SELECT MAX(ts) FROM tape WHERE ts<=?',(now,)).fetchone()[0]
            qn,qp=quote(now),quote(cut)
            covered=db.execute('''SELECT COALESCE(SUM(MAX(0,MIN(stop,ts+5000,?)-MAX(ts,?))),0)
                FROM quote_intervals WHERE valid=1 AND ts>=? AND ts<=?''',(now,cut,cut-5000,now)).fetchone()[0]
            issues=[]
            if first is None or now-first<40000:issues.append('warmup')
            if count<20:issues.append('few_trades')
            if b+s<50:issues.append('low_classified_volume')
            if b+s+u==0 or Fraction(b+s,b+s+u)<Fraction(9,10):issues.append('unknown_side')
            if baseline==0:issues.append('zero_buy_baseline')
            if last is None or now-last>5000:issues.append('stale_trade')
            if not good(qn,now):issues.append('quote_now')
            if not good(qp,cut):issues.append('quote_before')
            if covered<8000:issues.append('quote_coverage')
            ratio=Fraction(b,10)/Fraction(baseline,30) if baseline else None
            sell_share=Fraction(s,b+s) if b+s else None
            comparable=qn is not None and qp is not None and qn[3] and qp[3]
            bid_change=qn[1]-qp[1] if comparable else None
            ask_change=qn[2]-qp[2] if comparable else None
            mid_change=Fraction(bid_change+ask_change,20) if comparable else None
            conditions={
                'buy_rate_falls':ratio is not None and ratio<Fraction(1,2),
                'sell_dominates':sell_share is not None and sell_share>Fraction(65,100),
                'midpoint_falls':mid_change is not None and mid_change<=Fraction(-1,10),
                'both_quotes_fall':bool(comparable and bid_change<0 and ask_change<0)}
            alert=not issues and all(conditions.values())
            row={'ts':now,'time':datetime.fromtimestamp(now/1000,Z).strftime('%H:%M:%S'),
                 'eligible':int(not issues),'alert':int(alert),'onset':int(alert and not previous),
                 'status':'INSUFFICIENT_DATA' if issues else 'BUY_WEAKENING' if alert else 'NO_SIGNAL',
                 'issues':'|'.join(sorted(issues)), 'buy_10s':b,'sell_10s':s,'unknown_10s':u,
                 'baseline_buy_30s':baseline,'trade_count_10s':count,'covered_ms':covered,
                 'buy_rate':float(Fraction(b,10)),'baseline_buy_rate':float(Fraction(baseline,30)),
                 'buy_ratio':float(ratio) if ratio is not None else None,
                 'sell_share':float(sell_share) if sell_share is not None else None,
                 'midpoint_change_points':float(mid_change) if mid_change is not None else None,
                 'bid_change_points':float(Fraction(bid_change,10)) if comparable else None,
                 'ask_change_points':float(Fraction(ask_change,10)) if comparable else None,
                 'midpoint_half_ticks':qn[1]+qn[2] if qn is not None and qn[3] else None}
            row.update({k:int(v) for k,v in conditions.items()})
            previous=alert
            # Independent future labels, kept separate from every decision calculation above.
            for h in (60,180,300):
                target=now+h*1000;future=quote(target)
                future_trade=db.execute('SELECT MAX(ts) FROM tape WHERE ts<=?',(target,)).fetchone()[0]
                usable=(target<=end and good(future,target) and future_trade is not None and target-future_trade<=5000
                        and row['midpoint_half_ticks'] is not None)
                row[f'delta_mid_{h}s']=float(Fraction(future[1]+future[2]-row['midpoint_half_ticks'],20)) if usable else None
            results.append(row)
        out=ROOT/'reports';out.mkdir(exist_ok=True)
        stem='buy_exhaustion_afternoon_20261016_oracle'
        with (out/(stem+'.csv')).open('w',newline='') as f:
            w=csv.DictWriter(f,fieldnames=list(results[0]));w.writeheader();w.writerows(results)
        manifest={'method':'Raw JSONL → independent SQLite interval SQL and exact Fraction arithmetic; no imports from production formula or adapter',
          'source_sha256':hashes,'raw_afternoon_rows':source_counts,'samples':len(results),
          'alert_seconds':sum(r['alert'] for r in results),'onsets':sum(r['onset'] for r in results)}
        (out/(stem+'_manifest.json')).write_text(json.dumps(manifest,ensure_ascii=False,indent=2))
        print(json.dumps(manifest,ensure_ascii=False,indent=2),flush=True)

if __name__=='__main__':run()
