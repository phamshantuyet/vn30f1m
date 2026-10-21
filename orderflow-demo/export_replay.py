"""Compile recorded events into one-second UI patches; all volume/peak math in Python.
Includes all Quote observations when computing historical peaks, not just sampled books.
The displayed book and bars are snapshots as of each one-second boundary.
"""
import gzip
import json
import math
import sqlite3
from pathlib import Path
from replay_server import build_cache
from flow_assessment import FlowAssessment
from export_large_trades import export_large_trades

ROOT=Path(__file__).resolve().parent
TIMEFRAMES=(30,60,180,300,900)

def export(symbol='41I1GB000'):
    db=sqlite3.connect(build_cache(symbol))
    meta=json.loads(db.execute("SELECT value FROM metadata WHERE key='stats'").fetchone()[0])
    out=ROOT/'dist'/'replay-data';out.mkdir(exist_ok=True)
    assessor=FlowAssessment()
    first=(meta['start_ms']-meta['base_ms'])/1000
    bars=[None]*len(TIMEFRAMES)
    history={};book=[[],[]];quote_ts=None;price=None;volume={'BUY':0,'SELL':0,'UNKNOWN':0};trades=quotes=0
    rows=iter(db.execute('SELECT ts,kind,payload FROM events ORDER BY ts,kind,source_order'))
    pending=next(rows,None)
    records=0
    with gzip.open(out/'41I1GB000-latest.pending.gz','wt',compresslevel=6) as f:
        f.write('[')
        boundaries=[first]+list(range(math.floor(first)+1,math.ceil(meta['duration'])))+[meta['duration']]
        for second in boundaries:
            target=meta['base_ms']+round(second*1000)
            changed=set();changed_levels=[set() for _ in TIMEFRAMES]
            while pending is not None and pending[0]<=target:
                ts,kind,payload=pending;data=json.loads(payload);relative=(ts-meta['base_ms'])/1000
                if kind==0:
                    assessor.push_quote(relative,data)
                    book=data;quote_ts=ts;quotes+=1
                    for levels,side in zip(book,('bid','ask')):
                        for p,q in levels:
                            h=history.setdefault(p,{'price':p});entry=h.setdefault(side,{'max':q,'maxAt':relative})
                            if q>entry['max']:entry.update(max=q,maxAt=relative)
                            entry.update(last=q,lastAt=relative);changed.add(p)
                else:
                    price,qty,side,_=data;volume[side]+=qty;trades+=1
                    assessor.push_trade(relative,price,qty,side)
                    field={'BUY':'buy','SELL':'sell'}.get(side,'unknown')
                    for j,tf in enumerate(TIMEFRAMES):
                        bt=(ts//(tf*1000))*tf-meta['base_ms']//1000
                        b=bars[j]
                        if b is None or b['time']!=bt:
                            b=bars[j]={'time':bt,'buy':0,'sell':0,'unknown':0,'total':0,'levels':{},'open':price,'close':price,'high':price,'low':price}
                        b['close']=price;b['high']=max(b['high'],price);b['low']=min(b['low'],price)
                        b['total']+=qty;b[field]+=qty
                        l=b['levels'].setdefault(price,{'buy':0,'sell':0,'unknown':0});l[field]+=qty;changed_levels[j].add(price)
                pending=next(rows,None)
            serialized=[]
            for j,b in enumerate(bars):
                if b is None or not changed_levels[j]:
                    serialized.append(None);continue
                levels=[[p,b['levels'][p]['sell'],b['levels'][p]['buy'],b['levels'][p]['unknown']] for p in sorted(changed_levels[j])]
                poc=max(sorted(b['levels']),key=lambda p:sum(b['levels'][p].values()))
                serialized.append([b['time'],b['open'],b['high'],b['low'],b['close'],b['buy'],b['sell'],b['unknown'],poc,levels,b['total'],b['buy']-b['sell']])
            record=[second,price,book,quote_ts,volume['BUY'],volume['SELL'],volume['UNKNOWN'],trades,quotes,
                    [[p,history[p]] for p in sorted(changed)],serialized,sum(volume.values()),volume['BUY']-volume['SELL'],assessor.assess(second)]
            if records:f.write(',')
            json.dump(record,f,separators=(',',':'));records+=1
        f.write(']')
    (out/'41I1GB000-latest.pending.gz').replace(out/'41I1GB000-latest.json.gz')
    meta.update({'first_second':first,'timeframes':TIMEFRAMES,'records':records,'assessment_version':assessor.config.version,'resolution_seconds':1,'data_file':'41I1GB000-latest.json.gz'})
    (out/'meta.json').write_text(json.dumps(meta,separators=(',',':')))
    assert sum(volume.values())==meta['tape_volume']
    assert trades==meta['trades'] and quotes==meta['quotes']
    export_large_trades(symbol)
    print(json.dumps({'records':records,'compressed_bytes':(out/meta['data_file']).stat().st_size,'volume':sum(volume.values()),'trades':trades,'quotes':quotes,'prices':len(history)}),flush=True)

if __name__=='__main__':export()
