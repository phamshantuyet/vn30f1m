"""Export BUY/SELL matches so the UI can configure any nonnegative threshold."""
import json
import sqlite3
from pathlib import Path
from replay_server import build_cache

def export_large_trades(symbol='41I1GB000'):
    db=sqlite3.connect(build_cache(symbol))
    meta=json.loads(db.execute("SELECT value FROM metadata WHERE key='stats'").fetchone()[0])
    trades=[]
    for ts,payload in db.execute('SELECT ts,payload FROM events WHERE kind=1 ORDER BY ts,source_order'):
        price,qty,side,_=json.loads(payload)
        if qty>0 and side in ('BUY','SELL'):
            trades.append([(ts-meta['base_ms'])/1000,price,qty,side])
    result={'symbol':symbol,'day':meta['day'],'default_threshold':50,'minimum_threshold':0,'trades':trades}
    out=Path(__file__).resolve().parent/'dist'/'replay-data'/'large-trades.json'
    out.parent.mkdir(exist_ok=True)
    temp=out.with_suffix('.pending.json');temp.write_text(json.dumps(result,separators=(',',':')));temp.replace(out)
    print(f'Trade candidates: {len(trades)} rows; >50: {sum(t[2]>50 for t in trades)}',flush=True)
    return result

if __name__=='__main__':export_large_trades()
