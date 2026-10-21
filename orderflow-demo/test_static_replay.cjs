const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const zlib=require('node:zlib');
const Store=require('./dist/replay-static.js');
const dir=path.join(__dirname,'dist/replay-data');
const meta=JSON.parse(fs.readFileSync(path.join(dir,'meta.json')));
const records=JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(dir,meta.data_file))));
const large=JSON.parse(fs.readFileSync(path.join(dir,'large-trades.json'))).trades;
assert(large.every(t=>t[2]>0&&['BUY','SELL'].includes(t[3])));
const store=new Store(meta,records,large);
for(const tf of meta.timeframes){
 const end=store.snapshot(meta.duration,tf,3600);
 assert.deepEqual(end.large_trades,large.filter(t=>t[2]>50));
 assert.equal(end.frame.volume,meta.tape_volume);
 assert.equal(end.frame.tradeCount,meta.trades);
 assert.equal(end.quotesProcessed,meta.quotes);
 assert.equal(end.bars.reduce((s,b)=>s+b.total,0),44573);
 for(const bar of end.bars){
  assert.equal(bar.delta,bar.buy-bar.sell);
  assert.equal(bar.total,[...bar.levels.values()].reduce((s,l)=>s+l.buy+l.sell+l.unknown,0));
 }
 const first=store.snapshot(meta.first_second,tf,600);
 assert.deepEqual(first.large_trades,[]);
 assert.equal(first.frame.tradeCount,0);
 assert.equal(first.bars.length,0);
 assert(first.history.length<10);
 assert(first.history.every(([p,e])=>['bid','ask'].every(side=>!e[side]||e[side].lastAt<=meta.first_second)));
}
for(const t of [900.163,900.9,901,1800]){const state=store.snapshot(t,60,600);assert.deepEqual(state.large_trades,large.filter(r=>r[0]<=state.large_trades_as_of&&r[2]>50));}
console.log('PASS: tape totals, all 5 candle sizes, per-level reconciliation, final-hour volume, rewind excludes future trades and peaks');

for(const threshold of [0,20,50,100,200,1000000]){const state=store.snapshot(meta.duration,60,600,threshold);assert.deepEqual(state.large_trades,large.filter(t=>t[2]>threshold));assert.equal(state.large_threshold,threshold);const early=store.snapshot(901,60,600,threshold);assert(early.large_trades.every(t=>t[0]<=901&&t[2]>threshold));}
assert.throws(()=>store.snapshot(1000,60,600,-1));console.log('PASS: configurable thresholds including 0, lower than 50, equality exclusion, empty results, and replay cutoffs');

assert(!store.snapshot(9899.999,60,600).sessionBreak); // 11:29:59.999
const morning=store.snapshot(9900,60,600); // 11:30 local
assert(morning.sessionBreak);
const noon=store.snapshot(11700,60,600);
assert(noon.sessionBreak&&noon.bookHeld&&!noon.frame.gap);
assert(noon.quoteAge>30);assert(noon.bars.length>0);assert(noon.end<11700);
const late=store.snapshot(15299,60,600);
assert(late.bookHeld);assert.deepEqual(late.frame.bids,noon.frame.bids);assert.deepEqual(late.frame.asks,noon.frame.asks);
assert.equal(late.frame.tradeCount,noon.frame.tradeCount);assert.equal(late.end,noon.end);
const reopen=store.snapshot(15300,60,600);
assert(!reopen.sessionBreak&&!reopen.bookHeld);
console.log('PASS: lunch retains book and chart with unchanged trades; 13:00 restores normal freshness rules');
