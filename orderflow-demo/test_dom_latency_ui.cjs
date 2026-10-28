const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const html=fs.readFileSync(__dirname+'/dist/dom-latency.html','utf8');
const code=html.match(/<script>([\s\S]*?)<\/script>/)[1];
const elements=new Map(),sockets=[],frames=[],basisRequests=[];
const element=id=>{if(!elements.has(id))elements.set(id,{textContent:'',innerHTML:'',scrollTop:0});return elements.get(id);};
const bookRow=p=>element('book').innerHTML.match(new RegExp(`<tr[^>]*data-price="${p}"[^>]*>[\\s\\S]*?<\\/tr>`))?.[0]||'';
const sandbox={
 document:{getElementById:element},location:{protocol:'http:',host:'localhost:8097'},
 window:{addEventListener(){}},performance:{now:()=>1},Date,Intl,JSON,Math,Number,String,
 setTimeout(){},requestAnimationFrame(callback){frames.push(callback);},
 fetch(url){
  basisRequests.push(url);
  const day=new URL(url,'http://localhost').searchParams.get('day');
  const start=Date.parse(day+'T09:00:00+07:00');
  return Promise.resolve({ok:true,json:()=>Promise.resolve({day,points:[
   [start,'market',4.5],[start+5000,'market',5.25],[start+10000,'market',-8],
   [start+5000,'estimated',3.75]]})});
 },
 WebSocket:class{constructor(url){this.url=url;sockets.push(this);}close(){}emit(data){this.onmessage({data:JSON.stringify(data)});}}
};
vm.runInNewContext(code,sandbox);
assert.equal(sockets.length,3);
assert.equal(sockets[0].url,'ws://localhost:8097/api/ws/raw');
assert.equal(sockets[1].url,'ws://localhost:8097/api/ws/influence');
assert.equal(sockets[2].url,'ws://localhost:8097/api/ws/index');
assert.equal(element('impact-badge').textContent,'ĐANG CHỜ DNSE');
const historical=Date.now()-200;
sockets[0].emit({type:'hello',symbol:'41I1GB000',day:'2026-10-19',status:'connected',
 book:[[[18000,500]],[[18001,400]]],last_price:18000,reference_price:1799.5,
 trade_stats_source_ms:historical,trade_stats:{totalVolumeTraded:123,grossTradeAmount:4567.25,avgPrice:1800.4,
  highestPrice:1801.2,lowestPrice:1799.8,openPrice:1800},
 max_bids:[[17280,300,historical]],max_asks:[[19600,400,historical]]});
assert.equal(element('trade-total-volume').textContent,'123');
assert.equal(element('trade-gross-amount').textContent,'4.567,25');
assert.equal(element('trade-avg-price').textContent,'1.800,4');
assert.equal(element('trade-high-price').textContent,'1.801,2');
assert.equal(element('trade-low-price').textContent,'1.799,8');
assert.equal(element('trade-open-price').textContent,'1.800');
assert.equal(element('change-reference').textContent,'1.799,5');
assert.equal(element('change-vs-reference').textContent,'+0,5 điểm (+0,03%)');
assert.equal(element('change-vs-open').textContent,'0,0 điểm (0,00%)');
sockets[2].emit({type:'hello',day:'2026-10-19',status:'connected',
 market:{kind:'market',received_ms:historical,source_ms:historical,value:1795.25,
  change:5.25,change_pct:.29,prior:1790,high:1800,low:1780,up:12,down:15,flat:3,
  volume:1000000,amount:100.5},
 estimated:{kind:'estimated',received_ms:historical,source_ms:historical,value:1796,
  change:6,change_pct:.34,up:13,down:14,flat:3,volume:1005000,amount:101.2}});
assert.equal(element('index-market-value').textContent,'1.795,25');
assert.equal(element('index-estimated-value').textContent,'1.796,00');
assert.equal(element('index-market-range').textContent,'1.790,00 / 1.800,00 / 1.780,00');
assert.match(html,/<span class="box-number">11<\/span>Số mã tăng \/ giảm \/ đứng giá/);
assert.match(element('breadth-market-bar').innerHTML,/class="breadth-segment up" style="width:40%"/);
assert.match(element('breadth-market-counts').innerHTML,/Tăng: 12/);
assert.match(element('breadth-market-counts').innerHTML,/Giảm: 15/);
assert.match(element('breadth-estimated-counts').innerHTML,/Đứng giá: 3/);
assert.equal(element('index-basis-market').textContent,'+4,75');
assert.equal(element('index-basis-estimated').textContent,'+4,00');
assert.match(element('book').innerHTML,/data-price="17280"/);
assert.match(element('book').innerHTML,/data-price="19600"/);
assert.match(element('book').innerHTML,/class="price-gap"/);
assert.match(element('book-range').textContent,/4 mức giá Quote/);
element('book-wrap').scrollTop=123;
const received=Date.now()-100;
sockets[0].emit({type:'events',sent_ms:received+30,lost:0,events:[
 {kind:'quote',source_ms:received-20,received_ms:received,projected_ms:received+2,
  bids:[[18000,600]],asks:[[18001,400]]},
 {kind:'quote',source_ms:received-19,received_ms:received,projected_ms:received+2,
  bids:[[18000,450]],asks:[[18001,350]]},
 {kind:'trade',source_ms:received,received_ms:received+1,projected_ms:received+3,
  price:18001,qty:70,side:'BUY',trade_stats:{totalVolumeTraded:193,
   grossTradeAmount:6000,avgPrice:1800.5,highestPrice:1802,
   lowestPrice:1799.8,openPrice:1800}}
]});
assert.equal(frames.length,1);
frames.shift()();
assert.equal(element('book-wrap').scrollTop,123);
assert.equal(element('counts').textContent,'2 / 1');
assert.equal(element('project-lag').textContent,'2 ms');
assert.equal(element('server-lag').textContent,'28 ms');
assert.match(element('book').innerHTML,/data-price="18000"[^>]*><td class="peak">600<\/td><td class="bid"[^>]*>450<\/td>/);
assert.match(element('book').innerHTML,/class="best-bid/);
assert.match(element('book').innerHTML,/class="best-ask/);
assert.equal(element('bid-depth').textContent,'450');
assert.equal(element('ask-depth').textContent,'350');
assert.equal(element('spread').textContent,'0.1');
assert.equal(element('quote-burst').textContent,'2');
assert.equal(element('last-price').textContent,'1800.1');
assert.equal(element('change-vs-reference').textContent,'+0,6 điểm (+0,03%)');
assert.equal(element('change-vs-open').textContent,'+0,1 điểm (+0,01%)');
assert.equal(element('trade-total-volume').textContent,'193');
assert.equal(element('trade-gross-amount').textContent,'6.000');
assert.equal(element('trade-avg-price').textContent,'1.800,5');
assert.equal(element('index-future-last').textContent,'1.800,1');
assert.equal(element('index-basis-market').textContent,'+4,85');
assert.equal(element('index-basis-estimated').textContent,'+4,10');
sockets[2].emit({type:'events',events:[{kind:'estimated',received_ms:historical+1,
 source_ms:historical+1,value:1801,change:11,change_pct:.61,up:16,down:11,flat:3}]});
assert.equal(element('index-basis-estimated').textContent,'-0,90');
assert.match(element('breadth-estimated-bar').innerHTML,/class="breadth-segment up" style="width:53.333333333333336%"/);
assert.match(element('breadth-estimated-counts').innerHTML,/Tăng: 16/);
assert.match(element('log').innerHTML,/<td>2<\/td><td>1<\/td>/);
sockets[0].emit({type:'events',sent_ms:received+35,lost:0,events:[
 {kind:'reference',source_ms:received+5,received_ms:received+5,price:1801}
]});
assert.equal(element('change-vs-reference').textContent,'-0,9 điểm (-0,05%)');
assert.equal(element('change-vs-reference').className,'change-down');
const candleMinute=Date.parse('2026-10-19T02:15:00Z');
const sendCandle=(volume,closed,updated_ms=candleMinute+30000,minute_ms=candleMinute)=>
 sockets[0].emit({type:'events',sent_ms:received+35,lost:0,events:[
  {kind:'ohlcv',source_ms:minute_ms,received_ms:updated_ms,
   candle:{minute_ms,updated_ms,received_ms:updated_ms,open:1800,high:1801,
           low:1799,close:1800.5,volume,closed}}
 ]});
sendCandle(1500,false);
assert.equal(element('ohlcv-alert').className,'ohlcv-alert');
sendCandle(1501,false,candleMinute+35000);
assert.equal(element('ohlcv-alert').className,'ohlcv-alert is-warning');
assert.equal(element('ohlcv-volume').textContent,'1.501 HĐ');
assert.match(element('ohlcv-minute').textContent,/09:15.*chưa đóng/);
sendCandle(1700,true,candleMinute+60000);
assert.equal(element('ohlcv-alert').className,'ohlcv-alert');
assert.match(element('ohlcv-state').textContent,/Nến đã đóng/);
sendCandle(2000,false,candleMinute+65000);
assert.equal(element('ohlcv-alert').className,'ohlcv-alert');
sendCandle(100,false,candleMinute+70000,candleMinute+60000);
assert.equal(element('ohlcv-alert').className,'ohlcv-alert');
assert.match(element('ohlcv-minute').textContent,/09:16.*chưa đóng/);
for(const qty of [700,650]){
 sockets[0].emit({type:'events',sent_ms:received+40,lost:0,events:[
  {kind:'quote',source_ms:received+10,received_ms:received+10,projected_ms:received+11,
   bids:[[18000,qty]],asks:[[18001,370]]}
 ]});
}
assert.equal(frames.length,1);
frames.shift()();
assert.match(element('book').innerHTML,/data-price="18000"[^>]*><td class="peak">700<\/td><td class="bid"[^>]*>650<\/td>/);
// Once the top-of-book moves away, retain the last observed size in grey.
sockets[0].emit({type:'events',sent_ms:received+50,lost:0,events:[
 {kind:'quote',source_ms:received+20,received_ms:received+20,projected_ms:received+21,
  bids:[[18030,900]],asks:[[18031,300]]}
]});
frames.shift()();
assert.match(element('book').innerHTML,/data-price="18000"[^>]*><td class="peak">700<\/td><td class="bid last-seen"[^>]*>650<\/td>/);
assert.match(bookRow(18001),/<td class="ask last-seen"[^>]*>370<\/td>/);
assert.equal(element('bid-depth').textContent,'900');
assert.equal(element('ask-depth').textContent,'300');
// A fresh page gets those last observations from the server hello payload.
sockets[0].emit({type:'hello',symbol:'41I1GB000',day:'2026-10-19',status:'connected',
 book:[[[18030,900]],[[18031,300]]],last_price:18001,reference_price:1799.5,
 ohlcv_latest:{minute_ms:candleMinute,updated_ms:candleMinute+60000,volume:1700,closed:true},
 book_source_ms:received+20,
 max_bids:[[18000,700,received+10],[18030,900,received+20]],
 max_asks:[[18001,400,received],[18031,300,received+20]],
 last_bids:[[18000,650,received+10],[18030,900,received+20]],
 last_asks:[[18001,370,received+10],[18031,300,received+20]],
 trade_stats_source_ms:received,
 trade_stats:{totalVolumeTraded:193,grossTradeAmount:6000,avgPrice:1800.5,
  highestPrice:1802,lowestPrice:1799.8,openPrice:1800}});
assert.match(element('book').innerHTML,/data-price="18000"[^>]*><td class="peak">700<\/td><td class="bid last-seen"[^>]*>650<\/td>/);
assert.equal(element('trade-total-volume').textContent,'193');
assert.equal(element('trade-avg-price').textContent,'1.800,5');
assert.equal(element('ohlcv-alert').className,'ohlcv-alert');
assert.equal(element('ohlcv-volume').textContent,'1.700 HĐ');
// A delayed Trade with older source time must not roll the statistics back.
sockets[0].emit({type:'events',sent_ms:received+60,lost:0,events:[
 {kind:'trade',source_ms:received-1,received_ms:received+50,projected_ms:received+51,
  price:18001,qty:1,side:'BUY',trade_stats:{totalVolumeTraded:1,
   grossTradeAmount:1,avgPrice:1,highestPrice:1,lowestPrice:1,openPrice:1}}
]});
assert.equal(element('trade-total-volume').textContent,'193');
assert.equal(element('trade-avg-price').textContent,'1.800,5');
assert.equal(frames.length,1);
frames.shift()();
const influenceAt=Date.parse('2026-10-19T02:15:00Z');
sockets[1].emit({type:'hello',day:'2026-10-19',status:'connected',cursor:0,
 latest:{received_ms:influenceAt,items:[['HPG',-.4],['VCB',1.25]]}});
assert.equal(element('impact-badge').textContent,'DNSE VN30/1D · REALTIME');
assert.match(element('impact-map').innerHTML,/data-ticker="VCB"[^>]*data-impact="\+1.25"/);
assert.match(element('impact-map').innerHTML,/data-ticker="HPG"[^>]*data-impact="-0.40"/);
assert.match(html,/<span class="box-number">8<\/span>100% Stacked Area Chart/);
assert.match(element('impact-area').innerHTML,/data-series="VCB:up"/);
assert.match(element('impact-area').innerHTML,/data-series="HPG:down"/);
assert.match(element('impact-area').innerHTML,/data-ticker="VCB"[^>]*data-impact="\+1.25"[^>]*data-share="75.8"/);
assert.match(element('impact-area').innerHTML,/data-ticker="HPG"[^>]*data-impact="-0.40"[^>]*data-share="24.2"/);
assert.match(html,/<span class="box-number">10<\/span>Index Contribution Chart/);
assert.match(element('contribution-chart').innerHTML,/class="contribution-bar up"[^>]*data-ticker="VCB"[^>]*data-contribution="\+1.25"/);
assert.match(element('contribution-chart').innerHTML,/class="contribution-bar down"[^>]*data-ticker="HPG"[^>]*data-contribution="-0.40"/);
const positiveBar=element('contribution-chart').innerHTML.match(/<rect class="contribution-bar up" x="([\d.]+)"[^>]*data-ticker="VCB"/);
const negativeBar=element('contribution-chart').innerHTML.match(/<rect class="contribution-bar down" x="([\d.]+)"[^>]*data-ticker="HPG"/);
assert.ok(Number(positiveBar[1])>=520);
assert.ok(Number(negativeBar[1])<520);
assert.match(element('contribution-time').textContent,/09:15:00/);
assert.equal(element('contribution-up-total').textContent,'+1.25 điểm');
assert.equal(element('contribution-down-total').textContent,'-0.40 điểm');
sockets[1].emit({type:'events',lost:0,events:[
 {received_ms:influenceAt+30000,items:[['HPG',-.6],['VCB',.8]]}
]});
assert.equal(frames.length,1);
frames.shift()();
assert.match(element('impact-map').innerHTML,/data-time="09:15"[^>]*data-impact="\+0.80"/);
assert.doesNotMatch(element('impact-map').innerHTML,/data-impact="\+1.25"/);
assert.match(element('impact-area').innerHTML,/data-ticker="VCB"[^>]*data-impact="\+0.80"[^>]*data-share="57.1"/);
assert.doesNotMatch(element('impact-area').innerHTML,/data-impact="\+1.25"/);
assert.match(element('contribution-chart').innerHTML,/data-ticker="VCB"[^>]*data-contribution="\+0.80"/);
assert.doesNotMatch(element('contribution-chart').innerHTML,/data-contribution="\+1.25"/);
assert.equal(element('contribution-up-total').textContent,'+0.80 điểm');
assert.equal(element('contribution-down-total').textContent,'-0.60 điểm');
sockets[1].emit({type:'events',lost:0,events:[
 {received_ms:influenceAt+60000,items:[['HPG',-.2],['VCB',.9]]}
]});
assert.equal(frames.length,1);
frames.shift()();
assert.match(element('impact-map').innerHTML,/data-time="09:16"/);
assert.match(element('impact-description').textContent,/2 mốc đã thu trong phiên/);
assert.match(html,/id="impact-interval"[^>]*min="1"[^>]*step="1"[^>]*value="1"/);
sockets[1].emit({type:'events',lost:0,events:[
 {received_ms:influenceAt+120000,items:[['HPG',-.1],['VCB',.5]]}
]});
frames.shift()();
assert.match(element('impact-description').textContent,/3 mốc đã thu trong phiên/);
const contributionAtLatest=element('contribution-chart').innerHTML;
element('impact-interval').value='2';
element('impact-interval').onchange();
assert.match(element('impact-description').textContent,/2 mốc đã thu trong phiên · khung 2 phút/);
assert.match(element('impact-map').innerHTML,/data-time="09:16"[^>]*data-impact="\+0.50"/);
assert.doesNotMatch(element('impact-map').innerHTML,/data-time="09:17"/);
assert.match(element('impact-area-description').textContent,/2\/2 mốc có ảnh hưởng khác 0 · khung 2 phút/);
assert.equal([...element('impact-area').innerHTML.matchAll(/data-up=/g)].length,2);
element('impact-interval').value='3';
element('impact-interval').onchange();
assert.match(element('impact-description').textContent,/1 mốc đã thu trong phiên · khung 3 phút/);
assert.match(element('impact-map').innerHTML,/data-time="09:15"[^>]*data-impact="\+0.50"/);
assert.equal([...element('impact-area').innerHTML.matchAll(/data-up=/g)].length,1);
assert.equal(element('contribution-chart').innerHTML,contributionAtLatest);
sockets[1].emit({type:'events',lost:0,events:[
 {received_ms:influenceAt+1000,items:[['HPG',-9],['VCB',9]]}
]});
assert.equal(frames.length,0);
assert.equal(element('contribution-chart').innerHTML,contributionAtLatest);
element('impact-chart-wrap').scrollLeft=120;
element('impact-chart-wrap').onscroll();
assert.equal(element('impact-area-wrap').scrollLeft,120);
element('impact-area-wrap').scrollLeft=40;
element('impact-area-wrap').onscroll();
assert.equal(element('impact-chart-wrap').scrollLeft,40);
element('impact-interval').value='0';
element('impact-interval').onchange();
assert.equal(element('impact-interval').value,'3');
sockets[1].emit({type:'hello',day:'2026-10-20',status:'connected',cursor:0,latest:null});
assert.match(element('contribution-chart').innerHTML,/Chưa có bản tin ảnh hưởng VN30 trong phiên này/);
assert.equal(element('contribution-up-total').textContent,'—');
assert.equal(element('contribution-down-total').textContent,'—');
setImmediate(()=>{
 assert.ok(basisRequests.some(url=>url.includes('day=2026-10-19')));
 assert.match(element('basis-chart').innerHTML,/09:00/);
 assert.match(element('basis-chart').innerHTML,/14:45/);
 assert.match(element('basis-chart').innerHTML,/class="vn30-baseline" x1="40"[^>]*x2="910"/);
 assert.match(element('basis-chart').innerHTML,/class="positive" d="/);
 assert.match(element('basis-chart').innerHTML,/class="negative" d="/);
 assert.doesNotMatch(element('basis-chart').innerHTML,/class="series estimated"/);
 const svg=element('basis-chart').innerHTML;
 const baselineY=Number(svg.match(/class="vn30-baseline"[^>]*y1="([\d.]+)"/)[1]);
 const futureY=Number(svg.match(/class="point negative"[^>]*cy="([\d.]+)"/)[1]);
 assert.ok(futureY>baselineY);
 assert.match(element('basis-latest').textContent,/-8,00 điểm/);
 console.log('PASS: DOM latency page renders basis and live one-minute volume alert');
});
