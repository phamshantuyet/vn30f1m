const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const html=fs.readFileSync(__dirname+'/dist/dom-latency.html','utf8');
const code=html.match(/<script>([\s\S]*?)<\/script>/)[1];
const elements=new Map(),sockets=[],frames=[],basisRequests=[],ohlcvRequests=[],tradeSpeedRequests=[],vn30StockRequests=[],intervals=[];
const documentEvents={},windowEvents={};
let perfNow=1;
const historyMinute=Date.parse('2026-10-19T02:13:00Z');
const tradeSpeedMinute=Math.floor(Date.now()/1000)*1000-120000;
const element=id=>{if(!elements.has(id))elements.set(id,{
 textContent:'',innerHTML:'',scrollTop:0,attributes:{},
 setAttribute(name,value){this.attributes[name]=value;}
});return elements.get(id);};
const bookRow=p=>element('book').innerHTML.match(new RegExp(`<tr[^>]*data-price="${p}"[^>]*>[\\s\\S]*?<\\/tr>`))?.[0]||'';
const sandbox={
 document:{getElementById:element,fullscreenElement:null,
  addEventListener(name,callback){documentEvents[name]=callback;}},
 location:{protocol:'http:',host:'localhost:8097'},
 window:{addEventListener(name,callback){windowEvents[name]=callback;}},
 performance:{now:()=>perfNow},Date,Intl,JSON,Math,Number,String,
 setTimeout(){},setInterval(callback,delay){intervals.push({callback,delay});},
 requestAnimationFrame(callback){frames.push(callback);},
 fetch(url){
  if(url==='/api/vn30-stocks'){
   vn30StockRequests.push(url);
   const stocks=Array.from({length:30},(_,i)=>({symbol:i===0?'HPG':i===1?'BID':`T${i}`}));
   Object.assign(stocks[0],{reference:21.2,last:21.3,low:21.1,high:21.5,
    trade_source_ms:tradeSpeedMinute});
   Object.assign(stocks[1],{reference:40,last:39.5,low:39.2,high:40.3,
    trade_source_ms:tradeSpeedMinute+1000});
   return Promise.resolve({ok:true,json:()=>Promise.resolve({day:'2026-10-19',stocks})});
  }
  if(url==='/api/trade-speed'){
   tradeSpeedRequests.push(url);
   return Promise.resolve({ok:true,json:()=>Promise.resolve({
    day:'2026-10-19',symbol:'41I1GB000',cursor:5,seconds:[
     [tradeSpeedMinute,2,100],[tradeSpeedMinute+1000,1,25]
    ]})});
  }
  if(url==='/api/ohlcv-1m'){
   ohlcvRequests.push(url);
   return Promise.resolve({ok:true,json:()=>Promise.resolve({
    day:'2026-10-19',symbol:'41I1GB000',candles:[
     {minute_ms:historyMinute,updated_ms:historyMinute+60000,received_ms:historyMinute+60000,
      open:1800,high:1801.2,low:1800,close:1801.2,volume:1600,closed:true},
     {minute_ms:historyMinute+60000,updated_ms:historyMinute+120000,received_ms:historyMinute+120000,
      open:1800,high:1800,low:1798.2,close:1798.2,volume:1800,closed:true},
     {minute_ms:historyMinute+120000,updated_ms:historyMinute+120000,received_ms:historyMinute+120000,
      open:1800,high:1800.2,low:1799.9,close:1800.2,volume:100,closed:false}
    ]})});
  }
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
element('trade-speed-range').value='900';
assert.match(html,/id="trade-speed-fullscreen"[^>]*aria-pressed="false"/);
assert.equal(element('trade-speed-fullscreen').textContent,'');
element('trade-speed-fullscreen').onclick();
assert.equal(element('trade-speed-panel').className,'panel trade-speed is-expanded');
assert.equal(element('trade-speed-fullscreen').textContent,'↙ Thu nhỏ');
assert.equal(element('trade-speed-fullscreen').attributes['aria-pressed'],'true');
windowEvents.keydown({key:'Escape'});
assert.equal(element('trade-speed-panel').className,'panel trade-speed');
assert.equal(element('trade-speed-fullscreen').textContent,'⛶ Toàn màn hình');
assert.equal(sockets.length,3);
assert.equal(sockets[0].url,'ws://localhost:8097/api/ws/raw');
assert.equal(sockets[1].url,'ws://localhost:8097/api/ws/influence');
assert.equal(sockets[2].url,'ws://localhost:8097/api/ws/index');
assert.equal(element('impact-badge').textContent,'ĐANG CHỜ DNSE');
const historical=Date.now()-200;
sockets[0].emit({type:'hello',symbol:'41I1GB000',day:'2026-10-19',status:'connected',
 book:[[[18000,500]],[[18001,400]]],last_price:18000,reference_price:1799.5,
 hit_bids:[18000],hit_asks:[],
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
assert.match(bookRow(18000),/class="trade-mark bid-hit"[^>]*>\^<\/span>/);
assert.doesNotMatch(bookRow(18001),/class="trade-mark ask-hit"/);
element('book-wrap').scrollTop=123;
const received=Date.now()-100;
sockets[0].emit({type:'events',sent_ms:received+30,lost:0,events:[
 {kind:'quote',source_ms:received-20,received_ms:received,projected_ms:received+2,
  bids:[[18000,600]],asks:[[18001,400]]},
 {kind:'quote',source_ms:received-19,received_ms:received,projected_ms:received+2,
  bids:[[18000,450]],asks:[[18001,350]]},
 {seq:6,kind:'trade',source_ms:received,received_ms:received+1,projected_ms:received+3,
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
assert.match(bookRow(18001),/class="trade-mark ask-hit"[^>]*>▼<\/span>/);
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
 sockets[0].emit({type:'events',sent_ms:updated_ms,lost:0,events:[
  {kind:'ohlcv',source_ms:minute_ms,received_ms:updated_ms,
   candle:{minute_ms,updated_ms,received_ms:updated_ms,open:1800,high:1801,
           low:1799,close:1800.5,volume,closed}}
 ]});
sendCandle(1500,false);
assert.equal(element('ohlcv-alert').className,'ohlcv-alert');
assert.equal(element('ohlcv-countdown').textContent,'30 s');
sendCandle(1501,false,candleMinute+35000);
assert.equal(element('ohlcv-alert').className,'ohlcv-alert is-warning');
assert.equal(element('ohlcv-volume').textContent,'1.501 HĐ');
assert.equal(element('ohlcv-countdown').textContent,'25 s');
perfNow+=1000;
intervals.find(item=>item.delay===1000).callback();
assert.equal(element('ohlcv-countdown').textContent,'24 s');
perfNow+=25000;
intervals.find(item=>item.delay===1000).callback();
assert.equal(element('ohlcv-countdown').textContent,'0 s · chờ xác nhận');
assert.match(element('ohlcv-minute').textContent,/09:15.*chưa đóng/);
assert.match(element('ohlcv-history').innerHTML,/09:15.*Đang hình thành.*1\.501.*\+0,5.*\+0,03%/);
sendCandle(1700,true,candleMinute+60000);
assert.equal(element('ohlcv-alert').className,'ohlcv-alert');
assert.match(element('ohlcv-state').textContent,/Nến đã đóng/);
assert.equal(element('ohlcv-countdown').textContent,'Đã đóng');
assert.match(element('ohlcv-history').innerHTML,/09:15.*Đã đóng.*1\.700/);
sendCandle(2000,false,candleMinute+65000);
assert.equal(element('ohlcv-alert').className,'ohlcv-alert');
assert.doesNotMatch(element('ohlcv-history').innerHTML,/2\.000/);
sendCandle(100,false,candleMinute+70000,candleMinute+60000);
assert.equal(element('ohlcv-alert').className,'ohlcv-alert');
assert.match(element('ohlcv-minute').textContent,/09:16.*chưa đóng/);
assert.equal(element('ohlcv-countdown').textContent,'50 s');
assert.doesNotMatch(element('ohlcv-history').innerHTML,/09:16/);
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
 hit_bids:[18000],hit_asks:[18001],
 ohlcv_latest:{minute_ms:candleMinute,updated_ms:candleMinute+60000,
  open:1800,high:1801,low:1799,close:1800.5,volume:1700,closed:true},
 book_source_ms:received+20,
 max_bids:[[18000,700,received+10],[18030,900,received+20]],
 max_asks:[[18001,400,received],[18031,300,received+20]],
 last_bids:[[18000,650,received+10],[18030,900,received+20]],
 last_asks:[[18001,370,received+10],[18031,300,received+20]],
 trade_stats_source_ms:received,
 trade_stats:{totalVolumeTraded:193,grossTradeAmount:6000,avgPrice:1800.5,
  highestPrice:1802,lowestPrice:1799.8,openPrice:1800}});
assert.match(element('book').innerHTML,/data-price="18000"[^>]*><td class="peak">700<\/td><td class="bid last-seen"[^>]*>650<\/td>/);
assert.match(bookRow(18000),/class="trade-mark bid-hit"[^>]*>\^<\/span>/);
assert.match(bookRow(18001),/class="trade-mark ask-hit"[^>]*>▼<\/span>/);
assert.equal(element('trade-total-volume').textContent,'193');
assert.equal(element('trade-avg-price').textContent,'1.800,5');
assert.equal(element('ohlcv-alert').className,'ohlcv-alert');
assert.equal(element('ohlcv-volume').textContent,'1.700 HĐ');
assert.equal(element('ohlcv-countdown').textContent,'Đã đóng');
// A delayed Trade with older source time must not roll the statistics back.
sockets[0].emit({type:'events',sent_ms:received+60,lost:0,events:[
 {seq:7,kind:'trade',source_ms:received-1,received_ms:received+50,projected_ms:received+51,
  price:18001,qty:1,side:'BUY',trade_stats:{totalVolumeTraded:1,
   grossTradeAmount:1,avgPrice:1,highestPrice:1,lowestPrice:1,openPrice:1}}
]});
assert.equal(element('trade-total-volume').textContent,'193');
assert.equal(element('trade-avg-price').textContent,'1.800,5');
assert.equal(frames.length,1);
frames.shift()();
const influenceAt=Date.parse('2026-10-19T02:15:00Z');
sockets[1].emit({type:'hello',day:'2026-10-19',status:'connected',cursor:0,
 extrema:{HPG:[-.5,0],VCB:[-4,1.4]},
 total_extrema:{up:2.5,down:-1.2},
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
assert.match(html,/id="contribution-rows"/);
assert.match(html,/class="contribution-table"[^]*?<th scope="col">Mã<\/th><th scope="col">Âm<\/th><th scope="col">Dương<\/th>/);
assert.match(html,/id="contribution-up-peak"/);
assert.match(html,/id="contribution-down-peak"/);
const contributionRows=element('contribution-rows').innerHTML;
assert.equal((contributionRows.match(/<tr data-ticker=/g)||[]).length,2);
assert.match(contributionRows,/^<tr data-ticker="VCB" data-contribution="\+1.25"><td class="ticker">VCB<\/td><td class="down"><span class="contribution-only-peak">\(-4.00\)<\/span><\/td><td class="up"><div class="contribution-side up"><i class="bar" style="width:100.00%"><\/i><strong>\+1.25 <span class="contribution-peak">\(\+1.40\)<\/span><\/strong><\/div><\/td><\/tr>/);
assert.match(contributionRows,/<tr data-ticker="HPG" data-contribution="-0.40"><td class="ticker">HPG<\/td><td class="down"><div class="contribution-side down"><i class="bar" style="width:32.00%"><\/i><strong>-0.40 <span class="contribution-peak">\(-0.50\)<\/span><\/strong><\/div><\/td><td class="flat">—<\/td><\/tr>/);
assert.doesNotMatch(contributionRows,/data-negative-extreme|data-positive-extreme/);
assert.equal(element('contribution-scale').textContent,'Thang: ±1.25 điểm');
assert.match(element('contribution-time').textContent,/09:15:00/);
assert.equal(element('contribution-up-total').textContent,'+1.25 điểm');
assert.equal(element('contribution-down-total').textContent,'-0.40 điểm');
assert.equal(element('contribution-up-peak').textContent,'+2.50 điểm');
assert.equal(element('contribution-down-peak').textContent,'-1.20 điểm');
sockets[1].emit({type:'events',lost:0,events:[
 {received_ms:influenceAt+30000,items:[['HPG',-.6],['VCB',.8]]}
]});
assert.equal(frames.length,1);
frames.shift()();
assert.match(element('impact-map').innerHTML,/data-time="09:15"[^>]*data-impact="\+0.80"/);
assert.doesNotMatch(element('impact-map').innerHTML,/data-impact="\+1.25"/);
assert.match(element('impact-area').innerHTML,/data-ticker="VCB"[^>]*data-impact="\+0.80"[^>]*data-share="57.1"/);
assert.doesNotMatch(element('impact-area').innerHTML,/data-impact="\+1.25"/);
assert.match(element('contribution-rows').innerHTML,/data-ticker="VCB" data-contribution="\+0.80"/);
assert.match(element('contribution-rows').innerHTML,/data-ticker="VCB" data-contribution="\+0.80"[^]*?\+0.80 <span class="contribution-peak">\(\+1.40\)<\/span>/);
assert.match(element('contribution-rows').innerHTML,/data-ticker="HPG" data-contribution="-0.60"[^]*?<div class="contribution-side down"><i class="bar" style="width:75.00%"><\/i><strong>-0.60 <span class="contribution-peak">\(-0.60\)<\/span>/);
assert.doesNotMatch(element('contribution-rows').innerHTML,/data-contribution="\+1.25"/);
assert.equal(element('contribution-up-total').textContent,'+0.80 điểm');
assert.equal(element('contribution-down-total').textContent,'-0.60 điểm');
assert.equal(element('contribution-up-peak').textContent,'+2.50 điểm');
assert.equal(element('contribution-down-peak').textContent,'-1.20 điểm');
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
const contributionAtLatest=element('contribution-rows').innerHTML;
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
assert.equal(element('contribution-rows').innerHTML,contributionAtLatest);
sockets[1].emit({type:'events',lost:0,events:[
 {received_ms:influenceAt+1000,items:[['HPG',-9],['VCB',9]]}
]});
assert.equal(frames.length,1);
frames.shift()();
assert.notEqual(element('contribution-rows').innerHTML,contributionAtLatest);
assert.match(element('contribution-rows').innerHTML,/data-ticker="VCB" data-contribution="\+0.50"[^]*?\+0.50 <span class="contribution-peak">\(\+9.00\)<\/span>/);
assert.match(element('contribution-rows').innerHTML,/data-ticker="HPG" data-contribution="-0.10"[^]*?-0.10 <span class="contribution-peak">\(-9.00\)<\/span>/);
assert.equal(element('contribution-up-total').textContent,'+0.50 điểm');
assert.equal(element('contribution-down-total').textContent,'-0.10 điểm');
assert.equal(element('contribution-up-peak').textContent,'+9.00 điểm');
assert.equal(element('contribution-down-peak').textContent,'-9.00 điểm');
sockets[1].emit({type:'events',lost:0,events:[
 {received_ms:influenceAt+180000,items:[['HPG',.3],['VCB',.5]]}
]});
assert.equal(frames.length,1);
frames.shift()();
assert.match(element('contribution-rows').innerHTML,/data-ticker="HPG" data-contribution="\+0.30"[^]*?<td class="down"><span class="contribution-only-peak">\(-9.00\)<\/span><\/td><td class="up"><div class="contribution-side up"><i class="bar" style="width:60.00%"><\/i><strong>\+0.30 <span class="contribution-peak">\(\+0.30\)<\/span>/);
assert.equal(element('contribution-up-total').textContent,'+0.80 điểm');
assert.equal(element('contribution-down-total').textContent,'0.00 điểm');
assert.equal(element('contribution-up-peak').textContent,'+9.00 điểm');
assert.equal(element('contribution-down-peak').textContent,'-9.00 điểm');
sockets[1].emit({type:'events',lost:0,events:[
 {received_ms:influenceAt+240000,items:[['HPG',0],['VCB',0]]}
]});
assert.equal(frames.length,1);
frames.shift()();
assert.equal((element('contribution-rows').innerHTML.match(/class="current-zero">0.00<\/small>/g)||[]).length,2);
assert.equal((element('contribution-rows').innerHTML.match(/class="contribution-only-peak"/g)||[]).length,4);
assert.doesNotMatch(element('contribution-rows').innerHTML,/class="bar"/);
sockets[1].emit({type:'events',lost:0,events:[
 {received_ms:influenceAt+300000,items:[['HPG',0],['VCB',-1]]}
]});
assert.equal(frames.length,1);
frames.shift()();
assert.match(element('contribution-rows').innerHTML,/data-ticker="VCB" data-contribution="-1.00"[^]*?<strong>-1.00 <span class="contribution-peak">\(-4.00\)<\/span>[^]*?<td class="up"><span class="contribution-only-peak">\(\+9.00\)<\/span>/);
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
assert.match(element('contribution-rows').innerHTML,/Chưa có bản tin ảnh hưởng VN30 trong phiên này/);
assert.equal(element('contribution-up-total').textContent,'—');
assert.equal(element('contribution-down-total').textContent,'—');
assert.equal(element('contribution-up-peak').textContent,'—');
assert.equal(element('contribution-down-peak').textContent,'—');
setImmediate(async()=>{
 assert.match(html,/<span class="box-number">17<\/span>30 cổ phiếu thành phần VN30/);
 assert.ok(html.indexOf('id="vn30-stocks-rows"')>html.indexOf('id="trade-speed-chart"'));
 assert.ok(html.indexOf('id="vn30-stocks-rows"')<html.indexOf('Thông số độ trễ và nhịp Quote'));
 assert.ok(vn30StockRequests.length>=1);
 assert.match(element('vn30-stocks-status').textContent,/2\/30 mã có đủ giá/);
 const stockRows=element('vn30-stocks-rows').innerHTML;
 assert.equal((stockRows.match(/<tr>/g)||[]).length,30);
 assert.match(stockRows,/<td class="symbol"><a href="https:\/\/banggia\.dnse\.com\.vn\/phan-tich-ky-thuat\/HPG" target="_blank" rel="noopener noreferrer"[^>]*>HPG<\/a><\/td>/);
 assert.match(stockRows,/phan-tich-ky-thuat\/BID" target="_blank" rel="noopener noreferrer"/);
 assert.match(stockRows,/HPG.*21,30.*21,20.*21,10.*21,50.*class="up">\+0,47%/);
 assert.match(stockRows,/BID.*39,50.*40,00.*39,20.*40,30.*class="down">-1,25%/);
 const stockHead=element('vn30-stocks-head');
 const stockOrder=()=>[...element('vn30-stocks-rows').innerHTML.matchAll(/<td class="symbol"><a [^>]*>([A-Z0-9]+)<\/a><\/td>/g)].map(match=>match[1]);
 const sortStocks=key=>stockHead.onclick({target:{closest:()=>({dataset:{sort:key}})}});
 assert.equal((stockHead.innerHTML.match(/data-sort=/g)||[]).length,7);
 assert.equal(stockOrder()[0],'BID');
 sortStocks('change');
 assert.equal(stockOrder()[0],'HPG');
 assert.equal(stockOrder()[1],'BID');
 assert.match(stockHead.innerHTML,/aria-sort="descending"><button[^>]*data-sort="change"/);
 sortStocks('change');
 assert.equal(stockOrder()[0],'BID');
 assert.equal(stockOrder()[1],'HPG');
 assert.match(stockHead.innerHTML,/aria-sort="ascending"><button[^>]*data-sort="change"/);
 for(const key of ['last','reference','low','high','trade_source_ms']){
  sortStocks(key);
  assert.equal(stockOrder()[0],'BID');
  assert.match(stockOrder()[2],/^T\d+$/);
 }
 intervals.find(item=>item.delay===2000).callback();
 await new Promise(setImmediate);
 assert.equal(stockOrder()[0],'BID');
 assert.match(stockHead.innerHTML,/aria-sort="descending"><button[^>]*data-sort="trade_source_ms"/);
 sortStocks('symbol');
 assert.equal(stockOrder()[0],'BID');
 sortStocks('symbol');
 assert.equal(stockOrder()[0],'T9');
 const panel=element('trade-speed-panel'),button=element('trade-speed-fullscreen');
 panel.requestFullscreen=()=>{
  sandbox.document.fullscreenElement=panel;documentEvents.fullscreenchange();
  return Promise.resolve();
 };
 sandbox.document.exitFullscreen=()=>{
  sandbox.document.fullscreenElement=null;documentEvents.fullscreenchange();
  return Promise.resolve();
 };
 await button.onclick();
 assert.equal(button.textContent,'↙ Thu nhỏ');
 assert.equal(button.attributes['aria-pressed'],'true');
 await button.onclick();
 assert.equal(button.textContent,'⛶ Toàn màn hình');
 assert.equal(button.attributes['aria-pressed'],'false');
 assert.match(html,/<span class="box-number">16<\/span>Tốc độ khớp lệnh theo thời gian/);
 assert.ok(html.indexOf('id="trade-speed-chart"')>html.indexOf('id="ohlcv-history"'));
 assert.ok(html.indexOf('id="trade-speed-chart"')<html.indexOf('Thông số độ trễ và nhịp Quote'));
 assert.ok(tradeSpeedRequests.length>=1);
 assert.match(element('trade-speed-status').textContent,/giây có khớp/);
 assert.match(element('trade-speed-chart').innerHTML,/class="orders-line"/);
 assert.match(element('trade-speed-chart').innerHTML,/class="contracts-line"/);
 assert.equal((element('trade-speed-chart').innerHTML.match(/class="ema-line"/g)||[]).length,2);
 element('trade-speed-range').value='all';
 element('trade-speed-range').onchange();
 assert.match(element('trade-speed-chart').innerHTML,/Lệnh\/s · tối đa 2/);
 assert.match(element('trade-speed-chart').innerHTML,/HĐ\/s · tối đa 100/);
 element('trade-speed-chart').getBoundingClientRect=()=>({left:0,width:1000});
 element('trade-speed-chart').onmousemove({clientX:60});
 assert.match(element('trade-speed-hover').textContent,/2 lệnh\/s \(EMA20 2,00\) · 100 HĐ\/s \(EMA20 100,00\)/);
 const emaPath=element('trade-speed-chart').innerHTML.match(/class="ema-line" d="M60\.00 [\d.]+L([\d.]+) ([\d.]+)/);
 assert.ok(emaPath);
 assert.ok(Math.abs(Number(emaPath[2])-(145-(2-2/21)/2*112))<.02);
 element('trade-speed-chart').onmousemove({clientX:Number(emaPath[1])});
 assert.match(element('trade-speed-hover').textContent,/1 lệnh\/s \(EMA20 1,90\) · 25 HĐ\/s \(EMA20 92,86\)/);
 assert.match(html,/<span class="box-number">15<\/span>Lịch sử nến 1 phút có volume/);
 assert.ok(html.indexOf('id="ohlcv-history"')<html.indexOf('Thông số độ trễ và nhịp Quote'));
 assert.ok(ohlcvRequests.length>=1);
 const history=element('ohlcv-history').innerHTML;
 assert.match(element('ohlcv-history-summary').textContent,/3 nến vượt 1.500 HĐ/);
 assert.match(history,/09:13.*1\.600.*1\.800,0.*1\.801,2.*\+1,2.*\+0,07%/);
 assert.match(history,/09:14.*1\.800.*1\.800,0.*1\.798,2.*-1,8.*-0,10%/);
 assert.match(history,/09:15.*Đã đóng.*1\.700/);
 assert.doesNotMatch(history,/09:16/);
 assert.ok(history.indexOf('09:15')<history.indexOf('09:14'));
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
 console.log('PASS: DOM latency page renders basis, live alert, countdown, and high-volume candle history');
});
