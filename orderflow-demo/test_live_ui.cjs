const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const html = fs.readFileSync(__dirname+'/dist/orderflow-live.html','utf8');
const code = html.match(/<script>([\s\S]*?)<\/script>/)[1];
const elements = new Map(), sources = [], requests = [], intervals = [];
const ctx = new Proxy({}, {get:(o,k)=>o[k]??(()=>{}),set:(o,k,v)=>(o[k]=v,true)});
function element(id){
 if(!elements.has(id))elements.set(id,{
  value:id==='large-side'?'ALL':'',textContent:'',innerHTML:'',style:{},checked:true,disabled:false,
  classList:{add(){},remove(){},toggle(){}},addEventListener(event,fn){this['on'+event]=fn;},
  setAttribute(k,v){this[k]=v;},setCustomValidity(){},querySelector(){return null;},
  getBoundingClientRect(){return {width:1000,height:500,top:0,left:0};},
  getContext(){return ctx;},setPointerCapture(){},hasPointerCapture(){return false;}
 });return elements.get(id);
}
const meta={base_ms:1792342800000,duration:32401,start_ms:1792375200000,end_ms:1792375201000,symbol:'41I1GB000',day:'2026-10-19',trades:1};
function snapshot(t=32401){return {meta:{...meta},cursor:t,start:t-600,end:t,history:[],bars:[],frame:{price:18000,tradeCount:1,tradePriceLevels:2,volume:60,bids:[],asks:[],gap:true},flowSpeed:[{seconds:10,buy:20,sell:10,unknown:0,trades:2,buy_per_sec:2,sell_per_sec:1,delta_per_sec:1,available:true},{seconds:60,buy:120,sell:60,unknown:0,trades:12,buy_per_sec:2,sell_per_sec:1,delta_per_sec:1,available:false}],capture:{status:'connected',invalid:0},quoteAge:null,quotesProcessed:0,assessment:null,finished:false,revision:1,persistedTrades:1};}
let now=0;
const sandbox={
 document:{getElementById:element,querySelector:()=>element('terminal'),addEventListener(){},activeElement:{tagName:'BODY'}},
 window:{addEventListener(){}},location:{protocol:'http:',host:'localhost:8097'},Intl,Date,Map,Math,Number,String,JSON,URLSearchParams,console,
 devicePixelRatio:1,innerWidth:1200,innerHeight:900,
 localStorage:{getItem(){return null;},setItem(){}},performance:{now:()=>now},
 ResizeObserver:class{observe(){}},setInterval(fn){intervals.push(fn);},requestAnimationFrame(fn){fn();},
 WebSocket:class{constructor(url){this.url=url;sources.push(this);}close(){this.closed=true;this.onclose?.();}emit(data){this.onmessage({data:JSON.stringify({type:'state',data})});}emitPersisted(data){this.onmessage({data:JSON.stringify({type:'persisted',...data})});}disconnect(){this.onclose?.();}},
 fetch:async url=>{requests.push(String(url));return {ok:true,json:async()=>{
  if(url==='/api/meta')return meta;
  if(String(url).startsWith('/api/state'))return snapshot(Number(new URLSearchParams(String(url).split('?')[1]).get('t')));
  return {trades:[[32400,18000,60,'BUY']],count:101,buy:6060,sell:0,as_of:32401,next:String(url).includes('before=')?null:'1792375200000:1'};
 }}}};
vm.runInNewContext(code,sandbox);
const flush=async()=>{for(let n=0;n<5;n++)await new Promise(setImmediate);};
(async()=>{
 await flush();assert.equal(sources.length,1);assert.match(sources[0].url,/^ws:\/\/localhost:8097\/api\/ws\?/);
 sources[0].emit(snapshot());await flush();
 assert.equal(element('trade-price-count').textContent,'2');
 assert.match(element('status').textContent,/LIVE/);assert.equal(element('play').textContent,'Tạm dừng');
 assert.match(element('mode-badge').textContent,/LIVE/);
 assert.match(element('speed-grid').innerHTML,/2\.0 HĐ\/s/);
 assert.match(element('speed-grid').innerHTML,/60 GIÂY GẦN NHẤT · THIẾU DỮ LIỆU/);
 const largeRequests=requests.filter(r=>r.startsWith('/api/large-trades')).length;
 sources[0].emitPersisted({day:meta.day,trades:2});await flush();
 assert.equal(requests.filter(r=>r.startsWith('/api/large-trades')).length,largeRequests+1);
 const returned=snapshot();returned.history=[[18000,{price:18000,bid:{max:600,maxAt:32400,last:400,lastAt:32401,returnFrom:500,returnTo:400,returnAt:32401,returnDelta:-100,returnCount:1}}]];
 sources[0].emit(returned);assert.match(element('book-rows').innerHTML,/500→400/);
 assert(requests.some(r=>r.startsWith('/api/large-trades')));
 assert(!requests.some(r=>r.startsWith('/api/state')));
 element('large-next').onclick();await flush();
 assert(sources[0].closed);assert(requests.some(r=>r.includes('before=')&&r.includes('t=32401')));
 element('live').onclick();await flush();assert.equal(sources.length,2);
 sources[1].emit(snapshot());await flush();
 element('timeline').oninput({target:{value:32400}});intervals[0]();await flush();
 assert.match(element('mode-badge').textContent,/REPLAY/);
 assert(sources[1].closed);assert(requests.some(r=>r.includes('/api/state?t=32400')));
 assert.equal(element('clock').textContent,'09:00:00');
 element('live').onclick();await flush();sources[2].emit(snapshot());await flush();
 element('timeframe').onchange({target:{value:'300'}});intervals[0]();await flush();
 assert.equal(sources.length,4);assert(sources[2].closed);assert.match(sources[3].url,/tf=300/);
 sources[2].emit({...snapshot(),frame:{...snapshot().frame,price:1}}); // stale stream ignored
 assert.equal(element('last-price').textContent,'1800.0');
 sources[3].disconnect();await new Promise(resolve=>setTimeout(resolve,550));intervals[0]();await flush();
 assert.equal(sources.length,5);assert.match(sources[4].url,/api\/ws\?/);
 element('timeline').oninput({target:{value:32400}});await flush();
 sandbox.location.protocol='https:';element('live').onclick();await flush();
 assert.match(sources[5].url,/^wss:\/\/localhost:8097\/api\/ws\?/);
 console.log('PASS: WebSocket live push, persisted event, separate history API, rewind, LIVE resume, timeframe resubscribe, stale-socket rejection and reconnect');
})().catch(e=>{console.error(e);process.exitCode=1;});
