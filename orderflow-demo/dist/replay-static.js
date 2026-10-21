/* Apply Python-precomputed bar and book-history patches. No volume/peak formulas here. */
class RecordedReplayStore {
  constructor(meta, records, largeTrades=[]) {
    this.meta=meta;this.records=records;this.largeTrades=largeTrades;this.reset();
  }
  reset(){this.index=-1;this.lastTradeSecond=null;this.history=new Map();this.bars=this.meta.timeframes.map(()=>new Map());}
  snapshot(seconds,tf,windowSize,largeMin=50){
    if(!Number.isSafeInteger(largeMin)||largeMin<0)throw Error('Ngưỡng khối lượng phải là số nguyên không âm');
    const t=Math.max(this.meta.first_second,Math.min(this.meta.duration,seconds));
    let lo=0,hi=this.records.length;
    while(lo<hi){const mid=(lo+hi)>>1;if(this.records[mid][0]<=t)lo=mid+1;else hi=mid;}
    const target=Math.max(0,lo-1);
    if(target<this.index)this.reset();
    for(let i=this.index+1;i<=target;i++){
      const record=this.records[i];
      if(record[7]>(i?this.records[i-1][7]:0))this.lastTradeSecond=record[0];
      for(const [price,entry]of record[9])this.history.set(price,entry);
      for(let j=0;j<this.bars.length;j++){
        const patch=record[10][j];if(!patch)continue;
        const [time,open,high,low,close,buy,sell,unknown,poc,levels]=patch;
        let bar=this.bars[j].get(time);
        if(!bar){bar={levels:new Map()};this.bars[j].set(time,bar);}
        Object.assign(bar,{time,open,high,low,close,buy,sell,unknown,poc});
        // The precomputed values are assigned, never added a second time.
        bar.total=patch[10];bar.delta=patch[11];
        for(const [price,sell,buy,unknown]of levels)bar.levels.set(price,{sell,buy,unknown});
      }
    }
    this.index=target;
    const r=this.records[target],baseSecond=this.meta.base_ms/1000;
    const book=r[2],quoteAge=r[3]==null?null:(this.meta.base_ms+t*1000-r[3])/1000;
    const bids=book[0].map(([price,qty])=>({price,qty})),asks=book[1].map(([price,qty])=>({price,qty}));
    const localSeconds=((this.meta.base_ms+t*1000+7*3600*1000)%(24*3600*1000))/1000;
    const sessionBreak=localSeconds>=11*3600+30*60&&localSeconds<13*3600;
    const validBook=!!(bids.length&&asks.length&&bids[0].price<asks[0].price);
    const bookHeld=sessionBreak&&validBook&&quoteAge!=null&&quoteAge>30;
    const latestQuote=r[3]==null?null:(r[3]-this.meta.base_ms)/1000;
    const latestData=Math.max(latestQuote??-Infinity,this.lastTradeSecond??-Infinity);
    const viewEnd=sessionBreak&&Number.isFinite(latestData)&&t-latestData>30?latestData:t;
    const start=Math.max(0,Math.floor((baseSecond+Math.max(0,viewEnd-windowSize+1))/tf)*tf-baseSecond);
    const gap=!validBook||quoteAge==null||(quoteAge>30&&!bookHeld);
    const price=r[1]??bids[0]?.price??asks[0]?.price??18520;
    return {cursor:t,start,end:viewEnd,sessionBreak,bookHeld,bars:[...this.bars[this.meta.timeframes.indexOf(tf)].values()].filter(b=>b.time>=start),
      large_trades:this.largeTrades.filter(trade=>trade[0]<=r[0]&&trade[2]>largeMin),large_trades_as_of:r[0],large_threshold:largeMin,assessment:r[13]??null,history:[...this.history],quoteAge,quotesProcessed:r[8],finished:t>=this.meta.duration,
      frame:{price,bids,asks,gap,buy:r[4],sell:r[5],unknown:r[6],volume:r[11],cvd:r[12],tradeCount:r[7]}};
  }
}
if(typeof module!=='undefined')module.exports=RecordedReplayStore;
