"""Causal, source-time order-flow assessment; experimental explanatory rules.
Quote OFI is a proxy from recorded book images, not a cancellation classifier.
"""
from bisect import bisect_right
from dataclasses import asdict, dataclass

@dataclass(frozen=True)
class AssessmentConfig:
    windows: tuple = (30,60,180)
    quote_hold_seconds: float = 5
    min_quote_coverage: float = 0.8
    min_trades: int = 20
    min_classified_volume: int = 50
    min_classification_ratio: float = 0.9
    max_trade_age: float = 10
    tape_imbalance: float = 0.15
    price_move_ticks: int = 2
    absorption_imbalance: float = 0.25
    absorption_max_excursion_ticks: int = 1
    version: str = 'flow-rules-v1'

class FlowAssessment:
    def __init__(self,config=None):
        self.config=config or AssessmentConfig()
        self.qt=[];self.books=[];self.coverage=[];self.obi_area=[];self.ofi=[];self.ofi_abs=[]
        self.tt=[];self.prices=[];self.volumes=[(0,0,0)]

    def push_trade(self,t,price,qty,side):
        self.tt.append(t);self.prices.append(price)
        previous=self.volumes[-1];v=list(previous)
        v[{'BUY':0,'SELL':1}.get(side,2)]+=qty
        self.volumes.append(tuple(v))

    def push_quote(self,t,book):
        bids,asks=book
        valid=bool(bids and asks and bids[0][0]<asks[0][0])
        bb,bq=bids[0] if bids else (None,0)
        aa,aq=asks[0] if asks else (None,0)
        b3=sum(q for _,q in bids[:3]);a3=sum(q for _,q in asks[:3])
        obi=(b3-a3)/(b3+a3) if b3+a3 else 0
        cov=area=ofi=absolute=0
        if self.qt:
            previous=self.books[-1];dt=max(0,min(t-self.qt[-1],self.config.quote_hold_seconds))
            cov=self.coverage[-1]+(dt if previous[0] else 0)
            area=self.obi_area[-1]+(dt*previous[5] if previous[0] else 0)
            ofi=self.ofi[-1];absolute=self.ofi_abs[-1]
            if valid and previous[0] and t-self.qt[-1]<=self.config.quote_hold_seconds:
                _,pb,pbq,pa,paq,_=previous
                bid=(bq if bb>=pb else 0)-(pbq if bb<=pb else 0)
                ask=-(aq if aa<=pa else 0)+(paq if aa>=pa else 0)
                ofi+=bid+ask;absolute+=abs(bid)+abs(ask)
        self.qt.append(t);self.books.append((valid,bb,bq,aa,aq,obi))
        self.coverage.append(cov);self.obi_area.append(area);self.ofi.append(ofi);self.ofi_abs.append(absolute)

    def _quote_at(self,t):
        i=bisect_right(self.qt,t)-1
        if i<0:return None
        b=self.books[i]
        return b if b[0] and t-self.qt[i]<=self.config.quote_hold_seconds else None

    def _integrals(self,t):
        i=bisect_right(self.qt,t)-1
        if i<0:return 0,0,0,0
        valid,*_=self.books[i]
        dt=max(0,min(t-self.qt[i],self.config.quote_hold_seconds)) if valid else 0
        return self.coverage[i]+dt,self.obi_area[i]+dt*self.books[i][5],self.ofi[i],self.ofi_abs[i]

    def window(self,t,seconds):
        c=self.config;start=t-seconds
        lo=bisect_right(self.tt,start);hi=bisect_right(self.tt,t)
        before=self.volumes[lo];after=self.volumes[hi]
        buy,sell,unknown=[a-b for a,b in zip(after,before)]
        classified=buy+sell;total=classified+unknown
        ratio=classified/total if total else None
        imbalance=(buy-sell)/classified if classified else None
        tape=1 if imbalance is not None and imbalance>=c.tape_imbalance else -1 if imbalance is not None and imbalance<=-c.tape_imbalance else 0
        prices=self.prices[lo:hi]
        change=prices[-1]-prices[0] if prices else None
        move=1 if change is not None and change>=c.price_move_ticks else -1 if change is not None and change<=-c.price_move_ticks else 0
        now=self._quote_at(t);then=self._quote_at(start)
        bid_change=now[1]-then[1] if now and then else None
        ask_change=now[3]-then[3] if now and then else None
        quote=0
        if bid_change is not None:
            if bid_change>=0 and ask_change>=0 and bid_change+ask_change>0:quote=1
            if bid_change<=0 and ask_change<=0 and bid_change+ask_change<0:quote=-1
        end_i=self._integrals(t);start_i=self._integrals(start)
        coverage=max(0,min(1,(end_i[0]-start_i[0])/seconds))
        mean_obi=(end_i[1]-start_i[1])/(end_i[0]-start_i[0]) if end_i[0]>start_i[0] else None
        ofi=end_i[2]-start_i[2];abs_flow=end_i[3]-start_i[3]
        structure=0;extremes=[]
        for j in range(3):
            a=bisect_right(self.tt,start+j*seconds/3);b=bisect_right(self.tt,start+(j+1)*seconds/3)
            segment=self.prices[a:b]
            if len(segment)<3:break
            extremes.append((max(segment),min(segment)))
        if len(extremes)==3:
            if all(extremes[j+1][0]>extremes[j][0] and extremes[j+1][1]>extremes[j][1] for j in (0,1)):structure=1
            if all(extremes[j+1][0]<extremes[j][0] and extremes[j+1][1]<extremes[j][1] for j in (0,1)):structure=-1
        issues=[]
        if not self.qt or start<self.qt[0]:issues.append('Chưa đủ lịch sử cửa sổ')
        if hi-lo<c.min_trades or classified<c.min_classified_volume:issues.append('Ít giao dịch hoặc khối lượng có chiều')
        if ratio is None or ratio<c.min_classification_ratio:issues.append('Tỷ lệ xác định chiều thấp')
        if not hi or t-self.tt[hi-1]>c.max_trade_age:issues.append('Tape thiếu giao dịch gần đây')
        if coverage<c.min_quote_coverage or now is None or then is None:issues.append('Quote thiếu, quá hạn hoặc giao nhau')
        usable=not issues
        absorption=None
        if usable and prices and imbalance>=c.absorption_imbalance and max(prices)-prices[0]<=c.absorption_max_excursion_ticks and change<=0 and quote<=0:
            absorption='BUY trội nhưng giá không tiến lên: có thể bên bán hấp thụ; chưa xác nhận đảo chiều.'
        elif usable and prices and imbalance<=-c.absorption_imbalance and prices[0]-min(prices)<=c.absorption_max_excursion_ticks and change>=0 and quote>=0:
            absorption='SELL trội nhưng giá không giảm thêm: có thể bên mua hấp thụ; chưa xác nhận đảo chiều.'
        direction=0
        if usable and not absorption:
            if tape==move==quote==1 and structure>=0:direction=1
            elif tape==move==quote==-1 and structure<=0:direction=-1
        reasons=[]
        if issues:reasons=issues
        else:
            reasons.append('BUY chủ động trội' if tape==1 else 'SELL chủ động trội' if tape==-1 else 'BUY/SELL chưa lệch rõ')
            reasons.append('Giá tăng ít nhất 2 tick' if move==1 else 'Giá giảm ít nhất 2 tick' if move==-1 else 'Giá chưa đi đủ 2 tick')
            reasons.append('Best Bid/Ask cùng dịch lên' if quote==1 else 'Best Bid/Ask cùng dịch xuống' if quote==-1 else 'Bid/Ask chưa đồng thuận hoặc đứng giá')
            if structure and structure!=move:reasons.append('Cấu trúc đỉnh/đáy ngược chiều: cần chờ')
        return {'seconds':seconds,'usable':usable,'direction':direction,'buy':buy,'sell':sell,'unknown':unknown,'delta':buy-sell,
                'trade_count':hi-lo,'classification':ratio,'imbalance':imbalance,'price_ticks':change,
                'bid_ticks':bid_change,'ask_ticks':ask_change,'quote_coverage':coverage,'obi3':mean_obi,
                'ofi':ofi,'ofi_normalized':ofi/abs_flow if abs_flow else None,
                'structure':structure,'structure_available':len(extremes)==3,'absorption':absorption,'reasons':reasons}

    def assess(self,t):
        windows=[self.window(t,w) for w in self.config.windows]
        short,minute,three=windows
        direction=0
        if all(w['usable'] for w in windows):
            if minute['direction']==three['direction']!=0 and short['direction']==minute['direction']:
                direction=minute['direction']
        usable=sum(w['usable'] for w in windows)
        if direction:
            explanation='Tape, giá và Best Bid/Ask đồng thuận ở cả 30s, 1m và 3m.'
        elif usable<3:
            explanation='Chưa đủ dữ liệu hợp lệ ở cả ba cửa sổ; không suy ra hướng từ Bid Depth.'
        elif any(w['absorption'] for w in windows):
            explanation='Lực khớp chủ động chưa tạo được tiến triển giá; có dấu hiệu hấp thụ, cần xác nhận.'
        else:
            explanation='Các cửa sổ hoặc Tape, giá và Quote chưa cùng xác nhận một hướng.'
        return {'version':self.config.version,'as_of':t,'direction':direction,'usable_windows':usable,
                'explanation':explanation,'windows':windows}
