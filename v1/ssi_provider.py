"""SSI SDK conversion to the common market event contract."""
import asyncio
from datetime import datetime
import os
import time

from events import EventSink, tick_event
from replay_server import TZ


def subscribed(topics, prefix, symbol):
    """Match a symbol in either a single or SDK-batched subscription topic."""
    for topic in topics:
        if not isinstance(topic,str) or not topic.startswith(prefix+'.'):
            continue
        members=topic[len(prefix)+1:].split('@',1)[0].split('-')
        if symbol in members or '*' in members:
            return True
    return False


def timestamp(value, day):
    text=str(value).strip()
    if text.replace('.','',1).isdigit() and len(text.split('.')[0])>=10:
        number=float(text)
        return round(number if number>=1e12 else number*1000)
    if len(text)>=8 and text[2:3]==':' and text[5:6]==':':
        text=day+'T'+text
    dt=None
    try:
        dt=datetime.fromisoformat(text.replace('Z','+00:00'))
    except ValueError:
        for fmt in ('%Y/%m/%d %H:%M:%S','%d/%m/%Y %H:%M:%S','%Y/%m/%d'):
            try:
                dt=datetime.strptime(text,fmt);break
            except ValueError:
                pass
    if dt is None:
        raise ValueError('Invalid SSI source timestamp')
    if dt.tzinfo is None:
        dt=dt.replace(tzinfo=TZ)
    return round(dt.timestamp()*1000)


class SSIEvents:
    def __init__(self,runtime):
        self.runtime=runtime
        self.sink=EventSink(runtime)
        self.previous_candle=None

    def receive(self,message,received_ms=None):
        from ssi_sdk.models.streaming import (QuoteMessage, TradeMessage, MarketDataMessage,
                                               IntervalMessage, IndexTickMessage, IndexSummaryMessage)
        store=self.runtime.store()
        symbol=getattr(message,'symbol',None)
        if isinstance(message,IndexSummaryMessage):
            if message.index!='VN30':
                return
            raw=message.data
            source=timestamp(raw.get('t'),store.day)
            if datetime.fromtimestamp(source/1000,TZ).date().isoformat()!=store.day:
                raise ValueError('SSI index from a different session')
            def number(key):
                value=raw.get(key)
                return None if value in (None,'') else float(value)
            def count(key):
                value=number(key)
                return None if value is None else int(value)
            value=number('v')
            if value is None or value<=0:
                return
            snapshot={'kind':'market','source_ms':source,'received_ms':received_ms or round(time.time()*1000),
                      'value':value,'change':number('c'),'change_pct':number('cr'),
                      'prior':value-number('c') if number('c') is not None else None,
                      'high':None,'low':None,'up':count('adv'),'down':count('dec'),
                      'flat':count('nc'),'volume':count('tmq'),'amount':number('tmv')}
            self.sink.emit('index',snapshot)
            return
        if isinstance(message,IndexTickMessage):
            if message.symbol!='VN30' or message.value is None:
                return
            source=timestamp(message.trading_time,store.day)
            if datetime.fromtimestamp(source/1000,TZ).date().isoformat()!=store.day:
                raise ValueError('SSI index from a different session')
            previous=store.index_latest.get('market') or {}
            self.sink.emit('index',{'kind':'market','source_ms':source,
                'received_ms':received_ms or round(time.time()*1000),'value':float(message.value),
                'change':message.change,'change_pct':message.change_percent,
                'prior':float(message.value)-float(message.change) if message.change is not None else None,
                'high':previous.get('high'),'low':previous.get('low'),
                'up':previous.get('up'),'down':previous.get('down'),'flat':previous.get('flat'),
                'volume':previous.get('volume'),'amount':previous.get('amount')})
            return
        if store.failed or symbol not in (self.runtime.symbol,*self.runtime.vn30_symbols):
            return
        received_ms=received_ms or round(time.time()*1000)
        future=symbol==self.runtime.symbol
        def trade_price(value):
            return None if value is None else float(value)/(1 if future else 1000)
        source=timestamp(message.trading_time,store.day) if message.trading_time else received_ms
        if datetime.fromtimestamp(source/1000,TZ).date().isoformat()!=store.day:
            raise ValueError('SSI event from a different session')
        if isinstance(message,MarketDataMessage):
            if message.ref_price is None:
                return
            event={'symbol':symbol,'kind':'reference','source_ms':source,
                   'received_ms':received_ms,'reference':float(message.ref_price)}
            self.sink.emit('reference' if future else 'stock',event)
        elif isinstance(message,QuoteMessage) and future:
            if not message.trading_time:
                raise ValueError('Missing quote source timestamp')
            if len(message.bid_prices)!=len(message.bid_volumes) or len(message.ask_prices)!=len(message.ask_volumes):
                raise ValueError('Mismatched SSI quote levels')
            self.sink.emit('tick',tick_event(symbol=symbol,expected_symbol=symbol,day=store.day,
                source_ms=source,received_ms=received_ms,
                bids=list(zip(message.bid_prices,message.bid_volumes)),
                asks=list(zip(message.ask_prices,message.ask_volumes))))
        elif isinstance(message,TradeMessage):
            if not message.trading_time:
                raise ValueError('Missing trade source timestamp')
            if future:
                side=str(getattr(message.side,'value',message.side)).upper()
                self.sink.emit('tick',tick_event(symbol=symbol,expected_symbol=symbol,day=store.day,
                    source_ms=source,received_ms=received_ms,price=message.price,
                    quantity=message.quantity,side={'B':'BUY','S':'SELL'}.get(side,side),
                    total_volume=message.total_volume,
                    stats={'openPrice':message.open,'highestPrice':message.high,
                           'lowestPrice':message.low,'avgPrice':message.avg}))
            else:
                self.sink.emit('stock',{'symbol':symbol,'kind':'trade','source_ms':source,
                    'received_ms':received_ms,'last':trade_price(message.price),
                    'high':trade_price(message.high),'low':trade_price(message.low)})
        elif isinstance(message,IntervalMessage) and future:
            minute=timestamp(message.interval_time,store.day)//60000*60000
            candle={'minute_ms':minute,'updated_ms':source,'received_ms':received_ms,
                    'open':float(message.open),'high':float(message.high),'low':float(message.low),
                    'close':float(message.close),'volume':message.volume,'closed':source>=minute+60000}
            if self.previous_candle and minute>self.previous_candle['minute_ms']:
                self.sink.emit('candle',{**self.previous_candle,'closed':True,'updated_ms':source,'received_ms':received_ms})
            self.sink.emit('candle',candle)
            if self.previous_candle is None or (minute,source)>=(self.previous_candle['minute_ms'],self.previous_candle['updated_ms']):
                self.previous_candle=candle


async def collect(runtime,key,secret,url=None,ohlc_symbol_type=None):
    from ssi_sdk import AsyncAuth, AsyncStream, Config, ReauthenticationRequired
    from ssi_sdk.enums import Timeframe
    config=Config(api_key=key,api_secret=secret,
                  private_key=os.getenv('SSI_PRIVATE_KEY',''),client_id=os.getenv('SSI_CLIENT_ID',''),
                  api_url=os.getenv('SSI_API_URL'),streaming_url=url)
    delay=1
    while not runtime.stop.is_set():
        authenticated=False
        try:
            runtime.connection('connecting')
            async with AsyncAuth(config) as auth:
                # This dashboard reads market data only; do not open a trading OTP session.
                await auth.authenticate()
                authenticated=True
                async with AsyncStream(auth) as client:
                    events=SSIEvents(runtime)
                    connection_state={'fatal':None}
                    def on_connection(event):
                        if event.state=='disconnected':
                            runtime.connection('reconnecting')
                        elif event.state=='reconnected':
                            runtime.connection('connected')
                        elif event.state=='failed':
                            connection_state['fatal']=event.error or RuntimeError('SSI socket failed')
                            runtime.connection('error',type(event.error).__name__ if event.error else 'WebSocketError')
                    client.streaming.on_connection=on_connection
                    def receive(message):
                        try:
                            events.receive(message)
                        except (ValueError,TypeError,AttributeError,OverflowError):
                            store=runtime.store()
                            with store.lock:
                                store.invalid+=1
                                store.changed.notify_all()
                    client.streaming.on_data=receive
                    await client.streaming.connect()
                    symbols=[runtime.symbol,*runtime.vn30_symbols]
                    await client.streaming.subscribe_symbol_quote([runtime.symbol])
                    await client.streaming.subscribe_symbol_trade(symbols)
                    await client.streaming.subscribe_market(symbols)
                    await client.streaming.subscribe_symbol_ohlcv([runtime.symbol],Timeframe.MINUTE_1)
                    await client.streaming.subscribe_index_trade(['VN30'])
                    await client.streaming.subscribe_index_summary(['VN30'])
                    subscriptions=await client.streaming.list_subscription()
                    topics=subscriptions.get('data',[])
                    required=(('quote',runtime.symbol),('trade',runtime.symbol))
                    missing=[f'{channel}.{symbol}' for channel,symbol in required
                             if not subscribed(topics,channel,symbol)]
                    if missing:
                        raise ValueError('SSI subscriptions rejected: '+','.join(missing))
                    runtime.capabilities['reference']=subscribed(topics,'market',runtime.symbol)
                    runtime.capabilities['stocks']=all(
                        subscribed(topics,'trade',symbol) and subscribed(topics,'market',symbol)
                        for symbol in runtime.vn30_symbols)
                    runtime.capabilities['candle']=subscribed(topics,'trade',runtime.symbol) and any(
                        topic.startswith('trade.'+runtime.symbol+'@1m') for topic in topics)
                    runtime.capabilities['index']=subscribed(topics,'trade.index','VN30') or subscribed(topics,'indexsummary','VN30')
                    runtime.capabilities['breadth']=subscribed(topics,'indexsummary','VN30')
                    runtime.connection('connected')
                    delay=1
                    while not runtime.stop.is_set():
                        if connection_state['fatal'] is not None:
                            raise connection_state['fatal']
                        runtime.store()
                        await asyncio.sleep(.5)
        except Exception as exc:
            if isinstance(exc,ReauthenticationRequired):
                runtime.connection('error','SSI requires a new OTP')
                print('SSI requires a new OTP',flush=True)
                return
            if isinstance(exc,ValueError) and str(exc).startswith('SSI subscriptions rejected:'):
                runtime.connection('error','SSI subscription denied; check market data scopes')
                print(str(exc),flush=True)
                return
            code=getattr(exc,'code',None)
            if str(code)=='401105':
                message='SSI 401105: API credentials rejected or OTP required'
                runtime.connection('error',message)
                print(message,flush=True)
                return
            runtime.connection('error',type(exc).__name__)
            print('SSI connection unavailable: '+type(exc).__name__,flush=True)
            if not authenticated:
                # Authentication is a non-idempotent request; leave retry to an operator.
                return
        for _ in range(delay*2):
            if runtime.stop.is_set():
                return
            await asyncio.sleep(.5)
        delay=min(60,delay*2)
