"""DNSE SDK adapter; all SDK payload handling stays in this module."""
import asyncio, json, os, time
from functools import partial
from dnse_normalize import normalize, normalize_influence, normalize_market_index, normalize_reference_price, normalize_ohlcv_1m
from dnse_normalize import normalize_vn30_stock
from events import EventSink

def bootstrap_vn30_stocks(runtime, key, secret):
    """Fill the mid-session stock snapshot before new WSS ticks arrive."""
    from dnse import DNSEClient
    client=DNSEClient(key,secret)
    counts={'reference':0,'trade':0}
    failures=0
    for symbol in runtime.vn30_symbols:
        if runtime.stop.is_set():
            break
        for kind,fetch in (('reference',client.get_security_definition),
                           ('trade',client.get_latest_trade)):
            if runtime.stop.is_set():
                break
            try:
                status,body=fetch(symbol,board_id='G1')
                if status!=200:
                    continue
                data=json.loads(body)
                rows=data if kind=='reference' else data.get('trades',[])
                row=next((row for row in rows if row.get('symbol')==symbol and
                          row.get('boardId')=='G1'),None)
                if row is not None:
                    store=runtime.store()
                    EventSink(runtime).emit("stock",normalize_vn30_stock(
                        row,kind,store.day,runtime.vn30_set))
                    counts[kind]+=1
            except Exception:
                # WSS remains the live source when an optional REST snapshot fails.
                failures+=1
    print(f'VN30 REST snapshot: reference={counts["reference"]}/30 '
          f'trade={counts["trade"]}/30 failures={failures}',flush=True)


async def collect(runtime, key, secret, url, ohlc_symbol_type='VN30F1M'):
    # SDK timestamp parser uses local time; never depend on the host timezone.
    os.environ['TZ']='Asia/Bangkok'
    if hasattr(time,'tzset'):
        time.tzset()
    from dnse import TradingClient
    class MarketClient(TradingClient):
        async def _dispatch_message(self,data):
            # SDK 2.4.1 PriceLevel expects qtty; docs also use quantity.
            if data.get('T')=='q':
                for side in ('bid','offer'):
                    for level in data.get(side) or []:
                        if 'qtty' not in level and 'quantity' in level:
                            level['qtty']=level['quantity']
            await super()._dispatch_message(data)
    sink=EventSink(runtime)
    delay=1
    while not runtime.stop.is_set():
        client=None
        try:
            runtime.connection('connecting')
            client=MarketClient(api_key=key,api_secret=secret,base_url=url,
                                 encoding='msgpack',auto_reconnect=True,max_retries=3,timeout=15)
            client.on('reconnecting',lambda _:runtime.connection('reconnecting'))
            client.on('reconnected',lambda _:runtime.connection('connected'))
            # Expose error type, never arbitrary exception text containing credentials.
            client.on('error',lambda e:runtime.connection('error',type(e).__name__))
            def receive(kind,obj):
                store=runtime.store()
                if store.failed:
                    return
                try:
                    event=normalize(obj,kind,runtime.symbol,store.day)
                    if store.status=='invalid':
                        store.connection('connected')
                    sink.emit("tick",event)
                except (ValueError,TypeError,AttributeError,OverflowError):
                    store.invalid+=1
                    store.connection('invalid','Invalid market payload; see invalid count')
            def receive_influence(obj):
                store=runtime.store()
                if store.failed:
                    return
                try:
                    sink.emit("contribution",normalize_influence(obj))
                except (ValueError,TypeError,AttributeError,OverflowError):
                    with store.lock:
                        store.influence_invalid+=1
                        store.changed.notify_all()
            def receive_index(kind,obj):
                store=runtime.store()
                if store.failed:
                    return
                try:
                    sink.emit("index",normalize_market_index(obj,kind,store.day))
                except (ValueError,TypeError,AttributeError,OverflowError):
                    with store.lock:
                        store.index_invalid+=1
                        store.changed.notify_all()
            def receive_reference(obj):
                store=runtime.store()
                if store.failed:
                    return
                try:
                    price,received=normalize_reference_price(obj,runtime.symbol)
                    sink.emit("reference",{"reference":price,"received_ms":received})
                except (ValueError,TypeError,AttributeError,OverflowError):
                    with store.lock:
                        store.invalid+=1
                        store.changed.notify_all()
            def receive_vn30(kind,obj):
                store=runtime.store()
                if store.failed:
                    return
                try:
                    EventSink(runtime).emit("stock",normalize_vn30_stock(
                        obj,kind,store.day,runtime.vn30_set))
                except (ValueError,TypeError,AttributeError,OverflowError):
                    with store.lock:
                        store.vn30_stock_invalid+=1
                        store.changed.notify_all()
            def receive_trade_extra(obj):
                if obj.symbol==runtime.symbol:
                    receive(1,obj)
                elif obj.symbol in runtime.vn30_set:
                    receive_vn30('trade',obj)
            def receive_security_definition(obj):
                if obj.symbol==runtime.symbol:
                    receive_reference(obj)
                elif obj.symbol in runtime.vn30_set:
                    receive_vn30('reference',obj)
            def receive_ohlcv(closed,obj):
                store=runtime.store()
                if store.failed:
                    return
                try:
                    sink.emit("candle",normalize_ohlcv_1m(
                        obj,runtime.symbol,store.day,closed,ohlc_symbol_type))
                except (ValueError,TypeError,AttributeError,OverflowError):
                    with store.lock:
                        store.ohlcv_invalid+=1
                        store.changed.notify_all()
            # Register before SUBSCRIBE so the first pushed records cannot race registration.
            client.on('trade_extra',receive_trade_extra)
            client.on('quote',partial(receive,0))
            client.on('market_index_influence',receive_influence)
            client.on('market_index',partial(receive_index,'market'))
            client.on('estimated_market_index',partial(receive_index,'estimated'))
            client.on('security_definition',receive_security_definition)
            client.on('ohlc',partial(receive_ohlcv,False))
            client.on('ohlc_closed',partial(receive_ohlcv,True))
            await asyncio.wait_for(client.connect(),20)
            await client.subscribe_trade_extra([runtime.symbol,*runtime.vn30_symbols],encoding='msgpack',board_id='G1')
            await client.subscribe_quotes([runtime.symbol],encoding='msgpack',board_id='G1')
            await client.subscribe_market_index('VN30',encoding='msgpack')
            await client.subscribe_estimated_market_index('VN30',encoding='msgpack')
            await client.subscribe_sec_def([runtime.symbol,*runtime.vn30_symbols],encoding='msgpack',board_id='G1')
            # Derivative OHLC is keyed by symbolType (VN30F1M), not contract ID.
            await client.subscribe_ohlc([ohlc_symbol_type],resolution='1',encoding='msgpack')
            await client.subscribe_ohlc_closed([ohlc_symbol_type],resolution='1',encoding='msgpack')
            await client.subscribe_market_index_influence('VN30',resolution=1,encoding='msgpack')
            runtime.connection('connected')
            delay=1
            while not runtime.stop.is_set():
                await asyncio.sleep(.5)
                store=runtime.store()  # rotate storage automatically at local midnight
                if store.failed:
                    raise RuntimeError('Storage unavailable')
                # SDK retries internally; supervise task death as well as SDK events.
                task=client._message_handler_task
                if task is None or task.done():
                    raise RuntimeError('SDK receiver stopped')
                if not client._connection.is_connected and runtime.status=='connected':
                    raise RuntimeError('SDK socket closed')
        except asyncio.CancelledError:
            break
        except Exception as exc:
            runtime.connection('error',type(exc).__name__)
            print('DNSE connection unavailable: '+type(exc).__name__,flush=True)
        finally:
            if client:
                try:
                    await asyncio.wait_for(client.disconnect(),10)
                except Exception:
                    pass
        if runtime.store().failed:
            break
        for _ in range(delay*2):
            if runtime.stop.is_set():
                break
            await asyncio.sleep(.5)
        delay=min(60,delay*2)
    if not runtime.store().failed:
        runtime.connection('stopped')
