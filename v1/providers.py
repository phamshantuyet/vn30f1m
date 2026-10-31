"""Provider registry. SDK imports and capability differences stay here."""
from dataclasses import dataclass
from typing import Callable

@dataclass(frozen=True)
class Provider:
    collect: Callable
    capabilities: dict
    bootstrap: Callable | None = None
    credential_prefix: str = 'SSI'
    stream_url_env: str = 'SSI_STREAM_URL'
    default_stream_url: str | None = None


def select_provider(name):
    if name=='hybrid':
        from hybrid_provider import collect
        return Provider(collect,dict.fromkeys(('quote','trade','reference','candle','stocks','index','breadth','estimated_index','contribution'),True))
    if name=='dnse':
        from dnse_provider import collect, bootstrap_vn30_stocks
        return Provider(collect,dict.fromkeys(('quote','trade','reference','candle','stocks','index','breadth','estimated_index','contribution'),True),bootstrap_vn30_stocks,'DNSE','DNSE_WSS_URL','wss://ws-openapi.dnse.com.vn')
    if name=='ssi-sdk':
        from ssi_provider import collect
        return Provider(collect,{'quote':True,'trade':True,'reference':True,'candle':True,'stocks':True,
                                'index':True,'breadth':True,'estimated_index':False,'contribution':False})
    raise ValueError('Unsupported ORDERFLOW_SDK: '+name)
