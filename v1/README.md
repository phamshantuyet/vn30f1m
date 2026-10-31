# DOM latency v1

Standalone runtime and assets inside v1.

Install: `.venv/bin/python -m pip install -r v1/requirements.txt`

Run: `bash v1/run.sh --port 8097`

The source selector at the top of the page offers SSI + DNSE, SSI, or DNSE.
Press Connect to restart the connection and reload the page. The selected
provider is saved in `v1/data/provider.json`; each provider uses its own data
directory. An explicit ORDERFLOW_SDK environment value overrides the saved
selection on startup.

Default provider: `ssi-sdk` (3.3.2). Select both using `ORDERFLOW_SDK=hybrid`,
or DNSE alone using `ORDERFLOW_SDK=dnse`. Hybrid requires both sets of API keys.
SSI supplies quotes, trades, candles, stocks and index data when its subscriptions
are active. DNSE supplies estimated index and per-symbol contributions, and fills
any SSI channel whose subscription is unavailable or whose connection is down.
Only the selected source for a channel is written to the shared hybrid database.
SSI uses SSI_API_KEY and SSI_API_SECRET in the project .env. Market-data authentication does not use an OTP. The private key is kept in SSI_PRIVATE_KEY for SDK configuration.

All boxes consume common API snapshots and event channels, not SDK objects.
`events.py` is the delivery boundary; `providers.py` declares source capabilities.
`dnse_provider.py` and `ssi_provider.py` convert source payloads, timestamps and units.

| Data channel | DNSE | SSI SDK 3 |
| --- | --- | --- |
| Quotes / trades / speed / large trades | Yes | Yes |
| Reference / price change | Yes | Yes |
| One-minute candles / volume alerts / candle history | Yes | Yes |
| VN30 constituent price table | Yes | Yes |
| Official VN30 / breadth / basis | Yes | Yes (`trade.index.VN30`, `indexsummary.VN30`; summary scope required for breadth) |
| Estimated VN30 | Yes | Not supplied by SSI |
| Per-symbol index contribution / historical extrema | Yes | Not supplied by this adapter |

Unsupported boxes display a source capability message and do not request missing streams.
A supported channel without ticks still displays waiting for data. Sources never fill
unavailable official index contributions by guessing them from stock price changes.
Futures/index prices are points, constituent prices are thousand VND, quantities are
individual shares/contracts, all event times are UTC epoch milliseconds.

`ORDERFLOW_SYMBOL` (or `--symbol`) chooses the instrument. `--ohlc-symbol-type` is a
DNSE adapter setting; SSI subscribes one-minute bars by instrument symbol.

Contract and routing checks: `.venv/bin/python -m unittest discover -s v1 -p 'test_*.py'`

SSI market subscriptions follow https://developers.ssi.com.vn/docs/api-reference/subscribeUnsubscribeMarketData. Stock trade prices arrive in VND and are converted to thousand VND; market reference and quote prices already use thousand VND.

SSI SDK usage follows `../docs/ssi-sdk-python-main/ssi-sdk-python-main/AGENT.md`
and `../docs/ssi-sdk-python-main/ssi-sdk-python-main/README.md` (version 3.3.2).
The adapter uses `AsyncAuth`/`AsyncStream`, verifies accepted topics through
`list_subscription()`, follows SDK connection events, and stops on a revoked
session or denied required market data scope.
