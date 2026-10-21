# BUY volume and speed: afternoon 16/10/2026

Exploratory ablation on 41I1GB000. Every second from 13:00 through 14:45; shared v1 quality gates and 120s warmup.

For W = 5, 10, 30s: Rv = 3 × BUY quantity(t-W,t] / BUY quantity(t-4W,t-W]. Rn uses BUY match count instead. Volume signal: Rv < 0.5. Combined signal: Rv < 0.5 AND Rn < 0.5. Baseline must be positive.

Contracts/second is volume divided by time. Match frequency measures a separate component. Rv = Rn × (current mean match size / baseline mean match size) when counts are positive.

| Variant | Complete spaced events | Lower 1m | Lower 3m | Lower 5m | Mean Δmid 5m |
|---|---:|---:|---:|---:|---:|
| volume_5 | 16 | 43.8% | 50.0% | 43.8% | -0.053 |
| volume_frequency_5 | 16 | 43.8% | 56.2% | 50.0% | -0.094 |
| volume_10 | 15 | 53.3% | 80.0% | 66.7% | -0.247 |
| volume_frequency_10 | 14 | 50.0% | 78.6% | 57.1% | -0.232 |
| volume_30 | 14 | 64.3% | 64.3% | 71.4% | -0.129 |
| volume_frequency_30 | 13 | 46.2% | 53.8% | 53.8% | -0.138 |
| full_v1 | 15 | 33.3% | 60.0% | 46.7% | -0.107 |

All onsets (overlapping future horizons; sensitivity check):

| Variant | N at 5m | Lower 1m | Lower 3m | Lower 5m | Mean Δmid 5m |
|---|---:|---:|---:|---:|---:|
| volume_5 | 270 | 47.0% | 51.2% | 50.0% | +0.012 |
| volume_frequency_5 | 242 | 45.3% | 50.4% | 48.3% | +0.040 |
| volume_10 | 145 | 47.2% | 57.2% | 46.9% | +0.052 |
| volume_frequency_10 | 115 | 44.4% | 56.3% | 47.0% | +0.017 |
| volume_30 | 87 | 51.7% | 55.2% | 54.0% | -0.057 |
| volume_frequency_30 | 62 | 60.9% | 64.5% | 58.1% | -0.262 |
| full_v1 | 101 | 44.8% | 55.4% | 49.5% | +0.064 |

The apparent strength of volume_10 in the spaced sample does not persist across all onsets: 5m lower rate is 46.9% across 145 available onsets versus 66.7% across 15 spaced complete events. Event selection materially changes the result.

Events are first onsets spaced at least 5 minutes per variant, selected before checking future label availability. Different variants select different times, so this is descriptive, not a matched experiment.

The full v1 reference is recalculated with the common 120s warmup; it differs from the earlier 40s-warmup report. The existing full-v1 report remains unchanged.

These variants were proposed after seeing the afternoon. This session is not a new independent holdout. A fall in BUY volume or frequency is an observed slowdown, not proof of a future price decline. One session and few events cannot establish predictive reliability.

Features and decisions are saved before forward labels. Independent verification rereads raw Tape and uses SQL sums/counts, without importing buy_speed or using the cache. Quote quality and future labels reuse the raw oracle whose 6,301 samples were already independently checked.

Run: `python3 orderflow-demo/backtest_buy_speed.py`, then `python3 orderflow-demo/verify_buy_speed_raw.py`. See `_predictions.csv`, `_alerts.csv`, `_spaced.csv`, `_summary.json` and `_verification.json`.
