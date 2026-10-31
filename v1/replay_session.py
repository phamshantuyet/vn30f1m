"""Display policy for the user-configured 11:30–13:00 pause (UTC+7).
Keep recorded data visible; never synthesize trades or extend signal validity.
"""
BREAK_START_SECONDS=11*3600+30*60
BREAK_END_SECONDS=13*3600
STALE_SECONDS=30

def display_policy(timestamp_ms,quote_ts,valid_book,last_data_ts):
    local_seconds=((timestamp_ms+7*3600*1000)%(24*3600*1000))/1000
    in_break=BREAK_START_SECONDS<=local_seconds<BREAK_END_SECONDS
    age=None if quote_ts is None else (timestamp_ms-quote_ts)/1000
    held=bool(in_break and valid_book and age is not None and age>STALE_SECONDS)
    hold_view=in_break and last_data_ts is not None and timestamp_ms-last_data_ts>STALE_SECONDS*1000
    return {'sessionBreak':in_break,'bookHeld':held,
            'view_ts':last_data_ts if hold_view else timestamp_ms}
