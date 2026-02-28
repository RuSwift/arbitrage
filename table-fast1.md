WebSocket benchmark: event rate vs symbol count (step +10, measure 15s)

*n_symbols ограничен числом доступных пар на бирже: max n = min(--max-symbols, available).*

| exchange | kind | n_symbols | events_total | events/s | ticker (n/s) | bookdepth (n/s) | kline (n/s) | avg_gap_ms | max_gap_ms | status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| gate | spot | 10 | 160 | 10.7 | 57/3.8 | 103/6.9 | 0/0 | 91.6 | 549.2 | ok |
| gate | perpetual | 10 | 173 | 11.5 | 75/5.0 | 98/6.5 | 0/0 | 85.0 | 588.7 | ok |

Summary:
gate spot: last_working n=10, available=141
gate perpetual: last_working n=10, available=127

*Статистика: ticker = book_ticker, bookdepth = order_book_update; kline по WS в коннекторах не передаётся (0).*
