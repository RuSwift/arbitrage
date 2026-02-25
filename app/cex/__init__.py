# CEX integrations (Binance, Bybit, OKX, KuCoin, HTX, MEXC, Bitfinex, etc.)

"""
Funding rate, funding schedule, index price — API comparison.

REST (Binance, Bybit, OKX, KuCoin)
---------------------------------
| What                    | Binance USD-M                          | Bybit (linear)                              | OKX                                          | KuCoin (Unified Account)                      |
|-------------------------|----------------------------------------|---------------------------------------------|----------------------------------------------|-----------------------------------------------|
| Current funding rate    | GET /fapi/v1/premiumIndex              | GET /v5/market/tickers?category=linear      | GET /api/v5/public/funding-rate               | GET /api/ua/v1/market/funding-rate            |
|                         | → lastFundingRate                      | → result.list[].fundingRate                 | → data[].fundingRate                          | → data.nextFundingRate                        |
| Next funding time       | GET /fapi/v1/premiumIndex              | GET /v5/market/tickers?category=linear      | GET /api/v5/public/funding-rate               | GET /api/ua/v1/market/funding-rate            |
|                         | → nextFundingTime                      | → result.list[].nextFundingTime             | → data[].nextFundingTime                      | → data.fundingTime                            |
| Funding time history    | GET /fapi/v1/fundingRate               | GET /v5/market/funding/history              | GET /api/v5/public/funding-rate-history      | GET /api/ua/v1/market/funding-rate-history    |
|                         | → array fundingTime                    | → fundingRateTimestamp                      | → array fundingTime                           | → data.list[].ts, fundingRate                 |
| Index price             | GET /fapi/v1/premiumIndex              | GET /v5/market/tickers?category=linear      | GET /api/v5/public/mark-price                 | GET /api/ua/v1/market/index-price             |
|                         | → indexPrice                           | → result.list[].indexPrice                  | → data[].idxPx                                | → data.items[].indexPrice                     |
| Base URL                | https://fapi.binance.com               | https://api.bybit.com                       | https://api.okx.com                           | https://api.kucoin.com                        |
| Inst/symbol             | BTCUSDT                                | BTCUSDT                                     | instId: BTC-USDT-SWAP                         | symbol: .XBTUSDTMFPI8H / XBTUSDTM             |

REST (HTX, MEXC, Bitfinex)
--------------------------
| What                    | HTX (Huobi linear)                     | MEXC                                        | Bitfinex                                     |
|-------------------------|----------------------------------------|---------------------------------------------|-----------------------------------------------|
| Current funding rate    | GET /linear-swap-api/v1/swap_funding_rate | GET /api/v1/contract/funding_rate/{symbol}  | GET /v2/status/deriv?keys=tBTCF0:USTF0       |
|                         | → data[].funding_rate                 | → data.fundingRate                          | → CURRENT_FUNDING (index 12)                  |
| Next funding time       | (deprecated: next_funding_time null)   | GET .../funding_rate/{symbol}                | GET /v2/status/deriv                          |
|                         |                                        | → data.nextSettleTime                        | → NEXT_FUNDING_EVT_MTS (index 8)               |
| Funding time history    | GET .../swap_historical_funding_rate  | GET /api/v1/contract/funding_rate/history   | GET /v2/status/deriv/{key}/hist               |
|                         | → data                                | → data.resultList[].settleTime, fundingRate | (or funding/stats/{symbol}/hist for FRR)      |
| Index price             | (market data / contract info)         | GET /api/v1/contract/index_price/{symbol}   | GET /v2/status/deriv → SPOT_PRICE (index 4)   |
|                         |                                        | → data.indexPrice                            | MARK_PRICE index 15                            |
| Base URL                | https://api.hbdm.com                   | https://api.mexc.com                        | https://api-pub.bitfinex.com                   |
| Inst/symbol             | contract_code: BTC-USDT               | symbol: BTC_USDT                            | keys: tBTCF0:USTF0 (perpetual)                |

WebSocket (Binance, Bybit, OKX, KuCoin)
---------------------------------------
| What                    | Binance USD-M                          | Bybit (linear)                              | OKX                                          | KuCoin (Classic Futures)                      |
|-------------------------|----------------------------------------|---------------------------------------------|----------------------------------------------|-----------------------------------------------|
| URL                     | wss://fstream.binance.com              | wss://stream.bybit.com/v5/public/linear    | wss://ws.okx.com:8443/ws/v5/public           | wss://ws-api-futures.kucoin.com               |
| Topic / channel         | <symbol>@markPrice or @markPrice@1s    | tickers.{symbol}                            | mark-price, funding-rate (args: instId)       | /contract/instrument:{symbol}                 |
| Interval                | 3000 ms or 1000 ms                     | 100 ms                                      | per channel                                   | mark.index.price 1s, funding.rate 1min        |
| Funding rate            | r                                      | data.fundingRate                            | funding-rate channel → fundingRate            | subject funding.rate → data.fundingRate       |
| Next funding time       | T                                      | data.nextFundingTime                        | funding-rate channel → nextFundingTime        | REST only (fundingTime)                       |
| Index price             | i                                      | data.indexPrice                             | mark-price → idxPx                            | subject mark.index.price → data.indexPrice    |
| Mark price              | p                                      | data.markPrice                              | mark-price → markPx                           | subject mark.index.price → data.markPrice     |
| Symbol format           | BTCUSDT                                | BTCUSDT                                     | BTC-USDT-SWAP                                 | XBTUSDTM                                      |

WebSocket (HTX, MEXC, Bitfinex)
-------------------------------
| What                    | HTX (Huobi linear)                     | MEXC                                        | Bitfinex                                     |
|-------------------------|----------------------------------------|---------------------------------------------|-----------------------------------------------|
| URL                     | wss://api.hbdm.com/linear-swap-ws      | wss://contract.mexc.com/edge                | wss://api-pub.bitfinex.com/ws/2                |
| Topic / channel         | public.$contract_code.funding_rate     | SubscribeFundingRate, SubscribeIndexPrice,  | subscribe status (key: deriv:tBTCF0:USTF0)     |
|                         |                                        | SubscribeFairPrice                          |                                               |
| Funding rate            | funding_rate in push                   | funding rate in push                        | CURRENT_FUNDING, NEXT_FUNDING_EVT_MTS          |
| Next funding time       | (deprecated in REST; not in WS)        | nextSettleTime if in ticker/funding push    | NEXT_FUNDING_EVT_MTS                           |
| Index / mark price      | (separate market WS or REST)           | SubscribeIndexPrice, SubscribeFairPrice    | MARK_PRICE, SPOT_PRICE in status               |
| Symbol format           | BTC-USDT                              | BTC_USDT                                    | tBTCF0:USTF0                                  |
"""
