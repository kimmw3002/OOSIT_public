import copy
import numpy as np

always_ticker = 'TQQQ'

_explanation = f'ALL {always_ticker}'

def backtest(start_date, end_date, get_nyse_open_dates, initialize_get_value,
             using_tickers = [always_ticker],    # NOTE: do NOT include "Cash"!!
             seed = 1.0,
             ):

    # basic template
    date_range = get_nyse_open_dates(start_date, end_date)
    get_value = initialize_get_value(start_date)
    portfolio_value = np.zeros(len(date_range))
    rebalancing_track = []

    ticker_count = seed / get_value(always_ticker, 0)

    for i in range(len(date_range)):
        portfolio_value[i] = ticker_count * get_value(always_ticker, i)

    return date_range, portfolio_value, rebalancing_track
