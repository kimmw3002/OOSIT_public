import copy
import numpy as np

_explanation = r"""
대표전략 250703-3-4를 기반으로, 다음을 추가:
DEF MODE에서 200일 이평선 대비 현재 주가 (SPY) 값의 괴리율을 비교하여 구간마다 다른 레버리지 적용:

200MA와 괴리(%) || 레버리지 비율
- 5 미만: -1 (PSQ)
- 5 ~ 10: 0 (현금)
- 10 ~ 15: 1 또는 2
- 15 이상: 2

여기서 5 ~ 10% 구간에서 10 ~ 15% 구간으로 넘어갈 때는 1 레버리지로 하고, 15% 이상에서 다시 10 ~ 15%로 돌아갈 때는 2 레버리지로 작동함.
"""

def backtest(start_date, end_date, get_nyse_open_dates, initialize_get_value,
             using_tickers = ['QQQ', 'TQQQ', 'PSQ'],    # NOTE: do NOT include "Cash"!!
             modes = {
                 # shall assume all unused money are in cash.
                 # NOTE: do NOT explicitly include "Cash" in the modes!
                 'Normal': {'QQQ': 0.5, 'TQQQ': 0.5},
                 'Defense': {},
                 'Aggressive': {'TQQQ': 1.0},
                 'Unknown': {},  # This is a fallback mode, not used in the logic. Dont delete it.
             },
             seed = 1.0, low_ma = 25, center_ma = 200, ma_ticker = 'SPY',
             # spymax gap for qqq defense not used in this version, but kept for future reference
             # max_drop_threshold_for_qqq_defense = 0.20,
             ma200_gap_threshold_for_qqq_defense = 0.10,
             psq_defense_exit_threshold = 0.05,
             defense_leverage_threshold_mid = 0.10,   # Below this: cash (0x)
             defense_leverage_threshold_high = 0.15,  # 10-15%: 1x or 2x, 15%+: 2x
             normal_dynamic_leverage = {
                 'last_year_qqq_underperform': 2.5,
                 'last_year_qqq_outperform': 1.5,
                 'last_year_qqq_unknown': 2.0,
                 'last_year_qqq_threshold': 0.15,
                 'lookback_days': 252,
             },
             aggressive_dynamic_leverage = {
                 'lookback_days': 252 * 3,  # 3 years
                 'low_threshold': 0.40,     # 40% threshold
                 'high_threshold': 0.60,    # 60% threshold
                 'high_leverage': 3.0,      # <40% gap: 3.0x leverage
                 'medium_leverage': 2.5,    # 40-60% gap: 2.5x leverage
                 'low_leverage': 2.0,       # >60% gap: 2.0x leverage
                 'default_leverage': 2.5,   # fallback when 3-year data not available
             },
             ):
    
    # NOTE: prevents weird bugs, dont modify this line. Better to have this line.
    modes = copy.deepcopy(modes)

    def reallocate(updating_mode, reallocation_dict, net_worth, idx, current_mode, force_trigger = False):
        nonlocal current_stocks, current_cash
        if modes[updating_mode] == reallocation_dict and not force_trigger:
            # NOTE: if the reallocation ratio is the same as the current mode "code-written" ratio, do nothing
            # by time, change of stock prices will change the "true" portfolio allocation from the "initial" allocation
            # to avoid unrealistic, daily reallocation, generally used with force_trigger=False
            # to FORCE reallocation, set "force_trigger=True": this can be useful when the user wants to force a portfolio reallocation
            return

        # if updating mode is the current mode, update current stocks based on reallocation
        # and write down to the rebalancing track log
        if updating_mode == current_mode:
            # Reallocate current stocks based on the reallocation_dict
            current_stocks = {ticker: 0.0 for ticker in using_tickers}
            for ticker in using_tickers:
                if ticker in reallocation_dict:
                    current_stocks[ticker] = net_worth * reallocation_dict[ticker] / current_stock_prices[ticker]

            current_cash = net_worth - sum(current_stocks[ticker] * current_stock_prices[ticker] for ticker in using_tickers)
            # save to the rebalancing track
            rebalancing_track.append((date_range[idx], f'{current_mode} {str(modes[current_mode])}', f'{current_mode} {str(reallocation_dict)}'))

        # update the updating mode allocation
        modes[updating_mode] = reallocation_dict

    def rebalance(net_worth, idx, mode_before, mode_after):
        nonlocal current_stocks, current_cash, current_mode, in_psq_defense_mode, was_below_10_percent
        if mode_before == mode_after:
            return
        else:
            # Only activate PSQ when Normal -> Defense transition occurs
            if mode_before == 'Normal' and mode_after == 'Defense':
                in_psq_defense_mode = True
                # Reset DEF mode tracking variables
                was_below_10_percent = True
            
            # Reset tracking when exiting Defense mode
            if mode_before == 'Defense' and mode_after != 'Defense':
                was_below_10_percent = True
            
            # Rebalance to the new mode
            current_stocks = {ticker: 0.0 for ticker in using_tickers}
            for ticker in using_tickers:
                if ticker in modes[mode_after].keys():
                    current_stocks[ticker] = net_worth * modes[mode_after][ticker] / current_stock_prices[ticker]

            current_cash = net_worth - sum(current_stocks[ticker] * current_stock_prices[ticker] for ticker in using_tickers)
            current_mode = mode_after

            # save to the rebalancing track
            rebalancing_track.append((date_range[idx], f'{mode_before} {str(modes[mode_before])}', f'{mode_after} {str(modes[mode_after])}'))

    current_mode = 'Unknown'    # Initial mode
    net_worth = seed  # Initial net worth is seed value

    # how many stocks you have
    current_stocks = {ticker: 0.0 for ticker in using_tickers}
    # how much cash you have: start with all cash
    current_cash = seed

    # This flag tracks if we are currently in the special PSQ phase.
    in_psq_defense_mode = False
    
    # Track if we were *below* 10% (for re-entry logic)
    was_below_10_percent = True

    # basic template
    date_range = get_nyse_open_dates(start_date, end_date)
    get_value = initialize_get_value(start_date)
    portfolio_value = np.zeros(len(date_range))
    rebalancing_track = []

    low_ma_string = f'MA{low_ma}'
    center_ma_string = f'MA{center_ma}'

    for i in range(len(date_range)):
        current_stock_prices = {ticker: get_value(ticker, i) for ticker in using_tickers}
        net_worth = sum(current_stocks[ticker] * current_stock_prices[ticker] for ticker in using_tickers)
        net_worth += current_cash
        portfolio_value[i] = net_worth

        if current_mode == 'Normal':
            if get_value(ma_ticker, i) < get_value(ma_ticker, i, center_ma_string):
                rebalance(net_worth, i, current_mode, 'Defense')
                
        elif current_mode == 'Defense':
            if get_value(ma_ticker, i) > get_value(ma_ticker, i, center_ma_string):
                rebalance(net_worth, i, current_mode, 'Normal')
            if get_value(ma_ticker, i, low_ma_string) < get_value(ma_ticker, i) < get_value(ma_ticker, i, center_ma_string):
                rebalance(net_worth, i, current_mode, 'Aggressive')

        elif current_mode == 'Aggressive':
            if get_value(ma_ticker, i) < get_value(ma_ticker, i, low_ma_string) < get_value(ma_ticker, i, center_ma_string):
                rebalance(net_worth, i, current_mode, 'Defense')
            if abs(get_value(ma_ticker, i) - get_value(ma_ticker, i, 'MAX')) < 1e-6: # Check if the current price is the maximum
                rebalance(net_worth, i, current_mode, 'Normal')

        # fail case: e.g. 'Unknown' mode at start
        else:
            if get_value(ma_ticker, i) > get_value(ma_ticker, i, center_ma_string):
                rebalance(net_worth, i, current_mode, 'Normal')
            elif get_value(ma_ticker, i, low_ma_string) < get_value(ma_ticker, i) < get_value(ma_ticker, i, center_ma_string):
                rebalance(net_worth, i, current_mode, 'Aggressive')
            else:
                rebalance(net_worth, i, current_mode, 'Defense')

        # dynamic leverage logic for 'Normal' mode
        try:
            lookback_qqq_return = ((get_value('QQQ', i) - get_value('QQQ', i - normal_dynamic_leverage['lookback_days'], center_ma_string))
                                    / get_value('QQQ', i - normal_dynamic_leverage['lookback_days'], center_ma_string))
            if lookback_qqq_return < normal_dynamic_leverage['last_year_qqq_threshold']:
                normal_mode_leverage = normal_dynamic_leverage['last_year_qqq_underperform']
            else:
                normal_mode_leverage = normal_dynamic_leverage['last_year_qqq_outperform']
        except IndexError:
            # lookback data not available, use default
            normal_mode_leverage = normal_dynamic_leverage['last_year_qqq_unknown']
        
        # reallocate the 'Normal' mode based on the dynamic leverage
        normal_qqq_alloc = (3.0 - normal_mode_leverage) / 2.0
        normal_tqqq_alloc = 1 - normal_qqq_alloc
        reallocate('Normal', {'QQQ': normal_qqq_alloc, 'TQQQ': normal_tqqq_alloc}, net_worth, i, current_mode)

        # NEW: Dynamic leverage logic for 'Aggressive' mode based on 3-year MA200 gap
        try:
            # Get MA200 price from 3 years ago
            three_year_ago_ma200 = get_value(ma_ticker, i - aggressive_dynamic_leverage['lookback_days'], center_ma_string)
            current_price = get_value(ma_ticker, i)
            
            # Calculate 3-year gap from MA200
            three_year_gap = (current_price - three_year_ago_ma200) / three_year_ago_ma200
            
            # Determine leverage based on gap thresholds
            if three_year_gap >= 0.65:
                agg_leverage = 2.0
            elif three_year_gap >= 0.45:
                agg_leverage = 2.5
            else:
                agg_leverage = 3.0
        except IndexError:
            # 3-year data not available, use default
            agg_leverage = aggressive_dynamic_leverage['default_leverage']
        
        agg_tqqq_alloc = (agg_leverage - 1.0) / 2.0  
        agg_qqq_alloc = 1.0 - agg_tqqq_alloc
        reallocate('Aggressive', {'QQQ': agg_qqq_alloc, 'TQQQ': agg_tqqq_alloc}, net_worth, i, current_mode)

        # 2. If the PSQ phase is active, check if we need to STOP it.
        if in_psq_defense_mode:
            ma_ticker_price = get_value(ma_ticker, i)
            ma_ticker_ma200_price = get_value(ma_ticker, i, center_ma_string)
            # The latching "off" condition:
            if ma_ticker_price < ma_ticker_ma200_price * (1 - psq_defense_exit_threshold):
                in_psq_defense_mode = False # Turn off the special mode for the rest of this Defense cycle.
        
        # 3. Apply the correct 'Defense' mode rules for today.
        defense_allocation = {}  # Default allocation
        
        # Skip PSQ logic if not in PSQ defense mode
        if not in_psq_defense_mode:
            try:
                # Calculate MA200 gap percentage
                ma200_gap_percentage = ((get_value(ma_ticker, i, center_ma_string) - get_value(ma_ticker, i)) 
                                        / get_value(ma_ticker, i, center_ma_string))
                
                # Update tracking variable
                if ma200_gap_percentage >= defense_leverage_threshold_high:
                    was_below_10_percent = False  # Reset when at high threshold or above
                elif ma200_gap_percentage < defense_leverage_threshold_mid:
                    was_below_10_percent = True
                
                # Determine defense allocation based on MA200 gap
                if ma200_gap_percentage < defense_leverage_threshold_mid:  # Below mid threshold (e.g., 10%)
                    defense_allocation = {}  # 0 leverage (cash)
                elif ma200_gap_percentage < defense_leverage_threshold_high:  # Mid to high threshold (e.g., 10-15%)
                    if was_below_10_percent:
                        # Coming from below mid threshold, use 1x leverage
                        defense_allocation = {'QQQ': 1.0}
                    else:
                        # From high threshold going down, use 2x leverage
                        defense_allocation = {'QQQ': 0.5, 'TQQQ': 0.5}
                else:  # High threshold or more (e.g., 15%+)
                    defense_allocation = {'QQQ': 0.5, 'TQQQ': 0.5}  # 2x leverage
                        
            except IndexError:
                # lookback data not available, use default empty allocation
                pass
        
        # Apply the allocation
        if in_psq_defense_mode:
            # Rule (1): While in the special PSQ mode, the 'Defense' allocation is 100% PSQ.
            reallocate('Defense', {'PSQ': 1.0}, net_worth, i, current_mode)
        else:
            # Rule (2): Use the dynamic allocation we calculated above
            reallocate('Defense', defense_allocation, net_worth, i, current_mode)

    return date_range, portfolio_value, rebalancing_track
