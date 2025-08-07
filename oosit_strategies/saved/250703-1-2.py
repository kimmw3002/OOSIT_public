import copy
import numpy as np

_explanation = r"""
베이스 전략 250702-1-3:

대표전략 250604-1-2에서 dynamic leverage 조건을 252영업일간의 ‘주가’ 괴리율이 아니라, 252영업일 전의 MA200과 현재의 주가의 괴리율로 따지도록 수정하고, Threshold(변수명: last_year_qqq_threshold)도 10%가 아니라 15%로 상향함.

현재 전략 250703-1-2:

DEF MODE에서 3년 전200일 이평선 대비 현재 주가 (SPY) 값의 수준을 비교하여 구간마다 다른 기준 적용.

200MA와 괴리(%) || 레버리지 비율

5 미만 -1 / 0

5 ~ 10 0 / 0.5

10 이상 1 / 1

오른쪽 수치는 3년 전 200일 이평선 대비 현재 주가 (SPY)값의 상승률이 33% + 5% = 38% (1.1^3 - 1 = 33%) 미만일 경우 적용. 같은 DEF 모드 내에서 오른쪽 수치와 왼쪽 수치 간 변경 허용. (200MA 괴리 5% 미만일 때 Defense mode allocation 변동이 Normal에서 Defense 변화 최소 시에만 한번만 발동하는 로직은 유지)
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
             normal_dynamic_leverage = {
                 'last_year_qqq_underperform': 2.5,
                 'last_year_qqq_outperform': 1.5,
                 'last_year_qqq_unknown': 2.0,
                 'last_year_qqq_threshold': 0.15,
                 'lookback_days': 252,
             },
             defense_dynamic_lookback_days = 252 * 3,
             defense_dynamic_threshold = 0.38,
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
        nonlocal current_stocks, current_cash, current_mode, in_psq_defense_mode
        if mode_before == mode_after:
            return
        else:
            # Only activate PSQ when Normal -> Defense transition occurs
            if mode_before == 'Normal' and mode_after == 'Defense':
                in_psq_defense_mode = True
            
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

        # DXY switching logic, when true then skip the rest of the loop
        if i > 0 and (get_value(ma_ticker, i) > get_value(ma_ticker, i, center_ma_string)
                and get_value('DX-Y.NYB', i) > get_value('DX-Y.NYB', i, center_ma_string)
                and get_value('DX-Y.NYB', i-1) <= get_value('DX-Y.NYB', i-1, center_ma_string)):
            rebalance(net_worth, i, current_mode, 'Aggressive')
        else:
            # If DXY condition is not met, continue with the other mode switching logic
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

        # 2. If the PSQ phase is active, check if we need to STOP it.
        if in_psq_defense_mode:
            ma_ticker_price = get_value(ma_ticker, i)
            ma_ticker_ma200_price = get_value(ma_ticker, i, center_ma_string)
            # The latching "off" condition:
            if ma_ticker_price < ma_ticker_ma200_price * (1 - psq_defense_exit_threshold):
                in_psq_defense_mode = False # Turn off the special mode for the rest of this Defense cycle.
        
        # 3. Apply the correct 'Defense' mode rules for today.
        defense_allocations = [{'PSQ': 1.0}, {}, {'QQQ': 1.0}]
        try:
            lookback_qqq_return_def = ((get_value('QQQ', i) - get_value('QQQ', i - defense_dynamic_lookback_days, center_ma_string))
                                    / get_value('QQQ', i - defense_dynamic_lookback_days, center_ma_string))
            if lookback_qqq_return_def < defense_dynamic_threshold:
                # if the return is low, change defense allocation logic
                defense_allocations = [{}, {'QQQ': 0.5}, {'QQQ': 1.0}]
        except IndexError:
            # lookback data not available, keep the basic values
            pass

        if in_psq_defense_mode:
            # Rule (1): While in the special mode, the 'Defense' allocation is 100% PSQ.
            reallocate('Defense', defense_allocations[0], net_worth, i, current_mode)
        else:
            # Rule (2): Otherwise, use the standard dynamic logic for the 'Defense' mode.
            qqq_defense_condition = (
                # the following line is commented out because it is not used in this version (SPYMAX gap logic)
                # get_value(ma_ticker, i) < get_value(ma_ticker, i, 'MAX') * (1 - max_drop_threshold_for_qqq_defense) and
                get_value(ma_ticker, i) < get_value(ma_ticker, i, center_ma_string) * (1 - ma200_gap_threshold_for_qqq_defense)
            )
            if qqq_defense_condition:
                reallocate('Defense', defense_allocations[2], net_worth, i, current_mode)
            else:
                reallocate('Defense', defense_allocations[1], net_worth, i, current_mode)

    return date_range, portfolio_value, rebalancing_track
