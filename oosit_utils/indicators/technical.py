"""
Technical indicators module.

This module provides computation of various technical indicators used in
financial analysis and trading strategies.
"""

import pandas as pd
import numpy as np
import re
import logging

logger = logging.getLogger(__name__)


class TechnicalIndicators:
    """Computes technical indicators for financial data."""
    
    def __init__(self, dataframe, source, default_labels, 
                 max_lookback_days=400):
        """
        Initialize technical indicators calculator.
        
        Args:
            dataframe: DataFrame with OHLCV data
            source: Data source (yfinance, MacroMicro, FRED, etc.)
            default_labels: Default column labels by source
            max_lookback_days: Maximum days to look back for MAX calculations (-1 for unlimited)
        """
        self.df = dataframe
        self.source = source
        self.default_labels = default_labels
        self.max_lookback_days = max_lookback_days
    
    def compute_indicator(self, indicator_name):
        """
        Compute the specified technical indicator.
        
        Args:
            indicator_name: Name of the indicator to compute
            
        Returns:
            List of indicator values or None if indicator not recognized
        """
        # Maximum value
        if indicator_name == 'MAX':
            return self._compute_max()
        
        # Moving average
        ma_match = re.match(r'^MA(\d+)$', indicator_name)
        if ma_match:
            period = int(ma_match.group(1))
            return self._compute_moving_average(period)
        
        # Standard deviation
        stdev_match = re.match(r'^STDEV(\d+)$', indicator_name)
        if stdev_match:
            period = int(stdev_match.group(1))
            return self._compute_moving_std(period)
        
        # RSI
        rsi_match = re.match(r'^RSI(\d+)?$', indicator_name)
        if rsi_match:
            period = int(rsi_match.group(1)) if rsi_match.group(1) else 14
            return self._compute_rsi(period)
        
        # RSI EMA
        rsi_ema_match = re.match(r'^RSI(\d+)? EMA$', indicator_name)
        if rsi_ema_match:
            rsi_period = int(rsi_ema_match.group(1)) if rsi_ema_match.group(1) else 14
            rsi_values = self._compute_rsi(rsi_period)
            return self._compute_ema(rsi_values, 9)
        
        # Stochastic %K
        k_match = re.match(r'^%K(?:(\d+)(?:,(\d+))?)?$', indicator_name)
        if k_match:
            k_period = int(k_match.group(1)) if k_match.group(1) else 14
            slowing_period = int(k_match.group(2)) if k_match.group(2) else 3
            k_values, _ = self._compute_stochastic(k_period, slowing_period, 3)
            return k_values
        
        # Stochastic %D
        d_match = re.match(r'^%D(?:(\d+)(?:,(\d+)(?:,(\d+))?)?)?$', indicator_name)
        if d_match:
            k_period = int(d_match.group(1)) if d_match.group(1) else 14
            slowing_period = int(d_match.group(2)) if d_match.group(2) else 3
            d_period = int(d_match.group(3)) if d_match.group(3) else 3
            _, d_values = self._compute_stochastic(k_period, slowing_period, d_period)
            return d_values
        
        # MACD Line
        if indicator_name == 'MACD line':
            return self._compute_macd_line()
        
        # MACD Signal
        if indicator_name == 'MACD signal':
            return self._compute_macd_signal()
        
        # Directional Movement Index
        if indicator_name == '+DI':
            plus_di, _ = self._compute_dmi()
            return plus_di
        
        if indicator_name == '-DI':
            _, minus_di = self._compute_dmi()
            return minus_di
        
        return None
    
    def _compute_max(self):
        """Compute rolling maximum values."""
        default_col = self.default_labels.get(self.source, 'Value')
        
        if self.max_lookback_days > 0:
            return self.df[default_col].rolling(
                window=self.max_lookback_days, 
                min_periods=1
            ).max().tolist()
        else:
            return self.df[default_col].expanding(min_periods=1).max().tolist()
    
    def _compute_moving_average(self, period):
        """Compute moving average using Open/Close price logic."""
        try:
            # Try to use Open/Close logic for more accurate MA - vectorized version
            n = len(self.df)
            ma_series = np.full(n, np.nan)
            
            if n >= period:
                open_vals = self.df['Open'].values
                close_vals = self.df['Close'].values
                
                # Vectorized computation for all valid indices
                for i in range(period - 1, n):
                    # Sum of past closes + today's open
                    if i == 0:
                        average_val = open_vals[i]
                    else:
                        past_sum = np.sum(close_vals[max(0, i-(period-1)):i])
                        average_val = (open_vals[i] + past_sum) / period
                    ma_series[i] = average_val
            
            return ma_series.tolist()
        except KeyError:
            # Fallback to simple rolling mean
            default_col = self.default_labels.get(self.source, 'Value')
            return self.df[default_col].rolling(window=period).mean().tolist()
    
    def _compute_moving_std(self, period):
        """Compute moving standard deviation."""
        try:
            # Try to use Open/Close logic
            std_series = []
            for i in range(len(self.df)):
                if i < period - 1:
                    std_series.append(np.nan)
                else:
                    today_open = self.df.iloc[i]['Open']
                    past_closes = self.df.iloc[i-(period-1):i]['Close']
                    values = [today_open] + list(past_closes)
                    std_val = np.std(values)
                    std_series.append(std_val)
            return std_series
        except KeyError:
            # Fallback to simple rolling std
            default_col = self.default_labels.get(self.source, 'Value')
            return self.df[default_col].rolling(window=period).std().tolist()
    
    def _compute_rsi(self, period=14):
        """Compute Wilder's RSI."""
        try:
            prices = self.df['Close'].values
        except KeyError:
            prices = self.df[self.default_labels.get(self.source, 'Value')].values
        
        n = len(prices)
        
        # Vectorized calculation of deltas
        deltas = np.zeros(n)
        deltas[1:] = prices[1:] - prices[:-1]
        
        # Vectorized gains and losses
        gains = np.where(deltas > 0, deltas, 0.0)
        losses = np.where(deltas < 0, -deltas, 0.0)
        
        avg_gain = [np.nan] * n
        avg_loss = [np.nan] * n
        rsi = [np.nan] * n
        
        if n > period:
            # Initial averages
            initial_avg_gain = np.mean(gains[1:period + 1])
            initial_avg_loss = np.mean(losses[1:period + 1])
            
            avg_gain[period] = initial_avg_gain
            avg_loss[period] = initial_avg_loss
            
            # First RSI calculation
            if avg_loss[period] == 0:
                rsi[period] = 100.0
            else:
                rs = avg_gain[period] / avg_loss[period]
                rsi[period] = 100.0 - (100.0 / (1.0 + rs))
            
            # Wilder's smoothing
            for i in range(period + 1, n):
                avg_gain[i] = ((avg_gain[i - 1] * (period - 1)) + gains[i]) / period
                avg_loss[i] = ((avg_loss[i - 1] * (period - 1)) + losses[i]) / period
                
                if avg_loss[i] == 0:
                    rsi[i] = 100.0
                else:
                    rs = avg_gain[i] / avg_loss[i]
                    rsi[i] = 100.0 - (100.0 / (1.0 + rs))
        
        return rsi
    
    def _compute_ema(self, prices, period):
        """Compute Exponential Moving Average."""
        prices = np.asarray(prices, dtype=float)
        n = len(prices)
        ema_values = [np.nan] * n
        
        # Find first non-NaN index
        first_valid_idx = None
        for i in range(n):
            if not np.isnan(prices[i]):
                first_valid_idx = i
                break
        
        if first_valid_idx is None or (n - first_valid_idx) < period:
            return ema_values
        
        # Initial EMA (simple average of first 'period' valid points)
        valid_block = prices[first_valid_idx:first_valid_idx + period]
        valid_block_no_nan = valid_block[~np.isnan(valid_block)]
        
        if len(valid_block_no_nan) < period:
            return ema_values
        
        initial_ema = np.mean(valid_block_no_nan)
        init_ema_index = first_valid_idx + period - 1
        ema_values[init_ema_index] = initial_ema
        
        # EMA multiplier
        alpha = 2.0 / (period + 1.0)
        
        # Compute remaining EMA values
        for i in range(init_ema_index + 1, n):
            if np.isnan(prices[i]):
                ema_values[i] = ema_values[i - 1]  # Carry forward
            else:
                prev_ema = ema_values[i - 1]
                if not np.isnan(prev_ema):
                    ema_values[i] = (prices[i] - prev_ema) * alpha + prev_ema
        
        return ema_values
    
    def _compute_stochastic(self, k_period, slowing_period, d_period):
        """Compute Stochastic Oscillator %K and %D."""
        high_vals = self.df['High']
        low_vals = self.df['Low']
        close_vals = self.df['Close']
        n = len(self.df)
        
        fast_k = [np.nan] * n
        stoch_k_slow = [np.nan] * n
        stoch_d = [np.nan] * n
        
        # Calculate Fast %K
        for i in range(k_period - 1, n):
            window_low = min(low_vals.iloc[i - (k_period - 1):i + 1])
            window_high = max(high_vals.iloc[i - (k_period - 1):i + 1])
            
            if (window_high - window_low) == 0:
                fast_k[i] = 50.0  # Handle flat market
            else:
                fast_k[i] = 100.0 * (close_vals.iloc[i] - window_low) / (window_high - window_low)
        
        # Calculate Slow %K (smoothed Fast %K)
        if slowing_period == 1:
            stoch_k_slow = list(fast_k)
        else:
            start_slow_k = (k_period - 1) + (slowing_period - 1)
            for i in range(start_slow_k, n):
                sum_fast_k = sum(fast_k[i - j] for j in range(slowing_period) if pd.notna(fast_k[i - j]))
                valid_count = sum(1 for j in range(slowing_period) if pd.notna(fast_k[i - j]))
                
                if valid_count == slowing_period:
                    stoch_k_slow[i] = sum_fast_k / slowing_period
        
        # Calculate %D (SMA of Slow %K)
        first_valid_slow_k_idx = next((i for i, val in enumerate(stoch_k_slow) if pd.notna(val)), -1)
        
        if first_valid_slow_k_idx != -1:
            start_d = first_valid_slow_k_idx + (d_period - 1)
            for i in range(start_d, n):
                sum_slow_k = sum(stoch_k_slow[i - j] for j in range(d_period) if pd.notna(stoch_k_slow[i - j]))
                valid_count = sum(1 for j in range(d_period) if pd.notna(stoch_k_slow[i - j]))
                
                if valid_count == d_period:
                    stoch_d[i] = sum_slow_k / d_period
        
        return stoch_k_slow, stoch_d
    
    def _compute_macd_line(self):
        """Compute MACD line (12-day EMA - 26-day EMA)."""
        try:
            prices = self.df['Close']
        except KeyError:
            prices = self.df[self.default_labels.get(self.source, 'Value')]
        
        n = len(prices)
        short_ema = self._compute_simple_ema(prices, 12)
        long_ema = self._compute_simple_ema(prices, 26)
        
        macd_values = [np.nan] * n
        for i in range(n):
            if pd.notna(short_ema[i]) and pd.notna(long_ema[i]):
                macd_values[i] = short_ema[i] - long_ema[i]
        
        return macd_values
    
    def _compute_macd_signal(self):
        """Compute MACD signal line (9-day EMA of MACD)."""
        macd_values = self._compute_macd_line()
        return self._compute_ema(macd_values, 9)
    
    def _compute_simple_ema(self, prices, period):
        """Compute simple EMA for MACD calculation."""
        n = len(prices)
        ema = [np.nan] * n
        
        if n >= period:
            # Initial simple average
            initial_sum = sum(prices.iloc[:period])
            ema[period - 1] = initial_sum / period
            
            # EMA multiplier
            alpha = 2.0 / (period + 1)
            
            # Subsequent EMA values
            for i in range(period, n):
                ema[i] = ((prices.iloc[i] - ema[i - 1]) * alpha) + ema[i - 1]
        
        return ema
    
    def _compute_dmi(self, period=14):
        """Compute Directional Movement Index (+DI and -DI)."""
        high_vals = self.df['High']
        low_vals = self.df['Low']
        close_vals = self.df['Close']
        n = len(self.df)
        
        plus_dm = [0.0] * n
        minus_dm = [0.0] * n
        tr_list = [0.0] * n
        
        # Calculate +DM, -DM, TR
        for i in range(1, n):
            up_move = high_vals.iloc[i] - high_vals.iloc[i - 1]
            down_move = low_vals.iloc[i - 1] - low_vals.iloc[i]
            
            # +DM
            if up_move > down_move and up_move > 0:
                plus_dm[i] = up_move
            
            # -DM
            if down_move > up_move and down_move > 0:
                minus_dm[i] = down_move
            
            # True Range
            high_low = high_vals.iloc[i] - low_vals.iloc[i]
            high_close = abs(high_vals.iloc[i] - close_vals.iloc[i - 1])
            low_close = abs(low_vals.iloc[i] - close_vals.iloc[i - 1])
            tr_list[i] = max(high_low, high_close, low_close)
        
        # Wilder's smoothing
        plus_dm14 = [np.nan] * n
        minus_dm14 = [np.nan] * n
        tr14 = [np.nan] * n
        plus_di = [np.nan] * n
        minus_di = [np.nan] * n
        
        if n > period:
            # Initial sums
            initial_plus_dm = sum(plus_dm[1:period + 1])
            initial_minus_dm = sum(minus_dm[1:period + 1])
            initial_tr = sum(tr_list[1:period + 1])
            
            plus_dm14[period] = initial_plus_dm
            minus_dm14[period] = initial_minus_dm
            tr14[period] = initial_tr
            
            # Wilder's smoothing
            for i in range(period + 1, n):
                plus_dm14[i] = plus_dm14[i - 1] - (plus_dm14[i - 1] / period) + plus_dm[i]
                minus_dm14[i] = minus_dm14[i - 1] - (minus_dm14[i - 1] / period) + minus_dm[i]
                tr14[i] = tr14[i - 1] - (tr14[i - 1] / period) + tr_list[i]
            
            # Calculate +DI and -DI
            for i in range(period, n):
                if pd.notna(plus_dm14[i]) and pd.notna(tr14[i]) and tr14[i] != 0:
                    plus_di[i] = 100.0 * (plus_dm14[i] / tr14[i])
                if pd.notna(minus_dm14[i]) and pd.notna(tr14[i]) and tr14[i] != 0:
                    minus_di[i] = 100.0 * (minus_dm14[i] / tr14[i])
        
        return plus_di, minus_di