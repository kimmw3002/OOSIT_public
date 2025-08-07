"""
Backtesting engine module.

This module orchestrates the execution of backtests across multiple strategies
and time periods, managing data access and result collection.
"""

import pandas as pd
import numpy as np
import logging
from ..data import DataManager
from ..strategies import StrategyManager

logger = logging.getLogger(__name__)


class BacktestPeriod:
    """Represents a backtesting time period."""
    def __init__(self, name, start_date, end_date):
        self.name = name
        self.start_date = start_date
        self.end_date = end_date


class BacktestResult:
    """Represents the result of a single backtest."""
    def __init__(self, strategy_name, display_name, period_name, date_range, 
                 portfolio_values, normalized_values, total_return, max_drawdown, 
                 rebalancing_log=None):
        self.strategy_name = strategy_name
        self.display_name = display_name
        self.period_name = period_name
        self.date_range = date_range
        self.portfolio_values = portfolio_values
        self.normalized_values = normalized_values  # Normalized to start at 100%
        self.total_return = total_return  # As percentage
        self.max_drawdown = max_drawdown  # As positive percentage
        self.rebalancing_log = rebalancing_log


class BacktestEngine:
    """Main backtesting engine that coordinates strategy execution."""
    
    def __init__(self, data_manager, strategy_manager):
        """
        Initialize the BacktestEngine.
        
        Args:
            data_manager: DataManager instance for data access
            strategy_manager: StrategyManager instance for strategy execution
        """
        self.data_manager = data_manager
        self.strategy_manager = strategy_manager
        
        # Results storage
        self.results = {}  # strategy_name -> period_name -> result
        
    def run_full_backtest(self, full_start_date, full_end_date, 
                         test_periods, strategy_params=None):
        """
        Run complete backtest across all strategies and periods.
        
        This method runs each strategy ONCE for the full period, then extracts
        sub-period data for analysis, following the original notebook approach.
        
        Args:
            full_start_date: Start date for full backtest period
            full_end_date: End date for full backtest period
            test_periods: List of additional test periods
            strategy_params: Optional dict of strategy_name -> parameter dict
            
        Returns:
            Dictionary containing all backtest results and summary data
        """
        logger.info(f"Starting full backtest from {full_start_date} to {full_end_date}")
        
        # Load strategies
        default_strategies, test_strategies = self.strategy_manager.load_all_strategies(self.data_manager)
        all_strategies = {**default_strategies, **test_strategies}
        
        # Create period list
        periods = [BacktestPeriod("Full Period", full_start_date, full_end_date)]
        for period_config in test_periods:
            periods.append(BacktestPeriod(
                period_config['period_name'],
                period_config['period_start_date'],
                period_config['period_end_date']
            ))
        
        # Execute backtests - RUN FULL PERIOD ONCE PER STRATEGY
        all_results = {}
        full_period = periods[0]  # First period is always the full period
        
        for strategy_name in all_strategies.keys():
            strategy_results = {}
            
            try:
                logger.info(f"Running {strategy_name} for full period {full_period.name}")
                # Run ONLY the full period backtest
                # Get strategy-specific parameters if provided
                kwargs = {}
                if strategy_params and strategy_name in strategy_params:
                    kwargs = strategy_params[strategy_name]
                
                full_result = self._execute_single_backtest(strategy_name, full_period, **kwargs)
                strategy_results["Full Period"] = full_result
                
                # Extract data for sub-periods from the full period result
                for period in periods[1:]:  # Skip full period
                    try:
                        logger.info(f"Extracting {period.name} data from full backtest for {strategy_name}")
                        extracted_result = self._extract_period_from_full_result(
                            full_result, period, strategy_name
                        )
                        strategy_results[period.name] = extracted_result
                        
                    except Exception as e:
                        import traceback
                        logger.error(f"Error extracting {period.name} for {strategy_name}: {e}")
                        logger.debug(f"Full traceback:\n{traceback.format_exc()}")
                        continue
                
            except Exception as e:
                import traceback
                logger.error(f"Error running {strategy_name} for full period: {e}")
                logger.debug(f"Full traceback:\n{traceback.format_exc()}")
                continue
            
            all_results[strategy_name] = strategy_results
        
        self.results = all_results
        
        # Generate summary data
        summary_data = self._generate_summary_data(periods)
        
        logger.info("Full backtest completed")
        return {
            'results': all_results,
            'summary': summary_data,
            'periods': periods
        }
    
    def _execute_single_backtest(self, strategy_name, period, **strategy_kwargs):
        """Execute a single backtest for one strategy and period."""
        # Execute the strategy
        date_range, portfolio_values, rebalancing_log = self.strategy_manager.execute_strategy(
            strategy_name, period.start_date, period.end_date, self.data_manager, **strategy_kwargs
        )
        
        # Normalize portfolio values to start at 100%
        normalized_values = self._normalize_to_percents(portfolio_values)
        
        # Calculate metrics
        total_return = self._calculate_return(portfolio_values)
        max_drawdown = self._calculate_max_drawdown(portfolio_values)
        
        # Get display name
        display_name = self._get_display_name(strategy_name)
        
        return BacktestResult(
            strategy_name=strategy_name,
            display_name=display_name,
            period_name=period.name,
            date_range=date_range,
            portfolio_values=portfolio_values,
            normalized_values=normalized_values,
            total_return=total_return,
            max_drawdown=max_drawdown,
            rebalancing_log=rebalancing_log
        )
    
    def _normalize_to_percents(self, portfolio_values):
        """Normalize portfolio values to start at 100%."""
        # Handle numpy arrays and lists properly
        if portfolio_values is None or len(portfolio_values) == 0:
            return []
        
        initial_value = portfolio_values[0]
        return [x / initial_value * 100 for x in portfolio_values]
    
    def _calculate_return(self, portfolio_values):
        """Calculate total return as percentage."""
        if len(portfolio_values) < 2:
            return 0.0
        
        initial_value = portfolio_values[0]
        final_value = portfolio_values[-1]
        
        return (final_value - initial_value) / initial_value * 100
    
    def _calculate_max_drawdown(self, portfolio_values):
        """Calculate maximum drawdown as positive percentage."""
        # Handle numpy arrays and lists properly
        if portfolio_values is None or len(portfolio_values) == 0:
            return 0.0
        
        peak = portfolio_values[0]
        max_drawdown = 0.0
        
        for value in portfolio_values:
            if value > peak:
                peak = value
            drawdown = (peak - value) / peak
            if drawdown > max_drawdown:
                max_drawdown = drawdown
        
        return max_drawdown * 100
    
    def _get_display_name(self, strategy_name):
        """Get display name for strategy."""
        # Remove "_backtest" suffix to get base name
        base_name = strategy_name.replace('_backtest', '')
        
        # Check if it's a default strategy using strategy_manager
        if base_name in self.strategy_manager.default_strategy_names:
            return '*' + base_name
        
        # For test strategies: just return the base name
        return base_name
    
    def _generate_summary_data(self, periods):
        """Generate summary data for all results."""
        # Get strategy names and display names
        default_names, test_names = self.strategy_manager.get_strategy_names()
        all_display_names = []
        strategy_name_mapping = {}
        
        # Create mapping between internal names and display names
        for strategy_name in self.results.keys():
            display_name = self._get_display_name(strategy_name)
            all_display_names.append(display_name)
            strategy_name_mapping[strategy_name] = display_name
        
        # Create summary DataFrames
        full_period_data = []
        period_returns_data = {period.name: [] for period in periods[1:]}  # Skip full period
        period_drawdowns_data = {period.name: [] for period in periods[1:]}
        
        # Collect data for each strategy
        for strategy_name, strategy_results in self.results.items():
            display_name = strategy_name_mapping[strategy_name]
            
            # Full period data
            if "Full Period" in strategy_results:
                full_result = strategy_results["Full Period"]
                full_period_data.append({
                    '전략명': display_name,
                    '총 기간 수익률(배)': full_result.total_return / 100,  # Convert to multiplier
                    '총 기간 최대 낙폭(%)': -full_result.max_drawdown  # Make negative
                })
            
            # Period-specific data
            for period in periods[1:]:  # Skip full period
                if period.name in strategy_results:
                    result = strategy_results[period.name]
                    period_returns_data[period.name].append(result.total_return)
                    period_drawdowns_data[period.name].append(-result.max_drawdown)
                else:
                    period_returns_data[period.name].append(0.0)
                    period_drawdowns_data[period.name].append(0.0)
        
        # Create DataFrames
        full_result_df = pd.DataFrame(full_period_data)
        
        periods_return_df = pd.DataFrame({'전략명': all_display_names})
        for period_name, returns in period_returns_data.items():
            periods_return_df[period_name] = returns
        
        periods_maxdd_df = pd.DataFrame({'전략명': all_display_names})  
        for period_name, drawdowns in period_drawdowns_data.items():
            periods_maxdd_df[period_name] = drawdowns
        
        # Create rebalancing log
        rebalancing_log_df = self._create_rebalancing_log(all_display_names, strategy_name_mapping)
        
        return {
            'full_result_df': full_result_df,
            'periods_return_df': periods_return_df,
            'periods_maxdd_df': periods_maxdd_df,
            'rebalancing_log_df': rebalancing_log_df,
            'strategy_names': all_display_names,
            'default_strategy_names': default_names
        }
    
    def _create_rebalancing_log(self, all_display_names, 
                              strategy_name_mapping):
        """Create consolidated rebalancing log."""
        # Collect all unique log dates
        log_dates = set()
        
        for strategy_name, strategy_results in self.results.items():
            for period_name, result in strategy_results.items():
                if result.rebalancing_log:
                    for log_entry in result.rebalancing_log:
                        try:
                            log_dates.add(pd.to_datetime(log_entry[0]))
                        except:
                            logger.warning(f"Invalid date in rebalancing log: {log_entry[0]}")
        
        log_dates = sorted(log_dates)
        
        if not log_dates:
            # Return empty DataFrame with expected structure
            return pd.DataFrame({'날짜': []})
        
        # Create DataFrame with proper initialization
        rebalancing_data = {'날짜': log_dates}
        
        for display_name in all_display_names:
            rebalancing_data[f'{display_name} (에서)'] = [None] * len(log_dates)
            rebalancing_data[f'{display_name} (으로)'] = [None] * len(log_dates)
        
        # Fill in rebalancing data
        for strategy_name, strategy_results in self.results.items():
            display_name = strategy_name_mapping[strategy_name]
            
            # Combine rebalancing logs from all periods
            all_logs = []
            for result in strategy_results.values():
                if result.rebalancing_log:
                    all_logs.extend(result.rebalancing_log)
            
            if not all_logs:
                continue
            
            try:
                # Group logs by date
                date_groups = {}
                for log_entry in all_logs:
                    log_date = pd.to_datetime(log_entry[0])
                    if log_date not in date_groups:
                        date_groups[log_date] = {'from': [], 'to': []}
                    
                    # Replace commas with semicolons in dictionary representations
                    from_entry = log_entry[1].replace(', ', '; ')
                    to_entry = log_entry[2].replace(', ', '; ')
                    date_groups[log_date]['from'].append(from_entry)
                    date_groups[log_date]['to'].append(to_entry)
                
                # Fill DataFrame
                for log_date, actions in date_groups.items():
                    try:
                        date_idx = log_dates.index(log_date)
                        from_string = " | ".join(actions['from'])
                        to_string = " | ".join(actions['to'])
                        
                        rebalancing_data[f'{display_name} (에서)'][date_idx] = from_string
                        rebalancing_data[f'{display_name} (으로)'][date_idx] = to_string
                        
                    except ValueError:
                        logger.warning(f"Date {log_date} not found in log_dates for {display_name}")
                        
            except Exception as e:
                logger.error(f"Error processing rebalancing log for {display_name}: {e}")
        
        return pd.DataFrame(rebalancing_data)
    
    def get_results_for_strategy(self, strategy_name):
        """Get all results for a specific strategy."""
        return self.results.get(strategy_name, {})
    
    def get_results_for_period(self, period_name):
        """Get all results for a specific period."""
        period_results = {}
        for strategy_name, strategy_results in self.results.items():
            if period_name in strategy_results:
                period_results[strategy_name] = strategy_results[period_name]
        return period_results
    
    def _extract_period_from_full_result(self, full_result, period, strategy_name):
        """
        Extract a sub-period from a full backtest result and create a new BacktestResult.
        
        This follows the original notebook approach of running the full period once
        and then extracting sub-periods for analysis.
        
        Args:
            full_result: BacktestResult from the full period run
            period: BacktestPeriod object for the sub-period
            strategy_name: Name of the strategy
            
        Returns:
            BacktestResult for the extracted period
        """
        # Find start and end indices using binary search approach like original
        extract_start_date = pd.to_datetime(period.start_date)
        extract_end_date = pd.to_datetime(period.end_date)
        
        start_idx = None
        end_idx = None
        
        # Binary search for start index
        for i, date in enumerate(full_result.date_range):
            if date >= extract_start_date and start_idx is None:
                start_idx = i
            if date <= extract_end_date:
                end_idx = i
        
        if start_idx is None or end_idx is None:
            raise ValueError(f"Date range {period.start_date} to {period.end_date} not found in full results")
        
        # Extract the sub-period data
        extracted_date_range = full_result.date_range[start_idx:end_idx + 1]
        extracted_portfolio_values = full_result.portfolio_values[start_idx:end_idx + 1]
        
        # Normalize the extracted values to start at 100% (like original extract_period function)
        normalized_values = self._normalize_to_percents(extracted_portfolio_values)
        
        # Calculate metrics for the extracted period
        total_return = self._calculate_return(extracted_portfolio_values)
        max_drawdown = self._calculate_max_drawdown(extracted_portfolio_values)
        
        # Extract rebalancing log for this period if it exists
        extracted_rebalancing_log = None
        if full_result.rebalancing_log:
            extracted_rebalancing_log = []
            for log_entry in full_result.rebalancing_log:
                try:
                    log_date = pd.to_datetime(log_entry[0])
                    if extract_start_date <= log_date and log_date <= extract_end_date:
                        extracted_rebalancing_log.append(log_entry)
                except:
                    continue
        
        return BacktestResult(
            strategy_name=strategy_name,
            display_name=full_result.display_name,
            period_name=period.name,
            date_range=extracted_date_range,
            portfolio_values=extracted_portfolio_values,
            normalized_values=normalized_values,
            total_return=total_return,
            max_drawdown=max_drawdown,
            rebalancing_log=extracted_rebalancing_log
        )

    def extract_period_data(self, strategy_name, period_name, 
                           extract_start, extract_end):
        """
        Extract data for a specific sub-period from a strategy result.
        
        Args:
            strategy_name: Name of the strategy
            period_name: Name of the period to extract from
            extract_start: Start date for extraction
            extract_end: End date for extraction
            
        Returns:
            Normalized portfolio values for the extracted period
        """
        if strategy_name not in self.results or period_name not in self.results[strategy_name]:
            raise ValueError(f"No results found for {strategy_name} in {period_name}")
        
        result = self.results[strategy_name][period_name]
        
        # Find start and end indices
        extract_start_date = pd.to_datetime(extract_start)
        extract_end_date = pd.to_datetime(extract_end)
        
        start_idx = None
        end_idx = None
        
        for i, date in enumerate(result.date_range):
            if date >= extract_start_date and start_idx is None:
                start_idx = i
            if date <= extract_end_date:
                end_idx = i
        
        if start_idx is None or end_idx is None:
            raise ValueError(f"Date range {extract_start} to {extract_end} not found in results")
        
        # Extract and normalize data
        extracted_values = result.portfolio_values[start_idx:end_idx + 1]
        return self._normalize_to_percents(extracted_values)