"""
Quick OOSIT Runner - Displays portfolio value comparison plots only.
Based on main.py but simplified to show only interactive plots.
"""

import sys
from pathlib import Path
import matplotlib.pyplot as plt
import pandas as pd
import re

# Add current directory to Python path to ensure imports work
sys.path.insert(0, str(Path(__file__).parent))

from oosit_utils import (
    DataManager, 
    StrategyManager, 
    BacktestEngine, 
    Config
)


def plot_portfolio_values(backtest_results):
    """Plot portfolio value comparison for each test strategy vs all defaults"""
    
    # Extract the results dictionary
    all_results = backtest_results.get('results', {})
    
    if not all_results:
        print("No results found")
        return
    
    # Build full_results dictionary for "Full Period" results only
    full_results = {}
    for strategy_name, strategy_results in all_results.items():
        if "Full Period" in strategy_results:
            result = strategy_results["Full Period"]
            full_results[strategy_name] = {
                'portfolio_values': result.portfolio_values,
                'dates': result.date_range
            }
    
    if not full_results:
        print("No full period results found")
        return
    
    # Get all strategy names
    all_strategies = list(full_results.keys())
    
    # Use the summary data from backtest_results to properly categorize strategies
    summary = backtest_results.get('summary', {})
    default_names = summary.get('default_strategy_names', [])
    
    # Read target.json directly to get proper categorization
    import json
    target_path = Path(__file__).parent / 'jsons' / 'target.json'
    with open(Path(target_path), 'r') as f:
        target_config = json.load(f)
    
    # Remove _backtest suffix for comparison if present
    test_strategies = []
    default_strategies = []
    
    for s in all_strategies:
        base_name = s.replace('_backtest', '')
        # Check if it's a default strategy using target.json
        if base_name in target_config.get('default_strategies', []):
            default_strategies.append(s)
        elif base_name in target_config.get('test_strategies', []):
            test_strategies.append(s)
    
    if not test_strategies:
        print("No test strategies found")
        return
    
    if not default_strategies:
        print("No default strategies found")
        return
    
    # Create a plot for each test strategy
    for test_strategy in test_strategies:
        plt.figure(figsize=(14, 8))
        
        # Plot the test strategy
        test_data = full_results.get(test_strategy, {})
        test_portfolio_values = test_data.get('portfolio_values', [])
        test_dates = test_data.get('dates', [])
        
        if len(test_portfolio_values) > 0 and len(test_dates) > 0:
            test_dates = pd.to_datetime(test_dates)
            plt.plot(test_dates, test_portfolio_values, linewidth=2.5, 
                    label=f'{test_strategy} (Test)', color='red', alpha=0.9)
        
        # Plot all default strategies
        colors = ['blue', 'green', 'purple', 'orange', 'brown', 'pink', 'gray', 'olive', 'cyan']
        for idx, default_strategy in enumerate(default_strategies):
            default_data = full_results.get(default_strategy, {})
            default_portfolio_values = default_data.get('portfolio_values', [])
            default_dates = default_data.get('dates', [])
            
            if len(default_portfolio_values) > 0 and len(default_dates) > 0:
                default_dates = pd.to_datetime(default_dates)
                color = colors[idx % len(colors)]
                # Remove 'default_' prefix for cleaner labels
                label = default_strategy.replace('default_', '')
                plt.plot(default_dates, default_portfolio_values, linewidth=1.5, 
                        label=label, color=color, alpha=0.7)
        
        # Formatting
        plt.title(f'Portfolio Value Comparison: {test_strategy} vs Default Strategies', 
                 fontsize=14, fontweight='bold')
        plt.xlabel('Date', fontsize=12)
        plt.ylabel('Portfolio Value ($)', fontsize=12)
        plt.legend(loc='best', fontsize=10)
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=45)
        
        # Set y-axis to logarithmic scale
        plt.yscale('log')
        
        # Format y-axis to show currency
        ax = plt.gca()
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
        
        plt.tight_layout()
        plt.show()


def main(config_file=None, data_directory=None, strategies_directory=None):
    """
    Quick runner - runs backtest and shows portfolio comparison plot only.
    
    Args:
        config_file: Path to configuration file
        data_directory: Directory containing CSV data files
        strategies_directory: Directory containing strategy ZIP files
    """
    print("Starting Quick OOSIT Runner...")
    
    try:
        # 1. Load Configuration
        print("Loading configuration...")
        config_manager = Config(config_file, require_file=bool(config_file))
        
        # Override directories if provided
        if data_directory:
            config_manager.update_config(data_directory=data_directory)
        if strategies_directory:
            config_manager.update_config(strategies_directory=strategies_directory)
        
        # Validate configuration
        errors = config_manager.validate_config()
        if errors:
            print("Configuration validation failed:")
            for error in errors:
                print(f"  - {error}")
            return
        
        # Sort test periods chronologically
        config_manager.sort_test_periods_by_date()
        
        # 2. Initialize Data Manager
        print("Initializing data manager...")
        data_manager = DataManager(
            data_directory=config_manager.config.data_directory,
            use_extended_data=config_manager.config.use_extended_data,
            redirect_dict=config_manager.config.redirect_dict,
            max_lookback_days=config_manager.config.max_lookback_days
        )
        print(f"Loaded data for {len(data_manager.get_available_assets())} assets")
        
        # 3. Initialize Strategy Manager
        print("Initializing strategy manager...")
        strategy_manager = StrategyManager(
            config_manager.config.strategies_directory
        )
        
        # 4. Initialize Backtesting Engine
        print("Initializing backtesting engine...")
        backtest_engine = BacktestEngine(data_manager, strategy_manager)
        
        # 5. Run Full Backtest
        print("Running backtests...")
        backtest_results = backtest_engine.run_full_backtest(
            full_start_date=config_manager.config.full_start_date,
            full_end_date=config_manager.config.full_end_date,
            test_periods=config_manager.get_test_periods_dict_list()
        )
        
        print("Backtesting completed")
        
        # 6. Display Plot
        print("Displaying portfolio value comparison plot...")
        plot_portfolio_values(backtest_results)
        
    except KeyboardInterrupt:
        print("Interrupted by user")
    except Exception as e:
        print(f"Error during quick run: {e}")
        raise


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Quick OOSIT Runner - Portfolio Plot Only")
    
    parser.add_argument(
        "--config", "-c",
        type=str,
        help="Path to configuration file (JSON format)"
    )
    
    parser.add_argument(
        "--data-dir", "-d",
        type=str,
        default="./csv_data",
        help="Directory containing CSV data files (default: ./csv_data)"
    )
    
    parser.add_argument(
        "--strategies-dir", "-s", 
        type=str,
        default="./oosit_strategies",
        help="Directory containing strategy ZIP files (default: ./oosit_strategies)"
    )
    
    args = parser.parse_args()
    
    # Run main pipeline
    main(
        config_file=args.config,
        data_directory=args.data_dir,
        strategies_directory=args.strategies_dir
    )