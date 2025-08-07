import json
import numpy as np
import pandas as pd
import itertools
import logging
from pathlib import Path
from datetime import datetime
import time
from oosit_utils.data import DataManager
from oosit_utils.strategies import StrategyManager
from oosit_utils.backtesting import BacktestEngine
from oosit_utils.config import Config
from oosit_utils.data.validator import DataValidator

# Configure file logging with UTF-8 encoding (overwrite mode)
file_handler = logging.FileHandler('oosit.log', mode='w', encoding='utf-8')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

# Configure console logging (set to WARNING to filter out INFO messages)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.WARNING)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

# Setup logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.handlers.clear()  # Clear any existing handlers to prevent duplicates
logger.addHandler(file_handler)
logger.addHandler(console_handler)
logger.propagate = False  # Prevent propagation to root logger


class ParameterSweeper:
    def __init__(self, config_file=None):
        """
        Initialize the ParameterSweeper.
        
        Args:
            config_file: Path to configuration file for OOSIT system
        """
        # Always use Config class which has defaults in BacktestConfig
        # If config_file is None, it will use the defaults from oosit_utils.config
        self.config = Config(config_file, require_file=False)
        self.data_manager = DataManager(
            data_directory=self.config.config.data_directory,
            use_extended_data=self.config.config.use_extended_data,
            redirect_dict=self.config.config.redirect_dict,
            max_lookback_days=self.config.config.max_lookback_days
        )
        self.strategy_manager = StrategyManager(
            strategies_directory=self.config.config.strategies_directory
        )
        self.results = {}
        
    def parse_parameter_key(self, key):
        """
        Parse parameter key notation (e.g., "normal_dynamic_leverage[last_year_qqq_threshold]").
        
        Args:
            key: Parameter key string with potential nested notation
            
        Returns:
            Tuple of (parameter_name, sub_key) or (parameter_name, None)
        """
        if '[' in key and ']' in key:
            # Extract the main parameter and sub-key
            main_param = key[:key.index('[')]
            sub_key = key[key.index('[')+1:key.index(']')]
            return main_param, sub_key
        else:
            return key, None
    
    def generate_parameter_grid(self, param_config):
        """
        Generate parameter grid based on min, max, and nsteps configuration.
        
        Args:
            param_config: Dictionary with parameter configurations
            
        Returns:
            Dictionary mapping parameter names to lists of values
        """
        param_grid = {}
        
        for param_key, config in param_config.items():
            min_val = config['min']
            max_val = config['max']
            nsteps = config['nsteps']
            
            # Generate values using numpy linspace
            values = np.linspace(min_val, max_val, nsteps)
            
            # Round to avoid floating point precision issues
            # Determine decimal places based on the step size
            if nsteps > 1:
                step_size = (max_val - min_val) / (nsteps - 1)
                # Find appropriate decimal places (max 10 to avoid over-precision)
                if step_size != 0:
                    decimal_places = min(10, max(0, -int(np.floor(np.log10(abs(step_size)))) + 2))
                else:
                    decimal_places = 6
            else:
                decimal_places = 6
            
            # Round values to avoid floating point errors and convert to Python float
            values = [float(round(v, decimal_places)) for v in values]
            param_grid[param_key] = values
            
        return param_grid
    
    def check_increase_conditions(self, combination, increase_conditions):
        """
        Check if a parameter combination satisfies all increase conditions.
        
        Args:
            combination: Dictionary of parameter values
            increase_conditions: List of lists, where each inner list contains
                               parameter keys that must be in increasing order
        
        Returns:
            Boolean indicating if all conditions are satisfied
        """
        if not increase_conditions:
            return True
            
        for condition_list in increase_conditions:
            # Get values for parameters in this condition
            values = []
            for param_key in condition_list:
                if param_key in combination:
                    values.append(combination[param_key])
                else:
                    # Handle nested parameters
                    main_param, sub_key = self.parse_parameter_key(param_key)
                    if main_param in combination:
                        if isinstance(combination[main_param], dict) and sub_key in combination[main_param]:
                            values.append(combination[main_param][sub_key])
                        else:
                            # Skip this condition if nested param not found
                            logger.warning(f"Nested parameter {param_key} not found in combination")
                            continue
                    else:
                        # Skip this condition if parameter not found
                        logger.warning(f"Parameter {param_key} not found in combination")
                        continue
            
            # Check if values are in increasing order
            if len(values) > 1:
                for i in range(len(values) - 1):
                    if values[i] >= values[i + 1]:
                        return False
                        
        return True
    
    def create_parameter_combinations(self, param_grid, increase_conditions=None):
        """
        Create all combinations of parameters from the grid.
        
        Args:
            param_grid: Dictionary mapping parameter names to lists of values
            increase_conditions: Optional list of lists specifying ordering constraints
            
        Returns:
            List of dictionaries, each representing a parameter combination
        """
        # Get parameter names and their values
        param_names = list(param_grid.keys())
        param_values = [param_grid[name] for name in param_names]
        
        # Generate all combinations using itertools.product
        all_combinations = []
        for values in itertools.product(*param_values):
            combo = dict(zip(param_names, values))
            all_combinations.append(combo)
        
        # Filter combinations based on increase conditions
        if increase_conditions:
            valid_combinations = []
            for combo in all_combinations:
                if self.check_increase_conditions(combo, increase_conditions):
                    valid_combinations.append(combo)
            
            filtered_count = len(all_combinations) - len(valid_combinations)
            logger.info(f"Filtered {filtered_count} combinations due to increase conditions")
            logger.info(f"Valid combinations: {len(valid_combinations)} / {len(all_combinations)}")
            return valid_combinations
        
        return all_combinations
    
    def prepare_strategy_parameters(self, strategy_module, parameters):
        """
        Prepare parameters for a strategy by merging with defaults.
        
        Args:
            strategy_module: The strategy module containing the backtest function
            parameters: Dictionary of parameter values to apply
            
        Returns:
            Dictionary of parameters ready to pass to the strategy
        """
        # Build kwargs with defaults first
        kwargs = {}
        
        # Get default parameters from the original function
        import inspect
        sig = inspect.signature(strategy_module.backtest)
        for param_name, param in sig.parameters.items():
            if param.default != inspect.Parameter.empty:
                kwargs[param_name] = param.default
        
        # Apply parameter modifications
        for param_key, value in parameters.items():
            main_param, sub_key = self.parse_parameter_key(param_key)
            
            if sub_key:
                # Handle nested parameter
                if main_param not in kwargs:
                    kwargs[main_param] = {}
                elif not isinstance(kwargs[main_param], dict):
                    logger.warning(f"Parameter {main_param} is not a dictionary, skipping {param_key}")
                    continue
                # Make a copy of the dict to avoid modifying the default
                if main_param in kwargs and isinstance(kwargs[main_param], dict):
                    kwargs[main_param] = kwargs[main_param].copy()
                kwargs[main_param][sub_key] = value
            else:
                # Handle simple parameter
                kwargs[main_param] = value
        
        return kwargs
    
    def run_parameter_sweep(self, param_json_path, output_dir="parameter_sweep_results"):
        """
        Run parameter sweep based on JSON configuration file.
        
        Args:
            param_json_path: Path to parameter customization JSON file
            output_dir: Directory to save results
            
        Returns:
            Dictionary containing all sweep results
        """
        # Create output directory
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        # Load parameter configuration first to get strategy names
        with open(Path(param_json_path), 'r') as f:
            param_configs = json.load(f)
        
        # Create folder name with test strategies and timestamp
        test_strategies_list = list(param_configs.keys())
        strategies_str = ",".join(test_strategies_list)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        folder_name = f"{strategies_str} ({timestamp})"
        
        # Add to output subdirectory
        run_output_path = output_path / folder_name
        run_output_path.mkdir(exist_ok=True)
        
        # Create custom strategy configuration with only the strategies we want to sweep
        strategy_config = {
            'default_strategies': [],
            'test_strategies': list(param_configs.keys())  # Only load strategies from input JSON
        }
        
        # Create a custom StrategyManager that only loads the requested strategies
        custom_strategy_manager = StrategyManager(
            strategies_directory=self.config.config.strategies_directory,
            strategy_config=strategy_config
        )
        
        # Load only the strategies specified in the parameter sweep JSON
        logger.info(f"Loading strategies from parameter sweep config: {list(param_configs.keys())}")
        custom_strategy_manager.load_all_strategies(self.data_manager)
        
        all_results = {}
        
        # Process each strategy in the configuration
        for strategy_name, param_config in param_configs.items():
            logger.info(f"Processing strategy: {strategy_name}")
            
            # Find the strategy module
            all_strategies = {**custom_strategy_manager.default_strategies, 
                            **custom_strategy_manager.test_strategies}
            
            if strategy_name not in all_strategies:
                logger.error(f"Strategy {strategy_name} not found in loaded strategies")
                continue
            
            strategy_module = all_strategies[strategy_name]
            
            # Extract increase conditions if present
            increase_conditions = None
            filtered_param_config = {}
            for key, value in param_config.items():
                if key == "_increase_condition":
                    increase_conditions = value
                    logger.info(f"Found increase conditions: {increase_conditions}")
                else:
                    filtered_param_config[key] = value
            
            # Generate parameter grid (excluding _increase_condition)
            param_grid = self.generate_parameter_grid(filtered_param_config)
            logger.info(f"Parameter grid: {param_grid}")
            
            # Create all parameter combinations with filtering
            combinations = self.create_parameter_combinations(param_grid, increase_conditions)
            logger.info(f"Total parameter combinations: {len(combinations)}")
            
            strategy_results = []
            
            # Run backtest for each parameter combination
            total_combinations = len(combinations)
            for i, params in enumerate(combinations):
                print(f"Running {strategy_name}: {i+1}/{total_combinations} ({(i+1)/total_combinations*100:.1f}%)")
                try:
                    # Prepare parameters for this combination
                    strategy_params = self.prepare_strategy_parameters(strategy_module, params)
                    
                    # Run backtest using BacktestEngine with custom strategy manager
                    engine = BacktestEngine(self.data_manager, custom_strategy_manager)
                    
                    # Use full period from config
                    full_start = self.config.config.full_start_date
                    full_end = self.config.config.full_end_date
                    test_periods = self.config.get_test_periods_dict_list()
                    
                    # Run the backtest with strategy parameters
                    backtest_results = engine.run_full_backtest(
                        full_start, full_end, test_periods,
                        strategy_params={strategy_name: strategy_params}
                    )
                    
                    # Extract key metrics
                    if strategy_name in backtest_results['results']:
                        full_period_result = backtest_results['results'][strategy_name].get('Full Period')
                        if full_period_result:
                            result_entry = {
                                'combination_id': i,
                                'parameters': params,
                                'total_return': full_period_result.total_return,
                                'max_drawdown': full_period_result.max_drawdown,
                                'final_value': full_period_result.portfolio_values[-1],
                                'start_value': full_period_result.portfolio_values[0]
                            }
                            
                            # Add period-specific results
                            for period_name, period_result in backtest_results['results'][strategy_name].items():
                                if period_name != 'Full Period':
                                    result_entry[f'{period_name}_return'] = period_result.total_return
                                    result_entry[f'{period_name}_drawdown'] = period_result.max_drawdown
                            
                            strategy_results.append(result_entry)
                    
                except Exception as e:
                    logger.error(f"Error running combination {i} for {strategy_name}: {e}")
                    continue
            
            # Save strategy results
            all_results[strategy_name] = strategy_results
            
            # Convert to DataFrame and save as CSV
            if strategy_results:
                df = pd.DataFrame(strategy_results)
                csv_path = run_output_path / f"{strategy_name}_results.csv"
                df.to_csv(csv_path, index=False, encoding='utf-8-sig')
                logger.info(f"Saved results to {csv_path}")
                
                # Also save a summary with best parameters
                best_idx = df['total_return'].idxmax()
                best_result = df.iloc[best_idx]
                
                summary = {
                    'strategy': strategy_name,
                    'total_combinations': len(combinations),
                    'best_combination': {
                        'parameters': best_result['parameters'],
                        'total_return': best_result['total_return'],
                        'max_drawdown': best_result['max_drawdown']
                    },
                    'parameter_ranges': param_config
                }
                
                summary_path = run_output_path / f"{strategy_name}_summary.json"
                with open(Path(summary_path), 'w', encoding='utf-8') as f:
                    json.dump(summary, f, indent=2, ensure_ascii=False)
                logger.info(f"Saved summary to {summary_path}")
        
        # Save overall results
        overall_results_path = run_output_path / "all_results.json"
        with open(Path(overall_results_path), 'w', encoding='utf-8') as f:
            json.dump(all_results, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Parameter sweep completed. Results saved to {run_output_path}")
        return all_results


def main():
    """Main function to run parameter sweep from command line."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Run parameter sweep for OOSIT strategies')
    
    parser.add_argument(
        "--config", "-c",
        type=str,
        help="Path to configuration file (JSON format)"
    )
    
    parser.add_argument(
        "--data-dir", "-d",
        type=str,
        help="Directory containing CSV data files"
    )
    
    parser.add_argument(
        "--strategies-dir", "-s", 
        type=str,
        help="Directory containing strategy files"
    )
    
    parser.add_argument(
        "--log-level", "-l",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)"
    )
    
    parser.add_argument('--output', default='parameter_sweep_results', help='Output directory for sweep results')
    
    args = parser.parse_args()
    
    # Default to parameter_customization.json
    param_json = Path('jsons') / 'parameter_customization.json'
    
    # Set up logging
    # Configure file logging with UTF-8 encoding (overwrite mode)
    file_handler = logging.FileHandler('oosit.log', mode='w', encoding='utf-8')
    file_handler.setLevel(getattr(logging, args.log_level.upper()))
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    
    # Configure console logging (set to WARNING or higher to filter out INFO)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING)
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, args.log_level.upper()))
    root_logger.handlers.clear()  # Clear existing handlers
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    # Set all existing loggers to not propagate to prevent duplicate messages
    for name in logging.root.manager.loggerDict:
        logging.getLogger(name).propagate = False
        logging.getLogger(name).handlers.clear()
        logging.getLogger(name).addHandler(file_handler)
        logging.getLogger(name).addHandler(console_handler)
    
    # Create sweeper with config
    sweeper = ParameterSweeper(config_file=args.config)
    
    # Override directories if provided
    if args.data_dir:
        sweeper.config.update_config(data_directory=args.data_dir)
        # Reinitialize data manager with new directory
        sweeper.data_manager = DataManager(
            data_directory=args.data_dir,
            use_extended_data=sweeper.config.config.use_extended_data,
            redirect_dict=sweeper.config.config.redirect_dict,
            max_lookback_days=sweeper.config.config.max_lookback_days
        )
    
    if args.strategies_dir:
        sweeper.config.update_config(strategies_directory=args.strategies_dir)
        # Reinitialize strategy manager with new directory
        sweeper.strategy_manager = StrategyManager(strategies_directory=args.strategies_dir)
    
    # Run sweep
    results = sweeper.run_parameter_sweep(param_json, args.output)
    
    print(f"\nParameter sweep completed successfully!")
    print(f"Total strategies processed: {len(results)}")
    for strategy, strategy_results in results.items():
        print(f"  {strategy}: {len(strategy_results)} combinations tested")


if __name__ == "__main__":
    main()