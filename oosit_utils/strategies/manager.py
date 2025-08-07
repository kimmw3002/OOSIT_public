"""Strategy management module for loading and managing trading strategies."""

import os
import zipfile
import importlib.util
import sys
import re
import shutil
import tempfile
import json
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class StrategyManager:
    """Manages loading and execution of trading strategies from ZIP files."""
    
    def __init__(self, strategies_directory="./oosit_strategies", strategy_config=None):
        """
        Initialize the StrategyManager.
        
        Args:
            strategies_directory: Directory containing strategy .py files
            strategy_config: Optional dict with 'default_strategies' and 'test_strategies' lists.
                           If not provided, will load from target.json
        """
        self.strategies_directory = Path(strategies_directory)
        self.working_directory = Path('./') 
        
        # Strategy storage
        self.default_strategies = {}
        self.test_strategies = {}
        self.default_strategy_names = []
        self.test_strategy_names = []
        
        # Use provided config or load from target.json
        if strategy_config is not None:
            self.target_config = strategy_config
        else:
            self.target_config = self._load_target_config()
        
        # Keep these for backward compatibility
        self.default_zip_names = []
        self.test_zip_names = []
        
        # Ensure strategies directory exists
        self.strategies_directory.mkdir(exist_ok=True)
    
    def load_all_strategies(self, data_manager=None):
        """
        Load all strategies from .py files based on target.json.
        
        Args:
            data_manager: DataManager instance (not used for injection anymore)
            
        Returns:
            Tuple of (default_strategies, test_strategies)
        """
        try:
            # Import Python files directly based on target.json
            imported_modules = self._import_python_files()
            
            # Categorize strategies based on target.json
            self._categorize_strategies_from_target(imported_modules)
            
            # Update zip names for backward compatibility
            self.default_zip_names = self.default_strategy_names.copy()
            self.test_zip_names = self.test_strategy_names.copy()
            
            logger.info(f"Loaded {len(self.default_strategies)} default strategies and {len(self.test_strategies)} test strategies")
            
            return self.default_strategies, self.test_strategies
            
        except Exception as e:
            logger.error(f"Error loading strategies: {e}")
            raise
    
    def _load_target_config(self):
        """Load target.json configuration."""
        target_json_path = self.working_directory / 'jsons' / 'target.json'
        if not target_json_path.exists():
            raise FileNotFoundError(
                f"jsons/target.json not found in {self.working_directory}. "
                "Please create a jsons/target.json file with 'default_strategies' and 'test_strategies' lists."
            )
        
        try:
            with open(Path(target_json_path), 'r') as f:
                config = json.load(f)
                logger.info(f"Loaded target.json: {config}")
                return config
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in target.json: {e}")
        except Exception as e:
            raise RuntimeError(f"Error loading target.json: {e}")
    
    def _create_temp_directory(self):
        """Create temporary directory for extracting strategy files."""
        self.temp_extraction_dir = tempfile.mkdtemp(prefix="oosit_strategies_")
        logger.debug(f"Created temporary directory: {self.temp_extraction_dir}")
    
    def _cleanup_temp_directory(self):
        """Clean up temporary directory."""
        if self.temp_extraction_dir and Path(self.temp_extraction_dir).exists():
            shutil.rmtree(self.temp_extraction_dir)
            logger.debug(f"Cleaned up temporary directory: {self.temp_extraction_dir}")
    
    def _extract_python_files(self):
        """Extract all Python files from ZIP files in the strategies directory."""
        zip_files = list(self.strategies_directory.glob('*.zip'))
        
        if not zip_files:
            logger.warning(f"No ZIP files found in {self.strategies_directory}")
            return
        
        for zip_file in zip_files:
            try:
                with zipfile.ZipFile(zip_file, 'r') as zf:
                    for file_info in zf.infolist():
                        if file_info.filename.endswith('.py'):
                            zf.extract(file_info, self.temp_extraction_dir)
                            logger.debug(f"Extracted {file_info.filename} from {zip_file.name}")
            except Exception as e:
                logger.error(f"Error extracting {zip_file}: {e}")
                raise
    
    def _import_python_files(self):
        """Import Python files directly from strategies directory and saved subfolder based on target.json."""
        imported_modules = {}
        
        # Get all strategy names from target.json
        all_strategies = (self.target_config.get('default_strategies', []) + 
                         self.target_config.get('test_strategies', []))
        
        # Define search directories
        search_directories = [
            self.strategies_directory,  # Main strategies directory
            self.strategies_directory / "saved"  # Saved subfolder
        ]
        
        # Add all search directories to Python path temporarily
        for search_dir in search_directories:
            if search_dir.exists():
                sys.path.insert(0, str(search_dir))
        
        try:
            for strategy_name in all_strategies:
                found = False
                
                # Search in each directory for the strategy file
                for search_dir in search_directories:
                    if not search_dir.exists():
                        continue
                        
                    py_file = search_dir / f"{strategy_name}.py"
                    
                    if py_file.exists():
                        module_name = strategy_name
                        
                        try:
                            # Import the module dynamically
                            spec = importlib.util.spec_from_file_location(module_name, py_file)
                            if spec is None or spec.loader is None:
                                logger.error(f"Could not create spec for {py_file}")
                                continue
                            
                            module = importlib.util.module_from_spec(spec)
                            spec.loader.exec_module(module)
                            imported_modules[module_name] = module
                            
                            logger.debug(f"Successfully imported {module_name} from {py_file}")
                            found = True
                            break  # Stop searching once we find the strategy
                            
                        except Exception as e:
                            logger.error(f"Error importing {py_file}: {e}")
                            continue
                
                if not found:
                    logger.error(f"Strategy file not found in any search directory: {strategy_name}.py")
        
        finally:
            # Remove all search directories from Python path
            for search_dir in search_directories:
                if search_dir.exists() and str(search_dir) in sys.path:
                    sys.path.remove(str(search_dir))
        
        return imported_modules
    
    def _categorize_strategies(self, modules):
        """Categorize strategies into default and test categories."""
        default_pattern = re.compile(r"^default \d+ \(.*\)_backtest$")
        
        for module_name, module in modules.items():
            if default_pattern.match(module_name):
                self.default_strategies[module_name] = module
            else:
                self.test_strategies[module_name] = module
        
        # Sort default strategies by number
        self.default_strategies = dict(sorted(
            self.default_strategies.items(),
            key=lambda item: int(re.search(r'\d+', item[0]).group())
        ))
    
    def _categorize_strategies_from_target(self, modules):
        """Categorize strategies based on target.json configuration."""
        # Process default strategies
        for strategy_name in self.target_config.get('default_strategies', []):
            if strategy_name in modules:
                self.default_strategies[strategy_name] = modules[strategy_name]
                self.default_strategy_names.append(strategy_name)
        
        # Process test strategies
        for strategy_name in self.target_config.get('test_strategies', []):
            if strategy_name in modules:
                self.test_strategies[strategy_name] = modules[strategy_name]
                self.test_strategy_names.append(strategy_name)
    
    def _get_zip_names(self):
        """Extract ZIP file names for configuration purposes."""
        self.default_zip_names = []
        self.test_zip_names = []
        
        zip_pattern = re.compile(r"^default \d+ \(.*\)$")
        
        for zip_file in self.strategies_directory.glob('*.zip'):
            zip_name_no_ext = zip_file.stem
            
            if zip_pattern.match(zip_name_no_ext):
                self.default_zip_names.append(zip_name_no_ext)
            else:
                self.test_zip_names.append(zip_name_no_ext)
        
        # Sort default names numerically
        self.default_zip_names.sort(key=lambda name: int(re.search(r'\d+', name).group()))
        self.test_zip_names.sort()  # Sort test names alphabetically
    
    def get_strategy_names(self):
        """
        Get display names for strategies.
        
        Returns:
            Tuple of (default_strategy_names, test_strategy_names)
        """
        # Return strategy names with asterisk for default strategies
        default_names = ['*' + name for name in self.default_strategy_names]
        test_names = self.test_strategy_names.copy()
        
        return default_names, test_names
    
    def execute_strategy(self, strategy_name, start_date, end_date, 
                        data_manager, **strategy_kwargs):
        """
        Execute a specific strategy.
        
        Args:
            strategy_name: Name of the strategy to execute
            start_date: Backtest start date
            end_date: Backtest end date
            data_manager: DataManager instance
            **strategy_kwargs: Additional keyword arguments to pass to the strategy
            
        Returns:
            Tuple of (date_range, portfolio_values, rebalancing_log)
        """
        # Find the strategy module
        strategy_module = None
        if strategy_name in self.default_strategies:
            strategy_module = self.default_strategies[strategy_name]
        elif strategy_name in self.test_strategies:
            strategy_module = self.test_strategies[strategy_name]
        else:
            raise ValueError(f"Strategy not found: {strategy_name}")
        
        # Create utility functions for passing as arguments
        from ..data.validator import DataValidator
        validator = DataValidator()
        
        def get_nyse_open_dates(start_date, end_date):
            return validator._get_nyse_open_dates(start_date, end_date)
        
        def initialize_get_value(backtest_start_date):
            return data_manager.get_data_accessor(backtest_start_date)
        
        # Execute the backtest function with dependencies as arguments
        try:
            if hasattr(strategy_module, 'backtest'):
                result = strategy_module.backtest(
                    start_date, 
                    end_date, 
                    get_nyse_open_dates, 
                    initialize_get_value,
                    **strategy_kwargs
                )
                
                # Handle different return formats
                if len(result) == 2:
                    date_range, portfolio_values = result
                    rebalancing_log = None
                elif len(result) == 3:
                    date_range, portfolio_values, rebalancing_log = result
                else:
                    raise ValueError(f"Invalid backtest result format from {strategy_name}")
                
                return date_range, portfolio_values, rebalancing_log
            else:
                raise AttributeError(f"Strategy {strategy_name} does not have a 'backtest' function")
                
        except Exception as e:
            import traceback
            logger.error(f"Error executing strategy {strategy_name}: {e}")
            logger.debug(f"Full traceback:\n{traceback.format_exc()}")
            raise
    
    def get_available_strategies(self):
        """Get all available strategy names."""
        return {
            'default': list(self.default_strategies.keys()),
            'test': list(self.test_strategies.keys())
        }
    
    def validate_strategy_files(self):
        """
        Validate that all required .py files exist in strategies directory or saved subfolder based on target.json.
        
        Returns:
            List of validation errors
        """
        errors = []
        
        # Get all strategy names from target.json
        all_strategies = (self.target_config.get('default_strategies', []) + 
                         self.target_config.get('test_strategies', []))
        
        # Define search directories
        search_directories = [
            self.strategies_directory,  # Main strategies directory
            self.strategies_directory / "saved"  # Saved subfolder
        ]
        
        for strategy_name in all_strategies:
            found = False
            
            # Search in each directory for the strategy file
            for search_dir in search_directories:
                if not search_dir.exists():
                    continue
                    
                py_file = search_dir / f"{strategy_name}.py"
                
                if py_file.exists():
                    found = True
                    break
            
            if not found:
                errors.append(f"Missing strategy file: {strategy_name}.py (searched in {[str(d) for d in search_directories]})")
        
        return errors