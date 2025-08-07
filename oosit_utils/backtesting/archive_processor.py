"""
Archive processing utilities for the backtesting system.

This module provides functionality to process archived backtest results,
maintaining compatibility with the main backtesting engine.
"""

import tarfile
import json
import tempfile
import shutil
import logging
from contextlib import contextmanager
from pathlib import Path
import pandas as pd

from ..data import DataManager
from ..strategies import StrategyManager
from .engine import BacktestEngine, BacktestResult

logger = logging.getLogger(__name__)


class ArchiveProcessor:
    """Process archived backtest results using OOSIT utilities."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.temp_dir = None
    
    @contextmanager
    def extract_archive(self, archive_path):
        """Context manager for safe archive extraction."""
        try:
            self.temp_dir = tempfile.mkdtemp(prefix="oosit_archive_")
            self.logger.info(f"Extracting archive to {self.temp_dir}")
            
            with tarfile.open(Path(archive_path), "r:gz") as tar:
                tar.extractall(path=self.temp_dir, filter="data")
            
            yield self.temp_dir
            
        finally:
            if self.temp_dir and Path(self.temp_dir).exists():
                shutil.rmtree(self.temp_dir)
                self.logger.info(f"Cleaned up temporary directory {self.temp_dir}")
    
    def load_archive(self, archive_path):
        """
        Load and process a backtest archive.
        
        Returns:
            tuple: (dataframes, strategy_results, config, rebalancing_log_df)
        """
        with self.extract_archive(archive_path) as temp_dir:
            # Load configuration
            config = self._load_config(temp_dir)
            
            # Setup data manager with proper configuration
            data_manager = self._create_data_manager(config)
            
            # Setup strategy manager
            strategy_manager = self._create_strategy_manager(temp_dir, config)
            
            # Create backtest engine for processing
            backtest_engine = BacktestEngine(data_manager, strategy_manager)
            
            # Process strategies
            results = self._process_archive_strategies(
                backtest_engine, strategy_manager, config, data_manager
            )
            
            return (
                data_manager.dataframes,
                results['strategy_results'],
                config,
                results['rebalancing_log_df']
            )
    
    def _load_config(self, temp_dir):
        """Load configuration from archive."""
        config_path = Path(temp_dir) / 'configure.json'
        if not config_path.exists():
            raise FileNotFoundError("configure.json not found in archive")
        
        with open(Path(config_path), 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        # Also load target.json if it exists
        target_path = Path(temp_dir) / 'target.json'
        if target_path.exists():
            with open(Path(target_path), 'r', encoding='utf-8') as f:
                target_config = json.load(f)
                config['default_strategies'] = target_config.get('default_strategies', [])
                config['test_strategies'] = target_config.get('test_strategies', [])
        
        return config
    
    def _create_data_manager(self, config):
        """Create data manager with archive configuration."""
        from ..data import DataValidator
        
        # Validate base data
        validator = DataValidator("./csv_data")
        is_valid, _, _ = validator.validate_all_files()
        
        if not is_valid:
            raise ValueError("Data validation failed. Check base CSV files.")
        
        # Create data manager with configuration
        return DataManager(
            data_directory="./csv_data",
            use_extended_data=config.get("use_extended_data", False),
            redirect_dict=config.get("redirect_dict") or config.get("data_redirection"),
            max_lookback_days=config.get("max_lookback_days", 400)
        )
    
    def _create_strategy_manager(self, temp_dir, config):
        """Create strategy manager for archive strategies."""
        # Prepare strategy config from archive config
        strategy_config = {
            'default_strategies': config.get('default_strategies', []),
            'test_strategies': config.get('testing_strategies', [])
        }
        
        # If no strategies in config, look for Python files in temp directory
        if not strategy_config['default_strategies'] and not strategy_config['test_strategies']:
            temp_path = Path(temp_dir)
            py_files = [f.name for f in temp_path.glob('*.py')]
            strategy_names = [f[:-3] for f in py_files]
            strategy_config['test_strategies'] = strategy_names
        
        # Create strategy manager with temp directory and config
        original_cwd = Path.cwd()
        try:
            import os
            os.chdir(temp_dir)
            return StrategyManager(strategies_directory=temp_dir, strategy_config=strategy_config)
        finally:
            os.chdir(str(original_cwd))
    
    def _process_archive_strategies(self, backtest_engine, strategy_manager, config, data_manager):
        """Process strategies from archive using backtest engine."""
        full_start_date = config['full_start_date']
        full_end_date = config['full_end_date']
        
        # Load strategies
        default_strategies, test_strategies = strategy_manager.load_all_strategies(data_manager)
        all_strategies = {**default_strategies, **test_strategies}
        
        strategy_results = {}
        rebalancing_tracks = {}
        
        # Process each strategy
        for strategy_name in all_strategies.keys():
            try:
                self.logger.info(f"Processing strategy: {strategy_name}")
                
                # Execute strategy with proper error handling
                date_range, pv, rebalancing_log = strategy_manager.execute_strategy(
                    strategy_name, full_start_date, full_end_date, data_manager
                )
                
                strategy_results[strategy_name] = (date_range, pv)
                rebalancing_tracks[strategy_name] = rebalancing_log
                
                self.logger.info(f"Successfully processed strategy: {strategy_name}")
                
            except IndexError as e:
                if "Negative index" in str(e):
                    self.logger.warning(
                        f"Strategy {strategy_name} requires more historical data than available"
                    )
                    strategy_results[strategy_name] = ([], [])
                    rebalancing_tracks[strategy_name] = []
                else:
                    raise
            except Exception as e:
                self.logger.error(f"Error processing strategy {strategy_name}: {e}")
                strategy_results[strategy_name] = ([], [])
                rebalancing_tracks[strategy_name] = []
        
        # Restore data manager state
        data_manager.restore_original_data()
        
        # Generate rebalancing log using engine's method
        display_names = [backtest_engine._get_display_name(name) for name in strategy_results.keys()]
        strategy_name_mapping = {name: backtest_engine._get_display_name(name) for name in strategy_results.keys()}
        
        # Create mock results for rebalancing log generation
        mock_results = {}
        for strategy_name, (date_range, pv) in strategy_results.items():
            # Check if date_range and pv have data (handle numpy arrays/lists properly)
            has_data = False
            try:
                if date_range is not None and pv is not None:
                    if hasattr(date_range, '__len__') and hasattr(pv, '__len__'):
                        has_data = len(date_range) > 0 and len(pv) > 0
                    else:
                        has_data = bool(date_range) and bool(pv)
            except:
                has_data = False
                
            if has_data:
                mock_results[strategy_name] = {
                    "Full Period": BacktestResult(
                        strategy_name=strategy_name,
                        display_name=strategy_name_mapping[strategy_name],
                        period_name="Full Period",
                        date_range=date_range,
                        portfolio_values=pv,
                        normalized_values=[],
                        total_return=0,
                        max_drawdown=0,
                        rebalancing_log=rebalancing_tracks.get(strategy_name)
                    )
                }
        
        # Temporarily set engine results and generate log
        backtest_engine.results = mock_results
        rebalancing_log_df = backtest_engine._create_rebalancing_log(
            display_names, strategy_name_mapping
        )
        
        return {
            'strategy_results': strategy_results,
            'rebalancing_log_df': rebalancing_log_df
        }