"""
Configuration management module.

This module handles loading and managing configuration settings for
backtesting runs, including default parameters and validation.
"""

import json
from pathlib import Path
import logging
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)


@dataclass
class TestPeriod:
    """Configuration for a test period."""
    period_name: str
    period_start_date: str
    period_end_date: str


@dataclass
class BacktestConfig:
    """Main configuration for backtesting runs."""
    # Data settings
    use_extended_data: bool = True
    max_lookback_days: int = 400
    redirect_dict: dict = None
    
    # Backtest periods
    full_start_date: str = "1999.12.22"
    full_end_date: str = "2024.12.31"
    test_periods: list = None
    
    # Strategy settings
    strategies_directory: str = "./oosit_strategies"
    data_directory: str = "./csv_data"
    
    # Output settings
    output_directory: str = "./oosit_results"
    font_name: str = "바탕"
    font_size: int = 11
    
    def __post_init__(self):
        # Set default test periods if none provided
        if self.test_periods is None:
            self.test_periods = self._get_default_test_periods()
        
        # Initialize redirect_dict as empty dict if None
        if self.redirect_dict is None:
            self.redirect_dict = {}
    
    
    def _get_default_test_periods(self):
        """Get default test periods for market downturns."""
        return [
            TestPeriod("2000년 닷컴버블 하락장", "2000.03.27", "2007.10.30"),
            TestPeriod("2008년 서브프라임 금융위기 하락장", "2007.10.30", "2010.12.09"),
            TestPeriod("2011년 그리스발 재정 위기 하락장", "2011.07.26", "2012.01.19"),
            TestPeriod("2015년 1차 상하이 증시 폭락 사태", "2015.07.20", "2015.10.28"),
            TestPeriod("2016년 2차 상하이 증시 폭락 사태", "2015.12.01", "2016.07.28"),
            TestPeriod("2018년 연방정부 셧다운 발 하락장", "2018.08.29", "2019.04.16"),
            TestPeriod("2020년 코로나-19발 하락장", "2020.02.19", "2020.06.04"),
            TestPeriod("2022년 인플레이션 발 하락장", "2021.11.19", "2023.12.13"),
            TestPeriod("2024년 경기 침체 우려 하락장", "2024.07.10", "2024.11.06"),
            TestPeriod("2011년대 이후 데이터", "2011.01.03", "2024.12.31"),
        ]


class Config:
    """Configuration manager for OOSIT backtesting system."""
    
    def __init__(self, config_file=None, require_file=False):
        """
        Initialize configuration manager.
        
        Args:
            config_file: Path to configuration file (JSON format)
            require_file: If True, raise error if config file is not found
        """
        self.config_file = config_file
        
        if config_file:
            if Path(config_file).exists():
                self.config = self.load_from_file(config_file)
            else:
                if require_file:
                    raise FileNotFoundError(f"Configuration file not found: {config_file}")
                else:
                    logger.warning(f"Configuration file not found: {config_file}. Using defaults.")
                    self.config = BacktestConfig()
        else:
            self.config = BacktestConfig()
    
    def load_from_file(self, config_file):
        """
        Load configuration from JSON file.
        
        Args:
            config_file: Path to JSON configuration file
            
        Returns:
            BacktestConfig object
        """
        try:
            with open(Path(config_file), 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Convert test periods
            test_periods = []
            if 'test_periods' in data:
                for period_data in data['test_periods']:
                    if isinstance(period_data, dict):
                        test_periods.append(TestPeriod(**period_data))
                    else:
                        # Handle legacy format
                        test_periods.append(TestPeriod(
                            period_data['period_name'],
                            period_data['period_start_date'],
                            period_data['period_end_date']
                        ))
            
            # Create config object
            config = BacktestConfig()
            
            # Update fields from loaded data
            for key, value in data.items():
                if key == 'test_periods':
                    config.test_periods = test_periods
                elif key == 'redirect_dict':
                    # Handle redirect_dict specially to ensure it's a dict
                    config.redirect_dict = value if isinstance(value, dict) else {}
                elif hasattr(config, key):
                    setattr(config, key, value)
            
            logger.info(f"Loaded configuration from {config_file}")
            return config
            
        except Exception as e:
            logger.error(f"Error loading configuration from {config_file}: {e}")
            logger.info("Using default configuration")
            return BacktestConfig()
    
    def save_to_file(self, config_file):
        """
        Save current configuration to JSON file.
        
        Args:
            config_file: Path to save configuration file
        """
        try:
            # Convert to dictionary
            config_dict = asdict(self.config)
            
            # Convert test periods to list of dicts
            config_dict['test_periods'] = [
                asdict(period) for period in self.config.test_periods
            ]
            
            with open(Path(config_file), 'w', encoding='utf-8') as f:
                json.dump(config_dict, f, ensure_ascii=False, indent=4)
            
            logger.info(f"Saved configuration to {config_file}")
            
        except Exception as e:
            logger.error(f"Error saving configuration to {config_file}: {e}")
            raise
    
    def update_config(self, **kwargs):
        """
        Update configuration parameters.
        
        Args:
            **kwargs: Configuration parameters to update
        """
        for key, value in kwargs.items():
            if hasattr(self.config, key):
                setattr(self.config, key, value)
                logger.debug(f"Updated config: {key} = {value}")
            else:
                logger.warning(f"Unknown configuration parameter: {key}")
    
    def add_test_period(self, name, start_date, end_date):
        """
        Add a new test period.
        
        Args:
            name: Name of the test period
            start_date: Start date in YYYY.MM.DD format
            end_date: End date in YYYY.MM.DD format
        """
        period = TestPeriod(name, start_date, end_date)
        self.config.test_periods.append(period)
        logger.info(f"Added test period: {name}")
    
    def remove_test_period(self, name):
        """
        Remove a test period by name.
        
        Args:
            name: Name of the test period to remove
            
        Returns:
            True if period was found and removed, False otherwise
        """
        for i, period in enumerate(self.config.test_periods):
            if period.period_name == name:
                del self.config.test_periods[i]
                logger.info(f"Removed test period: {name}")
                return True
        
        logger.warning(f"Test period not found: {name}")
        return False
    
    def validate_config(self):
        """
        Validate configuration settings.
        
        Returns:
            List of validation errors (empty if valid)
        """
        errors = []
        
        # Validate date formats
        import pandas as pd
        
        try:
            pd.to_datetime(self.config.full_start_date)
        except:
            errors.append(f"Invalid full_start_date format: {self.config.full_start_date}")
        
        try:
            pd.to_datetime(self.config.full_end_date)
        except:
            errors.append(f"Invalid full_end_date format: {self.config.full_end_date}")
        
        # Validate test periods
        for i, period in enumerate(self.config.test_periods):
            try:
                start = pd.to_datetime(period.period_start_date)
                end = pd.to_datetime(period.period_end_date)
                
                if start >= end:
                    errors.append(f"Test period {i+1} '{period.period_name}': start date must be before end date")
                    
            except:
                errors.append(f"Test period {i+1} '{period.period_name}': invalid date format")
        
        # Validate directories
        if not Path(self.config.data_directory).exists():
            errors.append(f"Data directory does not exist: {self.config.data_directory}")
        
        if not Path(self.config.strategies_directory).exists():
            errors.append(f"Strategies directory does not exist: {self.config.strategies_directory}")
        
        # Validate parameters
        if self.config.max_lookback_days < -1:
            errors.append("max_lookback_days must be -1 (unlimited) or positive")
        
        if self.config.font_size <= 0:
            errors.append("font_size must be positive")
        
        return errors
    
    def get_test_periods_dict_list(self):
        """
        Get test periods as list of dictionaries (for backward compatibility).
        
        Returns:
            List of test period dictionaries
        """
        return [asdict(period) for period in self.config.test_periods]
    
    def sort_test_periods_by_date(self):
        """Sort test periods by start date."""
        import pandas as pd
        
        self.config.test_periods.sort(
            key=lambda period: pd.to_datetime(period.period_start_date)
        )
        logger.info("Sorted test periods by start date")
    
    def get_summary(self):
        """
        Get configuration summary.
        
        Returns:
            Dictionary with configuration summary
        """
        return {
            'full_period': f"{self.config.full_start_date} to {self.config.full_end_date}",
            'num_test_periods': len(self.config.test_periods),
            'use_extended_data': self.config.use_extended_data,
            'max_lookback_days': self.config.max_lookback_days,
            'redirect_dict': self.config.redirect_dict,
            'strategies_directory': self.config.strategies_directory,
            'data_directory': self.config.data_directory,
            'output_directory': self.config.output_directory
        }
    
    def create_archive_config_dict(self):
        """
        Create configuration dictionary for archive generation.
        
        Returns:
            Configuration dictionary with strategy lists for archiving
        """
        return {
            'use_extended_data': self.config.use_extended_data,
            'max_lookback_days': self.config.max_lookback_days,
            'redirect_dict': self.config.redirect_dict,
            'full_start_date': self.config.full_start_date,
            'full_end_date': self.config.full_end_date,
            'test_periods': self.get_test_periods_dict_list(),
            'strategies_directory': self.config.strategies_directory,
            'data_directory': self.config.data_directory,
            'output_directory': self.config.output_directory,
            'font_name': self.config.font_name,
            'font_size': self.config.font_size,
            'default_strategies': [],  # Will be populated by strategy manager
            'testing_strategies': []   # Will be populated by strategy manager
        }