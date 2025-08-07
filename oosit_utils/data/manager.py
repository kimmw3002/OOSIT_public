"""Data management module for financial data access and indexing."""

import pandas as pd
import numpy as np
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from .validator import DataValidator
from ..common import NYSEDateCache, FilenameParser

logger = logging.getLogger(__name__)


class DataManager:
    """Manages validated financial data with efficient indexing and access."""
    
    def __init__(self, data_directory="./csv_data", use_extended_data=False, redirect_dict=None, max_lookback_days=400):
        """
        Initialize the DataManager.
        
        Args:
            data_directory: Directory containing CSV files
            use_extended_data: Whether to prefer extended data (prefixed with 'ext_')
            redirect_dict: Dictionary mapping original asset names to replacement asset names for data redirection
            max_lookback_days: Maximum days to look back for MAX calculations (-1 for unlimited)
        """
        self.data_directory = data_directory
        self.use_extended_data = use_extended_data
        self.redirect_dict = redirect_dict or {}
        self.max_lookback_days = max_lookback_days
        
        # Data storage
        self.dataframes = {}
        self.filenames = {}
        
        # NumPy array cache for optimized access
        self.data_arrays = {}  # name -> numpy array
        self.date_arrays = {}  # name -> date array
        self.metadata = {}     # name -> metadata dict
        
        # Initialize caches
        self.nyse_cache = NYSEDateCache()
        self.filename_parser = FilenameParser()
        
        # Date ranges for efficient indexing
        self.daily_date_range = None
        self.monthly_date_range = None
        
        # Start indices for each dataset
        self.daily_data_start_index = {}
        self.monthly_data_start_index = {}
        
        # Backup for restoration after redirection
        self.original_dataframes_backup = {}
        self.original_daily_indices_backup = {}
        self.original_monthly_indices_backup = {}
        
        # Default labels by source
        self.default_label_by_source = {
            'yfinance': 'Open',
            'MacroMicro': 'Value',
            'FRED': 'Value'
        }
        
        # Load and validate data
        self._load_data()
        
        # Apply redirection if provided
        if self.redirect_dict:
            self._apply_redirection()
    
    def _load_data(self):
        """Load and validate all data files with parallel processing."""
        # First validate all files
        validator = DataValidator(self.data_directory)
        is_valid, dataframes, filenames = validator.validate_all_files()
        
        if not is_valid:
            raise ValueError("Data validation failed. Cannot proceed with invalid data.")
        
        self.dataframes = dataframes
        self.filenames = filenames
        
        # Pre-compute NumPy arrays for fast access
        self._create_numpy_cache()
        
        # Create date range indices
        self._create_date_indices()
        
        logger.info(f"Loaded {len(self.dataframes)} datasets successfully")
    
    def _create_numpy_cache(self):
        """Pre-compute NumPy arrays for fast data access."""
        for name, filename in self.filenames.items():
            df = self.dataframes[name]
            source = self._extract_from_filename(filename, 'source')
            
            # Store metadata
            self.metadata[name] = {
                'source': source,
                'frequency': self._extract_from_filename(filename, 'frequency'),
                'start_date': self._extract_from_filename(filename, 'start_date'),
                'end_date': self._extract_from_filename(filename, 'end_date')
            }
            
            # Cache default data column as NumPy array
            label = self.default_label_by_source.get(source, 'Value')
            if label in df.columns:
                self.data_arrays[name] = df[label].values
            
            # Cache dates as NumPy array
            if 'Date' in df.columns:
                self.date_arrays[name] = df['Date'].values
        
        logger.info(f"Created NumPy cache for {len(self.data_arrays)} datasets")
    
    def _create_date_indices(self):
        """Create efficient date range indices for daily and monthly data."""
        daily_files = []
        monthly_files = []
        
        # Separate files by frequency
        for name, filename in self.filenames.items():
            frequency = self._extract_from_filename(filename, 'frequency')
            if frequency == 'daily':
                daily_files.append((name, filename))
            elif frequency == 'monthly':
                monthly_files.append((name, filename))
        
        # Create daily date range
        if daily_files:
            start_dates = []
            end_dates = []
            
            for name, filename in daily_files:
                start_date = pd.to_datetime(self._extract_from_filename(filename, 'start_date'))
                end_date = pd.to_datetime(self._extract_from_filename(filename, 'end_date'))
                start_dates.append(start_date)
                end_dates.append(end_date)
            
            # Create master daily date range using cache
            self.daily_date_range = self.nyse_cache.get_nyse_dates(
                min(start_dates).strftime('%Y-%m-%d'), 
                max(end_dates).strftime('%Y-%m-%d')
            )
            
            # Calculate start indices
            for name, filename in daily_files:
                start_date = pd.to_datetime(self._extract_from_filename(filename, 'start_date'))
                self.daily_data_start_index[name] = self._binary_search_date(self.daily_date_range, start_date)
        
        # Create monthly date range
        if monthly_files:
            start_dates = []
            end_dates = []
            
            for name, filename in monthly_files:
                start_date = pd.to_datetime(self._extract_from_filename(filename, 'start_date'))
                end_date = pd.to_datetime(self._extract_from_filename(filename, 'end_date'))
                start_dates.append(start_date)
                end_dates.append(end_date)
            
            # Create master monthly date range
            self.monthly_date_range = pd.date_range(
                start=min(start_dates), 
                end=max(end_dates), 
                freq='MS'
            ).tolist()
            
            # Calculate start indices
            for name, filename in monthly_files:
                start_date = pd.to_datetime(self._extract_from_filename(filename, 'start_date'))
                self.monthly_data_start_index[name] = self._binary_search_date(
                    self.monthly_date_range, 
                    start_date.replace(day=1)
                )
    
    def get_data_accessor(self, backtest_start_date):
        """
        Get a data accessor function configured for a specific backtest period.
        
        Args:
            backtest_start_date: Start date of the backtest period (YYYY.MM.DD format)
            
        Returns:
            Function that can access data by (name, date_range_index, optional_property)
        """
        start_date = pd.to_datetime(backtest_start_date)
        
        # Calculate start indices for this backtest
        daily_start_index = None
        monthly_start_index = None
        
        if self.daily_date_range:
            daily_start_index = self._binary_search_date(self.daily_date_range, start_date)
        
        if self.monthly_date_range:
            monthly_start_index = self._binary_search_date(
                self.monthly_date_range, 
                start_date.replace(day=1)
            )
        
        def get_value(name, date_range_index, optional_property=''):
            """
            Access data for a specific asset at a specific time index.
            
            Args:
                name: Asset name
                date_range_index: Index in the backtest date range
                optional_property: Specific property or technical indicator
                
            Returns:
                The requested data value
            """
            # Use extended data if available and configured
            actual_name = name
            if self.use_extended_data:
                ext_name = f"ext_{name}"
                if ext_name in self.dataframes:
                    actual_name = ext_name
            
            if actual_name not in self.dataframes:
                raise ValueError(f"Data not found for: {actual_name}")
            
            filename = self.filenames[actual_name]
            source = self._extract_from_filename(filename, 'source')
            frequency = self._extract_from_filename(filename, 'frequency')
            
            # Calculate the actual data index
            if frequency == 'daily':
                if daily_start_index is None:
                    raise ValueError("No daily data available for this backtest period")
                data_index = daily_start_index + date_range_index - self.daily_data_start_index[actual_name]
            elif frequency == 'monthly':
                if monthly_start_index is None:
                    raise ValueError("No monthly data available for this backtest period")
                # Convert daily index to monthly
                date1 = self.daily_date_range[daily_start_index] if daily_start_index is not None else start_date
                date2 = self.daily_date_range[daily_start_index + date_range_index] if daily_start_index is not None else start_date
                months_diff = 12 * (date2.year - date1.year) + (date2.month - date1.month)
                data_index = monthly_start_index + months_diff - self.monthly_data_start_index[actual_name]
            else:
                raise ValueError(f"Unsupported frequency: {frequency}")
            
            # Prevent negative index cycling - raise error for any negative index
            # This ensures strategies handle missing lookback data explicitly via try-except
            if data_index < 0:
                raise IndexError(f"Negative index {data_index} requested, which would cycle to end of data")
            
            # Get the requested property
            if optional_property == '':
                # Use NumPy array cache for faster access
                if actual_name in self.data_arrays:
                    value = self.data_arrays[actual_name][data_index]
                    # Convert numpy types to Python native types
                    if hasattr(value, 'item'):
                        return value.item()
                    return value
                else:
                    # Fallback to pandas for compatibility
                    label = self.default_label_by_source.get(source, 'Value')
                    value = self.dataframes[actual_name][label].iloc[data_index]
                    # Convert numpy types to Python native types
                    if hasattr(value, 'item'):
                        return value.item()
                    return value
            elif optional_property == 'Date':
                # Use NumPy date array cache for faster access
                if actual_name in self.date_arrays:
                    value = self.date_arrays[actual_name][data_index]
                    # Ensure consistent datetime handling
                    if hasattr(value, 'to_pydatetime'):
                        return value.to_pydatetime()
                    return value
                else:
                    # Fallback to pandas for compatibility
                    value = self.dataframes[actual_name]['Date'].iloc[data_index]
                    # Ensure we return a scalar datetime, not an array
                    if hasattr(value, '__len__') and not isinstance(value, str):
                        return value.iloc[0] if hasattr(value, 'iloc') else value[0]
                    return value
            else:
                # Handle special properties and technical indicators
                value = self._get_property_value(actual_name, source, data_index, optional_property)
                return value
        
        return get_value
    
    def _get_property_value(self, name, source, data_index, property_name):
        """Get a specific property value, computing technical indicators if needed."""
        from ..indicators import TechnicalIndicators
        
        dataframe = self.dataframes[name]
        
        # Check if property already exists in dataframe
        if property_name in dataframe.columns:
            # Access existing column (negative indices already checked in calling function)
            value = dataframe[property_name].iloc[data_index]
                
            # Convert numpy/pandas types to Python native types
            if hasattr(value, 'item'):
                return value.item()
            elif pd.api.types.is_datetime64_any_dtype(type(value)):
                # Keep datetime objects as-is for comparisons
                # Make sure we return a scalar, not a Series
                if hasattr(value, '__len__') and not isinstance(value, str):
                    return value.iloc[0] if hasattr(value, 'iloc') else value[0]
                return value
            return value
        
        # Try to get as a direct column
        try:
            # Access column directly (negative indices already checked in calling function)
            value = dataframe[property_name].iloc[data_index]
            # Convert numpy/pandas types to Python native types
            if hasattr(value, 'item'):
                return value.item()
            elif pd.api.types.is_datetime64_any_dtype(type(value)):
                # Keep datetime objects as-is for comparisons
                # Make sure we return a scalar, not a Series
                if hasattr(value, '__len__') and not isinstance(value, str):
                    return value.iloc[0] if hasattr(value, 'iloc') else value[0]
                return value
            return value
        except KeyError:
            pass
        
        # Check NumPy cache first for computed properties
        cache_key = f"{name}_{property_name}"
        if cache_key in self.data_arrays:
            value = self.data_arrays[cache_key][data_index]
            if hasattr(value, 'item'):
                return value.item()
            return value
        
        # Compute technical indicators on demand
        indicators = TechnicalIndicators(dataframe, source, self.default_label_by_source, max_lookback_days=self.max_lookback_days)
        computed_values = indicators.compute_indicator(property_name)
        
        if computed_values is not None:
            # Cache the computed values
            self.dataframes[name][property_name] = computed_values
            # Also cache as NumPy array for faster future access
            self.data_arrays[cache_key] = np.array(computed_values)
            value = computed_values[data_index]
            # Convert numpy/pandas types to Python native types
            if hasattr(value, 'item'):
                return value.item()
            return value
        
        raise ValueError(f'Undefined property: {property_name} for {name}')
    
    def _apply_redirection(self):
        """Apply data redirection based on redirect_dict."""
        if not self.redirect_dict:
            return
            
        logger.info(f"Applying data redirection: {self.redirect_dict}")
        
        for original_name, new_source_name in self.redirect_dict.items():
            # Check if both assets exist before attempting a swap
            if original_name in self.dataframes and new_source_name in self.dataframes:
                # 1. Backup and swap the DataFrame
                self.original_dataframes_backup[original_name] = self.dataframes[original_name].copy()
                self.dataframes[original_name] = self.dataframes[new_source_name].copy()
                
                # Also swap NumPy arrays
                if original_name in self.data_arrays and new_source_name in self.data_arrays:
                    self.data_arrays[original_name] = self.data_arrays[new_source_name].copy()
                if original_name in self.date_arrays and new_source_name in self.date_arrays:
                    self.date_arrays[original_name] = self.date_arrays[new_source_name].copy()
                if original_name in self.metadata and new_source_name in self.metadata:
                    self.metadata[original_name] = self.metadata[new_source_name].copy()

                # 2. Backup and swap the Daily Start Index (if it exists)
                if original_name in self.daily_data_start_index and new_source_name in self.daily_data_start_index:
                    self.original_daily_indices_backup[original_name] = self.daily_data_start_index[original_name]
                    self.daily_data_start_index[original_name] = self.daily_data_start_index[new_source_name]
                
                # 3. Backup and swap the Monthly Start Index (if it exists)
                if original_name in self.monthly_data_start_index and new_source_name in self.monthly_data_start_index:
                    self.original_monthly_indices_backup[original_name] = self.monthly_data_start_index[original_name]
                    self.monthly_data_start_index[original_name] = self.monthly_data_start_index[new_source_name]

                logger.info(f"Redirected '{original_name}' to use data and metadata from '{new_source_name}'")
            else:
                logger.warning(f"Cannot redirect '{original_name}' -> '{new_source_name}'. One or both not found in dataframes.")
    
    def restore_original_data(self):
        """Restore original data after redirection."""
        if self.original_dataframes_backup:
            logger.info("Restoring original data and metadata...")
            # Restore the original dataframes, daily indices, and monthly indices
            self.dataframes.update(self.original_dataframes_backup)
            self.daily_data_start_index.update(self.original_daily_indices_backup)
            self.monthly_data_start_index.update(self.original_monthly_indices_backup)
            
            # Clear backups
            self.original_dataframes_backup.clear()
            self.original_daily_indices_backup.clear()
            self.original_monthly_indices_backup.clear()
            
            logger.info("Restoration complete.")
    
    def _binary_search_date(self, date_list, target_date):
        """Binary search for a date in a sorted list."""
        left, right = 0, len(date_list) - 1
        
        while left <= right:
            mid = (left + right) // 2
            if date_list[mid] == target_date:
                return mid
            elif date_list[mid] < target_date:
                left = mid + 1
            else:
                right = mid - 1
        
        raise ValueError(f"Date {target_date} not found in available date range")
    
    def _extract_from_filename(self, filename, attribute):
        """Extract attribute from filename using cached parser."""
        return self.filename_parser.extract(filename, attribute)
    
    def get_available_assets(self):
        """Get list of available asset names."""
        return list(self.dataframes.keys())
    
    def get_date_range(self, frequency='daily'):
        """Get the master date range for the specified frequency."""
        if frequency == 'daily':
            return self.daily_date_range
        elif frequency == 'monthly':
            return self.monthly_date_range
        else:
            raise ValueError(f"Unsupported frequency: {frequency}")
    
    def get_asset_info(self, name):
        """Get information about a specific asset."""
        if name not in self.filenames:
            raise ValueError(f"Asset not found: {name}")
        
        filename = self.filenames[name]
        return {
            'name': name,
            'filename': filename,
            'source': self._extract_from_filename(filename, 'source'),
            'frequency': self._extract_from_filename(filename, 'frequency'),
            'start_date': self._extract_from_filename(filename, 'start_date'),
            'end_date': self._extract_from_filename(filename, 'end_date'),
            'columns': list(self.dataframes[name].columns)
        }