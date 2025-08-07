"""
Data validation module for financial CSV files.

This module validates CSV files with specific naming format:
name (start_date - end_date) (frequency) (source).csv
"""

import pandas as pd
import pandas_market_calendars as mcal
import os
import glob
import re
from pathlib import Path
import numpy as np
import logging

logger = logging.getLogger(__name__)


class DataValidator:
    """Validates financial CSV data files according to OOSIT format specifications."""
    
    def __init__(self, data_directory="./csv_data"):
        """
        Initialize the DataValidator.
        
        Args:
            data_directory: Directory containing CSV files to validate
        """
        self.data_directory = Path(data_directory)
        self.nyse_calendar = mcal.get_calendar('NYSE')
    
    def validate_all_files(self):
        """
        Validate all CSV files in the data directory.
        
        Returns:
            Tuple of (validation_success, dataframes_dict, filenames_dict)
        """
        # Get all CSV files but filter out those with [!] or _raw_ prefixes
        all_csv_files = list(self.data_directory.glob('*.csv'))
        csv_files = [f for f in all_csv_files 
                     if not (f.name.startswith('[!]') or f.name.startswith('_raw_'))]
        
        if not csv_files:
            logger.warning(f"No CSV files found in {self.data_directory}")
            return False, {}, {}
        
        # Load all dataframes
        dataframes = {}
        filenames = {}
        
        for csv_file in csv_files:
            try:
                name = self._extract_name_from_filename(csv_file.name)
                dataframes[name] = pd.read_csv(csv_file)
                filenames[name] = csv_file.name
                
                # Ensure Date column is datetime
                dataframes[name]['Date'] = pd.to_datetime(dataframes[name]['Date'])
                
            except Exception as e:
                logger.error(f"Error loading {csv_file}: {e}")
                return False, {}, {}
        
        # Validate each file
        validation_results = []
        for csv_file in csv_files:
            name = self._extract_name_from_filename(csv_file.name)
            is_valid = self._validate_single_file(csv_file.name, dataframes[name])
            validation_results.append(is_valid)
        
        all_valid = all(validation_results)
        
        if all_valid:
            logger.info("Data validation successful for all files")
        else:
            logger.error("Data validation failed for one or more files")
        
        return all_valid, dataframes, filenames
    
    def _validate_single_file(self, filename, dataframe):
        """Validate a single CSV file."""
        try:
            # Extract metadata from filename
            name = self._extract_name_from_filename(filename)
            start_date = pd.to_datetime(self._extract_from_filename(filename, 'start_date'))
            end_date = pd.to_datetime(self._extract_from_filename(filename, 'end_date'))
            frequency = self._extract_from_filename(filename, 'frequency')
            
            # Validate date range
            if dataframe['Date'].iloc[0] != start_date:
                logger.error(f'Wrong start date for {filename}, expected {dataframe["Date"].iloc[0].strftime("%Y.%m.%d")}')
                return False
            
            if dataframe['Date'].iloc[-1] != end_date:
                logger.error(f'Wrong end date for {filename}, expected {dataframe["Date"].iloc[-1].strftime("%Y.%m.%d")}')
                return False
            
            # Validate frequency-specific requirements
            if frequency == 'daily':
                return self._validate_daily_data(filename, dataframe, start_date, end_date)
            elif frequency == 'monthly':
                return self._validate_monthly_data(filename, dataframe, start_date, end_date)
            else:
                logger.error(f'Unsupported data frequency for {filename}: {frequency}')
                return False
            
        except Exception as e:
            logger.error(f"Error validating {filename}: {e}")
            return False
    
    def _validate_daily_data(self, filename, dataframe, 
                           start_date, end_date):
        """Validate daily frequency data against NYSE calendar."""
        expected_dates = self._get_nyse_open_dates(start_date, end_date)
        
        if len(expected_dates) != len(dataframe['Date']):
            logger.error(f'Date length mismatch for {filename}, NYSE: {len(expected_dates)}, file: {len(dataframe["Date"])}')
            return False
        
        for i, (actual_date, expected_date) in enumerate(zip(dataframe['Date'], expected_dates)):
            if actual_date != expected_date:
                logger.error(f'Date mismatch in {filename} at index {i}, NYSE: {expected_date}, file: {actual_date}')
                return False
        
        return self._validate_data_quality(filename, dataframe)
    
    def _validate_monthly_data(self, filename, dataframe,
                             start_date, end_date):
        """Validate monthly frequency data."""
        expected_dates = pd.date_range(start=start_date, end=end_date, freq='MS').tolist()
        
        if len(expected_dates) != len(dataframe['Date']):
            logger.error(f'Date length mismatch for {filename}, expected months: {len(expected_dates)}, file: {len(dataframe["Date"])}')
            return False
        
        for i, (actual_date, expected_date) in enumerate(zip(dataframe['Date'], expected_dates)):
            if actual_date != expected_date:
                logger.error(f'Date mismatch in {filename} at index {i}, expected: {expected_date}, file: {actual_date}')
                return False
        
        return self._validate_data_quality(filename, dataframe)
    
    def _validate_data_quality(self, filename, dataframe):
        """Validate data quality (no blank or NaN values in non-Date columns)."""
        for col in dataframe.drop(columns='Date').columns:
            if dataframe[col].isnull().any() or dataframe[col].eq('').any():
                logger.error(f'Blank or NaN data found in {filename}, column: {col}')
                return False
            
            # Check if values are numeric
            if not pd.api.types.is_numeric_dtype(dataframe[col]):
                try:
                    pd.to_numeric(dataframe[col])
                except (ValueError, TypeError):
                    logger.error(f'Non-numeric data found in {filename}, column: {col}')
                    return False
        
        return True
    
    def _get_nyse_open_dates(self, start_date, end_date):
        """Get NYSE open dates for the given range."""
        schedule = self.nyse_calendar.schedule(start_date=start_date, end_date=end_date)
        return schedule.index.tolist()
    
    def _extract_from_filename(self, filename, attribute):
        """Extract specific attribute from filename."""
        attribute_order = ['name', 'start_date', 'end_date', 'frequency', 'source']
        
        # Remove .csv extension and split by parentheses and dashes
        name_part = filename[:-4]  # Remove .csv
        parts = re.split(r'\s*\(|\)\s*|\s+\-\s+', name_part)
        parts = [x for x in parts if x]  # Remove empty strings
        
        if len(parts) < len(attribute_order):
            raise ValueError(f"Invalid filename format: {filename}")
        
        return parts[attribute_order.index(attribute)]
    
    def _extract_name_from_filename(self, filename):
        """Extract name from filename."""
        return self._extract_from_filename(filename, 'name')
    
    @staticmethod
    def is_blank_or_not_number(element):
        """Check if an element is blank or not a real number."""
        if isinstance(element, str) and element.strip() == "":
            return True
        try:
            float(element)
            return False
        except (ValueError, TypeError):
            return True