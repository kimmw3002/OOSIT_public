"""Cache utilities for optimized operations."""

import pandas as pd
import numpy as np
from functools import lru_cache
from pathlib import Path
import logging
import re

logger = logging.getLogger(__name__)


class NYSEDateCache:
    """Singleton cache for NYSE trading dates to avoid repeated calculations."""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
            
        self._initialized = True
        self._date_ranges = {}
        self._date_indices = {}
    
    @lru_cache(maxsize=1000)
    def get_nyse_dates(self, start_date, end_date):
        """Get NYSE trading dates for a date range (cached)."""
        # Convert to string for caching
        cache_key = f"{start_date}_{end_date}"
        
        if cache_key in self._date_ranges:
            return self._date_ranges[cache_key]
        
        # Use the same method as original to ensure consistency
        try:
            import pandas_market_calendars as mcal
            nyse = mcal.get_calendar('NYSE')
            schedule = nyse.schedule(start_date=start_date, end_date=end_date)
            nyse_dates = [pd.Timestamp(date).to_pydatetime() for date in schedule.index.tolist()]
        except:
            # Fallback to simple business days if pandas_market_calendars fails
            logger.warning("Failed to use pandas_market_calendars, falling back to bdate_range")
            dates = pd.bdate_range(start=start_date, end=end_date, freq='B')
            nyse_dates = [date.to_pydatetime() for date in dates]
        
        # Cache the result
        self._date_ranges[cache_key] = nyse_dates
        
        return nyse_dates
    
    def get_date_index_map(self, date_range):
        """Get a mapping from dates to indices for fast lookup."""
        # Create a hash of the date range for caching
        cache_key = hash(tuple(str(d) for d in date_range[:10]))  # Use first 10 dates for hash
        
        if cache_key in self._date_indices:
            return self._date_indices[cache_key]
        
        # Create index mapping
        date_to_idx = {date: idx for idx, date in enumerate(date_range)}
        
        # Cache it (limit cache size)
        if len(self._date_indices) < 100:
            self._date_indices[cache_key] = date_to_idx
        
        return date_to_idx
    
    def clear_cache(self):
        """Clear the cache (useful for testing)."""
        self._date_ranges = {}
        self._date_indices = {}


class FilenameParser:
    """Cached filename parser to avoid repeated regex operations."""
    
    def __init__(self):
        self._pattern = re.compile(
            r'^(.*?)\s*\((\d{4}\.\d{2}\.\d{2})\s*-\s*(\d{4}\.\d{2}\.\d{2})\)\s*\((\w+)\)\s*\((\w+)\)\.csv$'
        )
    
    @lru_cache(maxsize=1000)
    def parse(self, filename):
        """Parse filename and extract components (cached)."""
        match = self._pattern.match(filename)
        if not match:
            raise ValueError(f"Invalid filename format: {filename}")
        
        return {
            'name': match.group(1).strip(),
            'start_date': match.group(2),
            'end_date': match.group(3),
            'frequency': match.group(4),
            'source': match.group(5)
        }
    
    def extract(self, filename, field):
        """Extract specific field from filename."""
        parsed = self.parse(filename)
        return parsed.get(field)