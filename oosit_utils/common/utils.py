"""Common utility functions for OOSIT system."""

import pandas as pd
import numpy as np
from ..data.validator import DataValidator


def format_position(position):
    """
    Format position dictionary for display.
    
    Args:
        position: Dictionary mapping tickers to position weights
        
    Returns:
        Formatted string representation of the position
    """
    if not position:
        return "현금 100%"
    
    # Filter out negligible positions
    filtered_position = {k: v for k, v in position.items() if v > 0.001}
    
    if not filtered_position:
        return "현금 100%"
    
    # Format as "TICKER(percentage)"
    position_parts = [f"{ticker}({value:.1%})" for ticker, value in filtered_position.items()]
    return " : ".join(position_parts)


def clean_yfinance_data(df):
    """
    Clean data downloaded from yfinance to match OOSIT data format.
    
    This function:
    - Sorts data by date
    - Converts non-numeric values to NaN
    - Forward fills NaN values
    - Aligns data to NYSE open dates
    
    Args:
        df: DataFrame with yfinance data (must have Date column)
        
    Returns:
        Cleaned DataFrame aligned to NYSE open dates
    """
    # Sort by Date in ascending order (earliest first)
    df = df.sort_values(by='Date')
    
    # Convert non-numeric values to NaN for all columns except Date
    for col in df.columns:
        if col != 'Date':
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Forward fill NaN data
    df = df.ffill()
    
    # Get NYSE open dates for the data range
    start_date = df['Date'].iloc[0]
    end_date = df['Date'].iloc[-1]
    
    validator = DataValidator()
    dates = validator._get_nyse_open_dates(
        pd.to_datetime(start_date),
        pd.to_datetime(end_date)
    )
    
    # Create new dataframe with only NYSE open dates
    labels = df.columns.drop('Date').tolist()
    
    data_dict = {}
    data_dict['Date'] = dates
    for label in labels:
        data_dict[label] = [0] * len(dates)
    
    # Fill in data using two pointers approach
    for label in labels:
        search_index = 0
        latest_value = None
        
        for i in range(len(dates)):
            date = dates[i]
            
            while search_index < len(df) and df['Date'].iloc[search_index] <= date:
                latest_value = df[label].iloc[search_index]
                search_index += 1
            
            if latest_value is not None:
                data_dict[label][i] = latest_value
    
    result_df = pd.DataFrame(data_dict)
    return result_df