#!/usr/bin/env python3
"""
Data Extender - Creates extended data of leveraged ETFs
Converts the Data Extender notebook into a command-line script with interactive input.
"""

import pandas as pd
import re
import numpy as np
from pathlib import Path
import sys

# Add parent directory to path to import oosit_utils
sys.path.append(str(Path(__file__).parent.parent))
from oosit_utils.data.validator import DataValidator


def extend_etf_history(base_etf_file, leveraged_etf_file, leverage):
    """
    Creates a synthetic, back-filled price history for a leveraged or inverse ETF
    based on the historical price data of its underlying, non-leveraged ETF.

    This function can extend the history both into the past and into the future,
    filling any gaps where the base ETF has data but the leveraged/inverse one does not.
    It correctly handles both positive (e.g., 3 for TQQQ) and negative 
    (e.g., -1 for PSQ, -3 for SQQQ) leverage multipliers.

    Args:
        base_etf_file (str): Path to the CSV file for the base ETF (e.g., QQQ).
        leveraged_etf_file (str or None): Path to the CSV file for the leveraged/inverse ETF (e.g., TQQQ).
                                          If None, fabricates a fully synthetic leveraged ETF.
        leverage (float): The daily leverage multiplier (e.g., 3, -1, -2, -3).

    Returns:
        pd.DataFrame: A DataFrame containing the complete, extended price history.
    """
    # --- 1. Load and Prepare Data ---
    base_etf = pd.read_csv(base_etf_file, parse_dates=['Date'])
    
    # Handle fabrication mode
    if leveraged_etf_file is None:
        # Create a dummy leveraged ETF with just the first date
        leveraged_etf = pd.DataFrame({
            'Date': [base_etf['Date'].iloc[0]],
            'Open': [base_etf['Open'].iloc[0]],
            'High': [base_etf['High'].iloc[0]],
            'Low': [base_etf['Low'].iloc[0]],
            'Close': [base_etf['Close'].iloc[0]],
            'Adj Close': [base_etf['Adj Close'].iloc[0]]
        })
    else:
        leveraged_etf = pd.read_csv(leveraged_etf_file, parse_dates=['Date'])
    
    # Sort by date to ensure proper calculations and reset index
    base_etf = base_etf.sort_values('Date').reset_index(drop=True)
    leveraged_etf = leveraged_etf.sort_values('Date').reset_index(drop=True)

    # Keep only relevant columns
    base_etf = base_etf[['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close']]
    leveraged_etf = leveraged_etf[['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close']]

    # --- 2. Pre-calculate Base ETF's Component Returns (Vectorized) ---
    base_etf_prev_close = base_etf['Close'].shift(1)
    base_etf['Close_Return'] = base_etf['Close'].pct_change()
    base_etf['Overnight_Return'] = (base_etf['Open'] / base_etf_prev_close) - 1
    base_etf['Intraday_High_Return'] = (base_etf['High'] / base_etf['Open']) - 1
    base_etf['Intraday_Low_Return'] = (base_etf['Low'] / base_etf['Open']) - 1

    # --- 3. Initialize the Simulation ---
    # Create the estimated dataframe with the FULL date range from the base ETF
    estimated_etf = base_etf[['Date']].copy()
    
    # Create empty columns for our simulated data
    for col in ['Open', 'High', 'Low', 'Close', 'Adj Close']:
        estimated_etf[col] = np.nan

    # Manually "seed" the VERY FIRST ROW with a placeholder value.
    # This value is arbitrary; the scaling factor will correct it later.
    for col in ['Open', 'High', 'Low', 'Close', 'Adj Close']:
        estimated_etf.loc[0, col] = 100.0

    # --- 4. Run the Refined Simulation Loop (Handles Positive and Negative Leverage) ---
    for i in range(1, len(estimated_etf)):
        # Get previous day's simulated close
        prev_sim_close = estimated_etf.loc[i - 1, 'Close']
        
        # Get today's base ETF component returns
        base_overnight_ret = base_etf.loc[i, 'Overnight_Return']
        base_intraday_high_ret = base_etf.loc[i, 'Intraday_High_Return']
        base_intraday_low_ret = base_etf.loc[i, 'Intraday_Low_Return']
        base_close_ret = base_etf.loc[i, 'Close_Return']
        
        # Universal calculations for Open and Close
        sim_open = prev_sim_close * (1 + leverage * base_overnight_ret)
        sim_close = prev_sim_close * (1 + leverage * base_close_ret)
        
        # *** KEY LOGIC: Conditional High/Low calculation ***
        if leverage > 0:
            # Standard logic for long leveraged ETFs
            sim_high_point = sim_open * (1 + leverage * base_intraday_high_ret)
            sim_low_point = sim_open * (1 + leverage * base_intraday_low_ret)
        else:  # leverage < 0 for inverse ETFs
            # Inverted logic: Base ETF's high causes the inverse's low, and vice-versa
            sim_high_point = sim_open * (1 + leverage * base_intraday_low_ret) # Use base's low to calculate inverse's high
            sim_low_point = sim_open * (1 + leverage * base_intraday_high_ret)  # Use base's high to calculate inverse's low
        
        # The crucial correction step to ensure a valid OHLC bar
        final_sim_high = max(sim_open, sim_high_point, sim_close)
        final_sim_low = min(sim_open, sim_low_point, sim_close)
        
        # Assign values to the dataframe
        estimated_etf.loc[i, 'Open'] = sim_open
        estimated_etf.loc[i, 'Close'] = sim_close
        estimated_etf.loc[i, 'High'] = final_sim_high
        estimated_etf.loc[i, 'Low'] = final_sim_low

        # Adj Close logic is universal and based on the Close return
        prev_sim_adj_close = estimated_etf.loc[i - 1, 'Adj Close']
        estimated_etf.loc[i, 'Adj Close'] = prev_sim_adj_close * (1 + leverage * base_close_ret)

    # --- 5. Scale and Merge ---
    first_leveraged_date = leveraged_etf['Date'].min()
    
    # Handle cases where there is no overlap
    if first_leveraged_date not in estimated_etf['Date'].values:
        raise ValueError("No overlapping dates found between the two ETF files. Cannot scale the data.")
        
    for col in ['Open', 'High', 'Low', 'Close', 'Adj Close']:
        # Find the real value on its first day of trading
        first_real_value = leveraged_etf.loc[leveraged_etf['Date'] == first_leveraged_date, col].values[0]
        # Find our simulated value on that same day
        estimated_value_at_start = estimated_etf.loc[estimated_etf['Date'] == first_leveraged_date, col].values[0]
        # Calculate the scaling factor
        scaling_factor = first_real_value / estimated_value_at_start
        # Apply the scaling factor to the entire simulated history
        estimated_etf[col] *= scaling_factor
    
    # Merge estimated history with real history
    final_df = pd.merge(estimated_etf, leveraged_etf, on='Date', how='left', suffixes=('_est', '_real'))
    
    # Replace estimated data with real data where available
    for col in ['Open', 'High', 'Low', 'Close', 'Adj Close']:
        final_df[col] = final_df[col + '_real'].combine_first(final_df[col + '_est'])
    
    # Select and return final columns in order
    final_df = final_df[['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close']]
    
    return final_df


def list_csv_files(directory):
    """List all CSV files in the given directory."""
    csv_files = sorted([f.name for f in directory.glob("*.csv") 
                       if not f.name.startswith("ext_") 
                       and not f.name.startswith("[!]")
                       and not f.name.startswith("_raw_")])
    return csv_files


def get_file_selection(csv_files, prompt, allow_fabricate=False):
    """Get user's file selection from a list of CSV files."""
    print(f"\n{prompt}")
    print("-" * 50)
    if allow_fabricate:
        print(" 0. [FABRICATE - Create artificial leveraged ETF]")
    for i, file in enumerate(csv_files, 1):
        print(f"{i:2d}. {file}")
    print("-" * 50)
    
    while True:
        selection = input("\nEnter number or filename: ").strip()
        
        # Check if user entered a number
        try:
            index = int(selection)
            if allow_fabricate and index == 0:
                return None  # Special value for fabricate mode
            elif 1 <= index <= len(csv_files):
                return csv_files[index - 1]
            else:
                max_num = len(csv_files)
                min_num = 0 if allow_fabricate else 1
                print(f"Invalid number. Please enter a number between {min_num} and {max_num}.")
        except ValueError:
            # Check if user entered a filename
            if selection in csv_files:
                return selection
            elif selection.endswith('.csv') and selection in csv_files:
                return selection
            else:
                print("Invalid selection. Please enter a valid number or filename.")


def get_fabricated_etf_name():
    """Get name for fabricated ETF from user."""
    print("\nEnter name for the fabricated ETF:")
    print("Examples: TQQQ (for 3x QQQ), PSQ (for -1x QQQ), UPRO (for 3x SPY)")
    
    while True:
        name = input("\nETF name: ").strip().upper()
        if name and not name.endswith('.csv'):
            return name
        else:
            print("Please enter a valid ETF name (without .csv extension).")


def get_leverage_input():
    """Get leverage factor from user with validation."""
    print("\nEnter leverage factor:")
    print("  - Positive for leveraged ETFs (e.g., 3 for TQQQ, 2 for QLD)")
    print("  - Negative for inverse ETFs (e.g., -1 for PSQ, -3 for SQQQ)")
    
    while True:
        try:
            leverage = float(input("\nLeverage factor: "))
            if leverage == 0:
                print("Leverage cannot be zero. Please enter a non-zero value.")
            else:
                return leverage
        except ValueError:
            print("Invalid input. Please enter a numeric value.")


def main():
    """Main function to run the data extender with console inputs."""
    print("=" * 60)
    print("ETF Data Extender")
    print("Creates extended historical data for leveraged/inverse ETFs")
    print("=" * 60)
    
    # Get current directory
    current_dir = Path.cwd()
    csv_dir = current_dir if current_dir.name == "csv_data" else current_dir / "csv_data"
    
    if not csv_dir.exists():
        print(f"Error: CSV data directory not found at {csv_dir}")
        sys.exit(1)
    
    while True:
        # List available CSV files
        csv_files = list_csv_files(csv_dir)
        if not csv_files:
            print("No CSV files found in the csv_data directory.")
            sys.exit(1)
        
        # Get user selections
        base_file = get_file_selection(csv_files, "Select BASE ETF file (e.g., QQQ, SPY, VEU):")
        target_file = get_file_selection(csv_files, "Select TARGET ETF file to extend (e.g., TQQQ, PSQ, UPRO):", allow_fabricate=True)
        
        # Handle fabrication mode
        if target_file is None:
            fabricated_name = get_fabricated_etf_name()
            target_path = None
            print(f"\nFabricating artificial ETF: {fabricated_name}")
        else:
            target_path = csv_dir / target_file
            fabricated_name = None
        
        leverage = get_leverage_input()
        
        # Construct full file paths
        base_path = csv_dir / base_file
        
        print(f"\nProcessing:")
        print(f"  Base ETF:    {base_file}")
        if target_file:
            print(f"  Target ETF:  {target_file}")
        else:
            print(f"  Target ETF:  [FABRICATING {fabricated_name}]")
        print(f"  Leverage:    {leverage}")
        
        try:
            # Generate extended data
            print("\nGenerating extended historical data...")
            extended_data = extend_etf_history(str(base_path), str(target_path) if target_path else None, leverage)
            
            # Create output filename
            actual_start_date = extended_data['Date'].min().strftime('%Y.%m.%d')
            actual_end_date = extended_data['Date'].max().strftime('%Y.%m.%d')
            new_date_range = f"({actual_start_date} - {actual_end_date})"
            
            if target_file:
                # Replace date range in target filename
                date_range_pattern = r'\(\d{4}\.\d{2}\.\d{2}\s*-\s*\d{4}\.\d{2}\.\d{2}\)'
                updated_filename = re.sub(date_range_pattern, new_date_range, target_file)
                output_filename = f"ext_{updated_filename}"
            else:
                # Create filename for fabricated ETF
                # Extract source info from base filename (e.g., "(daily) (yfinance)")
                match = re.search(r'\)\s*(\([^)]+\)\s*\([^)]+\))\.csv$', base_file)
                source_info = match.group(1) if match else "(daily) (yfinance)"
                output_filename = f"ext_{fabricated_name} {new_date_range} {source_info}.csv"
            
            output_path = csv_dir / output_filename
            
            # Validate the extended data before saving
            print("\nValidating extended data...")
            validator = DataValidator(csv_dir)
            
            # Prepare data for validation (ensure Date is datetime)
            validation_df = extended_data.copy()
            validation_df['Date'] = pd.to_datetime(validation_df['Date'])
            
            is_valid = validator._validate_single_file(output_filename, validation_df)
            
            if is_valid:
                print("[OK] Extended data is valid and aligned with NYSE trading days")
            else:
                print("[WARNING] Extended data may not be fully aligned with NYSE trading days")
                print("         Consider running clean_csv_data.py on the output file if needed")
            
            # Save the result regardless of validation
            extended_data.to_csv(output_path, index=False)
            
            print(f"\n[OK] Successfully created: {output_filename}")
            print(f"[OK] Data range: {extended_data['Date'].min().date()} to {extended_data['Date'].max().date()}")
            print(f"[OK] Total rows: {len(extended_data):,}")
            if target_file is None:
                print(f"[OK] Fabricated {fabricated_name} with {leverage}x leverage from {base_file}")
            
        except FileNotFoundError:
            print(f"\nError: One or more input files not found.")
        except Exception as e:
            print(f"\nError: {e}")
        
        # Ask if user wants to continue
        print("\n" + "=" * 60)
        another = input("Extend another ETF? (y/n): ").strip().lower()
        if another != 'y':
            print("\nThank you for using ETF Data Extender!")
            break


if __name__ == "__main__":
    main()