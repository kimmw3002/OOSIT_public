"""
Console-based CSV data cleaning tool for OOSIT format files.
Cleans yfinance data to align with NYSE trading days.
"""

import pandas as pd
import sys
import os
from pathlib import Path

# Add parent directory to path to import oosit_utils
sys.path.append(str(Path(__file__).parent.parent))

from oosit_utils.common import clean_yfinance_data
from oosit_utils.data.validator import DataValidator


def clean_csv_file(csv_name):
    """
    Clean a CSV file and save both raw and cleaned versions.
    
    Args:
        csv_name: Name of the CSV file to clean
    """
    # Get the current directory
    current_dir = Path(__file__).parent
    csv_path = current_dir / csv_name
    
    # Check if file exists
    if not csv_path.exists():
        print(f"Error: File '{csv_name}' not found in {current_dir}")
        return False
    
    print(f"Loading '{csv_name}'...")
    
    try:
        # Read the CSV file
        df = pd.read_csv(csv_path)
        
        # Ensure Date column exists
        if 'Date' not in df.columns:
            print("Error: CSV file must have a 'Date' column")
            return False
        
        # Convert Date column to datetime
        df['Date'] = pd.to_datetime(df['Date'])
        
        # First, validate the current data
        print("Validating current data...")
        validator = DataValidator(current_dir)
        is_valid = validator._validate_single_file(csv_name, df)
        
        if is_valid:
            print(f"[VALID] Data is already valid and aligned with NYSE trading days.")
            print(f"        No cleaning needed for '{csv_name}'")
            return True
        
        print("[INVALID] Data validation failed. Proceeding with cleaning...")
        
        # Save original as _raw_ prefixed file
        raw_filename = f"_raw_{csv_name}"
        raw_path = current_dir / raw_filename
        df.to_csv(raw_path, index=False)
        print(f"Original file saved as: {raw_filename}")
        
        # Clean the data
        print("Cleaning data...")
        cleaned_df = clean_yfinance_data(df)
        
        # Save cleaned data with original filename
        cleaned_df.to_csv(csv_path, index=False)
        print(f"Cleaned data saved as: {csv_name}")
        
        # Show summary
        print("\nCleaning Summary:")
        print(f"Original rows: {len(df)}")
        print(f"Cleaned rows: {len(cleaned_df)} (NYSE trading days only)")
        print(f"Date range: {cleaned_df['Date'].iloc[0]} to {cleaned_df['Date'].iloc[-1]}")
        
        return True
        
    except Exception as e:
        print(f"Error processing file: {e}")
        return False


def main():
    """Main function to run the cleaning tool."""
    print("OOSIT CSV Data Cleaner")
    print("=" * 50)
    print("This tool cleans CSV data to align with NYSE trading days.")
    print("The original file will be saved with '_raw_' prefix.")
    print("=" * 50)
    
    # List available CSV files
    current_dir = Path(__file__).parent
    csv_files = [f.name for f in current_dir.glob("*.csv") 
                 if not f.name.startswith("_raw_")
                 and not f.name.startswith("[!]")]
    
    if not csv_files:
        print("No CSV files found in the current directory.")
        return
    
    print("\nAvailable CSV files:")
    for i, file in enumerate(csv_files, 1):
        print(f"{i}. {file}")
    
    # Get user input
    print("\nEnter CSV file(s) to clean:")
    print("  - Single file: enter filename or number")
    print("  - Multiple files: enter comma-separated numbers (e.g., 1,3,5)")
    print("  - All files: enter 'all'")
    user_input = input("> ").strip()
    
    # Determine which files to process
    files_to_process = []
    
    if user_input.lower() == 'all':
        files_to_process = csv_files
    elif ',' in user_input:
        # Multiple files by number
        numbers = user_input.split(',')
        for num_str in numbers:
            try:
                index = int(num_str.strip()) - 1
                if 0 <= index < len(csv_files):
                    files_to_process.append(csv_files[index])
                else:
                    print(f"Warning: Invalid number {num_str.strip()} - skipping")
            except ValueError:
                print(f"Warning: '{num_str.strip()}' is not a valid number - skipping")
    elif user_input.isdigit():
        # Single file by number
        index = int(user_input) - 1
        if 0 <= index < len(csv_files):
            files_to_process = [csv_files[index]]
        else:
            print("Invalid number. Please enter a valid file number.")
            return
    else:
        # Single file by name
        files_to_process = [user_input]
    
    if not files_to_process:
        print("No valid files selected.")
        return
    
    # Process selected files
    print(f"\nProcessing {len(files_to_process)} file(s)...")
    print("=" * 50)
    
    success_count = 0
    fail_count = 0
    
    for csv_name in files_to_process:
        print(f"\n[{success_count + fail_count + 1}/{len(files_to_process)}] Processing '{csv_name}'")
        print("-" * 40)
        
        success = clean_csv_file(csv_name)
        
        if success:
            success_count += 1
        else:
            fail_count += 1
    
    # Summary
    print("\n" + "=" * 50)
    print("SUMMARY:")
    print(f"  Total files: {len(files_to_process)}")
    print(f"  Successful: {success_count}")
    print(f"  Failed: {fail_count}")
    print("=" * 50)


if __name__ == "__main__":
    main()