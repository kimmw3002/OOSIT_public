#!/usr/bin/env python3
"""
Console-based Data Fetcher for OOSIT CSV files.
Fetches new data or updates existing CSV data interactively.
"""

import pandas as pd
import yfinance as yf
import argparse
import re
import sys
import time
import shutil
import csv
import json
from pathlib import Path
from datetime import datetime, date, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Add parent directory to path to import oosit_utils
sys.path.append(str(Path(__file__).parent.parent))
from oosit_utils.common import clean_yfinance_data


def parse_filename(filename):
    """
    Parse OOSIT filename format to extract components.
    
    Args:
        filename: CSV filename in OOSIT format
        
    Returns:
        dict with ticker, start_date, end_date, frequency, source
        or None if parsing fails
    """
    # Remove .csv extension
    if filename.endswith('.csv'):
        filename = filename[:-4]
    
    # Pattern: ticker (start - end) (frequency) (source)
    # Handle special prefixes
    original_filename = filename
    if filename.startswith('[!] '):
        filename = filename[4:]
    if filename.startswith('ext_'):
        filename = filename[4:]
    if filename.startswith('_raw_'):
        filename = filename[5:]
    
    # Remove 'copy' suffix if present
    if filename.endswith(' copy'):
        filename = filename[:-5]
    
    # Remove timestamp suffix if present (e.g., _20240801123456)
    if re.search(r'_\d{14}$', filename):
        filename = re.sub(r'_\d{14}$', '', filename)
    
    # Try to match the pattern
    pattern = r'^(.+?)\s+\((\d{4}\.\d{2}\.\d{2})\s*-\s*(\d{4}\.\d{2}\.\d{2})\)\s+\(([^)]+)\)\s+\(([^)]+)\)$'
    match = re.match(pattern, filename)
    
    if match:
        return {
            'ticker': match.group(1),
            'start_date': match.group(2),
            'end_date': match.group(3),
            'frequency': match.group(4),
            'source': match.group(5),
            'original_filename': original_filename + '.csv'
        }
    
    return None


def check_duplicate(ticker, source, csv_dir):
    """
    Check if ticker+source combination already exists.
    
    Args:
        ticker: Stock ticker symbol
        source: Data source (yfinance, MacroMicro, etc.)
        csv_dir: Directory containing CSV files
        
    Returns:
        List of existing filenames with same ticker+source
    """
    duplicates = []
    
    for file in csv_dir.glob('*.csv'):
        if file.name.startswith('.'):  # Skip hidden files
            continue
            
        parsed = parse_filename(file.name)
        if parsed and parsed['ticker'] == ticker and parsed['source'] == source:
            duplicates.append(file.name)
    
    return duplicates


def fetch_yfinance_data(ticker, start_date='1900.01.01', end_date=None):
    """
    Fetch data from Yahoo Finance.
    
    Args:
        ticker: Stock ticker symbol
        start_date: Start date in YYYY.MM.DD format
        end_date: End date in YYYY.MM.DD format (default: yesterday)
        
    Returns:
        Tuple of (dataframe, actual_start_date, actual_end_date)
    """
    if end_date is None:
        # Default to yesterday to avoid incomplete intraday data
        end_date = (date.today() - timedelta(days=1)).strftime('%Y.%m.%d')
    
    # Convert date format for yfinance
    start_ts = pd.Timestamp(start_date.replace('.', '-'))
    end_ts = pd.Timestamp(end_date.replace('.', '-')) + pd.Timedelta(days=1)
    
    print(f"Fetching {ticker} from Yahoo Finance...")
    
    # Download data
    df = yf.download(
        ticker, 
        start=start_ts, 
        end=end_ts, 
        interval="1d", 
        auto_adjust=False,
        multi_level_index=False,
        progress=False
    )
    
    if df.empty:
        raise ValueError(f"No data found for ticker {ticker}")
    
    # Reset index to get Date as column
    df = df.reset_index()
    
    # Get actual date range
    actual_start = df['Date'].iloc[0].strftime('%Y.%m.%d')
    actual_end = df['Date'].iloc[-1].strftime('%Y.%m.%d')
    
    return df, actual_start, actual_end


def fetch_macromicro_data(config):
    """
    Fetch data from MacroMicro using Selenium.
    
    Args:
        config: dict with url, name, frequency
        
    Returns:
        Tuple of (success, filename)
    """
    # Set up Chrome options
    options = webdriver.ChromeOptions()
    # options.add_argument('--headless')  # Disabled - can cause issues with some sites
    
    # Set download directory
    download_dir = str(Path.cwd())
    prefs = {
        "download.default_directory": download_dir,
        "profile.default_content_setting_values.automatic_downloads": 1,
    }
    options.add_experimental_option("prefs", prefs)
    
    driver = None
    original_file = None
    
    try:
        print(f"Fetching {config['name']} from MacroMicro...")
        
        # Initialize WebDriver
        driver = webdriver.Chrome(options=options)
        driver.get(config['url'])
        
        # Wait for chart to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "highcharts-container"))
        )
        
        # Generate unique filename
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        original_file_name = f"data_{timestamp}.csv"
        
        # Execute JavaScript to download data
        js_code = f"""
        // Check if Highcharts exists and has charts
        if (typeof Highcharts === 'undefined' || !Highcharts.charts || Highcharts.charts.length === 0) {{
            throw new Error('No Highcharts found on page');
        }}

        // Find the first valid chart
        let chart = null;
        for (let i = 0; i < Highcharts.charts.length; i++) {{
            if (Highcharts.charts[i] && Highcharts.charts[i].series && Highcharts.charts[i].series.length > 0) {{
                chart = Highcharts.charts[i];
                break;
            }}
        }}

        if (!chart) {{
            throw new Error('No valid chart with data found');
        }}

        // Extract data from first series
        const series = chart.series[0];
        const xData = series.xData || series.data.map(point => point.x);
        const yData = series.yData || series.data.map(point => point.y);

        // Create CSV content with real newlines
        let csvContent = "Date,Value\\n";
        for (let i = 0; i < xData.length; i++) {{
            let xDate = new Date(xData[i]).toISOString().split('T')[0];
            let yValue = yData[i];
            csvContent += xDate + "," + yValue + "\\n";
        }}

        // Download
        const blob = new Blob([csvContent], {{ type: 'text/csv;charset=utf-8;' }});
        const url = URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.setAttribute("href", url);
        link.setAttribute("download", "{original_file_name}");
        document.body.appendChild(link);
        link.click();
        link.remove();
        URL.revokeObjectURL(url);
        """
        driver.execute_script(js_code)
        
        # Wait for download
        original_file = Path(download_dir) / original_file_name
        timeout = 10
        start_time = time.time()
        
        while not original_file.exists():
            if time.time() - start_time > timeout:
                raise TimeoutError("File download took too long!")
            time.sleep(0.5)
        
        # Process the downloaded file
        process_csv_first_column(str(original_file))
        
        # Rename file with dates
        renamed_file = rename_file_with_dates(download_dir, original_file_name, config['name'], config['frequency'])
        
        return True, renamed_file
        
    except Exception as e:
        print(f"Error fetching MacroMicro data: {e}")
        return False, None
        
    finally:
        if driver:
            driver.quit()
        # Clean up temporary file if it exists
        if original_file and original_file.exists() and original_file.name.startswith('data_'):
            try:
                original_file.unlink()
            except:
                pass


def process_csv_first_column(file_path):
    """Process CSV to clean date format in first column."""
    temp_file_path = file_path + ".tmp"
    
    with open(file_path, 'r', encoding='utf-8') as csv_file, \
         open(temp_file_path, 'w', encoding='utf-8', newline='') as temp_file:
        csv_reader = csv.reader(csv_file)
        csv_writer = csv.writer(temp_file)
        
        for row in csv_reader:
            if len(row) > 0:
                row[0] = row[0].split("T")[0]  # Remove time component
            csv_writer.writerow(row)
    
    Path(temp_file_path).replace(file_path)


def rename_file_with_dates(download_dir, original_file_name, data_name, freq):
    """Rename MacroMicro file with OOSIT naming convention."""
    original_file = Path(download_dir) / original_file_name
    
    with open(original_file, 'r', encoding='utf-8') as csv_file:
        csv_reader = list(csv.reader(csv_file))
        
        if len(csv_reader) < 2:
            raise ValueError("CSV file does not contain enough rows")
        
        start_date = csv_reader[1][0].replace("-", ".")
        end_date = csv_reader[-1][0].replace("-", ".")
    
    new_file_name = f"{data_name} ({start_date} - {end_date}) ({freq}) (MacroMicro).csv"
    new_file = Path(download_dir) / new_file_name
    
    # Check if target file already exists
    if new_file.exists():
        # Generate a unique name with timestamp
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        new_file_name = f"{data_name} ({start_date} - {end_date}) ({freq}) (MacroMicro)_{timestamp}.csv"
        new_file = Path(download_dir) / new_file_name
    
    original_file.rename(new_file)
    return new_file_name


def update_macromicro_urls(csv_dir, name, url):
    """Update macromicro_url.json with new URL."""
    macromicro_url_file = csv_dir / "macromicro_url.json"
    
    # Load existing URLs
    if macromicro_url_file.exists():
        with open(macromicro_url_file, 'r') as f:
            urls = json.load(f)
    else:
        urls = {}
    
    # Check if URL has changed
    if name in urls and urls[name] == url:
        print(f"    URL for {name} already exists in macromicro_url.json")
        return
    
    # Add/update the URL
    urls[name] = url
    
    # Save back to file
    with open(macromicro_url_file, 'w') as f:
        json.dump(urls, f, indent=4)
    
    print(f"    Updated macromicro_url.json with {name}: {url}")
    print(f"    Successfully saved macromicro_url.json")


def interactive_mode(csv_dir):
    """Interactive mode for fetching single data files."""
    print("\nInteractive Data Fetcher")
    print("=" * 50)
    print("\nSelect data source:")
    print("1. Yahoo Finance (yfinance)")
    print("2. MacroMicro")
    print("0. Exit")
    
    choice = input("\nEnter choice (0-2): ").strip()
    
    if choice == '0':
        return
    elif choice == '1':
        # Yahoo Finance
        ticker = input("\nEnter ticker symbol: ").strip().upper()
        if not ticker:
            print("Invalid ticker symbol")
            return
        
        # Check for duplicates
        duplicates = check_duplicate(ticker, 'yfinance', csv_dir)
        
        prefix = ""
        if duplicates:
            print(f"\n[!] WARNING: {ticker} with source yfinance already exists:")
            for dup in duplicates:
                print(f"    Existing: {dup}")
            print("    New file will be saved with [!] prefix")
            prefix = "[!] "
        
        try:
            # Fetch data
            df, start_date, end_date = fetch_yfinance_data(ticker)
            
            # Save file
            filename = f"{prefix}{ticker} ({start_date} - {end_date}) (daily) (yfinance).csv"
            filepath = csv_dir / filename
            df.to_csv(filepath, index=False)
            
            print(f"\nSuccess! Saved: {filename}")
            print(f"Data points: {len(df)}")
            
        except Exception as e:
            print(f"\nError: {e}")
            
    elif choice == '2':
        # MacroMicro
        print("\nEnter MacroMicro data details:")
        url = input("URL: ").strip()
        name = input("Data name: ").strip()
        
        print("\nSelect frequency:")
        print("1. daily")
        print("2. monthly")
        freq_choice = input("Enter choice (1-2): ").strip()
        
        if freq_choice == '1':
            frequency = 'daily'
        elif freq_choice == '2':
            frequency = 'monthly'
        else:
            print("Invalid frequency choice")
            return
        
        # Check for duplicates
        duplicates = check_duplicate(name, 'MacroMicro', csv_dir)
        
        if duplicates:
            print(f"\n[!] WARNING: {name} with source MacroMicro already exists:")
            for dup in duplicates:
                print(f"    Existing: {dup}")
            print("    New file will be saved with [!] prefix")
        
        config = {
            'url': url,
            'name': name,
            'frequency': frequency
        }
        
        success, filename = fetch_macromicro_data(config)
        
        if success:
            # The file was downloaded to current directory, move it to csv_dir
            source_path = Path.cwd() / filename
            
            # Handle duplicate naming
            if duplicates:
                new_filename = f"[!] {filename}"
                dest_path = csv_dir / new_filename
                filename = new_filename
            else:
                dest_path = csv_dir / filename
            
            # Move the file from current directory to csv_dir
            if source_path.exists():
                shutil.move(str(source_path), str(dest_path))
            
            # Update macromicro_url.json with the URL
            update_macromicro_urls(csv_dir, name, url)
                
            print(f"\nSuccess! Saved: {filename}")
        else:
            print("\nFailed to fetch data")
            
    else:
        print("Invalid choice")


def validate_all_csvs(csv_dir):
    """Validate all CSV files in the directory."""
    print("\n\nValidating all CSV files...")
    print("=" * 50)
    
    # Import validator
    sys.path.append(str(Path(__file__).parent.parent))
    from oosit_utils.data.validator import DataValidator
    
    validator = DataValidator(csv_dir)
    csv_files = sorted([f.name for f in csv_dir.glob('*.csv') if f.is_file() and not f.name.startswith('_raw_')])
    
    valid_count = 0
    invalid_count = 0
    
    for idx, filename in enumerate(csv_files, 1):
        print(f"\n[{idx}/{len(csv_files)}] Validating {filename}...")
        
        try:
            # Read the file
            df = pd.read_csv(csv_dir / filename)
            if 'Date' in df.columns:
                df['Date'] = pd.to_datetime(df['Date'])
            
            # Validate
            is_valid = validator._validate_single_file(filename, df)
            
            if is_valid:
                print(f"    [OK] VALID")
                valid_count += 1
            else:
                print(f"    [X] INVALID - Data not aligned with NYSE trading days")
                invalid_count += 1
                
        except Exception as e:
            print(f"    [X] ERROR - {str(e)}")
            invalid_count += 1
    
    # Summary
    print("\n" + "=" * 50)
    print("VALIDATION SUMMARY:")
    print(f"  Total files: {len(csv_files)}")
    print(f"  Valid: {valid_count}")
    print(f"  Invalid: {invalid_count}")
    
    if invalid_count > 0:
        print("\n[!] Some files failed validation. Consider running clean_csv_data.py")
    else:
        print("\n[OK] All files are valid!")
    
    print("=" * 50)
    
    return valid_count, invalid_count


def update_all_csvs(csv_dir):
    """Update mode - backup and update all CSV files."""
    print("\nUpdate Mode - Updating all CSV files")
    print("=" * 50)
    
    # Load MacroMicro URLs
    macromicro_urls = {}
    macromicro_url_file = csv_dir / "macromicro_url.json"
    if macromicro_url_file.exists():
        with open(macromicro_url_file, 'r') as f:
            macromicro_urls = json.load(f)
        print(f"Loaded {len(macromicro_urls)} MacroMicro URLs")
    else:
        print("Warning: macromicro_url.json not found")
    
    # Create backup directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = csv_dir / timestamp
    backup_dir.mkdir(exist_ok=True)
    
    print(f"\nCreating backup: {timestamp}/")
    
    # Get all CSV files
    csv_files = [f for f in csv_dir.glob('*.csv') if f.is_file()]
    print(f"Moving {len(csv_files)} files to backup...")
    
    # Move files to backup
    for file in csv_files:
        shutil.move(str(file), str(backup_dir / file.name))
    
    print("\nUpdating CSV files:")
    print("-" * 50)
    
    # Track statistics
    updated = 0
    manual_required = 0
    processed_tickers = set()  # Track processed ticker+source to avoid duplicates
    
    # Get list of files from backup to process
    backup_files = sorted(backup_dir.glob('*.csv'))
    
    for idx, backup_file in enumerate(backup_files, 1):
        filename = backup_file.name
        print(f"\n[{idx}/{len(backup_files)}] {filename}")
        
        # Parse filename
        parsed = parse_filename(filename)
        if not parsed:
            print("    [!] Cannot parse filename - copying with [!] prefix")
            shutil.copy2(str(backup_file), str(csv_dir / f"[!] {filename}"))
            manual_required += 1
            continue
        
        ticker = parsed['ticker']
        source = parsed['source']
        
        # Handle special cases
        if filename.startswith('ext_'):
            print("    [!] Manual update required for extended files")
            shutil.copy2(str(backup_file), str(csv_dir / f"[!] {filename}"))
            manual_required += 1
            continue
            
        if source not in ['yfinance', 'MacroMicro']:
            print(f"    [!] {source} source not supported for auto-update")
            shutil.copy2(str(backup_file), str(csv_dir / f"[!] {filename}"))
            manual_required += 1
            continue
        
        # Handle _raw_ files
        if filename.startswith('_raw_'):
            # Check if we already processed the base ticker
            ticker_source_key = f"{ticker}|{source}"
            if ticker_source_key in processed_tickers:
                print(f"    Skipping - base ticker {ticker} already updated")
                # Just copy the raw file back
                shutil.copy2(str(backup_file), str(csv_dir / filename))
                continue
        
        # Check if this is a cleaned version of a _raw_ file we'll process later
        raw_filename = f"_raw_{filename}"
        if any(f.name == raw_filename for f in backup_files):
            print(f"    Skipping - will be updated via {raw_filename}")
            continue
        
        # Update the file
        ticker_source_key = f"{ticker}|{source}"
        if ticker_source_key not in processed_tickers:
            try:
                if source == 'yfinance':
                    print(f"    Updating from Yahoo Finance...")
                    df, start_date, end_date = fetch_yfinance_data(ticker)
                    new_filename = f"{ticker} ({start_date} - {end_date}) (daily) (yfinance).csv"
                    
                    # Check if we need to apply cleaning (either this is a _raw_ file or a _raw_ version exists)
                    has_raw_version = any(f.name.startswith(f"_raw_{ticker} ") and source in f.name for f in backup_files)
                    needs_cleaning = filename.startswith('_raw_') or has_raw_version
                    
                    if needs_cleaning:
                        print(f"    Applying data cleaning...")
                        
                        # Convert dates for cleaning
                        df['Date'] = pd.to_datetime(df['Date'])
                        
                        # Clean the data
                        cleaned_df = clean_yfinance_data(df)
                        
                        # Save cleaned version
                        cleaned_df.to_csv(csv_dir / new_filename, index=False)
                        
                        # Save raw version
                        raw_new_filename = f"_raw_{new_filename}"
                        df.to_csv(csv_dir / raw_new_filename, index=False)
                        
                        print(f"    Saved cleaned: {new_filename}")
                        print(f"    Saved raw: {raw_new_filename}")
                    else:
                        # No cleaning needed, just save the fetched data
                        df.to_csv(csv_dir / new_filename, index=False)
                        print(f"    Saved: {new_filename}")
                    
                    processed_tickers.add(ticker_source_key)
                    updated += 1
                    
                elif source == 'MacroMicro':
                    # Check if we have URL for this ticker
                    if ticker in macromicro_urls:
                        print(f"    Updating from MacroMicro...")
                        config = {
                            'url': macromicro_urls[ticker],
                            'name': ticker,
                            'frequency': parsed['frequency']
                        }
                        
                        success, new_filename = fetch_macromicro_data(config)
                        
                        if success:
                            # Move downloaded file from current directory to csv_dir
                            source_file = Path.cwd() / new_filename
                            if source_file.exists():
                                shutil.move(str(source_file), str(csv_dir / new_filename))
                                print(f"    Saved: {new_filename}")
                                processed_tickers.add(ticker_source_key)
                                updated += 1
                            else:
                                print(f"    [!] Error: Downloaded file not found")
                                shutil.copy2(str(backup_file), str(csv_dir / f"[!] {filename}"))
                                manual_required += 1
                        else:
                            print(f"    [!] Error fetching data")
                            shutil.copy2(str(backup_file), str(csv_dir / f"[!] {filename}"))
                            manual_required += 1
                    else:
                        print(f"    [!] MacroMicro URL not found for {ticker}")
                        shutil.copy2(str(backup_file), str(csv_dir / f"[!] {filename}"))
                        manual_required += 1
                    
            except Exception as e:
                print(f"    [!] Error updating: {str(e)}")
                shutil.copy2(str(backup_file), str(csv_dir / f"[!] {filename}"))
                manual_required += 1
        else:
            # Already processed this ticker+source
            print(f"    Already updated via another file")
    
    # Summary
    print("\n" + "=" * 50)
    print("UPDATE SUMMARY:")
    print(f"  Total files: {len(backup_files)}")
    print(f"  Updated: {updated}")
    print(f"  Manual required: {manual_required}")
    print(f"\nBackup saved in: {backup_dir}")
    print("\nFiles marked with [!] require manual update")
    print("=" * 50)
    
    # Run validation on all files
    validate_all_csvs(csv_dir)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='OOSIT Data Fetcher')
    parser.add_argument('--update', action='store_true', 
                       help='Update all CSV files to newest available data')
    args = parser.parse_args()
    
    # Get CSV directory
    csv_dir = Path(__file__).parent
    
    print("OOSIT Data Fetcher")
    print("=" * 50)
    
    if args.update:
        update_all_csvs(csv_dir)
    else:
        while True:
            interactive_mode(csv_dir)
            
            another = input("\n\nFetch another file? (y/n): ").strip().lower()
            if another != 'y':
                break
        
        print("\nThank you for using OOSIT Data Fetcher!")


if __name__ == "__main__":
    main()