"""
Compute MA200/Open discrepancy for SPY and VEU across all possible date ranges
Based on plot_qqq_stdev.py code structure
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
from oosit_utils import DataManager
import pandas as pd
import matplotlib.pyplot as plt

def compute_ma200_discrepancy():
    # Initialize DataManager to load all CSV data
    csv_path = Path(__file__).parent.parent / "csv_data"
    data_manager = DataManager(data_directory=str(csv_path))
    
    # Get the available date range
    date_range = data_manager.get_date_range('daily')
    
    # Process both SPY and VEU
    assets = ['SPY', 'VEU']
    results = {}
    
    for asset in assets:
        print(f"\nProcessing {asset}...")
        
        # Check if asset exists
        if asset not in data_manager.get_available_assets():
            print(f"Warning: {asset} not found in available assets")
            continue
            
        # Get asset info to find earliest date with MA200
        asset_info = data_manager.get_asset_info(asset)
        print(f"{asset} Asset Info: {asset_info}")
        
        # We need at least 200 days of data before we can have MA200
        # Start from the earliest possible date for this asset
        asset_start_date = asset_info['start_date']
        
        # Find the index in date_range for this asset's start date
        start_idx = data_manager._binary_search_date(date_range, pd.to_datetime(asset_start_date))
        
        # We need to wait for MA200 to be available (200 days after start)
        ma200_start_idx = start_idx + 200
        
        if ma200_start_idx >= len(date_range):
            print(f"Not enough data for MA200 calculation for {asset}")
            continue
            
        # Set backtest start date to when MA200 becomes available
        backtest_start = date_range[ma200_start_idx].strftime("%Y.%m.%d")
        get_value = data_manager.get_data_accessor(backtest_start)
        
        # Collect discrepancy values
        discrepancy_values = []
        dates = []
        raw_data = []
        
        # Loop through all dates from MA200 start to end
        for i in range(len(date_range) - ma200_start_idx):
            try:
                # Get values
                ma200_val = get_value(asset, i, 'MA200')
                open_val = get_value(asset, i, 'Open')
                date_val = get_value(asset, i, 'Date')
                
                if ma200_val is not None and open_val is not None and not pd.isna(ma200_val) and not pd.isna(open_val) and ma200_val != 0:
                    # Calculate discrepancy: (Open - MA200) / MA200
                    discrepancy = (open_val - ma200_val) / ma200_val
                    discrepancy_values.append(discrepancy)
                    dates.append(date_val)
                    raw_data.append({
                        'date': date_val,
                        'open': open_val,
                        'ma200': ma200_val,
                        'discrepancy': discrepancy,
                        'discrepancy_pct': discrepancy * 100
                    })
                    
            except (IndexError, ValueError) as e:
                # Skip if data not available for this date
                continue
        
        if discrepancy_values:
            results[asset] = {
                'dates': dates,
                'discrepancy_values': discrepancy_values,
                'raw_data': raw_data
            }
            
            print(f"\nCollected {len(discrepancy_values)} data points for {asset}")
            print(f"Date range: {dates[0]} to {dates[-1]}")
            print(f"Min discrepancy: {min(discrepancy_values)*100:.2f}%")
            print(f"Max discrepancy: {max(discrepancy_values)*100:.2f}%")
            print(f"Average discrepancy: {sum(discrepancy_values)/len(discrepancy_values)*100:.2f}%")
    
    return results

def plot_discrepancy(results):
    """Plot the MA200/Open discrepancy for SPY and VEU"""
    
    # Create figure with subplots
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
    
    # Plot SPY
    if 'SPY' in results:
        spy_data = results['SPY']
        discrepancy_pct = [d * 100 for d in spy_data['discrepancy_values']]
        ax1.plot(spy_data['dates'], discrepancy_pct, linewidth=1, color='blue', alpha=0.8)
        ax1.axhline(y=0, color='red', linestyle='--', alpha=0.5)
        ax1.set_title('SPY: (Open - MA200) / MA200 Discrepancy Over Time')
        ax1.set_ylabel('Discrepancy (%)')
        ax1.grid(True, alpha=0.3)
        ax1.tick_params(axis='x', rotation=45)
    
    # Plot VEU
    if 'VEU' in results:
        veu_data = results['VEU']
        discrepancy_pct = [d * 100 for d in veu_data['discrepancy_values']]
        ax2.plot(veu_data['dates'], discrepancy_pct, linewidth=1, color='green', alpha=0.8)
        ax2.axhline(y=0, color='red', linestyle='--', alpha=0.5)
        ax2.set_title('VEU: (Open - MA200) / MA200 Discrepancy Over Time')
        ax2.set_xlabel('Date')
        ax2.set_ylabel('Discrepancy (%)')
        ax2.grid(True, alpha=0.3)
        ax2.tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    # Compute the discrepancy
    results = compute_ma200_discrepancy()
    
    # Plot the results
    if results:
        plot_discrepancy(results)
    else:
        print("No results to display")