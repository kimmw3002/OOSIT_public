#!/usr/bin/env python3
"""
Parameter Sweep Results Analyzer
Analyzes and displays top performing parameter combinations from sweep results
"""

import pandas as pd
import json
from pathlib import Path
import sys
from datetime import datetime


def list_csv_files(directory):
    """List all CSV files in the given directory recursively."""
    csv_files = []
    for path in directory.rglob("*_results.csv"):
        # Get relative path from the base directory
        relative_path = path.relative_to(directory)
        csv_files.append(relative_path)
    return sorted(csv_files)


def get_csv_file_input():
    """Get CSV file path from user via console input."""
    base_dir = Path.cwd() / "parameter_sweep_results"
    
    if not base_dir.exists():
        print("\nError: parameter_sweep_results directory not found!")
        sys.exit(1)
    
    csv_files = list_csv_files(base_dir)
    
    if not csv_files:
        print("\nNo CSV files found in parameter_sweep_results directory!")
        sys.exit(1)
    
    print("\nAvailable CSV files:")
    print("-" * 60)
    for i, file in enumerate(csv_files, 1):
        print(f"{i:3d}. {file.as_posix()}")
    print("-" * 60)
    
    while True:
        user_input = input(f"\nSelect a file number (1-{len(csv_files)}): ").strip()
        
        try:
            file_index = int(user_input) - 1
            if 0 <= file_index < len(csv_files):
                selected_file = csv_files[file_index]
                file_path = base_dir / selected_file
                print(f"\nSelected: {selected_file}")
                return file_path
            else:
                print(f"Please enter a number between 1 and {len(csv_files)}")
        except ValueError:
            print("Invalid input. Please enter a number.")


def get_metric_headers(df):
    """Get headers that end with _return or _drawdown."""
    metric_headers = []
    for col in df.columns:
        if col.endswith("_return") or col.endswith("_drawdown"):
            metric_headers.append(col)
    return metric_headers


def select_headers(headers):
    """Allow user to select multiple headers from numbered list."""
    print("\nAvailable metrics to analyze:")
    print("-" * 60)
    for i, header in enumerate(headers, 1):
        metric_type = "[RETURN] (higher is better)" if header.endswith("_return") else "[DRAWDOWN] (lower is better)"
        print(f"{i:2d}. {header:<50} {metric_type}")
    print("-" * 60)
    
    print("\nSelect metrics (enter numbers separated by comma, or 'all' for all metrics):")
    
    while True:
        selection = input("Selection: ").strip().lower()
        
        if selection == "all":
            return headers
        
        try:
            # Parse comma-separated numbers
            indices = [int(x.strip()) for x in selection.split(",")]
            
            # Validate indices
            selected = []
            for idx in indices:
                if 1 <= idx <= len(headers):
                    selected.append(headers[idx - 1])
                else:
                    print(f"Invalid number: {idx}. Please enter numbers between 1 and {len(headers)}")
                    selected = None
                    break
            
            if selected:
                return selected
                
        except ValueError:
            print("Invalid input. Please enter numbers separated by comma (e.g., 1,3,5) or 'all'")


def get_top_n():
    """Get number of top results to display."""
    print("\nHow many top parameter sets to show for each metric?")
    
    while True:
        try:
            n = int(input("Number (e.g., 10): ").strip())
            if n > 0:
                return n
            else:
                print("Please enter a positive number.")
        except ValueError:
            print("Invalid input. Please enter a number.")


def parse_parameters(param_str):
    """Parse parameter string to readable format."""
    try:
        # Convert string representation of dict to actual dict
        params = eval(param_str)
        if isinstance(params, dict):
            return ", ".join([f"{k}={v}" for k, v in params.items()])
    except:
        pass
    return param_str


def display_top_results(df, selected_headers, top_n, output_file=None, csv_path=None):
    """Display top N results for each selected metric and optionally save to file."""
    output_lines = []
    
    # Add metadata if saving to file
    if output_file and csv_path:
        output_lines.append("=" * 80)
        output_lines.append("PARAMETER SWEEP ANALYSIS REPORT")
        output_lines.append("=" * 80)
        output_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        output_lines.append(f"Source File: {csv_path}")
        output_lines.append(f"Total Combinations: {len(df)}")
        output_lines.append(f"Metrics Analyzed: {len(selected_headers)}")
        output_lines.append(f"Top N Results: {top_n}")
    
    # Header
    output_lines.append("\n" + "=" * 80)
    output_lines.append("TOP PARAMETER COMBINATIONS")
    output_lines.append("=" * 80)
    
    for header in selected_headers:
        output_lines.append(f"\n{'-' * 80}")
        
        # Determine if this is a return (higher is better) or drawdown (lower is better)
        if header.endswith("_return"):
            output_lines.append(f"[RETURN] TOP {top_n} for {header} (HIGHER IS BETTER)")
            sorted_df = df.nlargest(top_n, header)
        else:  # drawdown
            output_lines.append(f"[DRAWDOWN] TOP {top_n} for {header} (LOWER IS BETTER)")
            sorted_df = df.nsmallest(top_n, header)
        
        output_lines.append("-" * 80)
        
        # Display results
        for i, (idx, row) in enumerate(sorted_df.iterrows(), 1):
            params = parse_parameters(row['parameters'])
            value = row[header]
            
            # Format value based on type
            if header.endswith("_return"):
                formatted_value = f"{value:,.2f}%" if abs(value) < 1000 else f"{value:,.0f}%"
                symbol = "+"
            else:  # drawdown
                formatted_value = f"{value:.2f}%" if abs(value) < 1000 else f"{value:.0f}%"
                symbol = "-"
            
            output_lines.append(f"\n  {i:2d}. [{symbol}] {formatted_value}")
            output_lines.append(f"      Parameters: {params}")
            
            # Show additional key metrics
            if 'total_return' in df.columns and header != 'total_return':
                output_lines.append(f"      Total Return: {row['total_return']:,.2f}%")
            if 'max_drawdown' in df.columns and header != 'max_drawdown':
                output_lines.append(f"      Max Drawdown: {row['max_drawdown']:.2f}%")
    
    # Print to console
    for line in output_lines:
        print(line)
    
    # Save to file if specified
    if output_file:
        with open(output_file, 'w', encoding='utf-8') as f:
            for line in output_lines:
                f.write(line + '\n')
        print(f"\n[SAVED] Analysis results saved to: {output_file}")


def main():
    """Main function to run the parameter analyzer."""
    print("=" * 80)
    print("PARAMETER SWEEP RESULTS ANALYZER")
    print("Analyze top performing parameter combinations from sweep results")
    print("=" * 80)
    
    try:
        # Step 1: Get CSV file from command line argument or interactive input
        if len(sys.argv) > 1:
            # Use command line argument
            csv_path = Path(sys.argv[1])
            if not csv_path.exists():
                print(f"Error: File not found: {csv_path}")
                sys.exit(1)
            if not csv_path.suffix == ".csv":
                print(f"Error: Not a CSV file: {csv_path}")
                sys.exit(1)
        else:
            # Interactive input
            csv_path = get_csv_file_input()
        
        print(f"\nLoading: {csv_path}")
        
        # Step 2: Load data
        df = pd.read_csv(csv_path)
        print(f"Loaded {len(df)} parameter combinations")
        
        # Step 3: Get metric headers
        metric_headers = get_metric_headers(df)
        if not metric_headers:
            print("\nNo metrics found ending with '_return' or '_drawdown'")
            return
        
        print(f"\nFound {len(metric_headers)} metrics")
        
        # Step 4: Select headers
        selected_headers = select_headers(metric_headers)
        print(f"\nSelected {len(selected_headers)} metric(s) for analysis")
        
        # Step 5: Get top N
        top_n = get_top_n()
        
        # Step 6: Display results and save to file
        # Generate output filename based on CSV filename
        csv_stem = csv_path.stem  # Get filename without extension
        if csv_stem.endswith("_results"):
            output_stem = csv_stem[:-8]  # Remove "_results" suffix
        else:
            output_stem = csv_stem
        output_file = Path(f"{output_stem}_analysis.txt")
        display_top_results(df, selected_headers, top_n, output_file, csv_path)
        
        print("\n" + "=" * 80)
        print("Analysis complete!")
        print("=" * 80)
        
    except FileNotFoundError:
        print(f"\nError: CSV file not found")
    except pd.errors.EmptyDataError:
        print(f"\nError: CSV file is empty")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()