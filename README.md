# OOSIT - Office of Overseas Securities Investing and Trading

Financial backtesting framework for investment strategies.

## Installation

```bash
pip install pandas numpy matplotlib python-docx pandas-market-calendars yfinance schedule pytz selenium
```

To update latest-info critical packages to their newest versions:

```bash
pip install --upgrade pandas-market-calendars yfinance pytz
```

## JSON Configuration Setup

**Important**: This repository includes template JSON files that are excluded from version control. You must create your own JSON configuration files before using the system.

The following JSON files are ignored by git (see `.gitignore`) and must be created locally:
- `jsons/target.json` - Strategy configuration
- `jsons/parameter_customization.json` - Parameter sweep settings
- `jsons/email_config.json` - Email credentials (never commit!)

### Creating Configuration Files

1. **Strategy Configuration** (`jsons/target.json`):
   ```json
   {
     "default_strategies": ["strategy_name_1", "strategy_name_2"],
     "test_strategies": ["test_strategy_1", "test_strategy_2"]
   }
   ```
   - Lists which strategies to run as defaults vs test strategies
   - Strategy names must match Python files in `oosit_strategies/`

2. **Parameter Customization** (`jsons/parameter_customization.json`):
   ```json
   {
     "strategy_name": {
       "parameter_name": {
         "min": 0.0,
         "max": 0.2,
         "nsteps": 21
       }
     }
   }
   ```
   - Defines parameter ranges for optimization
   - Use bracket notation for nested parameters: `"param[key]"`

3. **Email Configuration** (`jsons/email_config.json`):
   ```json
   {
     "smtp_server": "smtp.gmail.com",
     "smtp_port": 587,
     "sender_email": "your_email@gmail.com",
     "sender_password": "your_app_password"
   }
   ```
   - Required for MarketWatch email functionality
   - Use app-specific passwords, not your main password
   - **WARNING**: Never commit this file!

4. **MarketWatch Configuration** (`jsons/marketwatch.json`):
   ```json
   {
     "recipient@email.com": "strategy_name"
   }
   ```
   - Maps email recipients to their assigned strategies

5. **Optional Configuration Files**:
   - `jsons/default_config.json` - Override default backtesting settings
   - `jsons/dxy_config.json` - DXY-specific configuration
   - `jsons/veu_redirect_config.json` - VEU redirect settings

### Configuration File Templates

If you need examples, you can create template files with sample values, then copy and modify them:

```bash
# Create a template target.json
echo '{"default_strategies": ["250702-1-3"], "test_strategies": ["250703-1-2"]}' > jsons/target.json

# Create a template email_config.json (modify with your credentials)
echo '{"smtp_server": "smtp.gmail.com", "smtp_port": 587, "sender_email": "your_email@gmail.com", "sender_password": "your_app_password"}' > jsons/email_config.json
```

### Security Notes

- The `.gitignore` file ensures sensitive configuration files (especially `email_config.json`) are never committed
- Always double-check before committing that no credentials are included
- Use environment variables or secure credential stores for production deployments

## Usage

```bash
# Run backtest
python main.py

# With custom config
python main.py --config jsons/my_config.json

# With custom directories
python main.py --data-dir ./my_data --strategies-dir ./my_strategies

# With debug logging
python main.py --log-level DEBUG

# Create sample config
python main.py --create-sample-config

# View results
python viewer.py

# Quick plot - view portfolio value comparison only
python quick.py

# Market watch - automatic strategy analysis
python marketwatch/marketwatch.py

# Market watch console-only version (no email)
python marketwatch/marketwatch_nomail.py
```

### Command Line Options

- `-c, --config`: Configuration file path
- `-d, --data-dir`: CSV data directory (default: ./csv_data)
- `-s, --strategies-dir`: Strategy directory (default: ./oosit_strategies)
- `-l, --log-level`: Log level - DEBUG, INFO, WARNING, ERROR (default: INFO)
- `--create-sample-config`: Create sample configuration file and exit

## Configuration Files

All JSON configuration files are stored in the `jsons/` directory for better organization:
- `jsons/target.json`: Strategy configuration (default and test strategies)
- `jsons/default_config.json`: Default backtesting configuration
- `jsons/dxy_config.json`: DXY-specific configuration
- `jsons/veu_redirect_config.json`: VEU redirect configuration
- `jsons/parameter_customization.json`: Parameter sweep configuration
- `jsons/marketwatch.json`: Market watch strategy assignments
- `jsons/email_config.json`: Email credentials for market watch

## Data Format

### CSV File Requirements

**Naming Convention:**

```
name (start_date - end_date) (frequency) (source).csv
```

**Example:**

```
TQQQ (2010.02.11 - 2024.12.31) (daily) (yfinance).csv
```

**Requirements:**

- Dates in `YYYY.MM.DD` format
- Frequency: `daily` or `monthly`
- `Date` column required
- Chronological order (oldest to newest)

**Supported Sources:**

- `yfinance`: Yahoo Finance (default column: `Open`)
- `MacroMicro`: MacroMicro (default column: `Value`)
- `FRED`: Federal Reserve Economic Data (default column: `Value`)

### CSV Data Utilities

**clean_csv_data.py**
- Interactive tool to clean CSV files by aligning data with NYSE trading days
- Validates existing data and only cleans if necessary
- Preserves original files with `_raw_` prefix before cleaning

**data_fetcher.py**
- Fetches new market data from Yahoo Finance or MacroMicro
- Update mode (`--update`) backs up and refreshes all CSV files
- Automatically detects and prevents duplicate ticker/source combinations

**data_extender.py**
- Creates synthetic historical data for leveraged/inverse ETFs
- Extends limited ETF history using base ETF data and leverage multipliers
- Supports both positive (3x) and negative (-1x) leverage factors

**plot_ext_open_prices.py**
- Quick visualization tool for extended ETF data files
- Plots open prices for all `ext_*.csv` files in a grid layout

## Strategy Configuration

`jsons/target.json`:

```json
{
  "default_strategies": ["250702-1-3"],
  "test_strategies": ["250703-1-2"]
}
```

Strategy files must include:

```python
_explanation = r"""Strategy description..."""

def backtest(start_date, end_date, get_nyse_open_dates, initialize_get_value):
    # Implementation
    return date_range, portfolio_values, rebalancing_log
```

## Output

Results saved to `./oosit_results/test_strategies [flag] (YYMMDD-HHMMSS)/`:

Folder naming convention:

- `test_strategies`: Comma-joined string of test strategies from jsons/target.json (e.g., XXX,YYY,ZZZ)
- `[flag]`: Configuration indicator (omitted for default config):
  - No flag: Default config (jsons/default_config.json or no config specified)
  - `[DXY]`: Using jsons/dxy_config.json
  - `[VEU]`: Using jsons/veu_redirect_config.json
  - `[filename]`: For other config files (e.g., `[myconfig]` for jsons/myconfig.json)
- `(YYMMDD-HHMMSS)`: Timestamp of execution

Examples:

- `250704-1-1 (241231-145032)` - Using default config
- `250704-1-1 [DXY] (241231-145032)` - Using jsons/dxy_config.json
- `250704-1-1 [myconfig] (241231-145032)` - Using jsons/myconfig.json

Contents:

- Word documents: Strategy reports (with configured fonts and right-aligned dates)
- CSV reports: Analysis results
- Archive: `test_strategies.tgz` or `test_strategies [flag].tgz`

## Market Watch

The `marketwatch/marketwatch.py` script automatically analyzes multiple strategies and sends email reports.

A console-only version `marketwatch/marketwatch_nomail.py` is also available that outputs results to the terminal without sending emails - useful for testing or when email is not required.

1. Reads strategy list from `jsons/marketwatch.json`
2. Collects all unique tickers needed by all strategies
3. Downloads full historical data from yfinance (from year 2000 onwards)
4. Runs backtests for the last 3 years for each strategy (optimized to avoid duplicate runs)
5. Sends email reports to each recipient with their strategy results

### Configuration

**jsons/marketwatch.json** (strategy assignments):
```json
{
  "recipient@email.com": "strategy_file_name"
}
```

**jsons/email_config.json** (email credentials):
```json
{
  "smtp_server": "smtp.gmail.com",
  "smtp_port": 587,
  "sender_email": "your_email@gmail.com",
  "sender_password": "your_app_password"
}
```

### Email Setup

1. Enable 2-factor authentication on your Gmail account
2. Generate an App Password:
   - Go to Google Account settings
   - Security → 2-Step Verification → App passwords
   - Generate password for "Mail"
3. Use the 16-character app password in `jsons/email_config.json`

### Features

- **Optimized execution**: Each unique strategy runs only once, even if assigned to multiple recipients
- **HTML email reports**: Professional formatting with:
  - Previous mode and allocation (historical data)
  - Current mode and allocation (with live data)
  - Real-time prices for all tickers
- **Error handling**: Continues sending to other recipients if one fails

Example email report includes:
- Strategy name and execution timestamp
- Market status (pre-market, regular, after-hours)
- Historical vs current mode comparison
- Portfolio allocations
- Live ticker prices

## Quick Plot

The `quick.py` script provides a simplified way to run backtests and immediately view portfolio value comparison plots:

```bash
# Basic usage
python quick.py

# With custom config
python quick.py --config jsons/my_config.json

# With custom directories
python quick.py --data-dir ./my_data --strategies-dir ./my_strategies
```

Features:
- Runs the same backtesting pipeline as `main.py`
- Uses `jsons/target.json` to determine default and test strategies
- Creates individual plots for each test strategy:
  - Each plot shows one test strategy (in red) vs all default strategies
  - Default strategies are shown in different colors with labels
- Interactive matplotlib plotting for easy navigation
- No report generation or file output

## Parameter Sweeper

The `parameter_sweeper.py` script performs grid search to find optimal strategy parameters:

```bash
# Basic usage (defaults to jsons/parameter_customization.json)
python parameter_sweeper.py

# With custom config
python parameter_sweeper.py --config jsons/my_config.json

# With custom directories and logging
python parameter_sweeper.py -d ./my_data -s ./my_strategies -l DEBUG

# Specify output directory
python parameter_sweeper.py --output ./my_results
```

### Parameter Customization Format

Create a JSON file specifying parameter ranges:

```json
{
  "strategy_name": {
    "parameter_name": {
      "min": 0.0,
      "max": 0.2,
      "nsteps": 21
    },
    "nested_param[sub_key]": {
      "min": 0.0,
      "max": 0.3,
      "nsteps": 31
    },
    "_increase_condition": [
      ["param1", "param2", "param3"],
      ["nested_param[key1]", "nested_param[key2]", "nested_param[key3]", "nested_param[key4]"]
    ]
  }
}
```

- Use bracket notation `param[key]` for nested dictionary parameters
- `nsteps` determines how many values to test (using numpy.linspace)
- **Note**: Only strategies specified in the parameter JSON file will be run (not all strategies from jsons/target.json)
- **_increase_condition**: Optional list of ordering constraints. Each inner list specifies parameters that must be in strictly increasing order. Lists can contain any number of parameters (e.g., `["param1", "param2", "param3"]` ensures param1 < param2 < param3). Supports nested bracket notation. Invalid combinations are automatically filtered out before backtesting.

### Output Structure

Results saved to `parameter_sweep_results/{strategies} (YYYYMMDD_HHMMSS)/`:

- Folder name includes comma-separated list of strategies and timestamp
- `{strategy_name}_results.csv`: All parameter combinations and metrics
- `{strategy_name}_summary.json`: Best parameters and performance summary (with UTF-8 encoding for international characters)
- `all_results.json`: Complete results for all strategies (with UTF-8 encoding)

### Parameter Analyzer

Analyze sweep results to find top performing parameter combinations:

```bash
python parameter_analyzer.py  # Interactive mode
python parameter_analyzer.py parameter_sweep_results/250705-1-1_results.csv  # Direct analysis
```

- Ranks parameters by returns (higher is better) or drawdowns (lower is better)
- Saves top N combinations to `analysis.txt`

## MarketWatch NYSE Scheduler Auto-Startup

The `marketwatch/marketwatch_nyse_scheduler.py` runs automated market analysis at NYSE market open times. There are two ways to run it:

### Quick Guide for Beginners

**Option 1: Manual Run (Temporary)**
- Run the scheduler in a terminal window
- Automatically restarts if it crashes
- Stops when you close the terminal

**Option 2: Auto-Start Service (Permanent)**
- Installs as a system service
- Starts automatically when your computer boots
- Runs in the background

### Windows

**Option 1: Manual Run with Auto-Restart**
```bash
# Run the scheduler with automatic restart on crash
marketwatch\marketwatch_scheduler_windows.bat
```
- Double-click the .bat file or run it in Command Prompt
- Keep the window open to see logs
- Press Ctrl+C to stop

**Option 2: Windows Task Scheduler - Hidden Mode (Recommended for Auto-Start)**
```bash
# Run as Administrator to set up auto-start in hidden mode
marketwatch\setup_windows_autostart_hidden.bat
```
- Right-click and "Run as Administrator"
- This installs the scheduler to start automatically in the background
- No visible command window - runs completely hidden
- The scheduler will start automatically 1 minute after system boot
- **Runs on battery power**: The task is configured to start and continue running even on battery mode

**Managing the Windows Service**

To check if it's running (no visible window):
```bash
tasklist | findstr python
```

To remove auto-start:
```bash
# Run as Administrator
marketwatch\remove_autostart.bat

# Or manually:
schtasks /delete /tn "MarketWatch NYSE Scheduler" /f
```

**Beginner's Guide: How to Remove Auto-Start**

If you want to stop the MarketWatch scheduler from starting automatically when Windows boots:

1. **Using the remove_autostart.bat file** (Easiest method):
   - Navigate to the `marketwatch` folder
   - Right-click on `remove_autostart.bat`
   - Select "Run as Administrator"
   - A command window will appear briefly, showing that the auto-start task has been removed
   - Press any key to close the window when prompted

2. **What this does**:
   - Removes the Windows scheduled task that starts MarketWatch at boot
   - MarketWatch will no longer run automatically
   - You can still run MarketWatch manually using the .bat files

**Beginner's Guide: How to Verify Auto-Start Status**

To check if MarketWatch is set to auto-start and is currently running:

1. **Using the verify_autostart.ps1 file**:
   - Navigate to the `marketwatch` folder
   - Right-click on `verify_autostart.ps1`
   - Select "Run with PowerShell"
   - If Windows asks about execution policy, type `Y` and press Enter

2. **What the verification script shows**:
   - **Python processes**: Shows if MarketWatch is currently running
   - **Scheduled task**: Shows if auto-start is configured and when it last ran
   - **Battery settings**: Shows if it will run on battery power
   - **Startup log**: Shows if the process was started automatically or manually
   - **Recent events**: Shows task scheduler history

3. **Understanding the output**:
   - `[Y]` means Yes/Enabled/Working
   - `[N]` means No/Disabled/Not found
   - `[!]` means Warning/Information

4. **Common scenarios**:
   - If Python is NOT running after restart, the script provides commands to manually trigger the task
   - If no startup log exists, it means the scheduler hasn't started yet
   - "AUTO-STARTED" in the log means it was started by Windows automatically

To check logs:
```bash
# View Task Scheduler history
eventvwr.msc
# Navigate to: Applications and Services Logs → Microsoft → Windows → TaskScheduler → Operational

# Or check task status
schtasks /query /tn "MarketWatch NYSE Scheduler" /v
```

### macOS/Linux

**Option 1: Manual Run with Auto-Restart**
```bash
# Make executable (first time only)
chmod +x marketwatch/marketwatch_scheduler_unix.sh

# Run the scheduler
./marketwatch/marketwatch_scheduler_unix.sh
```
- Runs in your terminal window
- Press Ctrl+C to stop

**Option 2: System Service (Recommended for Auto-Start)**
```bash
# Make setup script executable
chmod +x marketwatch/setup_unix_autostart.sh

# Run setup
./marketwatch/setup_unix_autostart.sh
```
- The setup script detects your OS (macOS or Linux)
- Creates the appropriate service configuration files
- **Important**: The script will display activation commands that you must run manually

**After running the setup script, activate the service:**

For macOS:
```bash
# Load the service (printed by setup script)
launchctl load ~/Library/LaunchAgents/com.oosit.marketwatch-scheduler.plist
```

For Linux:
```bash
# Enable and start the service (printed by setup script)
systemctl --user daemon-reload
systemctl --user enable marketwatch-scheduler.service
systemctl --user start marketwatch-scheduler.service
```

**Managing the Service (Optional)**

These commands are available AFTER running the setup script, if you need to check status or troubleshoot:

**For macOS (uses launchd):**
```bash
# Check if it's running
launchctl list | grep marketwatch

# View logs
tail -f ~/Library/Logs/com.oosit.marketwatch-scheduler.log

# Stop the service
launchctl stop com.oosit.marketwatch-scheduler

# Start the service
launchctl start com.oosit.marketwatch-scheduler

# Disable auto-start
launchctl unload ~/Library/LaunchAgents/com.oosit.marketwatch-scheduler.plist
```

**For Linux (uses systemd):**
```bash
# Check if it's running
systemctl --user status marketwatch-scheduler.service

# View logs
journalctl --user -u marketwatch-scheduler.service -f

# Stop the service
systemctl --user stop marketwatch-scheduler.service

# Start the service
systemctl --user start marketwatch-scheduler.service

# Disable auto-start
systemctl --user disable marketwatch-scheduler.service
```

### Removing Auto-Start

If you want to stop the scheduler from starting automatically:

**Windows:**
```bash
# Run as Administrator
marketwatch\remove_autostart.bat
```

**macOS:**
```bash
launchctl unload ~/Library/LaunchAgents/com.oosit.marketwatch-scheduler.plist
rm ~/Library/LaunchAgents/com.oosit.marketwatch-scheduler.plist
```

**Linux:**
```bash
systemctl --user disable marketwatch-scheduler.service
systemctl --user stop marketwatch-scheduler.service
rm ~/.config/systemd/user/marketwatch-scheduler.service
systemctl --user daemon-reload
```

### Features

- **Automatic restart**: If the scheduler crashes, it will automatically restart after 10 seconds
- **System startup**: Scheduler starts automatically when the system boots
- **Background operation**: On all platforms, runs invisibly without terminal windows
- **Logging**: All output is logged for debugging
- **Graceful shutdown**: Use Ctrl+C to stop the scheduler cleanly (manual mode only)

## Architecture Improvements

### Shared Utilities

All common functionality has been centralized in `oosit_utils`:

- **Data cleaning**: `clean_yfinance_data()` for consistent data preprocessing
- **Position formatting**: `format_position()` for displaying portfolio positions
- **Technical indicators**: Computed dynamically via `TechnicalIndicators` class
- **Strategy execution**: Unified through `StrategyManager.execute_strategy()`

### Configuration System

The configuration system has been enhanced for consistency:

- **Default configuration**: Provided by `BacktestConfig` class in `oosit_utils`
- **Configuration priority**: Command-line flags > config file > defaults
- **Consistent handling**: Both `main.py` and `parameter_sweeper.py` use the same configuration pattern

### Critical Fixes

Recent improvements to ensure all configuration values are properly utilized:

- **max_lookback_days**: Now correctly passed from config to `DataManager` and then to `TechnicalIndicators`
- **output_directory**: Now properly used by `ReportGenerator` instead of being hardcoded
- **parameter_sweeper.py**:
  - Now only runs strategies specified in the input JSON file (using custom StrategyManager configuration similar to marketwatch.py)
  - Defaults to `jsons/parameter_customization.json` (no longer requires JSON file as command-line argument)
  - Fixed UTF-8 encoding for JSON files to properly save Korean and other international characters
  - **Critical bug fix**: Parameters now properly propagate to strategies through kwargs support in `execute_strategy()` and `BacktestEngine`

### Benefits

- **Consistency**: Changes to utilities automatically propagate to all modules
- **Maintainability**: Single source of truth for shared logic
- **Extensibility**: Easy to add new indicators or utilities
- **Configuration reliability**: All config values are now properly honored throughout the system
