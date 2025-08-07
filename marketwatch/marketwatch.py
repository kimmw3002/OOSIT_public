"""
Market Watch - Real-time strategy analysis using OOSIT framework
Fetches full historical data from yfinance and runs strategies on last 3 years
"""

import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
import yfinance as yf
import pandas as pd
import logging
import os
import tempfile
import inspect
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import from oosit_utils
from oosit_utils import StrategyManager, DataManager, format_position, clean_yfinance_data, Config

# Set up logging
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class YFinanceDataDownloader:
    """
    Downloads data from yfinance and saves it in CSV format compatible with DataManager.
    """
    def __init__(self, tickers, start_date, end_date, temp_dir):
        self.tickers = tickers
        self.start_date = start_date
        self.end_date = end_date
        self.temp_dir = temp_dir
        self.csv_files_created = []
        self.ticker_mapping = {}  # Maps original ticker names to safe filenames
        
    def download_and_save(self):
        """Download data from yfinance and save as CSV files."""
        print(f"\n전체 가능한 데이터를 다운로드 중입니다 ({self.start_date} ~ {self.end_date})...")
        
        # Download all tickers at once for better performance
        print(f"  {len(self.tickers)}개 티커를 한번에 다운로드 중...", end="", flush=True)
        
        try:
            if len(self.tickers) == 1:
                # For single ticker, download without group_by to avoid multi-level columns
                bulk_data = yf.download(
                    self.tickers[0],  # Pass as string, not list
                    start=self.start_date,
                    end=self.end_date,
                    progress=False,
                    auto_adjust=False,
                    prepost=True,
                    multi_level_index=False
                )
                if not bulk_data.empty:
                    ticker_data_dict = {self.tickers[0]: bulk_data}
                else:
                    ticker_data_dict = {}
            else:
                # For multiple tickers, use group_by='ticker'
                bulk_data = yf.download(
                    tickers=self.tickers,
                    start=self.start_date,
                    end=self.end_date,
                    progress=False,
                    auto_adjust=False,
                    prepost=True,
                    group_by='ticker',
                    threads=True  # Use threading for faster downloads
                )
                
                ticker_data_dict = {}
                if not bulk_data.empty:
                    for ticker in self.tickers:
                        try:
                            # Extract data for this ticker
                            ticker_data = bulk_data[ticker].copy()
                            
                            # Check if ticker data is empty (all NaN)
                            if not ticker_data.isna().all().all():
                                # Drop rows where all values are NaN
                                ticker_data = ticker_data.dropna(how='all')
                                if not ticker_data.empty:
                                    # Reorder columns to match individual download order
                                    ticker_data = ticker_data[['Adj Close', 'Close', 'High', 'Low', 'Open', 'Volume']]
                                    ticker_data_dict[ticker] = ticker_data
                        except KeyError:
                            # Ticker not found in bulk data
                            pass
            
            print(f" 완료")
            
            # Process and save each ticker's data in parallel
            def process_ticker(ticker, ticker_data):
                """Process a single ticker's data and save to CSV"""
                try:
                    df = ticker_data.copy()
                    
                    # Reset index to make Date a column
                    df.reset_index(inplace=True)
                    
                    # Clean the data using shared utility
                    df = clean_yfinance_data(df)
                    
                    # Convert Date to string format expected by DataManager
                    df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
                    
                    # Save to CSV with proper filename format
                    actual_start = df['Date'].min().replace('-', '.')
                    actual_end = df['Date'].max().replace('-', '.')
                    
                    # Replace problematic characters in ticker name for filename
                    safe_ticker = ticker.replace('-', '_').replace('.', '_')
                    # Format: name_source_frequency_startdate_enddate.csv
                    filename = f"{safe_ticker} ({actual_start} - {actual_end}) (daily) (yfinance).csv"
                    filepath = Path(self.temp_dir) / filename
                    
                    # Store mapping if ticker name was changed
                    if safe_ticker != ticker:
                        self.ticker_mapping[ticker] = safe_ticker
                    
                    df.to_csv(filepath, index=False)
                    
                    return ticker, filepath, len(df), None
                    
                except Exception as e:
                    return ticker, None, 0, str(e)
            
            # Process all tickers in parallel
            print(f"  {len(ticker_data_dict)}개 티커를 병렬로 처리 중...")
            max_workers = min(8, len(ticker_data_dict))  # Limit to 8 parallel workers
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all processing tasks
                futures = {
                    executor.submit(process_ticker, ticker, data): ticker 
                    for ticker, data in ticker_data_dict.items()
                }
                
                # Collect results as they complete
                for future in as_completed(futures):
                    ticker, filepath, days, error = future.result()
                    if error:
                        print(f"  {ticker}: 실패 - {error}")
                    elif filepath:
                        self.csv_files_created.append(filepath)
                        print(f"  {ticker}: 완료 ({days}일)")
                    else:
                        print(f"  {ticker}: 데이터 없음")
            
        except Exception as e:
            print(f"\n  벌크 다운로드 실패: {e}")
            print("  개별 다운로드로 대체합니다...")
            
            # Fallback to individual downloads if bulk fails
            for ticker in self.tickers:
                print(f"  {ticker} 다운로드 중...", end="", flush=True)
                try:
                    df = yf.download(ticker, start=self.start_date, end=self.end_date, 
                                   progress=False, auto_adjust=False, multi_level_index=False, prepost=True)
                    
                    if df.empty:
                        print(f" 실패 (데이터 없음)")
                        continue
                    
                    df.reset_index(inplace=True)
                    df = clean_yfinance_data(df)
                    df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
                    
                    actual_start = df['Date'].min().replace('-', '.')
                    actual_end = df['Date'].max().replace('-', '.')
                    
                    safe_ticker = ticker.replace('-', '_').replace('.', '_')
                    filename = f"{safe_ticker} ({actual_start} - {actual_end}) (daily) (yfinance).csv"
                    filepath = Path(self.temp_dir) / filename
                    
                    if safe_ticker != ticker:
                        self.ticker_mapping[ticker] = safe_ticker
                    
                    df.to_csv(filepath, index=False)
                    self.csv_files_created.append(filepath)
                    
                    print(f" 완료 ({len(df)}일)")
                    
                except Exception as e:
                    print(f" 실패: {e}")
        
        print(f"\n총 {len(self.csv_files_created)}개 티커 데이터 다운로드 완료")
        return len(self.csv_files_created) > 0
    




def run_strategy_for_analysis(strategy_name, strategy_manager, data_manager, backtest_start_date=None, include_live_data=True):
    """
    Run a strategy using StrategyManager's execute_strategy method.
    include_live_data: If False, excludes the last day (live data) from analysis
    """
    # Get the full data range
    daily_dates = data_manager.get_date_range('daily')
    if not daily_dates:
        print("데이터가 없습니다.")
        return None
    
    # Determine start and end dates
    data_start_date = daily_dates[0].strftime('%Y-%m-%d')
    
    # If excluding live data, use second-to-last date as end date
    if not include_live_data and len(daily_dates) > 1:
        data_end_date = daily_dates[-2].strftime('%Y-%m-%d')
    else:
        data_end_date = daily_dates[-1].strftime('%Y-%m-%d')
    
    if backtest_start_date:
        start_date = backtest_start_date
    else:
        start_date = data_start_date
    
    # Run the strategy using StrategyManager
    try:
        date_range, portfolio_values, rebalancing_log = strategy_manager.execute_strategy(
            strategy_name, start_date, data_end_date, data_manager
        )
        
        if date_range and len(portfolio_values) > 0:
            # Get the final position
            if rebalancing_log and len(rebalancing_log) > 0:
                last_rebalance = rebalancing_log[-1]
                # Handle tuple format: (date, mode_string, position_string)
                if isinstance(last_rebalance, tuple):
                    date = last_rebalance[0] if len(last_rebalance) > 0 else data_end_date
                    mode_info = last_rebalance[1] if len(last_rebalance) > 1 else 'Unknown'
                    position_info = last_rebalance[2] if len(last_rebalance) > 2 else ''
                    
                    # Extract mode name from position_info string (element 2) which contains the actual applied reallocation
                    # The position_info has format: "ModeName {dict}" and we need the VERY LAST mode
                    mode = 'Unknown'
                    if position_info:
                        import re
                        # Extract all mode names that appear before a dictionary
                        # This handles Normal, Defense, Aggressive, Unknown, etc.
                        mode_pattern = r'([A-Za-z]+)\s*\{'
                        mode_matches = re.findall(mode_pattern, position_info)
                        if mode_matches:
                            # Get the VERY LAST mode from position_info
                            mode = mode_matches[-1]
                    elif mode_info:
                        # Fallback to mode_info if position_info is empty
                        import re
                        mode_pattern = r'([A-Za-z]+)\s*\{'
                        mode_matches = re.findall(mode_pattern, mode_info)
                        if mode_matches:
                            mode = mode_matches[-1]
                    
                    # Parse position from position_info string
                    position = {}
                    if position_info and '{' in position_info:
                        try:
                            import ast
                            # Find all dictionaries in the string and use the last one
                            dict_pattern = r'\{[^{}]*\}'
                            dict_matches = re.findall(dict_pattern, position_info)
                            if dict_matches:
                                # Use the VERY LAST dictionary found
                                position = ast.literal_eval(dict_matches[-1])
                            else:
                                # Fallback to original method
                                dict_str = position_info[position_info.rfind('{'):]
                                position = ast.literal_eval(dict_str)
                        except:
                            position = {}
                else:
                    # Handle dictionary format (if any strategies use it)
                    date = last_rebalance.get('Date', data_end_date)
                    mode = last_rebalance.get('Mode', 'Unknown')
                    position = last_rebalance.get('Position', {})
                
                return {
                    'date': date,
                    'mode': mode,
                    'position': position,
                    'portfolio_value': portfolio_values[-1] if len(portfolio_values) > 0 else 1.0,
                    'start_date': start_date,
                    'end_date': data_end_date,
                    'total_days': len(date_range) if date_range else 0
                }
            else:
                return {
                    'date': data_end_date,
                    'mode': 'Unknown',
                    'position': {},
                    'portfolio_value': portfolio_values[-1] if len(portfolio_values) > 0 else 1.0,
                    'start_date': start_date,
                    'end_date': data_end_date,
                    'total_days': len(date_range) if date_range else 0
                }
                    
    except Exception as e:
        print(f"전략 실행 중 오류: {e}")
        import traceback
        traceback.print_exc()
        
    return None





def get_premarket_prices(tickers):
    """
    Get live prices for all tickers, supporting pre-market, regular, and after-market sessions.
    Returns a tuple of (prices_dict, market_status)
    """
    live_prices = {}
    market_status = "확인 불가"
    
    def fetch_ticker_price(ticker):
        """Fetch price for a single ticker"""
        try:
            info = yf.Ticker(ticker).info
            state = info.get('marketState')
            price = None
            current_status = "장 마감/대체"
            
            if state == 'PRE':
                price = info.get('preMarketPrice')
                current_status = "프리마켓"
            elif state == 'REGULAR':
                price = info.get('regularMarketPrice')
                current_status = "정규장"
            elif state == 'POST':
                price = info.get('postMarketPrice')
                current_status = "애프터마켓"
            
            if price is None:
                price = info.get('regularMarketPrice', info.get('previousClose'))
            
            if price is None:
                # Try to get the latest price from history
                hist = yf.Ticker(ticker).history(period="2d", prepost=True)
                if not hist.empty:
                    price = hist['Close'].iloc[-1]
                    current_status = "최근 체결가"
            
            return ticker, price, current_status, None
            
        except Exception as e:
            return ticker, None, None, str(e)
    
    print(f"\n실시간 가격 조회 중 ({len(tickers)}개 티커)...", end="", flush=True)
    
    # Fetch all prices in parallel
    max_workers = min(8, len(tickers))  # Limit to 8 parallel workers
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all fetch tasks
        futures = [executor.submit(fetch_ticker_price, ticker) for ticker in tickers]
        
        # Collect results
        for future in as_completed(futures):
            ticker, price, status, error = future.result()
            
            if error:
                print(f"\n오류: {ticker} 가격 조회 실패: {error}")
                return None, None
            
            if price is None:
                print(f"\n오류: {ticker}의 실시간 가격을 가져올 수 없습니다.")
                return None, None
            
            live_prices[ticker] = price
            if ticker == 'SPY':  # Use SPY as the reference for market status
                market_status = status
            
            print(".", end="", flush=True)
    
    print(" 완료.")
    return live_prices, market_status


def collect_all_tickers(strategy_manager, config):
    """Collect all unique tickers needed by all strategies"""
    all_tickers = set()
    
    for key, strategy_name in config.items():
        # Check if strategy exists
        if (strategy_name not in strategy_manager.default_strategies and 
            strategy_name not in strategy_manager.test_strategies):
            print(f"전략 {strategy_name}을 찾을 수 없습니다. marketwatch.json을 확인하세요.")
            continue
        
        # Get the strategy module for ticker extraction
        if strategy_name in strategy_manager.default_strategies:
            strategy_module = strategy_manager.default_strategies[strategy_name]
        else:
            strategy_module = strategy_manager.test_strategies[strategy_name]
        
        # Extract tickers from strategy parameters
        sig = inspect.signature(strategy_module.backtest)
        params = sig.parameters
        
        using_tickers = ['QQQ', 'TQQQ', 'PSQ', 'SPY']  # Default
        if 'using_tickers' in params and params['using_tickers'].default != inspect.Parameter.empty:
            using_tickers = list(params['using_tickers'].default)
            # Ensure SPY is included
            if 'SPY' not in using_tickers:
                using_tickers.append('SPY')
        
        # Check if strategy uses DXY
        if hasattr(strategy_module, '__code__') or hasattr(strategy_module.backtest, '__code__'):
            code = strategy_module.backtest.__code__ if hasattr(strategy_module.backtest, '__code__') else strategy_module.__code__
            if 'DX-Y.NYB' in str(code.co_consts):
                if 'DX-Y.NYB' not in using_tickers:
                    using_tickers.append('DX-Y.NYB')
        
        all_tickers.update(using_tickers)
    
    return list(all_tickers)


def run_all_strategies(strategies, strategy_manager, data_manager, downloader, live_prices=None, market_status=None):
    """Run all strategies with the shared data and return results"""
    results = {}
    unique_strategies = list(set(strategy for _, strategy in strategies))
    strategy_results = {}
    
    def run_single_strategy(strategy_name):
        """Run a single strategy and return results"""
        try:
            # Check if strategy exists
            if (strategy_name not in strategy_manager.default_strategies and 
                strategy_name not in strategy_manager.test_strategies):
                return strategy_name, None, f"전략 {strategy_name}을 찾을 수 없습니다."
            
            print(f"\n{'='*70}")
            print(f"분석 시작: {strategy_name}")
            print('='*70)
            
            # Get available date range
            daily_dates = data_manager.get_date_range('daily')
            if not daily_dates:
                print("데이터가 없습니다.")
                return strategy_name, None, "데이터가 없습니다"
            
            min_date = daily_dates[0]
            max_date = daily_dates[-1]
            
            # Calculate backtest start date (3 years from end date)
            # Find the first NYSE open date that is at least 3 years before the end date
            end_date = datetime.now()
            three_years_ago = end_date - timedelta(days=365*3)
            
            # Find the first NYSE open date on or after three_years_ago
            backtest_start_date = None
            for date in daily_dates:
                if date.date() >= three_years_ago.date():
                    backtest_start_date = date.strftime('%Y-%m-%d')
                    break
            
            if not backtest_start_date:
                # If we don't have 3 years of data, use the earliest available date
                backtest_start_date = min_date.strftime('%Y-%m-%d')
                print(f"\n주의: 3년치 데이터가 부족합니다. 사용 가능한 최초 날짜부터 시작합니다.")
            
            data_days = (max_date - min_date).days
            print(f"\n다운로드된 데이터: {min_date.strftime('%Y-%m-%d')} ~ {max_date.strftime('%Y-%m-%d')} ({data_days}일)")
            
            # Run strategy twice: once without live data (historical only) and once with live data
            historical_result = run_strategy_for_analysis(strategy_name, strategy_manager, data_manager, backtest_start_date, include_live_data=False)
            current_result = run_strategy_for_analysis(strategy_name, strategy_manager, data_manager, backtest_start_date, include_live_data=True)
            
            # Get the strategy module for ticker extraction
            if strategy_name in strategy_manager.default_strategies:
                strategy_module = strategy_manager.default_strategies[strategy_name]
            else:
                strategy_module = strategy_manager.test_strategies[strategy_name]
            
            # Extract tickers from strategy parameters
            sig = inspect.signature(strategy_module.backtest)
            params = sig.parameters
            
            using_tickers = ['QQQ', 'TQQQ', 'PSQ', 'SPY']  # Default
            if 'using_tickers' in params and params['using_tickers'].default != inspect.Parameter.empty:
                using_tickers = list(params['using_tickers'].default)
                # Ensure SPY is included
                if 'SPY' not in using_tickers:
                    using_tickers.append('SPY')
            
            # Check if strategy uses DXY
            if hasattr(strategy_module, '__code__') or hasattr(strategy_module.backtest, '__code__'):
                code = strategy_module.backtest.__code__ if hasattr(strategy_module.backtest, '__code__') else strategy_module.__code__
                if 'DX-Y.NYB' in str(code.co_consts):
                    if 'DX-Y.NYB' not in using_tickers:
                        using_tickers.append('DX-Y.NYB')
            
            # Use live prices if provided, otherwise try to get them
            latest_prices = {}
            if live_prices:
                # Filter live prices for only the tickers used by this strategy
                latest_prices = {ticker: price for ticker, price in live_prices.items() if ticker in using_tickers}
            else:
                # Fallback to historical data if live prices not provided
                for ticker in using_tickers:
                    # Handle mapped ticker names
                    actual_ticker = downloader.ticker_mapping.get(ticker, ticker)
                    if actual_ticker in data_manager.dataframes:
                        df = data_manager.dataframes[actual_ticker]
                        if not df.empty and 'Close' in df.columns:
                            latest_prices[ticker] = df['Close'].iloc[-1]
            
            if historical_result and current_result:
                print("\n" + "="*65)
                time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                if market_status:
                    print(f" 마켓워치 분석 결과 (기준 시각: {time_str} | 시장: {market_status})")
                else:
                    print(f" 마켓워치 분석 결과 (기준 시각: {time_str})")
                print("="*65)
                
                # 1. Historical mode (before live data)
                print(f"\n1. 과거 데이터 기준 모드: {historical_result['mode']}")
                print(f"   포지션: {format_position(historical_result['position'])}")
                
                # 2. Current mode (with live data)
                print(f"\n2. 실시간 데이터 반영 모드: {current_result['mode']}")
                print(f"   포지션: {format_position(current_result['position'])}")
                
                # 3. Live prices
                if latest_prices:
                    print(f"\n3. 실시간 가격:")
                    price_strings = []
                    for ticker, price in sorted(latest_prices.items()):
                        price_strings.append(f"{ticker}: ${price:.2f}")
                    print("   " + ", ".join(price_strings))
                
                print("\n" + "="*65)
            else:
                print("전략 분석에 실패했습니다.")
            
            # Return the results for this strategy
            return strategy_name, {
                'historical': historical_result,
                'current': current_result,
                'latest_prices': latest_prices,
                'market_status': market_status,
                'using_tickers': using_tickers
            }, None
        
        except Exception as e:
            import traceback
            error_msg = f"전략 실행 중 오류: {str(e)}\n{traceback.format_exc()}"
            print(error_msg)
            return strategy_name, None, error_msg
    
    # Run all unique strategies sequentially
    print(f"\n총 {len(unique_strategies)}개 전략을 순차적으로 실행합니다...")
    
    for strategy_name in unique_strategies:
        strategy_name, result, error = run_single_strategy(strategy_name)
        if error:
            print(f"\n전략 {strategy_name} 실패: {error}")
            strategy_results[strategy_name] = None
        else:
            strategy_results[strategy_name] = result
    
    # Map results to each recipient
    for key, strategy_name in strategies:
        if strategy_name in strategy_results:
            results[key] = {
                'strategy_name': strategy_name,
                'result': strategy_results[strategy_name]
            }
    
    return results


def create_email_message(recipients, strategy_name, result, email_config):
    """Create email message with strategy analysis results for multiple recipients"""
    # Create message
    msg = MIMEMultipart('alternative')
    msg['Subject'] = f'OOSIT Market Watch Report - {strategy_name}'
    msg['From'] = email_config['sender_email']
    # Handle single recipient or list of recipients
    if isinstance(recipients, list):
        msg['To'] = ', '.join(recipients)
    else:
        msg['To'] = recipients
    
    # Generate email content
    if result and result['result']:
        res = result['result']
        historical = res.get('historical')
        current = res.get('current')
        prices = res.get('latest_prices', {})
        market_status = res.get('market_status', 'Unknown')
        
        # Create HTML content
        html = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; }}
                .header {{ background-color: #f0f0f0; padding: 10px; margin-bottom: 20px; }}
                .section {{ margin-bottom: 20px; }}
                .prices {{ background-color: #f9f9f9; padding: 10px; }}
                table {{ border-collapse: collapse; }}
                td {{ padding: 5px 10px; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2>OOSIT Market Watch Report</h2>
                <p>Strategy: <strong>{strategy_name}</strong></p>
                <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p>Market Status: {market_status}</p>
            </div>
            
            <div class="section">
                <h3>1. Previous Mode (Historical Data)</h3>
                <p><strong>Mode:</strong> {historical['mode'] if historical else 'N/A'}</p>
                <p><strong>Position:</strong> {format_position(historical['position']) if historical else 'N/A'}</p>
            </div>
            
            <div class="section">
                <h3>2. Current Mode (Live Data)</h3>
                <p><strong>Mode:</strong> {current['mode'] if current else 'N/A'}</p>
                <p><strong>Position:</strong> {format_position(current['position']) if current else 'N/A'}</p>
            </div>
            
            <div class="section">
                <h3>3. Current Prices</h3>
                <div class="prices">
                    <table>
        """
        
        for ticker, price in sorted(prices.items()):
            html += f"<tr><td>{ticker}:</td><td>${price:.2f}</td></tr>"
        
        html += """
                    </table>
                </div>
            </div>
        </body>
        </html>
        """
        
        # Create plain text version
        text = f"""
OOSIT Market Watch Report
Strategy: {strategy_name}
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Market Status: {market_status}

1. Previous Mode (Historical Data)
   Mode: {historical['mode'] if historical else 'N/A'}
   Position: {format_position(historical['position']) if historical else 'N/A'}

2. Current Mode (Live Data)
   Mode: {current['mode'] if current else 'N/A'}
   Position: {format_position(current['position']) if current else 'N/A'}

3. Current Prices
"""
        for ticker, price in sorted(prices.items()):
            text += f"   {ticker}: ${price:.2f}\n"
    else:
        text = f"Strategy analysis failed for {strategy_name}"
        html = f"<html><body><p>{text}</p></body></html>"
    
    # Attach parts
    part1 = MIMEText(text, 'plain')
    part2 = MIMEText(html, 'html')
    msg.attach(part1)
    msg.attach(part2)
    
    return msg


def send_all_emails(results, email_config):
    """Send emails using a single SMTP connection - one email per strategy to multiple recipients"""
    sent_emails = 0
    total_recipients = 0
    failed_strategies = 0
    
    # Group recipients by strategy
    strategy_groups = {}
    for recipient, result in results.items():
        if result:
            strategy_name = result['strategy_name']
            if strategy_name not in strategy_groups:
                strategy_groups[strategy_name] = {
                    'recipients': [],
                    'result': result
                }
            strategy_groups[strategy_name]['recipients'].append(recipient)
    
    # Sort strategies by recipient count (descending) - many waiting = first served
    sorted_strategies = sorted(
        strategy_groups.items(),
        key=lambda x: len(x[1]['recipients']),
        reverse=True
    )
    
    try:
        print(f"\n이메일 발송 중... (총 {len(sorted_strategies)}개 전략)")
        with smtplib.SMTP(email_config['smtp_server'], email_config['smtp_port']) as server:
            server.starttls()
            server.login(email_config['sender_email'], email_config['sender_password'])
            
            # Send one email per strategy to all its recipients
            for strategy_name, data in sorted_strategies:
                recipients = data['recipients']
                result = data['result']
                recipient_count = len(recipients)
                
                try:
                    msg = create_email_message(recipients, strategy_name, result, email_config)
                    server.send_message(msg)
                    print(f"✓ [{strategy_name}] 발송 완료 - {recipient_count}명 수신: {', '.join(recipients)}")
                    sent_emails += 1
                    total_recipients += recipient_count
                    
                    # Small delay between different strategy emails
                    if strategy_name != sorted_strategies[-1][0]:
                        time.sleep(0.1)
                        
                except Exception as e:
                    print(f"✗ [{strategy_name}] 발송 실패 ({recipient_count}명): {e}")
                    failed_strategies += 1
                        
        print(f"\n이메일 발송 완료:")
        print(f"  - 성공: {sent_emails}개 이메일 → {total_recipients}명 수신")
        print(f"  - 실패: {failed_strategies}개 전략")
        return sent_emails, failed_strategies
        
    except Exception as e:
        print(f"\n이메일 서버 연결 실패: {e}")
        return 0, len(results)


def main():
    """메인 실행 함수"""
    print("--- 실시간 전략 분석 마켓워치 ---")
    print("(전체 가능한 데이터를 yfinance에서 다운로드하여 최근 3년을 분석합니다)")
    
    # Load marketwatch.json configuration
    try:
        with open(Path(__file__).parent.parent / 'jsons' / 'marketwatch.json', 'r') as f:
            config = json.load(f)
            print(f"\nLoaded configuration from jsons/marketwatch.json:")
            for key, value in config.items():
                print(f"  {key}: {value}")
    except FileNotFoundError:
        print("\nError: jsons/marketwatch.json not found.")
        print("Please create a jsons/marketwatch.json file with your strategy configuration.")
        print("Example format:")
        print(json.dumps({"strategy_name": "strategy_file_name"}, indent=2))
        return
    except Exception as e:
        print(f"\nError loading jsons/marketwatch.json: {e}")
        return
    
    # Initialize StrategyManager with marketwatch config
    print("\n전략 매니저 초기화 중...")
    # Convert marketwatch.json format to target.json format
    # All strategies from marketwatch.json go into test_strategies
    strategy_config = {
        'default_strategies': [],
        'test_strategies': list(config.values())  # Get all strategy names
    }
    
    strategy_manager = StrategyManager(
        strategies_directory=str(Path(__file__).parent.parent / "oosit_strategies"),
        strategy_config=strategy_config
    )
    
    # Load strategies (without DataManager dependency)
    strategy_manager.load_all_strategies()
    print(f"StrategyManager 초기화 완료: {len(strategy_manager.default_strategies)}개 기본 전략, {len(strategy_manager.test_strategies)}개 테스트 전략")
    
    # Collect all unique tickers from all strategies
    print("\n모든 전략에서 필요한 티커 수집 중...")
    all_tickers = collect_all_tickers(strategy_manager, config)
    print(f"총 {len(all_tickers)}개의 고유 티커: {', '.join(sorted(all_tickers))}")
    
    # Calculate date range - download ALL available data
    end_date = datetime.now()
    # Download maximum available data (yfinance typically has data from ~2000 onwards)
    data_download_start = datetime(2000, 1, 1)
    
    # Create temporary directory for CSV files
    with tempfile.TemporaryDirectory() as temp_dir:
        # Download data from yfinance for all tickers at once
        downloader = YFinanceDataDownloader(
            tickers=all_tickers,
            start_date=data_download_start.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d'),
            temp_dir=temp_dir
        )
        
        if not downloader.download_and_save():
            print("데이터 다운로드에 실패했습니다.")
            return
        
        # Create DataManager with the downloaded CSV files and default config
        # Use default config for consistency with main.py
        config_manager = Config()
        data_manager = DataManager(
            data_directory=temp_dir,
            use_extended_data=config_manager.config.use_extended_data,
            redirect_dict=config_manager.config.redirect_dict,
            max_lookback_days=config_manager.config.max_lookback_days
        )
        
        # Manually add entries for original ticker names that were mapped
        # This allows strategies to access tickers by their original names
        for original, safe in downloader.ticker_mapping.items():
            if safe in data_manager.dataframes:
                data_manager.dataframes[original] = data_manager.dataframes[safe]
                data_manager.filenames[original] = data_manager.filenames[safe]
                if safe in data_manager.daily_data_start_index:
                    data_manager.daily_data_start_index[original] = data_manager.daily_data_start_index[safe]
                if safe in data_manager.monthly_data_start_index:
                    data_manager.monthly_data_start_index[original] = data_manager.monthly_data_start_index[safe]
        
        # Add live prices to the data for current analysis
        live_prices, market_status = get_premarket_prices(all_tickers)
        if live_prices:
            # Add today's live prices to each ticker's dataframe
            today_date = datetime.now().strftime('%Y-%m-%d')
            for ticker in all_tickers:
                if ticker in live_prices and ticker in data_manager.dataframes:
                    df = data_manager.dataframes[ticker]
                    # Check if today's date already exists
                    if today_date not in df['Date'].values:
                        # Create a new row with live price
                        new_row = pd.DataFrame({
                            'Date': [today_date],
                            'Open': [live_prices[ticker]],
                            'High': [live_prices[ticker]],
                            'Low': [live_prices[ticker]],
                            'Close': [live_prices[ticker]],
                            'Adj Close': [live_prices[ticker]],
                            'Volume': [0]  # Volume not available for live prices
                        })
                        # Append to dataframe
                        data_manager.dataframes[ticker] = pd.concat([df, new_row], ignore_index=True)
        
        # Run all strategies
        print("\n모든 전략을 자동으로 분석합니다...")
        strategies = list(config.items())
        results = run_all_strategies(strategies, strategy_manager, data_manager, downloader, live_prices, market_status)
        
        print("\n\n모든 전략 분석이 완료되었습니다.")
        
        # Load email configuration
        try:
            with open(Path(__file__).parent.parent / 'jsons' / 'email_config.json', 'r') as f:
                email_config = json.load(f)
            
            # Send all emails using single SMTP connection
            send_all_emails(results, email_config)
            
        except FileNotFoundError:
            print("\n경고: jsons/email_config.json을 찾을 수 없습니다. 이메일을 보내지 않습니다.")
        except Exception as e:
            print(f"\n이메일 설정 로드 중 오류 발생: {e}")


if __name__ == "__main__":
    main()