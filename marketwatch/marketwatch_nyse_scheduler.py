"""
MarketWatch NYSE-Aware Scheduler - Runs at NYSE market open accounting for DST
"""
import schedule
import time
import subprocess
import sys
from datetime import datetime, timedelta
import pytz
import logging
from zoneinfo import ZoneInfo
import pandas_market_calendars as mcal
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NYSEScheduler:
    def __init__(self):
        self.nyse_tz = pytz.timezone('America/New_York')
        self.local_tz = pytz.timezone('Asia/Seoul')  # KST
        self.market_open_time = "09:30"  # NYSE opens at 9:30 AM ET
        self.nyse_calendar = mcal.get_calendar('NYSE')
        
    def get_nyse_open_in_local_time(self):
        """Calculate when NYSE opens in local time, accounting for DST"""
        # Get current date in NYSE timezone
        now_nyse = datetime.now(self.nyse_tz)
        
        # Create market open time for today in NYSE timezone
        market_open_nyse = now_nyse.replace(
            hour=9, minute=30, second=0, microsecond=0
        )
        
        # Convert to local timezone
        market_open_local = market_open_nyse.astimezone(self.local_tz)
        
        return market_open_local.strftime("%H:%M")
    
    def get_nyse_pre_open_in_local_time(self):
        """Calculate 10 minutes before NYSE opens in local time, accounting for DST"""
        # Get current date in NYSE timezone
        now_nyse = datetime.now(self.nyse_tz)
        
        # Create pre-market time (9:20 AM ET) for today in NYSE timezone
        pre_open_nyse = now_nyse.replace(
            hour=9, minute=20, second=0, microsecond=0
        )
        
        # Convert to local timezone
        pre_open_local = pre_open_nyse.astimezone(self.local_tz)
        
        return pre_open_local.strftime("%H:%M")
    
    def is_nyse_trading_day(self):
        """Check if today is a NYSE trading day (Mon-Fri, excluding holidays)"""
        now_nyse = datetime.now(self.nyse_tz)
        
        # Check if today is a trading day using pandas-market-calendars
        today_str = now_nyse.strftime('%Y-%m-%d')
        valid_days = self.nyse_calendar.valid_days(start_date=today_str, end_date=today_str)
        
        return len(valid_days) > 0
    
    def get_next_scheduled_run(self):
        """Get the next scheduled run time (pre-market or market open) in local timezone"""
        now_nyse = datetime.now(self.nyse_tz)
        
        # Get the next trading day using pandas-market-calendars
        start_date = now_nyse.strftime('%Y-%m-%d')
        end_date = (now_nyse + timedelta(days=30)).strftime('%Y-%m-%d')  # Look ahead 30 days
        
        schedule = self.nyse_calendar.schedule(start_date=start_date, end_date=end_date)
        
        if len(schedule) == 0:
            # No trading days in the next 30 days (unlikely)
            raise ValueError("No trading days found in the next 30 days")
        
        # Find the next scheduled run (pre-market or market open)
        for idx, row in schedule.iterrows():
            market_open = row['market_open'].to_pydatetime()
            pre_market = market_open - timedelta(minutes=10)
            
            # Check pre-market time first
            if pre_market > now_nyse:
                # Convert to local time
                next_run_local = pre_market.astimezone(self.local_tz)
                return next_run_local
            # Then check market open time
            elif market_open > now_nyse:
                # Convert to local time
                next_run_local = market_open.astimezone(self.local_tz)
                return next_run_local
        
        # This shouldn't happen if schedule has data
        raise ValueError("Could not determine next scheduled run")

def run_marketwatch(timing="market_open"):
    """Execute marketwatch.py"""
    try:
        if timing == "pre_market":
            logger.info("Running MarketWatch 10 minutes before NYSE market open...")
        else:
            logger.info("Running MarketWatch at NYSE market open...")
        # Get the path to marketwatch.py in the same directory as this script
        marketwatch_path = Path(__file__).parent / 'marketwatch.py'
        subprocess.run([sys.executable, str(marketwatch_path)], check=True)
        logger.info("MarketWatch completed successfully!")
    except subprocess.CalledProcessError as e:
        logger.error(f"MarketWatch failed with error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

def run_marketwatch_pre_open():
    """Execute marketwatch.py for pre-market run"""
    run_marketwatch("pre_market")

def run_marketwatch_at_open():
    """Execute marketwatch.py for market open run"""
    run_marketwatch("market_open")

def setup_dynamic_schedule():
    """Setup schedule that adjusts for DST changes"""
    scheduler = NYSEScheduler()
    
    def schedule_next_run():
        """Schedule the next run and reschedule itself"""
        # Clear existing jobs
        schedule.clear()
        
        # Get current NYSE times in local timezone
        open_time = scheduler.get_nyse_open_in_local_time()
        pre_open_time = scheduler.get_nyse_pre_open_in_local_time()
        
        # Check what's actually being scheduled today
        now = datetime.now()
        today_str = now.strftime("%H:%M")
        
        pre_open_dt = datetime.strptime(pre_open_time, "%H:%M").replace(
            year=now.year, month=now.month, day=now.day
        )
        open_dt = datetime.strptime(open_time, "%H:%M").replace(
            year=now.year, month=now.month, day=now.day
        )
        
        logger.info(f"NYSE market hours in local time (accounting for DST):")
        
        if pre_open_dt > now:
            logger.info(f"  - Pre-market run: {pre_open_time} (10 min before open) - SCHEDULED")
        else:
            logger.info(f"  - Pre-market run: {pre_open_time} (10 min before open) - already passed today")
            
        if open_dt > now:
            logger.info(f"  - Market open run: {open_time} - SCHEDULED")
        else:
            logger.info(f"  - Market open run: {open_time} - already passed today")
        
        # Schedule pre-market runs (10 minutes before open)
        schedule.every().monday.at(pre_open_time).do(run_and_reschedule_pre)
        schedule.every().tuesday.at(pre_open_time).do(run_and_reschedule_pre)
        schedule.every().wednesday.at(pre_open_time).do(run_and_reschedule_pre)
        schedule.every().thursday.at(pre_open_time).do(run_and_reschedule_pre)
        schedule.every().friday.at(pre_open_time).do(run_and_reschedule_pre)
        
        # Schedule market open runs
        schedule.every().monday.at(open_time).do(run_and_reschedule_open)
        schedule.every().tuesday.at(open_time).do(run_and_reschedule_open)
        schedule.every().wednesday.at(open_time).do(run_and_reschedule_open)
        schedule.every().thursday.at(open_time).do(run_and_reschedule_open)
        schedule.every().friday.at(open_time).do(run_and_reschedule_open)
        
        # Also schedule a daily check to update times (in case of DST change)
        schedule.every().day.at("18:00").do(schedule_next_run)
        
        next_run = scheduler.get_next_scheduled_run()
        logger.info(f"Next run scheduled for: {next_run.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    def run_and_reschedule_pre():
        """Run pre-market marketwatch"""
        if scheduler.is_nyse_trading_day():
            run_marketwatch_pre_open()
        else:
            logger.info("Not a NYSE trading day, skipping pre-market run...")
    
    def run_and_reschedule_open():
        """Run market open marketwatch"""
        if scheduler.is_nyse_trading_day():
            run_marketwatch_at_open()
        else:
            logger.info("Not a NYSE trading day, skipping market open run...")
    
    # Initial schedule setup
    schedule_next_run()
    
    logger.info("NYSE Market Open Scheduler started!")
    logger.info("Configured to run at:")
    logger.info("  - 10 minutes before NYSE market open (9:20 AM ET)")
    logger.info("  - NYSE market open (9:30 AM ET)")
    logger.info("Runs every trading day and automatically adjusts for Daylight Saving Time")
    logger.info("Press Ctrl+C to stop...")
    
    return scheduler

def main():
    """Main scheduler loop"""
    scheduler = setup_dynamic_schedule()
    
    while True:
        try:
            schedule.run_pending()
            
            # Get seconds until next scheduled job
            idle_seconds = schedule.idle_seconds()
            
            if idle_seconds is None:
                # No jobs scheduled
                sleep_time = 30
                logger.debug("No jobs scheduled, sleeping for 30 seconds")
            elif idle_seconds < 0:
                # Job was scheduled in the past (should run immediately)
                sleep_time = 1
                logger.debug("Job overdue, checking again in 1 second")
            elif idle_seconds <= 60:
                # Next job is within 1 minute, check every second
                sleep_time = 1
                if idle_seconds > 1:
                    logger.info(f"Next job in {idle_seconds:.0f} seconds, switching to 1-second checks")
            else:
                # Next job is more than 1 minute away, check every 30 seconds
                sleep_time = 30
                logger.debug(f"Next job in {idle_seconds/60:.1f} minutes, using 30-second checks")
            
            time.sleep(sleep_time)
            
            # Log next run time periodically (every hour)
            if datetime.now().minute == 0 and datetime.now().second < 30:
                next_run = scheduler.get_next_scheduled_run()
                logger.info(f"Next scheduled run: {next_run.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                
        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user")
            break
        except Exception as e:
            logger.error(f"Error in scheduler: {e}")
            time.sleep(60)

if __name__ == "__main__":
    # Note: You'll need to install required libraries
    # pip install schedule pytz pandas-market-calendars
    main()