"""
OOSIT Main Runner - Entry point for the backtesting system.
"""

import argparse
import logging
import sys
from pathlib import Path

# Add current directory to Python path to ensure imports work
sys.path.insert(0, str(Path(__file__).parent))

from oosit_utils import (
    DataManager, 
    StrategyManager, 
    BacktestEngine, 
    ReportGenerator, 
    Config
)


class ConsoleFilter(logging.Filter):
    """Filter to allow only ERROR messages and specific INFO messages on console."""
    def filter(self, record):
        if record.levelno >= logging.ERROR:
            return True
        if record.levelno == logging.INFO:
            message = record.getMessage()
            # Show configuration summary
            if 'Configuration summary:' in message:
                return True
            # Show target.json loading info
            if 'target.json' in message:
                return True
            # Show only full period strategy runs (not test period extractions)
            if 'Running' in message and 'for full period' in message:
                return True
            return False
        return False

def setup_logging(level="DEBUG"):
    """Set up logging configuration."""
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.addFilter(ConsoleFilter())
    
    file_handler = logging.FileHandler('oosit.log', mode='w', encoding='utf-8')
    file_handler.setLevel(getattr(logging, level.upper()))
    
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            console_handler,
            file_handler
        ]
    )


def main(config_file=None, 
         data_directory=None,
         strategies_directory=None):
    """
    Main function to run the complete OOSIT backtesting pipeline.
    
    Args:
        config_file: Path to configuration file
        data_directory: Directory containing CSV data files
        strategies_directory: Directory containing strategy ZIP files
    """
    logger = logging.getLogger(__name__)
    logger.info("Starting OOSIT Backtesting System")
    
    try:
        # 1. Load Configuration
        logger.info("Loading configuration...")
        config_manager = Config(config_file, require_file=bool(config_file))
        
        # Override directories if provided
        if data_directory:
            config_manager.update_config(data_directory=data_directory)
        if strategies_directory:
            config_manager.update_config(strategies_directory=strategies_directory)
        
        # Validate configuration
        errors = config_manager.validate_config()
        if errors:
            logger.error("Configuration validation failed:")
            for error in errors:
                logger.error(f"  - {error}")
            return
        
        # Sort test periods chronologically
        config_manager.sort_test_periods_by_date()
        
        logger.info("Configuration loaded successfully")
        logger.info(f"Configuration summary: {config_manager.get_summary()}")
        
        # 2. Initialize Data Manager
        logger.info("Initializing data manager...")
        data_manager = DataManager(
            data_directory=config_manager.config.data_directory,
            use_extended_data=config_manager.config.use_extended_data,
            redirect_dict=config_manager.config.redirect_dict,
            max_lookback_days=config_manager.config.max_lookback_days
        )
        logger.info(f"Loaded data for {len(data_manager.get_available_assets())} assets")
        
        # 3. Initialize Strategy Manager
        logger.info("Initializing strategy manager...")
        strategy_manager = StrategyManager(
            config_manager.config.strategies_directory
        )
        
        # Validate strategy files
        validation_errors = strategy_manager.validate_strategy_files()
        if validation_errors:
            logger.warning("Strategy validation warnings:")
            for error in validation_errors:
                logger.warning(f"  - {error}")
        
        # 4. Initialize Backtesting Engine
        logger.info("Initializing backtesting engine...")
        backtest_engine = BacktestEngine(data_manager, strategy_manager)
        
        # 5. Run Full Backtest
        logger.info("Starting backtesting execution...")
        backtest_results = backtest_engine.run_full_backtest(
            full_start_date=config_manager.config.full_start_date,
            full_end_date=config_manager.config.full_end_date,
            test_periods=config_manager.get_test_periods_dict_list()
        )
        
        logger.info("Backtesting completed successfully")
        
        # 6. Generate Reports
        logger.info("Generating reports...")
        
        # Create config for report generation
        archive_config = config_manager.create_archive_config_dict()
        archive_config['default_strategies'] = strategy_manager.default_strategy_names
        archive_config['testing_strategies'] = strategy_manager.test_strategy_names
        archive_config['config_file'] = config_manager.config_file
        
        report_generator = ReportGenerator(
            config_manager.config.strategies_directory,
            config=archive_config
        )
        
        report_generator.generate_all_reports(backtest_results, archive_config)
        
        logger.info("Report generation completed successfully")
        
        # 7. Summary
        summary = backtest_results['summary']
        logger.info("\n" + "="*60)
        logger.info("BACKTESTING SUMMARY")
        logger.info("="*60)
        logger.info(f"Strategies tested: {len(summary['strategy_names'])}")
        logger.info(f"Default strategies: {len(summary['default_strategy_names'])}")
        logger.info(f"Test periods: {len(config_manager.config.test_periods)}")
        logger.info(f"Full period: {config_manager.config.full_start_date} to {config_manager.config.full_end_date}")
        logger.info("\nResults have been saved to:")
        logger.info("  - Individual strategy reports: ./oosit_results/*.docx")
        logger.info("  - CSV summaries: ./oosit_results/*.csv")
        logger.info("  - Archive: check the timestamp folder for the .tgz file")
        logger.info("="*60)
        
    except KeyboardInterrupt:
        logger.info("Backtesting interrupted by user")
    except Exception as e:
        logger.error(f"Error during backtesting: {e}", exc_info=True)
        raise
    finally:
        logger.info("OOSIT Backtesting System finished")


def create_sample_config():
    """Create a sample configuration file."""
    config_manager = Config()
    config_manager.save_to_file("jsons/sample_config.json")
    print("Sample configuration file created: jsons/sample_config.json")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OOSIT Backtesting System")
    
    parser.add_argument(
        "--config", "-c",
        type=str,
        help="Path to configuration file (JSON format)"
    )
    
    parser.add_argument(
        "--data-dir", "-d",
        type=str,
        default="./csv_data",
        help="Directory containing CSV data files (default: ./csv_data)"
    )
    
    parser.add_argument(
        "--strategies-dir", "-s", 
        type=str,
        default="./oosit_strategies",
        help="Directory containing strategy ZIP files (default: ./oosit_strategies)"
    )
    
    parser.add_argument(
        "--log-level", "-l",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)"
    )
    
    parser.add_argument(
        "--create-sample-config",
        action="store_true",
        help="Create a sample configuration file and exit"
    )
    
    args = parser.parse_args()
    
    # Set up logging
    setup_logging(args.log_level)
    
    # Create sample config if requested
    if args.create_sample_config:
        create_sample_config()
        sys.exit(0)
    
    # Run main pipeline
    main(
        config_file=args.config,
        data_directory=args.data_dir,
        strategies_directory=args.strategies_dir
    )