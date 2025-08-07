"""
OOSIT Backtest Viewer - GUI for analyzing archived backtest results.
"""

import tkinter as tk
from tkinter import ttk, messagebox
import tkinter.font as tkfont
import pandas as pd
import numpy as np
import json
from pathlib import Path
import logging
import sys

# Matplotlib imports
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk

# Configure matplotlib for better cross-platform compatibility
if sys.platform == 'darwin':
    matplotlib.use('TkAgg')

# OOSIT utilities
from oosit_utils import ArchiveProcessor as OOSITArchiveProcessor


class TrackingDataManager:
    """Wrapper for DataManager that tracks which data sources are used."""
    
    def __init__(self, data_manager):
        self.wrapped = data_manager
        self.used_data_sources = set()
        self.accessed_keys = set()  # Track which dataframe keys were accessed
        
        # Build a reverse mapping to track actual sources after redirection
        self.actual_source_map = {}
        
        # First, map each key to itself
        for key in data_manager.dataframes.keys():
            self.actual_source_map[key] = key
        
        # Then apply redirections to track actual sources
        if hasattr(data_manager, 'redirect_dict') and data_manager.redirect_dict:
            for original, target in data_manager.redirect_dict.items():
                if original in self.actual_source_map and target in self.actual_source_map:
                    # After redirection, original key points to target's data
                    self.actual_source_map[original] = target
        
        # Copy all attributes from wrapped data manager
        for attr in dir(data_manager):
            if not attr.startswith('_') and not hasattr(self, attr):
                setattr(self, attr, getattr(data_manager, attr))
    
    def get_data_accessor(self, backtest_start_date):
        """Get a data accessor that tracks usage."""
        original_accessor = self.wrapped.get_data_accessor(backtest_start_date)
        
        def tracking_get_value(name, date_range_index, optional_property=''):
            # Call the original accessor to get the value
            result = original_accessor(name, date_range_index, optional_property)
            
            # Track what data source is actually being used
            # First determine which key will be accessed after ext_ handling
            actual_key = name
            if self.wrapped.use_extended_data:
                ext_name = f"ext_{name}"
                if ext_name in self.wrapped.dataframes:
                    actual_key = ext_name
            
            # Track the actual key being accessed (where indicators will be stored)
            self.accessed_keys.add(actual_key)
            
            
            # Also track the source file for filtering
            if actual_key in self.actual_source_map:
                actual_source = self.actual_source_map[actual_key]
                self.used_data_sources.add(actual_source)
            else:
                # Fallback - shouldn't happen but just in case
                self.used_data_sources.add(actual_key)
            
            return result
        
        return tracking_get_value


class ArchiveProcessor(OOSITArchiveProcessor):
    """Viewer-specific archive processor that extends the base OOSIT processor."""
    
    def __init__(self):
        super().__init__()
        self.used_data_sources = set()
    
    def _create_data_manager(self, config):
        """Create data manager WITHOUT redundant validation.
        
        Override parent's method to skip validation since DataManager
        will validate the data anyway during initialization.
        """
        from oosit_utils.data import DataManager
        
        # Create data manager with configuration
        # DataManager.__init__ will call _load_data() which validates everything
        return DataManager(
            data_directory="./csv_data",
            use_extended_data=config.get("use_extended_data", False),
            redirect_dict=config.get("redirect_dict") or config.get("data_redirection"),
            max_lookback_days=config.get("max_lookback_days", 400)
        )
    
    def load_archive_with_ui_feedback(self, archive_path):
        """
        Load archive with UI-specific error handling.
        
        This wraps the parent's load_archive method to provide
        messagebox warnings for the GUI.
        """
        try:
            return self.load_archive_with_tracking(archive_path)
        except Exception as e:
            # Show user-friendly error in GUI
            messagebox.showerror(
                "Archive Loading Error",
                f"Failed to load archive: {e}"
            )
            raise
    
    def _process_archive_strategies(self, backtest_engine, strategy_manager, config, data_manager):
        """Override parent to capture dataframes before restoration."""
        # Call the parent's implementation but capture dataframes first
        full_start_date = config['full_start_date']
        full_end_date = config['full_end_date']
        
        # Load strategies
        default_strategies, test_strategies = strategy_manager.load_all_strategies(data_manager)
        all_strategies = {**default_strategies, **test_strategies}
        
        strategy_results = {}
        rebalancing_tracks = {}
        
        # Process each strategy
        for strategy_name in all_strategies.keys():
            try:
                self.logger.info(f"Processing strategy: {strategy_name}")
                
                # Execute strategy with proper error handling
                date_range, pv, rebalancing_log = strategy_manager.execute_strategy(
                    strategy_name, full_start_date, full_end_date, data_manager
                )
                
                strategy_results[strategy_name] = (date_range, pv)
                rebalancing_tracks[strategy_name] = rebalancing_log
                
                self.logger.info(f"Successfully processed strategy: {strategy_name}")
                
            except IndexError as e:
                if "Negative index" in str(e):
                    self.logger.warning(
                        f"Strategy {strategy_name} requires more historical data than available"
                    )
                    strategy_results[strategy_name] = ([], [])
                    rebalancing_tracks[strategy_name] = []
                else:
                    raise
            except Exception as e:
                self.logger.error(f"Error processing strategy {strategy_name}: {e}")
                strategy_results[strategy_name] = ([], [])
                rebalancing_tracks[strategy_name] = []
        
        # IMPORTANT: Capture dataframes BEFORE restoration
        import copy
        dataframes_with_indicators = {}
        for key, df in data_manager.wrapped.dataframes.items():
            dataframes_with_indicators[key] = df.copy()
            
        
        # NOW restore data manager state
        data_manager.restore_original_data()
        
        # Generate rebalancing log using engine's method
        display_names = [backtest_engine._get_display_name(name) for name in strategy_results.keys()]
        strategy_name_mapping = {name: backtest_engine._get_display_name(name) for name in strategy_results.keys()}
        
        # Create mock results for rebalancing log generation
        mock_results = {}
        for strategy_name, (date_range, pv) in strategy_results.items():
            # Check if date_range and pv have data (handle numpy arrays/lists properly)
            has_data = False
            try:
                if date_range is not None and pv is not None:
                    if hasattr(date_range, '__len__') and hasattr(pv, '__len__'):
                        has_data = len(date_range) > 0 and len(pv) > 0
                    else:
                        has_data = bool(date_range) and bool(pv)
            except:
                has_data = False
                
            if has_data:
                from oosit_utils.backtesting.engine import BacktestResult
                mock_results[strategy_name] = {
                    "Full Period": BacktestResult(
                        strategy_name=strategy_name,
                        display_name=strategy_name_mapping[strategy_name],
                        period_name="Full Period",
                        date_range=date_range,
                        portfolio_values=pv,
                        normalized_values=[],
                        total_return=0,
                        max_drawdown=0,
                        rebalancing_log=rebalancing_tracks.get(strategy_name)
                    )
                }
        
        # Temporarily set engine results and generate log
        backtest_engine.results = mock_results
        rebalancing_log_df = backtest_engine._create_rebalancing_log(
            display_names, strategy_name_mapping
        )
        
        return {
            'strategy_results': strategy_results,
            'rebalancing_log_df': rebalancing_log_df,
            'dataframes_with_indicators': dataframes_with_indicators
        }
    
    def load_archive_with_tracking(self, archive_path):
        """Load archive and track which data sources are used."""
        with self.extract_archive(archive_path) as temp_dir:
            # Load configuration
            config = self._load_config(temp_dir)
            
            # Setup data manager with tracking wrapper
            original_data_manager = self._create_data_manager(config)
            data_manager = TrackingDataManager(original_data_manager)
            
            # Setup strategy manager
            strategy_manager = self._create_strategy_manager(temp_dir, config)
            
            # Create backtest engine for processing
            from oosit_utils.backtesting import BacktestEngine
            backtest_engine = BacktestEngine(data_manager, strategy_manager)
            
            # Process strategies - our override will capture dataframes before restore
            results = self._process_archive_strategies(
                backtest_engine, strategy_manager, config, data_manager
            )
            
            # Store used data sources and accessed keys
            self.used_data_sources = data_manager.used_data_sources
            self.accessed_keys = data_manager.accessed_keys
            
            
            # Use the captured dataframes with indicators
            return (
                results['dataframes_with_indicators'],
                results['strategy_results'],
                config,
                results['rebalancing_log_df']
            )


class PlottingApp(tk.Tk):
    """Main GUI application for backtest visualization."""
    
    def __init__(self, tgz_files):
        super().__init__()
        self.title("OOSIT Backtest Viewer - Interactive Plotter & Log Viewer")
        
        # Platform-specific window sizing
        if sys.platform == 'darwin':  # macOS
            self.geometry("1400x900")
        else:
            self.geometry("1600x1000")
        
        # Set minimum window size to ensure controls are always visible
        self.minsize(1100, 650)
        
        # Center window on screen
        self.update_idletasks()
        width = self.winfo_width()
        height = self.winfo_height()
        x = (self.winfo_screenwidth() // 2) - (width // 2)
        y = (self.winfo_screenheight() // 2) - (height // 2)
        self.geometry(f'+{x}+{y}')
        
        # Initialize components
        self.archive_processor = ArchiveProcessor()
        self.logger = logging.getLogger(__name__)
        
        # Setup styling
        self._setup_styling()
        
        # Data storage
        self.dataframes = {}
        self.strategy_results = {}
        self.test_periods_data = []
        self.rebalancing_log_df = pd.DataFrame()
        self.used_data_sources = set()  # Track which data sources were used
        self.accessed_keys = set()  # Track which dataframe keys were accessed
        self.source_to_key_map = {}  # Map data sources to dataframe keys for plotting
        
        # UI state
        self.data_source_vars = {}
        self.label_vars = {}
        self.strategy_vars = {}
        self.selected_log_date = None
        
        # Create UI
        self._create_ui(tgz_files)
        
        # Initialize display
        self.archive_combo.set('Select an archive to begin...')
        self.generate_plot()
        
        # Bind window resize event for responsive updates
        self.bind('<Configure>', self._on_window_resize)
        self._resize_timer = None
        
        # Ensure proper shutdown when window is closed
        self.protocol("WM_DELETE_WINDOW", self._on_closing)
    
    def _on_closing(self):
        """Handle window closing event."""
        try:
            # Cancel any pending timers
            if hasattr(self, '_resize_timer') and self._resize_timer:
                self.after_cancel(self._resize_timer)
            
            # Close matplotlib figures
            plt.close('all')
            
            # Clear any matplotlib backend resources
            if hasattr(self, 'canvas'):
                self.canvas.get_tk_widget().destroy()
            
            # Destroy the window
            self.quit()
            self.destroy()
            
            # Update to process any remaining events
            self.update()
            
            # Force exit to ensure console doesn't hang
            import os
            os._exit(0)
        except:
            # Force exit even if there's an error
            import os
            os._exit(0)
    
    def _on_window_resize(self, event):
        """Handle window resize events with debouncing."""
        if self._resize_timer:
            self.after_cancel(self._resize_timer)
        self._resize_timer = self.after(300, self._handle_resize)
    
    def _handle_resize(self):
        """Handle window resize after debouncing."""
        # Update plot if we have data
        if hasattr(self, 'canvas') and self.canvas:
            self.canvas.draw_idle()
    
    def _setup_styling(self):
        """Setup UI styling and fonts."""
        style = ttk.Style(self)
        
        # Cross-platform font selection
        if sys.platform == 'darwin':
            font_family = 'SF Pro Display'
            base_size = 13
        elif sys.platform == 'win32':
            font_family = 'Segoe UI'
            base_size = 10
        else:
            font_family = 'DejaVu Sans'
            base_size = 10
        
        self.header_font = tkfont.Font(family=font_family, size=base_size+2, weight='bold')
        self.default_font = tkfont.Font(family=font_family, size=base_size)
        self.labelframe_font = tkfont.Font(family=font_family, size=base_size+1, weight='bold')
        
        self.option_add('*TCombobox*Listbox.font', self.default_font)
        style.configure('TLabel', font=self.header_font)
    
    def _create_ui(self, tgz_files):
        """Create the main user interface."""
        # Main container with padding to ensure controls aren't hidden
        main_container = ttk.Frame(self, padding="5")
        main_container.pack(fill=tk.BOTH, expand=True)
        
        # Configure grid weights for responsiveness
        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(0, weight=1)
        
        # Main layout
        main_pane = ttk.PanedWindow(main_container, orient=tk.HORIZONTAL)
        main_pane.pack(fill=tk.BOTH, expand=True)
        
        # Controls panel with fixed minimum width
        controls_frame = ttk.Frame(main_pane, padding=10)
        main_pane.add(controls_frame, weight=0)  # Don't let it grow
        
        # Plot and log panel
        plot_and_log_pane = ttk.PanedWindow(main_pane, orient=tk.VERTICAL)
        main_pane.add(plot_and_log_pane, weight=1)  # Let this expand
        
        # Plot area (70% of vertical space)
        plot_frame = ttk.Frame(plot_and_log_pane, padding=5)
        plot_and_log_pane.add(plot_frame, weight=7)
        
        # Log area (30% of vertical space)
        log_frame = tk.LabelFrame(
            plot_and_log_pane, 
            text="Rebalancing Log", 
            font=self.labelframe_font, 
            padx=5, 
            pady=5
        )
        plot_and_log_pane.add(log_frame, weight=3)
        
        # Set minimum sizes for panes
        self.after(100, lambda: self._configure_pane_sizes(main_pane, plot_and_log_pane))
        
        # Create sub-components
        self._create_controls(controls_frame, tgz_files)
        self._create_plot_area(plot_frame)
        self._create_log_viewer(log_frame)
    
    def _configure_pane_sizes(self, h_pane, v_pane):
        """Configure minimum sizes for panes after window is mapped."""
        try:
            # Set minimum width for controls panel
            h_pane.paneconfigure(h_pane.panes()[0], minsize=350)
            
            # Set minimum heights for plot and log areas
            if len(v_pane.panes()) >= 2:
                v_pane.paneconfigure(v_pane.panes()[0], minsize=400)  # Plot area
                v_pane.paneconfigure(v_pane.panes()[1], minsize=150)  # Log area
        except:
            pass  # Fail silently if panes aren't ready yet
    
    def _create_controls(self, parent, tgz_files):
        """Create control panel."""
        # Create scrollable container for all controls
        canvas = tk.Canvas(parent, highlightthickness=0)
        scrollbar = ttk.Scrollbar(parent, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)
        
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        # Archive selection
        ttk.Label(scrollable_frame, text="1. Select Archive:", font=self.default_font).pack(anchor='w', padx=5, pady=(5,0))
        self.archive_combo = ttk.Combobox(
            scrollable_frame, 
            state="readonly", 
            values=tgz_files, 
            font=self.default_font,
            width=40
        )
        self.archive_combo.pack(fill='x', expand=True, padx=5, pady=(2, 10))
        self.archive_combo.bind("<<ComboboxSelected>>", self.on_archive_select)
        
        # Date range selection
        ttk.Label(scrollable_frame, text="2. Set Date Range:", font=self.default_font).pack(anchor='w', padx=5)
        self.periods_combo = ttk.Combobox(scrollable_frame, state="readonly", font=self.default_font, width=40)
        self.periods_combo.pack(fill='x', expand=True, padx=5, pady=(2, 5))
        self.periods_combo.bind("<<ComboboxSelected>>", self.on_period_select)
        
        # Date entries
        date_frame = ttk.Frame(scrollable_frame)
        date_frame.pack(fill='x', padx=5, pady=(0, 10))
        
        self.start_date_var = tk.StringVar()
        self.end_date_var = tk.StringVar()
        
        ttk.Label(date_frame, text="Start:", font=self.default_font).pack(side='left', padx=(0,5))
        self.start_date_entry = ttk.Entry(
            date_frame, 
            textvariable=self.start_date_var, 
            font=self.default_font,
            width=12
        )
        self.start_date_entry.pack(side='left', padx=(0, 10))
        
        ttk.Label(date_frame, text="End:", font=self.default_font).pack(side='left', padx=(0,5))
        self.end_date_entry = ttk.Entry(
            date_frame, 
            textvariable=self.end_date_var, 
            font=self.default_font,
            width=12
        )
        self.end_date_entry.pack(side='left')
        
        # Bind enter key
        self.start_date_entry.bind('<Return>', self.generate_plot)
        self.end_date_entry.bind('<Return>', self.generate_plot)
        
        # Selection controls
        ttk.Label(scrollable_frame, text="3. Select Items to Plot:", font=self.default_font).pack(anchor='w', padx=5, pady=(5,5))
        
        # Dynamic height based on platform
        if sys.platform == 'darwin':
            checkbox_frame_height = 160
        else:
            checkbox_frame_height = 180
        
        # Strategy selection
        self.strategy_frame = tk.LabelFrame(
            scrollable_frame, 
            text="Strategies", 
            font=self.labelframe_font, 
            padx=5, 
            pady=5, 
            height=checkbox_frame_height
        )
        self.strategy_frame.pack_propagate(False)
        self.strategy_frame.pack(fill='x', padx=5, pady=5)
        
        # Data source selection
        self.data_source_frame = tk.LabelFrame(
            scrollable_frame, 
            text="Data Sources", 
            font=self.labelframe_font, 
            padx=5, 
            pady=5, 
            height=checkbox_frame_height
        )
        self.data_source_frame.pack_propagate(False)
        self.data_source_frame.pack(fill='x', padx=5, pady=5)
        
        # Label selection
        self.label_frame = tk.LabelFrame(
            scrollable_frame, 
            text="Data Labels", 
            font=self.labelframe_font, 
            padx=5, 
            pady=5, 
            height=checkbox_frame_height
        )
        self.label_frame.pack_propagate(False)
        self.label_frame.pack(fill='x', padx=5, pady=5)
        
        # Add generate button at bottom
        button_frame = ttk.Frame(scrollable_frame)
        button_frame.pack(fill='x', padx=5, pady=10)
        
        ttk.Button(
            button_frame,
            text="Generate Plot",
            command=self.generate_plot,
            style='Accent.TButton'
        ).pack(side='left', padx=(0,5))
        
        ttk.Button(
            button_frame,
            text="Clear All",
            command=self._clear_all_selections
        ).pack(side='left')
    
    def _create_plot_area(self, parent):
        """Create matplotlib plot area."""
        # Create figure with constrained size
        fig_dpi = 100
        if sys.platform == 'darwin':
            fig_size = (7, 4.5)  # Smaller for macOS
        else:
            fig_size = (8, 5)
        
        self.fig = plt.figure(figsize=fig_size, dpi=fig_dpi)
        self.ax = self.fig.add_subplot(111)
        
        # Set figure background
        self.fig.patch.set_facecolor('white')
        
        # Create canvas and toolbar in a frame
        plot_container = ttk.Frame(parent)
        plot_container.pack(fill=tk.BOTH, expand=True)
        
        # Toolbar at top
        toolbar_frame = ttk.Frame(plot_container)
        toolbar_frame.pack(side=tk.TOP, fill=tk.X)
        
        # Canvas below toolbar
        self.canvas = FigureCanvasTkAgg(self.fig, master=plot_container)
        self.canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        
        toolbar = NavigationToolbar2Tk(self.canvas, toolbar_frame)
        toolbar.update()
    
    def _create_log_viewer(self, parent):
        """Create log viewer treeview."""
        # Create frame for treeview and scrollbars
        tree_frame = ttk.Frame(parent)
        tree_frame.pack(fill='both', expand=True, padx=2, pady=2)
        
        self.log_tree = ttk.Treeview(tree_frame, show='headings', height=6)
        
        # Scrollbars
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.log_tree.yview)
        hsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.log_tree.xview)
        self.log_tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        
        # Grid layout
        tree_frame.grid_rowconfigure(0, weight=1)
        tree_frame.grid_columnconfigure(0, weight=1)
        self.log_tree.grid(row=0, column=0, sticky='nsew')
        vsb.grid(row=0, column=1, sticky='ns')
        hsb.grid(row=1, column=0, sticky='ew')
        
        # Style the treeview with better row height and much smaller font
        style = ttk.Style()
        
        # Create much smaller fonts for log
        if sys.platform == 'darwin':
            log_font = tkfont.Font(family='SF Pro Display', size=8)
            log_header_font = tkfont.Font(family='SF Pro Display', size=9, weight='bold')
        elif sys.platform == 'win32':
            log_font = tkfont.Font(family='Segoe UI', size=7)
            log_header_font = tkfont.Font(family='Segoe UI', size=8, weight='bold')
        else:
            log_font = tkfont.Font(family='DejaVu Sans', size=7)
            log_header_font = tkfont.Font(family='DejaVu Sans', size=8, weight='bold')
        
        style.configure("Treeview", font=log_font, rowheight=20)
        style.configure("Treeview.Heading", font=log_header_font)
        
        # Bind selection event
        self.log_tree.bind("<<TreeviewSelect>>", self.on_log_row_select)
    
    def on_archive_select(self, event=None):
        """Handle archive selection."""
        filepath = self.archive_combo.get()
        if not filepath:
            return
        
        # Show loading status
        basename = Path(filepath).name
        self.archive_combo.set(f"Loading {basename}...")
        self.update_idletasks()
        
        try:
            # Process archive with tracking
            dataframes, strategy_results, config, log_df = self.archive_processor.load_archive_with_ui_feedback(filepath)
            
            # Get used data sources and accessed keys from the archive processor
            self.used_data_sources = self.archive_processor.used_data_sources
            self.accessed_keys = self.archive_processor.accessed_keys
            
            # Build source to key mapping for plotting
            # This maps actual data sources (e.g., VEU) to the dataframe keys that contain their data with indicators
            self.source_to_key_map = {}
            
            # Get redirect_dict from config if available
            redirect_dict = config.get('redirect_dict', {})
            
            # Build reverse mapping: for each source, find ALL keys that were redirected to it
            # and merge their indicators
            for source in self.used_data_sources:
                # Find all keys redirected to this source
                redirected_keys = []
                for original_key, redirect_target in redirect_dict.items():
                    if redirect_target == source and original_key in self.accessed_keys:
                        redirected_keys.append(original_key)
                
                if redirected_keys:
                    # Multiple keys redirected to this source - merge their indicators
                    # Create a merged dataframe with all indicators
                    merged_df = None
                    for key in redirected_keys:
                        if key in dataframes:  # Use the dataframes parameter, not self.dataframes
                            if merged_df is None:
                                merged_df = dataframes[key].copy()
                            else:
                                # Add any missing columns from this dataframe
                                for col in dataframes[key].columns:
                                    if col not in merged_df.columns:
                                        merged_df[col] = dataframes[key][col]
                    
                    if merged_df is not None:
                        # Store the merged dataframe with a special key
                        merged_key = f"_merged_{source}"
                        dataframes[merged_key] = merged_df  # Add to dataframes dict before passing it
                        self.source_to_key_map[source] = merged_key
                else:
                    # No redirection, source maps to itself
                    self.source_to_key_map[source] = source
            
            # Update viewer - now dataframes includes the merged ones
            self._update_viewer_data(dataframes, strategy_results, config, log_df)
            self.archive_combo.set(filepath)
            
        except Exception as e:
            import traceback
            self.logger.error(f"Failed to process archive {filepath}: {e}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            messagebox.showerror("Error", f"Failed to process archive: {e}")
            self.archive_combo.set("Error. Select another archive.")
    
    def _update_viewer_data(self, dataframes, strategy_results, config, log_df):
        """Update viewer with new data."""
        self.dataframes = dataframes
        
        # Transform strategy names to display format
        self.strategy_results = {}
        default_strategies = config.get('default_strategies', [])
        
        default_counter = 1
        for strategy_name, result in strategy_results.items():
            if strategy_name in default_strategies:
                # Format as "default N (strategy_name)"
                display_name = f"default {default_counter} ({strategy_name})"
                default_counter += 1
            else:
                # Regular test strategy
                display_name = strategy_name
            
            self.strategy_results[display_name] = result
        
        self.test_periods_data = config.get('test_periods', [])
        self.rebalancing_log_df = log_df
        
        # Update period combo
        period_items = [
            f"{p['period_start_date']} - {p['period_end_date']} ({p['period_name']})"
            for p in self.test_periods_data
        ]
        self.periods_combo['values'] = period_items
        if period_items:
            self.periods_combo.set('')
        
        # Set date range
        self.start_date_var.set(config.get('full_start_date', ''))
        self.end_date_var.set(config.get('full_end_date', ''))
        
        # Update all controls including labels (dataframes now have all indicators)
        self._update_plot_controls()
        
        # Generate initial plot
        self.generate_plot()
    
    def on_period_select(self, event=None):
        """Handle test period selection."""
        selected_index = self.periods_combo.current()
        if selected_index < 0:
            return
        
        period_data = self.test_periods_data[selected_index]
        self.start_date_var.set(period_data['period_start_date'])
        self.end_date_var.set(period_data['period_end_date'])
        self.generate_plot()
    
    def on_log_row_select(self, event=None):
        """Handle log row selection for date highlighting."""
        selection = self.log_tree.selection()
        
        if not selection:
            self.selected_log_date = None
        else:
            try:
                item = self.log_tree.item(selection[0])
                date_str = item['values'][0]
                self.selected_log_date = pd.to_datetime(date_str)
            except (IndexError, ValueError):
                self.selected_log_date = None
        
        # Redraw plot with/without date line
        self._redraw_plot()
    
    def _create_scrollable_checkbox_area(self, parent_frame, item_names, var_dict, default_func, num_columns=2, is_strategy=False):
        """Create scrollable checkbox area."""
        # Create container frame for better organization with a border
        container = ttk.Frame(parent_frame, relief="sunken", borderwidth=1)
        container.pack(fill="both", expand=True, padx=2, pady=2)
        
        canvas = tk.Canvas(container, borderwidth=0, highlightthickness=0, bg='white')
        scrollbar = ttk.Scrollbar(container, orient="vertical", command=canvas.yview)
        canvas.configure(yscrollcommand=scrollbar.set)
        
        # Make scrollbar always visible by setting a dark color
        style = ttk.Style()
        if sys.platform == 'darwin':
            # macOS specific styling
            style.configure("Vertical.TScrollbar", 
                          troughcolor='#E8E8E8',
                          darkcolor='#D0D0D0', 
                          lightcolor='#F0F0F0',
                          bordercolor='#C0C0C0',
                          arrowcolor='#606060')
        else:
            # Windows/Linux styling
            style.configure("Vertical.TScrollbar", 
                          troughcolor='#E0E0E0', 
                          background='#C0C0C0',
                          darkcolor='#B0B0B0',
                          lightcolor='#D0D0D0',
                          bordercolor='#A0A0A0',
                          arrowcolor='#606060')
        
        content_frame = ttk.Frame(canvas)
        content_frame.bind(
            "<Configure>", 
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas_window = canvas.create_window((0, 0), window=content_frame, anchor="nw")
        
        # Bind canvas resize to adjust frame width
        def configure_frame_width(event):
            canvas_width = event.width
            canvas.itemconfig(canvas_window, width=canvas_width - 4)
        
        canvas.bind('<Configure>', configure_frame_width)
        
        # Pack scrollbar first to ensure it's always visible
        scrollbar.pack(side="right", fill="y", padx=(0, 2))
        canvas.pack(side="left", fill="both", expand=True, padx=(2, 0))
        
        # Enable mousewheel scrolling
        def _on_mousewheel(event):
            if sys.platform == 'darwin':
                canvas.yview_scroll(int(-1*(event.delta)), "units")
            else:
                canvas.yview_scroll(int(-1*(event.delta/120)), "units")
        
        def _on_enter(event):
            # Only bind mousewheel when mouse is over the canvas
            if sys.platform == 'darwin':
                canvas.bind_all("<MouseWheel>", _on_mousewheel)
            else:
                canvas.bind_all("<MouseWheel>", _on_mousewheel)
                canvas.bind_all("<Button-4>", lambda e: canvas.yview_scroll(-1, "units"))
                canvas.bind_all("<Button-5>", lambda e: canvas.yview_scroll(1, "units"))
        
        def _on_leave(event):
            # Unbind when mouse leaves to prevent interfering with other scrollable areas
            canvas.unbind_all("<MouseWheel>")
            if sys.platform != 'darwin':
                canvas.unbind_all("<Button-4>")
                canvas.unbind_all("<Button-5>")
        
        canvas.bind("<Enter>", _on_enter)
        canvas.bind("<Leave>", _on_leave)
        
        # Create checkboxes
        import re
        default_regex = re.compile(r"^default \d+ \(.*\)")
        
        for i, name in enumerate(sorted(item_names)):
            var = tk.BooleanVar(value=default_func(name))
            var_dict[name] = var
            
            # Format display text for strategies
            display_text = name
            if is_strategy and default_regex.match(name):
                match = re.search(r'\((.*)\)', name)
                if match:
                    display_text = '*' + match.group(1)
            
            row, col = i // num_columns, i % num_columns
            cb = tk.Checkbutton(
                content_frame, 
                text=display_text, 
                variable=var, 
                command=self.generate_plot, 
                font=self.default_font
            )
            cb.grid(row=row, column=col, sticky='w', padx=5, pady=3)
        
        # Ensure content frame expands to fill available width
        for col in range(num_columns):
            content_frame.columnconfigure(col, weight=1)
        
        # Update scroll region after all widgets are added
        content_frame.update_idletasks()
        canvas.configure(scrollregion=canvas.bbox("all"))
    
    def _update_plot_controls(self):
        """Update plot control checkboxes."""
        
        # Clear existing controls
        for frame in [self.strategy_frame, self.data_source_frame, self.label_frame]:
            for widget in frame.winfo_children():
                widget.destroy()
        
        self.strategy_vars.clear()
        self.data_source_vars.clear()
        self.label_vars.clear()
        
        # Strategy checkboxes
        strategy_names = list(self.strategy_results.keys())
        self._create_scrollable_checkbox_area(
            self.strategy_frame, 
            strategy_names, 
            self.strategy_vars, 
            lambda item: True, 
            2,
            is_strategy=True
        )
        
        # Data source checkboxes - only show used data sources
        available_sources = list(self.dataframes.keys())
        sources_to_show = [src for src in available_sources if src in self.used_data_sources] if self.used_data_sources else available_sources
        
        self._create_scrollable_checkbox_area(
            self.data_source_frame, 
            sources_to_show, 
            self.data_source_vars, 
            lambda item: False, 
            3
        )
        
        # Label checkboxes - collect all labels from accessed dataframe keys
        all_labels = set()
        
        # Use accessed_keys to get labels - these are the keys where indicators were computed
        if self.accessed_keys:
            for key in self.accessed_keys:
                if key in self.dataframes:
                    df = self.dataframes[key]
                    # Include ALL columns that exist in the dataframe after backtest
                    # This includes dynamically computed indicators like MA200
                    all_labels.update(col for col in df.columns if col != 'Date')
        elif self.used_data_sources:
            # Fallback to used_data_sources if accessed_keys not available
            for source in self.used_data_sources:
                if source in self.dataframes:
                    df = self.dataframes[source]
                    all_labels.update(col for col in df.columns if col != 'Date')
        else:
            # Final fallback to all dataframes
            for df in self.dataframes.values():
                all_labels.update(col for col in df.columns if col != 'Date')
        
        self._create_scrollable_checkbox_area(
            self.label_frame, 
            all_labels, 
            self.label_vars, 
            lambda item: item in {'Open', 'Value'}, 
            3
        )
    
    def generate_plot(self, event=None):
        """Generate plot and update log viewer."""
        self._redraw_plot()
        self._update_log_viewer()
    
    def _redraw_plot(self):
        """Redraw the matplotlib plot."""
        self.ax.clear()
        
        # Apply tight layout to prevent label cutoff
        self.fig.tight_layout()
        
        # Get selected items
        selected_sources = [name for name, var in self.data_source_vars.items() if var.get()]
        selected_labels = [name for name, var in self.label_vars.items() if var.get()]
        selected_strategies = [name for name, var in self.strategy_vars.items() if var.get()]
        
        # Get date range
        start_str = self.start_date_var.get()
        end_str = self.end_date_var.get()
        
        try:
            start_date = pd.to_datetime(start_str) if start_str else None
            end_date = pd.to_datetime(end_str) if end_str else None
        except (ValueError, TypeError):
            self.ax.text(
                0.5, 0.5, 
                'Invalid Date Format\nPlease use YYYY-MM-DD', 
                ha='center', va='center', 
                fontsize=12, color='red'
            )
            self.canvas.draw()
            return
        
        has_plotted = False
        
        # Plot data sources
        if selected_sources and selected_labels:
            has_plotted |= self._plot_data_sources(selected_sources, selected_labels, start_date, end_date)
        
        # Plot strategies
        if selected_strategies:
            has_plotted |= self._plot_strategies(selected_strategies, start_date, end_date)
        
        # Add date line if selected
        if self.selected_log_date:
            self.ax.axvline(x=self.selected_log_date, color='red', linewidth=1)
        
        # Configure plot
        if not has_plotted and not self.selected_log_date:
            self.ax.text(
                0.5, 0.5, 
                'Select an archive and items to plot', 
                ha='center', va='center', 
                fontsize=12, color='gray'
            )
        
        self.ax.set_yscale('log')
        self.ax.set_title("Normalized Performance (Log Scale)", fontsize=12, pad=10)
        self.ax.set_ylabel("Value (Normalized to 100 at Start)", fontsize=10)
        self.ax.set_xlabel("Date", fontsize=10)
        self.ax.grid(True, which="both", ls="--", linewidth=0.5, alpha=0.7)
        
        if has_plotted:
            # Position legend to avoid overlap
            handles, labels = self.ax.get_legend_handles_labels()
            if len(handles) <= 10:
                self.ax.legend(loc='best', fontsize=9, framealpha=0.9)
            else:
                # For many items, use smaller font and columns
                ncol = 2 if len(handles) > 15 else 1
                self.ax.legend(loc='upper left', bbox_to_anchor=(1.01, 1), 
                             fontsize=8, framealpha=0.9, ncol=ncol)
            
            # Rotate x-axis labels for better readability
            for label in self.ax.get_xticklabels():
                label.set_rotation(45)
                label.set_ha('right')
                label.set_fontsize(9)
        
        # Adjust layout to prevent label cutoff
        self.fig.tight_layout()
        self.canvas.draw()
    
    def _plot_data_sources(self, selected_sources, selected_labels, start_date, end_date):
        """Plot selected data sources and labels."""
        has_plotted = False
        
        for source in selected_sources:
            # Use the mapping to find the correct dataframe key
            # For redirected sources, this will point to the dataframe with computed indicators
            dataframe_key = self.source_to_key_map.get(source, source)
            df = self.dataframes.get(dataframe_key)
            if df is None:
                continue
            
            # Filter by date range
            mask = pd.Series(True, index=df.index)
            if start_date:
                mask &= (df['Date'] >= start_date)
            if end_date:
                mask &= (df['Date'] <= end_date)
            
            df_filtered = df[mask]
            if df_filtered.empty:
                continue
            
            # Get normalizer (first Open value if available)
            normalizer = None
            if 'Open' in df_filtered.columns:
                first_open = df_filtered['Open'].dropna()
                if not first_open.empty:
                    normalizer = first_open.iloc[0]
            
            # Plot each selected label
            for label in selected_labels:
                if label not in df.columns:
                    continue
                
                series = df_filtered[label].dropna()
                if series.empty:
                    continue
                
                # Normalize
                current_normalizer = normalizer if normalizer is not None else series.iloc[0]
                if pd.isna(current_normalizer) or current_normalizer == 0:
                    continue
                
                normalized_series = (series / current_normalizer) * 100
                
                self.ax.plot(
                    df_filtered.loc[series.index, 'Date'], 
                    normalized_series,
                    marker='.', 
                    linestyle='--', 
                    alpha=0.7, 
                    label=f'{source} - {label}'
                )
                has_plotted = True
        
        return has_plotted
    
    def _plot_strategies(self, selected_strategies, start_date, end_date):
        """Plot selected strategies."""
        has_plotted = False
        
        for strategy_name in selected_strategies:
            result = self.strategy_results.get(strategy_name)
            if not result:
                continue
            
            date_range, pv = result
            # Check if date_range has data (handle numpy arrays/lists)
            if date_range is None or (hasattr(date_range, '__len__') and len(date_range) == 0):
                continue
            
            # Create strategy DataFrame
            strategy_df = pd.DataFrame({
                'Date': pd.to_datetime(date_range), 
                'PV': pv
            })
            
            # Filter by date range
            mask = pd.Series(True, index=strategy_df.index)
            if start_date:
                mask &= (strategy_df['Date'] >= start_date)
            if end_date:
                mask &= (strategy_df['Date'] <= end_date)
            
            df_filtered = strategy_df[mask]
            if df_filtered.empty:
                continue
            
            # Normalize and plot
            series = df_filtered['PV'].dropna()
            if series.empty:
                continue
            
            initial_value = series.iloc[0]
            if pd.isna(initial_value) or initial_value == 0:
                continue
            
            normalized_series = (series / initial_value) * 100
            
            # Format label for default strategies
            import re
            default_regex = re.compile(r"^default \d+ \(.*\)")
            if default_regex.match(strategy_name):
                match = re.search(r'\((.*)\)', strategy_name)
                label = f'*{match.group(1)} (Portfolio)' if match else f'{strategy_name} (Portfolio)'
            else:
                label = f'{strategy_name} (Portfolio)'
            
            self.ax.plot(
                df_filtered.loc[series.index, 'Date'], 
                normalized_series,
                marker='.', 
                linestyle='-', 
                linewidth=1.5, 
                label=label
            )
            has_plotted = True
        
        return has_plotted
    
    def _clear_all_selections(self):
        """Clear all checkbox selections."""
        for var in self.strategy_vars.values():
            var.set(False)
        for var in self.data_source_vars.values():
            var.set(False)
        for var in self.label_vars.values():
            var.set(False)
        self.generate_plot()
    
    def _update_log_viewer(self):
        """Update the rebalancing log viewer."""
        # Clear selection state when rebuilding log
        self.selected_log_date = None
        
        # Clear existing items
        for item in self.log_tree.get_children():
            self.log_tree.delete(item)
        self.log_tree['columns'] = ()
        
        if self.rebalancing_log_df.empty:
            return
        
        # Get selected strategies and build display columns
        selected_strategies = [name for name, var in self.strategy_vars.items() if var.get()]
        display_cols = ['날짜']
        
        import re
        default_regex = re.compile(r"^default \d+ \(.*\)")
        
        if not self.rebalancing_log_df.empty:
            self.logger.debug(f"Rebalancing log columns: {list(self.rebalancing_log_df.columns)}")
        
        # Build display columns based on how they appear in the rebalancing log
        for strategy in selected_strategies:
            # Get the display name that would be used in the rebalancing log
            if default_regex.match(strategy):
                # Extract inner name and add asterisk for default strategies
                match = re.search(r'\((.*)\)', strategy)
                if match:
                    display_name = '*' + match.group(1)
                else:
                    display_name = strategy
            else:
                # Regular strategy name
                display_name = strategy
            
            # Check for columns with the display name
            for suffix in [' (에서)', ' (으로)']:
                full_col = f'{display_name}{suffix}'
                if full_col in self.rebalancing_log_df.columns:
                    display_cols.append(full_col)
                    self.logger.debug(f"Found column: {full_col}")
                else:
                    self.logger.debug(f"Column not found: {full_col}")
        
        if len(display_cols) <= 1:
            return
        
        # Filter log data
        log_to_show = self.rebalancing_log_df[display_cols].copy()
        
        # Apply date range filter
        try:
            start_date_str = self.start_date_var.get()
            end_date_str = self.end_date_var.get()
            
            if start_date_str and end_date_str:
                start_date = pd.to_datetime(start_date_str)
                end_date = pd.to_datetime(end_date_str)
                mask = (log_to_show['날짜'] >= start_date) & (log_to_show['날짜'] <= end_date)
                log_to_show = log_to_show[mask]
        except (ValueError, TypeError):
            pass
        
        # Remove empty rows
        non_date_cols = [c for c in log_to_show.columns if c != '날짜']
        log_to_show.dropna(subset=non_date_cols, how='all', inplace=True)
        
        if log_to_show.empty:
            return
        
        # Setup treeview columns
        self.log_tree['columns'] = list(log_to_show.columns)
        for col in log_to_show.columns:
            self.log_tree.heading(col, text=col, anchor='w')
            # Adjust column widths based on content
            if col == '날짜':
                width = 100
            elif '(에서)' in col:
                width = 350  # From columns need more space
            elif '(으로)' in col:
                width = 350  # To columns need more space
            else:
                width = 300
            self.log_tree.column(col, width=width, anchor='w', stretch=tk.YES)
        
        # Populate treeview
        for _, row in log_to_show.iterrows():
            values = list(row)
            values[0] = values[0].strftime('%Y-%m-%d')  # Format date
            str_values = ["" if pd.isna(v) else str(v) for v in values]
            item = self.log_tree.insert("", "end", values=str_values)
            
            # Add alternating row colors for better readability
            if len(self.log_tree.get_children()) % 2 == 0:
                self.log_tree.item(item, tags=('evenrow',))
        
        # Configure tag colors
        self.log_tree.tag_configure('evenrow', background='#F5F5F5')


def find_tgz_files():
    """Find .tgz files in current directory, oosit_results/, and oosit_tgz_archives/ subdirectories."""
    current_dir = Path('.')
    tgz_files = []
    
    # Search current directory
    tgz_files.extend(current_dir.glob('*.tgz'))
    
    # Search oosit_results/ subdirectories
    oosit_results_dir = current_dir / 'oosit_results'
    if oosit_results_dir.exists() and oosit_results_dir.is_dir():
        tgz_files.extend(oosit_results_dir.glob('**/*.tgz'))
    
    # Search oosit_tgz_archives/ subdirectories
    oosit_tgz_archives_dir = current_dir / 'oosit_tgz_archives'
    if oosit_tgz_archives_dir.exists() and oosit_tgz_archives_dir.is_dir():
        tgz_files.extend(oosit_tgz_archives_dir.glob('**/*.tgz'))
    
    return [str(f) for f in tgz_files]


def main():
    """Main entry point."""
    # Setup logging
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Handle high DPI displays
    if sys.platform == 'win32':
        try:
            from ctypes import windll
            windll.shcore.SetProcessDpiAwareness(1)
        except:
            pass
    
    # Find archive files
    tgz_files = find_tgz_files()
    if not tgz_files:
        messagebox.showerror(
            "No Archives Found", 
            "Could not find any .tgz archive files in this directory, "
            "oosit_results/, or oosit_tgz_archives/ subdirectories."
        )
        return
    
    try:
        # Launch application
        app = PlottingApp(tgz_files)
        app.mainloop()
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()