import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
from typing import Optional, Dict, List, Any
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import webbrowser

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTableWidget, QTableWidgetItem, QPushButton, QLineEdit, QLabel,
    QComboBox, QDateTimeEdit, QSpinBox, QCheckBox, QTabWidget,
    QTextEdit, QProgressBar, QFileDialog, QMessageBox, QSplitter,
    QGroupBox, QGridLayout, QTreeWidget, QTreeWidgetItem, QFrame,
    QScrollArea, QStatusBar, QToolBar, QAction, QHeaderView
)
from PyQt5.QtCore import (
    Qt, QThread, pyqtSignal, QTimer, QDateTime, QAbstractTableModel,
    QModelIndex, QSortFilterProxyModel
)
from PyQt5.QtGui import QFont, QIcon, QPalette, QColor

class MFTTableModel(QAbstractTableModel):
    """Optimized table model for large datasets"""
    
    def __init__(self, data=None):
        super().__init__()
        self._data = data if data is not None else pd.DataFrame()
        self._headers = list(self._data.columns) if not self._data.empty else []
        
    def rowCount(self, parent=QModelIndex()):
        return len(self._data)
    
    def columnCount(self, parent=QModelIndex()):
        return len(self._headers)
    
    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid() or not (0 <= index.row() < len(self._data)):
            return None
            
        if role == Qt.DisplayRole:
            value = self._data.iloc[index.row(), index.column()]
            if pd.isna(value):
                return ""
            return str(value)
        
        elif role == Qt.BackgroundRole:
            # Highlight deleted files based on InUse flag
            if 'InUse' in self._headers:
                inuse_col = self._headers.index('InUse')
                if not self._data.iloc[index.row(), inuse_col]:
                    return QColor(255, 200, 200)  # Light red for deleted
        
        return None
    
    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role == Qt.DisplayRole and orientation == Qt.Horizontal:
            return self._headers[section] if section < len(self._headers) else ""
        return None
    
    def update_data(self, new_data):
        self.beginResetModel()
        self._data = new_data
        self._headers = list(new_data.columns) if not new_data.empty else []
        self.endResetModel()

class DataLoader(QThread):
    """Background thread for loading large CSV files"""
    progress = pyqtSignal(int)
    finished = pyqtSignal(pd.DataFrame)
    error = pyqtSignal(str)
    
    def __init__(self, file_path, chunk_size=10000):
        super().__init__()
        self.file_path = file_path
        self.chunk_size = chunk_size
        
    def run(self):
        try:
            # Get total file size for progress calculation
            file_size = os.path.getsize(self.file_path)
            chunks = []
            bytes_read = 0
            
            # Read in chunks with progress updates
            for chunk in pd.read_csv(self.file_path, chunksize=self.chunk_size):
                chunks.append(chunk)
                bytes_read += len(chunk) * 100  # Approximate bytes
                progress_percent = min(int((bytes_read / file_size) * 100), 99)
                self.progress.emit(progress_percent)
            
            # Combine all chunks
            df = pd.concat(chunks, ignore_index=True)
            
            # Data preprocessing and optimization
            df = self.preprocess_data(df)
            
            self.progress.emit(100)
            self.finished.emit(df)
            
        except Exception as e:
            self.error.emit(str(e))
    
    def preprocess_data(self, df):
        """Preprocess and optimize the DataFrame"""
        # Convert timestamp columns
        timestamp_cols = [col for col in df.columns if any(x in col.lower() 
                         for x in ['created', 'modified', 'access', 'change'])]
        
        for col in timestamp_cols:
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            except:
                pass
        
        # Create full path column if not exists
        if 'FullPath' not in df.columns and 'ParentPath' in df.columns and 'FileName' in df.columns:
            df['FullPath'] = df['ParentPath'].astype(str) + '\\' + df['FileName'].astype(str)
        
        # Optimize data types
        for col in df.columns:
            if df[col].dtype == 'object' and col not in timestamp_cols:
                try:
                    # Try to convert to category for string columns with repeated values
                    if df[col].nunique() / len(df) < 0.5:  # Less than 50% unique values
                        df[col] = df[col].astype('category')
                except:
                    pass
        
        return df

class MFTAnalyzer(QMainWindow):
    def __init__(self, csv_file=None):
        super().__init__()
        self.df = pd.DataFrame()
        self.filtered_df = pd.DataFrame()
        self.db_path = None
        self.temp_db = None
        
        self.init_ui()
        self.setup_database()
        
        # If a CSV file is provided, load it automatically
        if csv_file:
            self.load_csv_file(csv_file)
        
    def load_csv_file(self, file_path):
        """Load a specific CSV file programmatically"""
        if not os.path.exists(file_path):
            QMessageBox.critical(self, "Error", f"File not found: {file_path}")
            return
        
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)
        self.status_bar.showMessage("Loading CSV file...")
        
        # Start loading in background thread
        self.loader = DataLoader(file_path)
        self.loader.progress.connect(self.progress_bar.setValue)
        self.loader.finished.connect(self.on_data_loaded)
        self.loader.error.connect(self.on_load_error)
        self.loader.start()
        
    def init_ui(self):
        self.setWindowTitle("MFT CSV Analyzer - Professional Edition")
        self.setGeometry(100, 100, 1600, 900)
        
        # Apply modern styling
        self.setStyleSheet("""
            QMainWindow {
                background-color: #f5f5f5;
            }
            QTabWidget::pane {
                border: 1px solid #c0c0c0;
                background-color: white;
            }
            QTabBar::tab {
                background-color: #e0e0e0;
                padding: 8px 16px;
                margin-right: 2px;
            }
            QTabBar::tab:selected {
                background-color: white;
                border-bottom: 2px solid #007acc;
            }
            QPushButton {
                background-color: #007acc;
                color: white;
                border: none;
                padding: 8px 16px;
                border-radius: 4px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #005a9e;
            }
            QPushButton:pressed {
                background-color: #004578;
            }
            QLineEdit, QComboBox {
                padding: 6px;
                border: 1px solid #c0c0c0;
                border-radius: 4px;
            }
            QGroupBox {
                font-weight: bold;
                border: 2px solid #c0c0c0;
                border-radius: 5px;
                margin-top: 10px;
                padding-top: 10px;
            }
        """)
        
        # Create central widget and main layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        
        # Create toolbar
        self.create_toolbar()
        
        # Create status bar
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.status_bar.addPermanentWidget(self.progress_bar)
        
        # Create tab widget
        self.tab_widget = QTabWidget()
        main_layout.addWidget(self.tab_widget)
        
        # Create tabs
        self.create_main_tab()
        self.create_search_tab()
        self.create_timeline_tab()
        self.create_deleted_files_tab()
        self.create_analysis_tab()
        self.create_export_tab()
    
    def create_toolbar(self):
        toolbar = QToolBar()
        self.addToolBar(toolbar)
        
        # File operations
        load_action = QAction("Load CSV", self)
        load_action.triggered.connect(self.load_csv)
        toolbar.addAction(load_action)
        
        toolbar.addSeparator()
        
        # View operations
        refresh_action = QAction("Refresh", self)
        refresh_action.triggered.connect(self.refresh_view)
        toolbar.addAction(refresh_action)
        
        clear_action = QAction("Clear Filters", self)
        clear_action.triggered.connect(self.clear_filters)
        toolbar.addAction(clear_action)
        
        toolbar.addSeparator()
        
        # Analysis operations
        analyze_action = QAction("Quick Analysis", self)
        analyze_action.triggered.connect(self.quick_analysis)
        toolbar.addAction(analyze_action)
    
    def create_main_tab(self):
        """Main data view tab"""
        main_tab = QWidget()
        self.tab_widget.addTab(main_tab, "Main View")
        layout = QVBoxLayout(main_tab)
        
        # Top controls
        controls_frame = QFrame()
        controls_layout = QHBoxLayout(controls_frame)
        
        self.load_btn = QPushButton("Load MFT CSV")
        self.load_btn.clicked.connect(self.load_csv)
        controls_layout.addWidget(self.load_btn)
        
        self.records_label = QLabel("Records: 0")
        controls_layout.addWidget(self.records_label)
        
        self.filtered_label = QLabel("Filtered: 0")
        controls_layout.addWidget(self.filtered_label)
        
        controls_layout.addStretch()
        
        layout.addWidget(controls_frame)
        
        # Create table with model
        self.table_model = MFTTableModel()
        self.table = QTableWidget()
        layout.addWidget(self.table)
        
        # Details panel
        self.details_text = QTextEdit()
        self.details_text.setMaximumHeight(150)
        layout.addWidget(self.details_text)
    
    def refresh_view(self):
        """Refresh the current view"""
        if hasattr(self, 'update_table_view'):
            self.update_table_view()
        if hasattr(self, 'update_search_results'):
            self.update_search_results()
        if hasattr(self, 'update_record_counts'):
            self.update_record_counts()
        if hasattr(self, 'status_bar'):
            self.status_bar.showMessage("View refreshed")

    def clear_filters(self):
        """Clear all active filters"""
    # Add this method to handle the clear filters toolbar action
        if hasattr(self, 'status_bar'):
            self.status_bar.showMessage("Clear filters functionality not implemented yet")

    def quick_analysis(self):
        """Run quick analysis and display in status bar"""
        # Add this method to handle the quick analysis toolbar action
        if hasattr(self, 'status_bar'):
            self.status_bar.showMessage("Quick analysis functionality not implemented yet")
    
    def create_search_tab(self):
        """Advanced search and filtering tab with improved layout"""
        search_tab = QWidget()
        self.tab_widget.addTab(search_tab, "Search & Filter")
        
        # Create main splitter for left (filters) and right (results)
        main_splitter = QSplitter(Qt.Horizontal)
        search_layout = QVBoxLayout(search_tab)
        search_layout.addWidget(main_splitter)
        
        # LEFT PANEL: FILTERS
        filter_panel = QWidget()
        filter_panel.setFixedWidth(400)
        filter_layout = QVBoxLayout(filter_panel)
        
        # Create scroll area for filters
        scroll = QScrollArea()
        scroll_widget = QWidget()
        scroll_layout = QVBoxLayout(scroll_widget)
        
        # Quick search
        quick_group = QGroupBox("Quick Search")
        quick_layout = QVBoxLayout(quick_group)
        
        self.quick_search = QLineEdit()
        self.quick_search.setPlaceholderText("Search filename, path, or content...")
        self.quick_search.textChanged.connect(self.apply_quick_filter)
        quick_layout.addWidget(QLabel("Search:"))
        quick_layout.addWidget(self.quick_search)
        
        scroll_layout.addWidget(quick_group)
        
        # File filters
        file_group = QGroupBox("File Filters")
        file_layout = QGridLayout(file_group)
        
        self.filename_filter = QLineEdit()
        self.filename_filter.setPlaceholderText("*.txt, *.exe, etc.")
        file_layout.addWidget(QLabel("Filename Pattern:"), 0, 0)
        file_layout.addWidget(self.filename_filter, 0, 1)
        
        self.extension_filter = QComboBox()
        self.extension_filter.setEditable(True)
        self.extension_filter.setPlaceholderText("Select extension")
        file_layout.addWidget(QLabel("Extension:"), 1, 0)
        file_layout.addWidget(self.extension_filter, 1, 1)
        
        self.path_filter = QLineEdit()
        self.path_filter.setPlaceholderText("Enter path pattern")
        file_layout.addWidget(QLabel("Path Contains:"), 2, 0)
        file_layout.addWidget(self.path_filter, 2, 1)
        
        scroll_layout.addWidget(file_group)
        
        # Size filters
        size_group = QGroupBox("Size Filters")
        size_layout = QGridLayout(size_group)
        
        self.size_min = QSpinBox()
        self.size_min.setMaximum(2147483647)
        self.size_min.setSuffix(" bytes")
        size_layout.addWidget(QLabel("Size Min:"), 0, 0)
        size_layout.addWidget(self.size_min, 0, 1)
        
        self.size_max = QSpinBox()
        self.size_max.setMaximum(2147483647)
        self.size_max.setValue(2147483647)
        self.size_max.setSuffix(" bytes")
        size_layout.addWidget(QLabel("Size Max:"), 1, 0)
        size_layout.addWidget(self.size_max, 1, 1)
        
        # Size units selector
        self.size_unit = QComboBox()
        self.size_unit.addItems(["Bytes", "KB", "MB", "GB"])
        self.size_unit.currentTextChanged.connect(self.update_size_units)
        size_layout.addWidget(QLabel("Unit:"), 2, 0)
        size_layout.addWidget(self.size_unit, 2, 1)
        
        scroll_layout.addWidget(size_group)
        
        # Date filters
        date_group = QGroupBox("Date Filters")
        date_layout = QGridLayout(date_group)
        
        self.date_column = QComboBox()
        date_layout.addWidget(QLabel("Date Column:"), 0, 0)
        date_layout.addWidget(self.date_column, 0, 1)
        
        self.date_from = QDateTimeEdit()
        self.date_from.setDateTime(QDateTime.currentDateTime().addDays(-365))
        self.date_from.setCalendarPopup(True)
        date_layout.addWidget(QLabel("Date From:"), 1, 0)
        date_layout.addWidget(self.date_from, 1, 1)
        
        self.date_to = QDateTimeEdit()
        self.date_to.setDateTime(QDateTime.currentDateTime())
        self.date_to.setCalendarPopup(True)
        date_layout.addWidget(QLabel("Date To:"), 2, 0)
        date_layout.addWidget(self.date_to, 2, 1)
        
        scroll_layout.addWidget(date_group)
        
        # Attribute filters
        attr_group = QGroupBox("Attribute Filters")
        attr_layout = QVBoxLayout(attr_group)
        
        self.is_directory_cb = QCheckBox("Directories Only")
        self.has_ads_cb = QCheckBox("Has Alternate Data Streams")
        self.is_ads_cb = QCheckBox("Is Alternate Data Stream")
        self.deleted_cb = QCheckBox("Deleted Files Only (InUse=False)")
        self.copied_cb = QCheckBox("Copied Files")
        self.si_fn_cb = QCheckBox("SI < FN (Timeline Anomaly)")
        
        attr_layout.addWidget(self.is_directory_cb)
        attr_layout.addWidget(self.has_ads_cb)
        attr_layout.addWidget(self.is_ads_cb)
        attr_layout.addWidget(self.deleted_cb)
        attr_layout.addWidget(self.copied_cb)
        attr_layout.addWidget(self.si_fn_cb)
        
        scroll_layout.addWidget(attr_group)
        
        # Filter buttons
        button_layout = QHBoxLayout()
        apply_btn = QPushButton("Apply Filters")
        apply_btn.clicked.connect(self.apply_advanced_filters)
        clear_btn = QPushButton("Clear All")
        clear_btn.clicked.connect(self.clear_filters)
        
        button_layout.addWidget(apply_btn)
        button_layout.addWidget(clear_btn)
        scroll_layout.addLayout(button_layout)
        
        # Filter summary
        self.filter_summary = QTextEdit()
        self.filter_summary.setMaximumHeight(80)
        self.filter_summary.setPlaceholderText("Applied filters will be summarized here...")
        scroll_layout.addWidget(QLabel("Active Filters:"))
        scroll_layout.addWidget(self.filter_summary)
        
        scroll_layout.addStretch()
        scroll.setWidget(scroll_widget)
        filter_layout.addWidget(scroll)
        
        # RIGHT PANEL: RESULTS
        results_panel = QWidget()
        results_layout = QVBoxLayout(results_panel)
        
        # Results header
        results_header = QHBoxLayout()
        self.results_count_label = QLabel("Results: 0")
        results_header.addWidget(self.results_count_label)
        results_header.addStretch()
        
        export_results_btn = QPushButton("Export Results")
        export_results_btn.clicked.connect(self.export_filtered_results)
        results_header.addWidget(export_results_btn)
        
        results_layout.addLayout(results_header)
        
        # Results table
        self.search_results_table = QTableWidget()
        self.search_results_table.setAlternatingRowColors(True)
        self.search_results_table.setSortingEnabled(True)
        results_layout.addWidget(self.search_results_table)
        
        # Results details
        self.results_details = QTextEdit()
        self.results_details.setMaximumHeight(120)
        self.results_details.setPlaceholderText("Click on a row to see detailed information...")
        results_layout.addWidget(self.results_details)
        
        # Add panels to splitter
        main_splitter.addWidget(filter_panel)
        main_splitter.addWidget(results_panel)
        
        # Set initial splitter sizes
        main_splitter.setSizes([400, 1000])
        
        # Connect table selection to show details
        self.search_results_table.cellClicked.connect(self.show_row_details)
    
    def create_timeline_tab(self):
        """Timeline analysis tab"""
        timeline_tab = QWidget()
        self.tab_widget.addTab(timeline_tab, "Timeline")
        layout = QVBoxLayout(timeline_tab)
        
        # Controls
        controls_layout = QHBoxLayout()
        
        generate_timeline_btn = QPushButton("Generate Timeline")
        generate_timeline_btn.clicked.connect(self.generate_timeline)
        controls_layout.addWidget(generate_timeline_btn)
        
        self.timeline_type = QComboBox()
        self.timeline_type.addItems(["Activity Overview", "File Creation", "File Modification", "File Access"])
        controls_layout.addWidget(self.timeline_type)
        
        controls_layout.addStretch()
        layout.addWidget(QFrame())  # Spacer
        
        # Timeline display area
        self.timeline_text = QTextEdit()
        self.timeline_text.setPlaceholderText("Timeline analysis will appear here...")
        layout.addWidget(self.timeline_text)
    
    def create_deleted_files_tab(self):
        """Deleted files analysis tab"""
        deleted_tab = QWidget()
        self.tab_widget.addTab(deleted_tab, "Deleted Files")
        layout = QVBoxLayout(deleted_tab)
        
        # Controls
        controls_layout = QHBoxLayout()
        
        scan_deleted_btn = QPushButton("Scan for Deleted Files")
        scan_deleted_btn.clicked.connect(self.scan_deleted_files)
        controls_layout.addWidget(scan_deleted_btn)
        
        self.deleted_count_label = QLabel("Deleted Files: 0")
        controls_layout.addWidget(self.deleted_count_label)
        
        controls_layout.addStretch()
        layout.addWidget(QFrame())  # Spacer
        
        # Deleted files table
        self.deleted_files_table = QTableWidget()
        layout.addWidget(self.deleted_files_table)
        
        # Recovery info
        recovery_info = QTextEdit()
        recovery_info.setMaximumHeight(100)
        recovery_info.setPlaceholderText("Recovery information and metadata will appear here...")
        layout.addWidget(recovery_info)
    
    def create_analysis_tab(self):
        """Statistical analysis tab"""
        analysis_tab = QWidget()
        self.tab_widget.addTab(analysis_tab, "Analysis")
        layout = QVBoxLayout(analysis_tab)
        
        # Analysis controls
        controls_layout = QHBoxLayout()
        
        run_analysis_btn = QPushButton("Run Full Analysis")
        run_analysis_btn.clicked.connect(self.run_full_analysis)
        controls_layout.addWidget(run_analysis_btn)
        
        self.analysis_type = QComboBox()
        self.analysis_type.addItems([
            "File Type Distribution",
            "Size Analysis", 
            "Timestamp Analysis",
            "Attribute Analysis",
            "Directory Structure"
        ])
        controls_layout.addWidget(self.analysis_type)
        
        export_chart_btn = QPushButton("Export Chart")
        export_chart_btn.clicked.connect(self.export_chart)
        controls_layout.addWidget(export_chart_btn)
        
        controls_layout.addStretch()
        layout.addWidget(QFrame())  # Spacer
        
        # Analysis results
        self.analysis_results = QTextEdit()
        layout.addWidget(self.analysis_results)
    
    def create_export_tab(self):
        """Export and reporting tab"""
        export_tab = QWidget()
        self.tab_widget.addTab(export_tab, "Export")
        layout = QVBoxLayout(export_tab)
        
        # Export options
        export_group = QGroupBox("Export Options")
        export_layout = QGridLayout(export_group)
        
        # Format selection
        self.export_format = QComboBox()
        self.export_format.addItems(["CSV", "JSON", "HTML Report", "Excel"])
        export_layout.addWidget(QLabel("Format:"), 0, 0)
        export_layout.addWidget(self.export_format, 0, 1)
        
        # Data selection
        self.export_data = QComboBox()
        self.export_data.addItems(["All Data", "Filtered Data", "Deleted Files Only", "Timeline Data"])
        export_layout.addWidget(QLabel("Data:"), 0, 2)
        export_layout.addWidget(self.export_data, 0, 3)
        
        # Export buttons
        export_btn = QPushButton("Export Data")
        export_btn.clicked.connect(self.export_data_func)
        export_layout.addWidget(export_btn, 1, 0, 1, 2)
        
        generate_report_btn = QPushButton("Generate Full Report")
        generate_report_btn.clicked.connect(self.generate_full_report)
        export_layout.addWidget(generate_report_btn, 1, 2, 1, 2)
        
        layout.addWidget(export_group)
        
        # Export preview
        preview_group = QGroupBox("Export Preview")
        preview_layout = QVBoxLayout(preview_group)
        
        self.export_preview = QTextEdit()
        self.export_preview.setPlaceholderText("Export preview will appear here...")
        preview_layout.addWidget(self.export_preview)
        
        layout.addWidget(preview_group)
    
    def setup_database(self):
        """Setup temporary SQLite database for fast queries"""
        self.temp_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.db_path = self.temp_db.name
        self.temp_db.close()
    
    def update_size_units(self):
        """Update size input units"""
        unit = self.size_unit.currentText()
        multiplier = {'Bytes': 1, 'KB': 1024, 'MB': 1024**2, 'GB': 1024**3}
        
        if unit in multiplier:
            suffix = f" {unit.lower()}"
            self.size_min.setSuffix(suffix)
            self.size_max.setSuffix(suffix)
    
    def load_csv(self):
        """Load CSV file with progress bar"""
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Load MFT CSV", "", "CSV Files (*.csv);;All Files (*)"
        )
        
        if file_path:
            self.load_csv_file(file_path)
    
    def on_data_loaded(self, df):
        """Handle successful data loading"""
        self.df = df
        self.filtered_df = df.copy()
        
        # Update UI
        self.update_table_view()
        self.update_record_counts()
        self.populate_filter_options()
        self.load_data_to_db()
        
        self.progress_bar.setVisible(False)
        self.status_bar.showMessage(f"Loaded {len(df)} records successfully")
        
        QMessageBox.information(self, "Success", f"Successfully loaded {len(df)} records from CSV file.")
    
    def on_load_error(self, error_msg):
        """Handle loading errors"""
        self.progress_bar.setVisible(False)
        self.status_bar.showMessage("Error loading file")
        QMessageBox.critical(self, "Error", f"Error loading CSV file:\n{error_msg}")
    
    def populate_filter_options(self):
        """Populate filter dropdown options based on loaded data"""
        if self.df.empty:
            return
        
        # Populate extension filter
        self.extension_filter.clear()
        self.extension_filter.addItem("")  # Add empty option
        if 'Extension' in self.df.columns:
            extensions = self.df['Extension'].dropna().unique()
            self.extension_filter.addItems(sorted(extensions))
        elif 'FileName' in self.df.columns:
            # Extract extensions from filename if Extension column doesn't exist
            extensions = self.df['FileName'].str.extract(r'\.([^.]+)$')[0].dropna().unique()
            self.extension_filter.addItems(sorted(extensions))
        
        # Populate date column filter
        self.date_column.clear()
        timestamp_cols = [col for col in self.df.columns if any(x in col.lower() 
                         for x in ['created', 'modified', 'access', 'change'])]
        if timestamp_cols:
            self.date_column.addItems(timestamp_cols)
    
    def load_data_to_db(self):
        """Load data to SQLite for fast queries"""
        try:
            conn = sqlite3.connect(self.db_path)
            self.df.to_sql('mft_records', conn, if_exists='replace', index=True)
            
            # Create indexes for common search fields
            cursor = conn.cursor()
            index_columns = ['FileName', 'ParentPath', 'EntryNumber']
            for col in index_columns:
                if col in self.df.columns:
                    try:
                        cursor.execute(f'CREATE INDEX idx_{col} ON mft_records({col})')
                    except:
                        pass  # Index might already exist
            
            conn.close()
        except Exception as e:
            print(f"Database error: {e}")
    
    def apply_quick_filter(self):
        """Apply quick search filter"""
        if self.df.empty:
            return
        
        search_text = self.quick_search.text().lower()
        if not search_text:
            self.filtered_df = self.df.copy()
        else:
            # Search in multiple columns
            mask = pd.Series([False] * len(self.df))
            search_columns = ['FileName', 'ParentPath', 'FullPath']
            
            for col in search_columns:
                if col in self.df.columns:
                    mask |= self.df[col].astype(str).str.lower().str.contains(search_text, na=False, regex=False)
            
            self.filtered_df = self.df[mask]
        
        self.update_search_results()
        self.update_record_counts()
        self.update_filter_summary()
    
    def apply_advanced_filters(self):
        """Apply advanced filters with improved logic"""
        if self.df.empty:
            return
        
        try:
            filtered = self.df.copy()
            active_filters = []
            
            # Quick search filter
            search_text = self.quick_search.text().strip()
            if search_text:
                mask = pd.Series([False] * len(filtered))
                search_columns = ['FileName', 'ParentPath', 'FullPath']
                
                for col in search_columns:
                    if col in filtered.columns:
                        mask |= filtered[col].astype(str).str.lower().str.contains(search_text.lower(), na=False, regex=False)
                
                filtered = filtered[mask]
                active_filters.append(f"Text search: '{search_text}'")
            
            # Filename pattern filter
            filename_pattern = self.filename_filter.text().strip()
            if filename_pattern and 'FileName' in filtered.columns:
                try:
                    # Convert wildcard pattern to regex
                    import re
                    pattern = filename_pattern.replace('*', '.*').replace('?', '.')
                    filtered = filtered[filtered['FileName'].astype(str).str.match(pattern, case=False, na=False)]
                    active_filters.append(f"Filename pattern: '{filename_pattern}'")
                except Exception as e:
                    QMessageBox.warning(self, "Filter Error", f"Invalid filename pattern: {str(e)}")
            
            # Extension filter
            ext_filter = self.extension_filter.currentText().strip()
            if ext_filter:
                if 'Extension' in filtered.columns:
                    filtered = filtered[filtered['Extension'].astype(str).str.lower() == ext_filter.lower()]
                elif 'FileName' in filtered.columns:
                    filtered = filtered[filtered['FileName'].astype(str).str.lower().str.endswith(f'.{ext_filter.lower()}', na=False)]
                active_filters.append(f"Extension: '.{ext_filter}'")
            
            # Path filter
            path_filter = self.path_filter.text().strip()
            if path_filter and 'ParentPath' in filtered.columns:
                filtered = filtered[filtered['ParentPath'].astype(str).str.lower().str.contains(path_filter.lower(), na=False, regex=False)]
                active_filters.append(f"Path contains: '{path_filter}'")
            
            # Size filters
            size_min = self.size_min.value()
            size_max = self.size_max.value()
            unit = self.size_unit.currentText()
            multiplier = {'Bytes': 1, 'KB': 1024, 'MB': 1024**2, 'GB': 1024**3}
            
            if 'FileSize' in filtered.columns:
                actual_min = size_min * multiplier.get(unit, 1)
                actual_max = size_max * multiplier.get(unit, 1)
                
                if size_min > 0:
                    filtered = filtered[filtered['FileSize'] >= actual_min]
                    active_filters.append(f"Size >= {size_min} {unit}")
                
                if size_max < 2147483647:
                    filtered = filtered[filtered['FileSize'] <= actual_max]
                    active_filters.append(f"Size <= {size_max} {unit}")
            
            # Date filters
            date_col = self.date_column.currentText()
            if date_col and date_col in filtered.columns:
                try:
                    date_from = self.date_from.dateTime().toPyDateTime()
                    date_to = self.date_to.dateTime().toPyDateTime()
                    
                    # Convert column to datetime if not already
                    filtered[date_col] = pd.to_datetime(filtered[date_col], errors='coerce')
                    
                    # Apply date range filter
                    date_mask = (filtered[date_col] >= date_from) & (filtered[date_col] <= date_to)
                    filtered = filtered[date_mask]
                    
                    active_filters.append(f"Date ({date_col}): {date_from.strftime('%Y-%m-%d')} to {date_to.strftime('%Y-%m-%d')}")
                except Exception as e:
                    QMessageBox.warning(self, "Date Filter Error", f"Error applying date filter: {str(e)}")
            
            # Attribute filters
            if self.is_directory_cb.isChecked() and 'IsDirectory' in filtered.columns:
                filtered = filtered[filtered['IsDirectory'] == True]
                active_filters.append("Directories only")
            
            if self.has_ads_cb.isChecked() and 'HasAds' in filtered.columns:
                filtered = filtered[filtered['HasAds'] == True]
                active_filters.append("Has ADS")
            
            if self.is_ads_cb.isChecked() and 'IsAds' in filtered.columns:
                filtered = filtered[filtered['IsAds'] == True]
                active_filters.append("Is ADS")
            
            if self.deleted_cb.isChecked() and 'InUse' in filtered.columns:
                filtered = filtered[filtered['InUse'] == False]
                active_filters.append("Deleted files (InUse=False)")
            
            if self.copied_cb.isChecked() and 'Copied' in filtered.columns:
                filtered = filtered[filtered['Copied'] == True]
                active_filters.append("Copied files")
            
            if self.si_fn_cb.isChecked() and 'SI<FN' in filtered.columns:
                filtered = filtered[filtered['SI<FN'] == True]
                active_filters.append("SI < FN anomaly")
            
            self.filtered_df = filtered
            self.update_search_results()
            self.update_record_counts()
            self.update_filter_summary(active_filters)
            
        except Exception as e:
            QMessageBox.critical(self, "Filter Error", f"Error applying filters: {str(e)}")
    
    def update_filter_summary(self, active_filters=None):
        """Update the filter summary display"""
        if active_filters is None:
            active_filters = []
        
        if not active_filters:
            self.filter_summary.setPlainText("No filters applied")
        else:
            summary = "Active filters:\n" + "\n".join(f"• {f}" for f in active_filters)
            self.filter_summary.setPlainText(summary)
    
    def update_search_results(self):
        """Update search results table with improved column selection"""
        if self.filtered_df.empty:
            self.search_results_table.setRowCount(0)
            self.results_count_label.setText("Results: 0")
            return
        
        # Select key columns to display
        key_columns = [
            'EntryNumber', 'FileName', 'ParentPath', 'FileSize', 
            'InUse', 'IsDirectory', 'Created0x10', 'LastModified0x10'
        ]
        available_columns = [col for col in key_columns if col in self.filtered_df.columns]
        
        if not available_columns:
            available_columns = list(self.filtered_df.columns[:8])  # First 8 columns
        
        display_data = self.filtered_df[available_columns]
        
        self.search_results_table.setRowCount(len(display_data))
        self.search_results_table.setColumnCount(len(available_columns))
        self.search_results_table.setHorizontalHeaderLabels(available_columns)
        
        # Populate search results (limit to 1000 for performance)
        display_rows = min(1000, len(display_data))
        for i in range(display_rows):
            for j, col in enumerate(available_columns):
                value = display_data.iloc[i, j]
                if pd.isna(value):
                    value = ""
                
                item = QTableWidgetItem(str(value))
                
                # Highlight deleted files
                if 'InUse' in display_data.columns and not display_data.iloc[i]['InUse']:
                    item.setBackground(QColor(255, 200, 200))
                
                self.search_results_table.setItem(i, j, item)
        
        # Auto-resize columns
        header = self.search_results_table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeToContents)
        
        # Update results count
        total_results = len(self.filtered_df)
        showing = min(display_rows, total_results)
        self.results_count_label.setText(f"Results: {total_results:,} (showing {showing:,})")
        
        if display_rows < len(self.filtered_df):
            self.status_bar.showMessage(f"Search results: showing first {display_rows:,} of {len(self.filtered_df):,} records")
    
    def show_row_details(self, row, column):
        """Show detailed information for selected row"""
        if row >= len(self.filtered_df):
            return
        
        selected_row = self.filtered_df.iloc[row]
        
        details = "<h4>Record Details</h4><table border='1'>"
        for col, value in selected_row.items():
            if pd.notna(value):
                details += f"<tr><td><b>{col}</b></td><td>{value}</td></tr>"
        details += "</table>"
        
        self.results_details.setHtml(details)
    
    def export_filtered_results(self):
        """Export current filtered results"""
        if self.filtered_df.empty:
            QMessageBox.warning(self, "Warning", "No filtered results to export.")
            return
        
        file_path, _ = QFileDialog.getSaveFileName(
            self, "Export Filtered Results", f"filtered_mft_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", 
            "CSV Files (*.csv);;Excel Files (*.xlsx);;JSON Files (*.json)"
        )
        
        if file_path:
            try:
                if file_path.endswith('.csv'):
                    self.filtered_df.to_csv(file_path, index=False)
                elif file_path.endswith('.xlsx'):
                    self.filtered_df.to_excel(file_path, index=False)
                elif file_path.endswith('.json'):
                    self.filtered_df.to_json(file_path, orient='records', date_format='iso')
                
                QMessageBox.information(self, "Success", f"Filtered results exported to:\n{file_path}")
            except Exception as e:
                QMessageBox.critical(self, "Export Error", f"Error exporting results: {str(e)}")
    
    def update_table_view(self):
        """Update the main table view"""
        if self.filtered_df.empty:
            return
        
        # Update main table
        self.table.setRowCount(len(self.filtered_df))
        self.table.setColumnCount(len(self.filtered_df.columns))
        self.table.setHorizontalHeaderLabels(list(self.filtered_df.columns))
        
        # Populate table (show first 1000 rows for performance)
        display_rows = min(1000, len(self.filtered_df))
        for i in range(display_rows):
            for j, col in enumerate(self.filtered_df.columns):
                value = self.filtered_df.iloc[i, j]
                if pd.isna(value):
                    value = ""
                item = QTableWidgetItem(str(value))
                
                # Highlight deleted files based on InUse flag
                if 'InUse' in self.filtered_df.columns and not self.filtered_df.iloc[i]['InUse']:
                    item.setBackground(QColor(255, 200, 200))
                
                self.table.setItem(i, j, item)
        
        # Auto-resize columns
        self.table.resizeColumnsToContents()
        
        if display_rows < len(self.filtered_df):
            self.status_bar.showMessage(f"Showing first {display_rows} of {len(self.filtered_df)} records")
    
    def update_record_counts(self):
        """Update record count labels"""
        self.records_label.setText(f"Records: {len(self.df):,}")
        self.filtered_label.setText(f"Filtered: {len(self.filtered_df):,}")
        
        if 'InUse' in self.df.columns:
            deleted_count = (~self.df['InUse']).sum()
            self.deleted_count_label.setText(f"Deleted Files: {deleted_count:,}")
    
    def scan_deleted_files(self):
        """Scan and display deleted files"""
        if self.df.empty:
            QMessageBox.warning(self, "Warning", "No data loaded.")
            return
        
        if 'InUse' not in self.df.columns:
            QMessageBox.information(self, "Info", "No InUse column found in data.")
            return
        
        deleted_files = self.df[self.df['InUse'] == False]
        
        if deleted_files.empty:
            QMessageBox.information(self, "Info", "No deleted files found.")
            return
        
        # Update deleted files table
        key_columns = ['EntryNumber', 'FileName', 'ParentPath', 'FileSize', 'Created0x10', 'LastModified0x10']
        available_columns = [col for col in key_columns if col in deleted_files.columns]
        
        self.deleted_files_table.setRowCount(len(deleted_files))
        self.deleted_files_table.setColumnCount(len(available_columns))
        self.deleted_files_table.setHorizontalHeaderLabels(available_columns)
        
        # Populate deleted files table
        for i in range(len(deleted_files)):
            for j, col in enumerate(available_columns):
                value = deleted_files.iloc[i][col]
                if pd.isna(value):
                    value = ""
                item = QTableWidgetItem(str(value))
                item.setBackground(QColor(255, 200, 200))  # Highlight deleted files
                self.deleted_files_table.setItem(i, j, item)
        
        self.deleted_files_table.resizeColumnsToContents()
        self.deleted_count_label.setText(f"Deleted Files: {len(deleted_files):,}")
    
    def generate_timeline(self):
        """Generate timeline analysis"""
        if self.df.empty:
            QMessageBox.warning(self, "Warning", "No data loaded.")
            return
        
        timeline_type = self.timeline_type.currentText()
        
        try:
            # Find timestamp columns
            timestamp_cols = [col for col in self.df.columns if any(x in col.lower() 
                             for x in ['created', 'modified', 'access', 'change'])]
            
            if not timestamp_cols:
                QMessageBox.warning(self, "Warning", "No timestamp columns found.")
                return
            
            timeline_html = self.create_timeline_visualization(timeline_type, timestamp_cols)
            
            # Save and open timeline
            timeline_file = tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False)
            timeline_file.write(timeline_html)
            timeline_file.close()
            
            webbrowser.open(f'file://{timeline_file.name}')
            
            # Update timeline text with summary
            summary = self.generate_timeline_summary(timestamp_cols)
            self.timeline_text.setHtml(summary)
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error generating timeline: {str(e)}")
    
    def create_timeline_visualization(self, timeline_type, timestamp_cols):
        """Create interactive timeline visualization using Plotly"""
        fig = make_subplots(
            rows=2, cols=1,
            subplot_titles=('File Activity Timeline', 'Activity Distribution'),
            specs=[[{"secondary_y": False}], [{"secondary_y": False}]]
        )
        
        # Process timestamp data
        for col in timestamp_cols[:3]:  # Limit to first 3 timestamp columns
            try:
                timestamps = pd.to_datetime(self.df[col], errors='coerce').dropna()
                if not timestamps.empty:
                    # Create histogram of activity over time
                    hist_data = timestamps.dt.date.value_counts().sort_index()
                    
                    fig.add_trace(
                        go.Scatter(
                            x=hist_data.index,
                            y=hist_data.values,
                            mode='lines+markers',
                            name=col,
                            line=dict(width=2)
                        ),
                        row=1, col=1
                    )
            except Exception as e:
                print(f"Error processing {col}: {e}")
        
        # Activity distribution (hourly)
        if timestamp_cols:
            try:
                main_timestamp = pd.to_datetime(self.df[timestamp_cols[0]], errors='coerce').dropna()
                if not main_timestamp.empty:
                    hourly_activity = main_timestamp.dt.hour.value_counts().sort_index()
                    
                    fig.add_trace(
                        go.Bar(
                            x=hourly_activity.index,
                            y=hourly_activity.values,
                            name='Hourly Activity',
                            marker_color='lightblue'
                        ),
                        row=2, col=1
                    )
            except Exception as e:
                print(f"Error creating hourly distribution: {e}")
        
        fig.update_layout(
            title=f"MFT Timeline Analysis - {timeline_type}",
            height=800,
            showlegend=True,
            template="plotly_white"
        )
        
        fig.update_xaxes(title_text="Date", row=1, col=1)
        fig.update_yaxes(title_text="File Count", row=1, col=1)
        fig.update_xaxes(title_text="Hour of Day", row=2, col=1)
        fig.update_yaxes(title_text="Activity Count", row=2, col=1)
        
        return fig.to_html(include_plotlyjs='cdn')
    
    def generate_timeline_summary(self, timestamp_cols):
        """Generate timeline summary text"""
        summary = "<h3>Timeline Analysis Summary</h3>"
        
        for col in timestamp_cols:
            try:
                timestamps = pd.to_datetime(self.df[col], errors='coerce').dropna()
                if not timestamps.empty:
                    earliest = timestamps.min()
                    latest = timestamps.max()
                    total_days = (latest - earliest).days
                    
                    summary += f"""
                    <h4>{col}</h4>
                    <ul>
                        <li>Earliest: {earliest.strftime('%Y-%m-%d %H:%M:%S')}</li>
                        <li>Latest: {latest.strftime('%Y-%m-%d %H:%M:%S')}</li>
                        <li>Time Span: {total_days} days</li>
                        <li>Total Records: {len(timestamps):,}</li>
                    </ul>
                    """
            except Exception as e:
                summary += f"<p>Error processing {col}: {str(e)}</p>"
        
        return summary
    
    def run_full_analysis(self):
        """Run comprehensive analysis"""
        if self.df.empty:
            QMessageBox.warning(self, "Warning", "No data loaded.")
            return
        
        analysis_type = self.analysis_type.currentText()
        
        try:
            if analysis_type == "File Type Distribution":
                result = self.analyze_file_types()
            elif analysis_type == "Size Analysis":
                result = self.analyze_file_sizes()
            elif analysis_type == "Timestamp Analysis":
                result = self.analyze_timestamps()
            elif analysis_type == "Attribute Analysis":
                result = self.analyze_attributes()
            elif analysis_type == "Directory Structure":
                result = self.analyze_directory_structure()
            else:
                result = "Analysis type not implemented yet."
            
            self.analysis_results.setHtml(result)
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Analysis error: {str(e)}")
    
    def analyze_file_types(self):
        """Analyze file type distribution"""
        result = "<h3>File Type Distribution (Top 20)</h3>"
        
        if 'Extension' in self.df.columns:
            extensions = self.df['Extension'].fillna('No Extension')
        elif 'FileName' in self.df.columns:
            extensions = self.df['FileName'].str.extract(r'\.([^.]+)')[0].fillna('No Extension')
        else:
            return "<p>No filename or extension data found.</p>"
        
        ext_counts = extensions.value_counts().head(20)
        
        result += "<table border='1'>"
        result += "<tr><th>Extension</th><th>Count</th><th>Percentage</th></tr>"
        
        total_files = len(self.df)
        for ext, count in ext_counts.items():
            percentage = (count / total_files) * 100
            ext_display = ext if ext != 'No Extension' else 'No Extension'
            if ext != 'No Extension':
                ext_display = f".{ext}"
            result += f"<tr><td>{ext_display}</td><td>{count:,}</td><td>{percentage:.2f}%</td></tr>"
        
        result += "</table>"
        return result
    
    def analyze_file_sizes(self):
        """Analyze file size distribution"""
        if 'FileSize' not in self.df.columns:
            return "<p>FileSize column not found.</p>"
        
        sizes = self.df['FileSize'].dropna()
        
        result = "<h3>File Size Analysis</h3>"
        result += f"""
        <table border='1'>
            <tr><th>Metric</th><th>Value</th></tr>
            <tr><td>Total Files</td><td>{len(sizes):,}</td></tr>
            <tr><td>Total Size</td><td>{sizes.sum():,} bytes ({sizes.sum() / (1024**3):.2f} GB)</td></tr>
            <tr><td>Average Size</td><td>{sizes.mean():.0f} bytes</td></tr>
            <tr><td>Median Size</td><td>{sizes.median():.0f} bytes</td></tr>
            <tr><td>Largest File</td><td>{sizes.max():,} bytes</td></tr>
            <tr><td>Smallest File</td><td>{sizes.min():,} bytes</td></tr>
        </table>
        """
        
        return result
    
    def analyze_timestamps(self):
        """Analyze timestamp patterns"""
        timestamp_cols = [col for col in self.df.columns if any(x in col.lower() 
                         for x in ['created', 'modified', 'access', 'change'])]
        
        if not timestamp_cols:
            return "<p>No timestamp columns found.</p>"
        
        result = "<h3>Timestamp Analysis</h3>"
        
        for col in timestamp_cols:
            try:
                timestamps = pd.to_datetime(self.df[col], errors='coerce').dropna()
                if not timestamps.empty:
                    result += f"""
                    <h4>{col}</h4>
                    <ul>
                        <li>Valid Timestamps: {len(timestamps):,}</li>
                        <li>Date Range: {timestamps.min().strftime('%Y-%m-%d')} to {timestamps.max().strftime('%Y-%m-%d')}</li>
                        <li>Most Active Day: {timestamps.dt.date.value_counts().index[0]}</li>
                        <li>Most Active Hour: {timestamps.dt.hour.value_counts().index[0]}:00</li>
                    </ul>
                    """
            except Exception as e:
                result += f"<p>Error analyzing {col}: {str(e)}</p>"
        
        return result
    
    def analyze_attributes(self):
        """Analyze file attributes"""
        result = "<h3>File Attributes Analysis</h3>"
        result += "<table border='1'><tr><th>Attribute</th><th>Count</th><th>Percentage</th></tr>"
        
        total_files = len(self.df)
        
        # Check various boolean attributes
        bool_attrs = ['InUse', 'IsDirectory', 'HasAds', 'IsAds', 'Copied', 'SI<FN', 'uSecZeros']
        
        for attr in bool_attrs:
            if attr in self.df.columns:
                try:
                    if self.df[attr].dtype == 'bool':
                        count = self.df[attr].sum()
                        percentage = (count / total_files) * 100
                        result += f"<tr><td>{attr}</td><td>{count:,}</td><td>{percentage:.2f}%</td></tr>"
                except:
                    result += f"<tr><td>{attr}</td><td>Error</td><td>-</td></tr>"
        
        result += "</table>"
        
        # Deleted files analysis
        if 'InUse' in self.df.columns:
            deleted_count = (~self.df['InUse']).sum()
            deleted_percentage = (deleted_count / total_files) * 100
            result += f"""
            <h4>Deletion Analysis</h4>
            <ul>
                <li>Deleted Files (InUse=False): {deleted_count:,} ({deleted_percentage:.2f}%)</li>
                <li>Active Files (InUse=True): {total_files - deleted_count:,} ({100 - deleted_percentage:.2f}%)</li>
            </ul>
            """
        
        return result
    
    def analyze_directory_structure(self):
        """Analyze directory structure"""
        if 'ParentPath' not in self.df.columns:
            return "<p>ParentPath column not found.</p>"
        
        # Count files per directory
        dir_counts = self.df['ParentPath'].value_counts().head(20)
        
        result = "<h3>Directory Structure Analysis</h3>"
        result += "<h4>Top 20 Directories by File Count</h4>"
        result += "<table border='1'><tr><th>Directory</th><th>File Count</th></tr>"
        
        for directory, count in dir_counts.items():
            result += f"<tr><td>{directory}</td><td>{count:,}</td></tr>"
        
        result += "</table>"
        
        return result
    
    def export_chart(self):
        """Export current analysis chart"""
        QMessageBox.information(self, "Export", "Chart export functionality would be implemented here.")
    
    def export_data_func(self):
        """Export data in selected format"""
        if self.df.empty:
            QMessageBox.warning(self, "Warning", "No data to export.")
            return
        
        export_format = self.export_format.currentText()
        data_type = self.export_data.currentText()
        
        # Select data to export
        if data_type == "All Data":
            export_df = self.df
        elif data_type == "Filtered Data":
            export_df = self.filtered_df
        elif data_type == "Deleted Files Only" and 'InUse' in self.df.columns:
            export_df = self.df[self.df['InUse'] == False]
        else:
            export_df = self.df
        
        # Get export file path
        if export_format == "CSV":
            file_path, _ = QFileDialog.getSaveFileName(self, "Export CSV", "", "CSV Files (*.csv)")
            if file_path:
                export_df.to_csv(file_path, index=False)
        
        elif export_format == "JSON":
            file_path, _ = QFileDialog.getSaveFileName(self, "Export JSON", "", "JSON Files (*.json)")
            if file_path:
                export_df.to_json(file_path, orient='records', date_format='iso')
        
        elif export_format == "Excel":
            file_path, _ = QFileDialog.getSaveFileName(self, "Export Excel", "", "Excel Files (*.xlsx)")
            if file_path:
                export_df.to_excel(file_path, index=False)
        
        elif export_format == "HTML Report":
            file_path, _ = QFileDialog.getSaveFileName(self, "Export HTML", "", "HTML Files (*.html)")
            if file_path:
                html_content = self.generate_html_report(export_df)
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(html_content)
        
        if 'file_path' in locals() and file_path:
            QMessageBox.information(self, "Success", f"Data exported successfully to:\n{file_path}")
    
    def generate_html_report(self, df):
        """Generate comprehensive HTML report"""
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>MFT Analysis Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
                .summary {{ background-color: #f9f9f9; padding: 15px; border-radius: 5px; margin: 20px 0; }}
                .deleted {{ background-color: #ffcccc; }}
            </style>
        </head>
        <body>
            <h1>MFT Analysis Report</h1>
            <div class="summary">
                <h2>Summary</h2>
                <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p>Total Records: {len(df):,}</p>
                <p>Deleted Files: {(~df['InUse']).sum() if 'InUse' in df.columns else 'N/A'}</p>
            </div>
            
            <h2>Data Sample (First 100 Records)</h2>
            {df.head(100).to_html(classes='data-table', table_id='data_table', escape=False)}
        </body>
        </html>
        """
        return html
    
    def generate_full_report(self):
        """Generate comprehensive analysis report"""
        if self.df.empty:
            QMessageBox.warning(self, "Warning", "No data loaded.")
            return
        
        # Generate all analyses
        file_types = self.analyze_file_types()
        sizes = self.analyze_file_sizes()
        timestamps = self.analyze_timestamps()
        attributes = self.analyze_attributes()
        directory = self.analyze_directory_structure()