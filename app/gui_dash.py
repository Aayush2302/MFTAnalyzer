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
            # Highlight deleted files
            if 'IsDeleted' in self._headers:
                deleted_col = self._headers.index('IsDeleted')
                if self._data.iloc[index.row(), deleted_col]:
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
                         for x in ['created', 'modified', 'accessed', 'time'])]
        
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
            if df[col].dtype == 'object':
                try:
                    # Try to convert to category for string columns with repeated values
                    if df[col].nunique() / len(df) < 0.5:  # Less than 50% unique values
                        df[col] = df[col].astype('category')
                except:
                    pass
        
        return df

class MFTAnalyzer(QMainWindow):
    def __init__(self):
        super().__init__()
        self.df = pd.DataFrame()
        self.filtered_df = pd.DataFrame()
        self.db_path = None
        self.temp_db = None
        
        self.init_ui()
        self.setup_database()
        
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
        load_action = QAction("📁 Load CSV", self)
        load_action.triggered.connect(self.load_csv)
        toolbar.addAction(load_action)
        
        toolbar.addSeparator()
        
        # View operations
        refresh_action = QAction("🔄 Refresh", self)
        refresh_action.triggered.connect(self.refresh_view)
        toolbar.addAction(refresh_action)
        
        clear_action = QAction("🗑️ Clear Filters", self)
        clear_action.triggered.connect(self.clear_filters)
        toolbar.addAction(clear_action)
        
        toolbar.addSeparator()
        
        # Analysis operations
        analyze_action = QAction("📊 Quick Analysis", self)
        analyze_action.triggered.connect(self.quick_analysis)
        toolbar.addAction(analyze_action)
    
    def create_main_tab(self):
        """Main data view tab"""
        main_tab = QWidget()
        self.tab_widget.addTab(main_tab, "📋 Main View")
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
    
    def create_search_tab(self):
        """Advanced search and filtering tab"""
        search_tab = QWidget()
        self.tab_widget.addTab(search_tab, "🔍 Search & Filter")
        layout = QVBoxLayout(search_tab)
        
        # Create search controls in a scroll area
        scroll = QScrollArea()
        scroll_widget = QWidget()
        scroll_layout = QVBoxLayout(scroll_widget)
        
        # Quick search
        quick_group = QGroupBox("Quick Search")
        quick_layout = QGridLayout(quick_group)
        
        self.quick_search = QLineEdit()
        self.quick_search.setPlaceholderText("Search filename, path, or content...")
        self.quick_search.textChanged.connect(self.apply_quick_filter)
        quick_layout.addWidget(QLabel("Search:"), 0, 0)
        quick_layout.addWidget(self.quick_search, 0, 1, 1, 3)
        
        scroll_layout.addWidget(quick_group)
        
        # Advanced filters
        advanced_group = QGroupBox("Advanced Filters")
        advanced_layout = QGridLayout(advanced_group)
        
        # File filters
        self.filename_filter = QLineEdit()
        self.filename_filter.setPlaceholderText("*.txt, *.exe, etc.")
        advanced_layout.addWidget(QLabel("Filename:"), 0, 0)
        advanced_layout.addWidget(self.filename_filter, 0, 1)
        
        self.extension_filter = QComboBox()
        self.extension_filter.setEditable(True)
        self.extension_filter.setPlaceholderText("File extension")
        advanced_layout.addWidget(QLabel("Extension:"), 0, 2)
        advanced_layout.addWidget(self.extension_filter, 0, 3)
        
        # Size filters
        self.size_min = QSpinBox()
        self.size_min.setMaximum(2147483647)
        self.size_min.setSuffix(" bytes")
        advanced_layout.addWidget(QLabel("Size Min:"), 1, 0)
        advanced_layout.addWidget(self.size_min, 1, 1)
        
        self.size_max = QSpinBox()
        self.size_max.setMaximum(2147483647)
        self.size_max.setValue(2147483647)
        self.size_max.setSuffix(" bytes")
        advanced_layout.addWidget(QLabel("Size Max:"), 1, 2)
        advanced_layout.addWidget(self.size_max, 1, 3)
        
        # Date filters
        self.date_from = QDateTimeEdit()
        self.date_from.setDateTime(QDateTime.currentDateTime().addDays(-365))
        advanced_layout.addWidget(QLabel("Date From:"), 2, 0)
        advanced_layout.addWidget(self.date_from, 2, 1)
        
        self.date_to = QDateTimeEdit()
        self.date_to.setDateTime(QDateTime.currentDateTime())
        advanced_layout.addWidget(QLabel("Date To:"), 2, 2)
        advanced_layout.addWidget(self.date_to, 2, 3)
        
        # Attribute filters
        attr_layout = QHBoxLayout()
        self.hidden_cb = QCheckBox("Hidden")
        self.system_cb = QCheckBox("System")
        self.readonly_cb = QCheckBox("Read-only")
        self.deleted_cb = QCheckBox("Deleted Only")
        
        attr_layout.addWidget(self.hidden_cb)
        attr_layout.addWidget(self.system_cb)
        attr_layout.addWidget(self.readonly_cb)
        attr_layout.addWidget(self.deleted_cb)
        attr_layout.addStretch()
        
        advanced_layout.addLayout(attr_layout, 3, 0, 1, 4)
        
        # Apply filters button
        apply_btn = QPushButton("Apply Filters")
        apply_btn.clicked.connect(self.apply_advanced_filters)
        advanced_layout.addWidget(apply_btn, 4, 0, 1, 4)
        
        scroll_layout.addWidget(advanced_group)
        
        # Results table
        results_group = QGroupBox("Search Results")
        results_layout = QVBoxLayout(results_group)
        
        self.search_results_table = QTableWidget()
        results_layout.addWidget(self.search_results_table)
        
        scroll_layout.addWidget(results_group)
        
        scroll.setWidget(scroll_widget)
        layout.addWidget(scroll)
    
    def create_timeline_tab(self):
        """Timeline analysis tab"""
        timeline_tab = QWidget()
        self.tab_widget.addTab(timeline_tab, "📈 Timeline")
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
        self.tab_widget.addTab(deleted_tab, "🗑️ Deleted Files")
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
        self.tab_widget.addTab(analysis_tab, "📊 Analysis")
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
        self.tab_widget.addTab(export_tab, "📤 Export")
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
    
    def load_csv(self):
        """Load CSV file with progress bar"""
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Load MFT CSV", "", "CSV Files (*.csv);;All Files (*)"
        )
        
        if file_path:
            self.progress_bar.setVisible(True)
            self.progress_bar.setValue(0)
            self.status_bar.showMessage("Loading CSV file...")
            
            # Start loading in background thread
            self.loader = DataLoader(file_path)
            self.loader.progress.connect(self.progress_bar.setValue)
            self.loader.finished.connect(self.on_data_loaded)
            self.loader.error.connect(self.on_load_error)
            self.loader.start()
    
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
                
                # Highlight deleted files
                if 'IsDeleted' in self.filtered_df.columns and self.filtered_df.iloc[i]['IsDeleted']:
                    item.setBackground(QColor(255, 200, 200))
                
                self.table.setItem(i, j, item)
        
        # Auto-resize columns
        self.table.resizeColumnsToContents()
        
        if display_rows < len(self.filtered_df):
            self.status_bar.showMessage(f"Showing first {display_rows} of {len(self.filtered_df)} records")
    
    def update_record_counts(self):
        """Update record count labels"""
        self.records_label.setText(f"Records: {len(self.df)}")
        self.filtered_label.setText(f"Filtered: {len(self.filtered_df)}")
        
        if 'IsDeleted' in self.df.columns:
            deleted_count = self.df['IsDeleted'].sum()
            self.deleted_count_label.setText(f"Deleted Files: {deleted_count}")
    
    def populate_filter_options(self):
        """Populate filter dropdown options based on loaded data"""
        if self.df.empty:
            return
        
        # Populate extension filter
        self.extension_filter.clear()
        if 'FileName' in self.df.columns:
            extensions = self.df['FileName'].str.extract(r'\.([^.]+)$')[0].dropna().unique()
            self.extension_filter.addItems(sorted(extensions))
    
    def load_data_to_db(self):
        """Load data to SQLite for fast queries"""
        try:
            conn = sqlite3.connect(self.db_path)
            self.df.to_sql('mft_records', conn, if_exists='replace', index=True)
            
            # Create indexes for common search fields
            cursor = conn.cursor()
            index_columns = ['FileName', 'ParentPath', 'RecordNumber']
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
                    mask |= self.df[col].astype(str).str.lower().str.contains(search_text, na=False)
            
            self.filtered_df = self.df[mask]
        
        self.update_table_view()
        self.update_record_counts()
    
    def apply_advanced_filters(self):
        """Apply advanced filters"""
        if self.df.empty:
            return
        
        filtered = self.df.copy()
        
        # Filename filter
        filename_filter = self.filename_filter.text()
        if filename_filter:
            pattern = filename_filter.replace('*', '.*').replace('?', '.')
            if 'FileName' in filtered.columns:
                filtered = filtered[filtered['FileName'].str.match(pattern, case=False, na=False)]
        
        # Extension filter
        ext_filter = self.extension_filter.currentText()
        if ext_filter and 'FileName' in filtered.columns:
            filtered = filtered[filtered['FileName'].str.endswith(f'.{ext_filter}', na=False)]
        
        # Size filters
        if 'Size' in filtered.columns:
            size_min = self.size_min.value()
            size_max = self.size_max.value()
            filtered = filtered[(filtered['Size'] >= size_min) & (filtered['Size'] <= size_max)]
        
        # Attribute filters
        if self.hidden_cb.isChecked() and 'Hidden' in filtered.columns:
            filtered = filtered[filtered['Hidden'] == True]
        
        if self.system_cb.isChecked() and 'System' in filtered.columns:
            filtered = filtered[filtered['System'] == True]
        
        if self.deleted_cb.isChecked() and 'IsDeleted' in filtered.columns:
            filtered = filtered[filtered['IsDeleted'] == True]
        
        self.filtered_df = filtered
        self.update_table_view()
        self.update_search_results()
        self.update_record_counts()
    
    def update_search_results(self):
        """Update search results table"""
        if self.filtered_df.empty:
            self.search_results_table.setRowCount(0)
            return
        
        # Show key columns in search results
        key_columns = ['FileName', 'FullPath', 'Size', 'IsDeleted']
        available_columns = [col for col in key_columns if col in self.filtered_df.columns]
        
        if not available_columns:
            available_columns = list(self.filtered_df.columns[:5])  # First 5 columns
        
        display_data = self.filtered_df[available_columns]
        
        self.search_results_table.setRowCount(len(display_data))
        self.search_results_table.setColumnCount(len(available_columns))
        self.search_results_table.setHorizontalHeaderLabels(available_columns)
        
        # Populate search results (limit to 500 for performance)
        display_rows = min(500, len(display_data))
        for i in range(display_rows):
            for j, col in enumerate(available_columns):
                value = display_data.iloc[i, j]
                if pd.isna(value):
                    value = ""
                self.search_results_table.setItem(i, j, QTableWidgetItem(str(value)))
        
        self.search_results_table.resizeColumnsToContents()
    
    def scan_deleted_files(self):
        """Scan and display deleted files"""
        if self.df.empty:
            QMessageBox.warning(self, "Warning", "No data loaded.")
            return
        
        if 'IsDeleted' not in self.df.columns:
            QMessageBox.information(self, "Info", "No deletion status column found in data.")
            return
        
        deleted_files = self.df[self.df['IsDeleted'] == True]
        
        if deleted_files.empty:
            QMessageBox.information(self, "Info", "No deleted files found.")
            return
        
        # Update deleted files table
        key_columns = ['FileName', 'ParentPath', 'Size', 'Created0x10', 'Modified0x10']
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
        self.deleted_count_label.setText(f"Deleted Files: {len(deleted_files)}")
    
    def generate_timeline(self):
        """Generate timeline analysis"""
        if self.df.empty:
            QMessageBox.warning(self, "Warning", "No data loaded.")
            return
        
        timeline_type = self.timeline_type.currentText()
        
        try:
            # Find timestamp columns
            timestamp_cols = [col for col in self.df.columns if any(x in col.lower() 
                             for x in ['created', 'modified', 'accessed', 'time'])]
            
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
        if 'FileName' not in self.df.columns:
            return "<p>FileName column not found.</p>"
        
        # Extract file extensions
        extensions = self.df['FileName'].str.extract(r'\.([^.]+)')[0].fillna('No Extension')
        ext_counts = extensions.value_counts().head(20)
        
        result = "<h3>File Type Distribution (Top 20)</h3><table border='1'>"
        result += "<tr><th>Extension</th><th>Count</th><th>Percentage</th></tr>"
        
        total_files = len(self.df)
        for ext, count in ext_counts.items():
            percentage = (count / total_files) * 100
            result += f"<tr><td>.{ext}</td><td>{count:,}</td><td>{percentage:.2f}%</td></tr>"
        
        result += "</table>"
        
        # Create visualization
        fig = px.pie(
            values=ext_counts.values,
            names=[f'.{ext}' for ext in ext_counts.index],
            title="File Type Distribution"
        )
        
        chart_file = tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False)
        chart_file.write(fig.to_html(include_plotlyjs='cdn'))
        chart_file.close()
        
        result += f"<p><a href='file://{chart_file.name}' target='_blank'>View Interactive Chart</a></p>"
        
        return result
    
    def analyze_file_sizes(self):
        """Analyze file size distribution"""
        if 'Size' not in self.df.columns:
            return "<p>Size column not found.</p>"
        
        sizes = self.df['Size'].dropna()
        
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
        
        # Size distribution categories
        size_categories = pd.cut(sizes, 
                                bins=[0, 1024, 1024**2, 1024**3, float('inf')],
                                labels=['< 1KB', '1KB - 1MB', '1MB - 1GB', '> 1GB'])
        size_dist = size_categories.value_counts()
        
        result += "<h4>Size Distribution</h4><table border='1'>"
        result += "<tr><th>Size Range</th><th>Count</th><th>Percentage</th></tr>"
        
        for category, count in size_dist.items():
            percentage = (count / len(sizes)) * 100
            result += f"<tr><td>{category}</td><td>{count:,}</td><td>{percentage:.2f}%</td></tr>"
        
        result += "</table>"
        
        return result
    
    def analyze_timestamps(self):
        """Analyze timestamp patterns"""
        timestamp_cols = [col for col in self.df.columns if any(x in col.lower() 
                         for x in ['created', 'modified', 'accessed', 'time'])]
        
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
        
        # Timestomp detection (compare SI vs FN timestamps)
        si_created = 'SI_Created' if 'SI_Created' in self.df.columns else None
        fn_created = 'FN_Created' if 'FN_Created' in self.df.columns else None
        
        if si_created and fn_created:
            si_times = pd.to_datetime(self.df[si_created], errors='coerce')
            fn_times = pd.to_datetime(self.df[fn_created], errors='coerce')
            
            mismatches = ((si_times != fn_times) & si_times.notna() & fn_times.notna()).sum()
            total_comparable = (si_times.notna() & fn_times.notna()).sum()
            
            if total_comparable > 0:
                mismatch_rate = (mismatches / total_comparable) * 100
                result += f"""
                <h4>Timestamp Anomaly Detection</h4>
                <ul>
                    <li>SI vs FN Creation Time Mismatches: {mismatches:,} ({mismatch_rate:.2f}%)</li>
                    <li>Potential Timestomping Indicators: {mismatches:,} files</li>
                </ul>
                """
        
        return result
    
    def analyze_attributes(self):
        """Analyze file attributes"""
        attr_columns = [col for col in self.df.columns if col.lower() in 
                       ['hidden', 'system', 'readonly', 'archive', 'compressed', 'encrypted']]
        
        if not attr_columns:
            return "<p>No attribute columns found.</p>"
        
        result = "<h3>File Attributes Analysis</h3>"
        result += "<table border='1'><tr><th>Attribute</th><th>Count</th><th>Percentage</th></tr>"
        
        total_files = len(self.df)
        for col in attr_columns:
            try:
                count = self.df[col].sum() if self.df[col].dtype == 'bool' else len(self.df[self.df[col] == True])
                percentage = (count / total_files) * 100
                result += f"<tr><td>{col}</td><td>{count:,}</td><td>{percentage:.2f}%</td></tr>"
            except:
                result += f"<tr><td>{col}</td><td>Error</td><td>-</td></tr>"
        
        result += "</table>"
        
        # Deleted files analysis
        if 'IsDeleted' in self.df.columns:
            deleted_count = self.df['IsDeleted'].sum()
            deleted_percentage = (deleted_count / total_files) * 100
            result += f"""
            <h4>Deletion Analysis</h4>
            <ul>
                <li>Deleted Files: {deleted_count:,} ({deleted_percentage:.2f}%)</li>
                <li>Active Files: {total_files - deleted_count:,} ({100 - deleted_percentage:.2f}%)</li>
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
        
        # Directory depth analysis
        depths = self.df['ParentPath'].str.count('\\')
        result += f"""
        <h4>Directory Depth Statistics</h4>
        <ul>
            <li>Maximum Depth: {depths.max()} levels</li>
            <li>Average Depth: {depths.mean():.1f} levels</li>
            <li>Most Common Depth: {depths.mode().iloc[0] if not depths.mode().empty else 'N/A'} levels</li>
        </ul>
        """
        
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
        elif data_type == "Deleted Files Only" and 'IsDeleted' in self.df.columns:
            export_df = self.df[self.df['IsDeleted'] == True]
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
                <p>Deleted Files: {df['IsDeleted'].sum() if 'IsDeleted' in df.columns else 'N/A'}</p>
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
        
        # Combine into full report
        full_report = f"""
        <html>
        <head>
            <title>Complete MFT Analysis Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
                .section {{ margin: 30px 0; border-bottom: 2px solid #007acc; }}
            </style>
        </head>
        <body>
            <h1>Complete MFT Analysis Report</h1>
            <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p>Total Records Analyzed: {len(self.df):,}</p>
            
            <div class="section">{file_types}</div>
            <div class="section">{sizes}</div>
            <div class="section">{timestamps}</div>
            <div class="section">{attributes}</div>
            <div class="section">{directory}</div>
        </body>
        </html>
        """
        
        # Save and show report
        report_file = tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False)
        report_file.write(full_report)
        report_file.close()
        
        webbrowser.open(f'file://{report_file.name}')
        
        # Update export preview
        self.export_preview.setHtml(f"<p>Full report generated and opened in browser.</p><p>File: {report_file.name}</p>")
    
    def refresh_view(self):
        """Refresh the current view"""
        self.update_table_view()
        self.update_record_counts()
    
    def clear_filters(self):
        """Clear all filters and show all data"""
        if not self.df.empty:
            self.filtered_df = self.df.copy()
            
            # Clear filter controls
            self.quick_search.clear()
            self.filename_filter.clear()
            self.extension_filter.setCurrentText("")
            self.size_min.setValue(0)
            self.size_max.setValue(2147483647)
            self.hidden_cb.setChecked(False)
            self.system_cb.setChecked(False)
            self.readonly_cb.setChecked(False)
            self.deleted_cb.setChecked(False)
            
            self.update_table_view()
            self.update_record_counts()
    
    def quick_analysis(self):
        """Perform quick analysis and show summary"""
        if self.df.empty:
            QMessageBox.warning(self, "Warning", "No data loaded.")
            return
        
        summary = f"""
        Quick Analysis Summary:
        
        Total Records: {len(self.df):,}
        Total Size: {self.df['Size'].sum() / (1024**3):.2f} GB (if Size column exists)
        Deleted Files: {self.df['IsDeleted'].sum() if 'IsDeleted' in self.df.columns else 'N/A'}
        
        Most Common Extensions:
        """
        
        if 'FileName' in self.df.columns:
            extensions = self.df['FileName'].str.extract(r'\.([^.]+)')[0].value_counts().head(5)
            for ext, count in extensions.items():
                summary += f"\n.{ext}: {count:,} files"
        
        QMessageBox.information(self, "Quick Analysis", summary)
    
    def closeEvent(self, event):
        """Clean up when closing application"""
        try:
            if self.db_path and os.path.exists(self.db_path):
                os.unlink(self.db_path)
        except:
            pass
        event.accept()

def main():
    """Main application entry point"""
    app = QApplication(sys.argv)
    
    # Set application properties
    app.setApplicationName("MFT CSV Analyzer")
    app.setApplicationVersion("1.0")
    app.setOrganizationName("Digital Forensics Tools")
    
    # Create and show main window
    analyzer = MFTAnalyzer()   # must be subclass of QMainWindow/QWidget
    analyzer.show()
    
    # Start event loop
    sys.exit(app.exec_())

if __name__ == '__main__':
    main()