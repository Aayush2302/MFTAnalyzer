# app/gui_generate.py
import os
import time
import string
import subprocess
import sys
import ctypes
import pandas as pd
from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QLabel, QComboBox, QPushButton,
    QFileDialog, QMessageBox, QDialog, QTextEdit,
    QProgressBar, QApplication
)
from PyQt5.QtCore import QThread, pyqtSignal, Qt

from app.gui_dash import MFTAnalyzer as DashboardPage
from app.gui_home import HomePage  


def resource_path(relative_path):
    """Get absolute path to resource (works for dev, onedir, onefile)"""
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, relative_path)  # onefile mode
    return os.path.join(os.path.abspath("."), relative_path)  # dev/onedir mode


def is_admin():
    """Check if the program is running with admin rights"""
    try:
        return ctypes.windll.shell32.IsUserAnAdmin()
    except:
        return False


rawcopy_path = resource_path("tools/RawCopy.exe")
mftecmd_path = resource_path("tools/MFTECmd.exe")


class ProgressDialog(QDialog):
    """Custom progress dialog with terminal output"""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("MFT Generation Progress")
        self.setModal(True)
        self.resize(600, 400)
        
        layout = QVBoxLayout()
        
        # Status label
        self.status_label = QLabel("Initializing...")
        self.status_label.setStyleSheet("font-size: 14px; font-weight: bold; padding: 10px;")
        layout.addWidget(self.status_label)
        
        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        layout.addWidget(self.progress_bar)
        
        # Terminal output
        terminal_label = QLabel("Terminal Output:")
        terminal_label.setStyleSheet("font-weight: bold; margin-top: 10px;")
        layout.addWidget(terminal_label)
        
        self.terminal_output = QTextEdit()
        self.terminal_output.setReadOnly(True)
        self.terminal_output.setStyleSheet("""
            QTextEdit {
                background-color: #1e1e1e;
                color: #ffffff;
                font-family: 'Courier New', monospace;
                font-size: 10px;
                border: 1px solid #444444;
            }
        """)
        layout.addWidget(self.terminal_output)
        
        self.setLayout(layout)
        
        # Prevent closing during operation
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowCloseButtonHint)
    
    def update_status(self, status, progress=None):
        """Update status label and optional progress"""
        self.status_label.setText(status)
        if progress is not None:
            self.progress_bar.setValue(progress)
    
    def append_terminal(self, text):
        """Append text to terminal output"""
        self.terminal_output.append(text)
        # Auto-scroll to bottom
        cursor = self.terminal_output.textCursor()
        cursor.movePosition(cursor.End)
        self.terminal_output.setTextCursor(cursor)


class MFTGeneratorThread(QThread):
    """Worker thread for MFT generation"""
    status_update = pyqtSignal(str, int)  # status, progress
    terminal_update = pyqtSignal(str)
    finished_success = pyqtSignal(str)  # csv_file_path
    finished_error = pyqtSignal(str)  # error_message
    
    def __init__(self, drive, save_path):
        super().__init__()
        self.drive = drive
        self.save_path = save_path
    
    def run(self):
        try:
            save_folder = os.path.dirname(self.save_path)
            save_name = os.path.basename(self.save_path)
            
            # Step 1: Generate MFT.bin
            self.status_update.emit("Generating MFT.bin file...", 10)
            self.terminal_update.emit(f"Starting MFT generation for drive {self.drive}")
            self.terminal_update.emit(f"Output path: {self.save_path}")

            if not is_admin():
                self.terminal_update.emit("ERROR: Program is not running as administrator!")
                self.finished_error.emit("Administrator rights are required to run RawCopy.")
                return

            # Run RawCopy with full path & admin
            ps_command = (
                f'Start-Process "{rawcopy_path}" '
                f'-ArgumentList \'/FileNamePath:{self.drive}\\$MFT\',\'/OutputPath:{save_folder}\',\'/OutputName:{save_name}\' '
                f'-Verb RunAs -Wait'
            )
            
            self.terminal_update.emit("Executing RawCopy command...")
            self.terminal_update.emit(f"Command: {ps_command}")
            
            process = subprocess.run(
                ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", ps_command],
                capture_output=True,
                text=True
            )
            
            if process.returncode != 0:
                self.terminal_update.emit(f"RawCopy stderr: {process.stderr}")
                raise subprocess.CalledProcessError(process.returncode, "RawCopy")
            
            self.terminal_update.emit("RawCopy completed successfully")
            self.status_update.emit("Waiting for MFT.bin file creation...", 30)
            
            # Wait until file exists
            timeout = 60
            waited = 0
            while (not os.path.exists(self.save_path) or os.path.getsize(self.save_path) == 0) and waited < timeout:
                self.terminal_update.emit(f"Waiting for file... ({waited}/{timeout}s)")
                time.sleep(2)
                waited += 2
                progress = 30 + (waited / timeout * 20)  # 30-50%
                self.status_update.emit(f"Waiting for MFT.bin file creation... ({waited}s)", int(progress))
            
            if not os.path.exists(self.save_path) or os.path.getsize(self.save_path) == 0:
                self.terminal_update.emit("ERROR: MFT.bin file was not created or is empty")
                self.finished_error.emit("MFT.bin file was not created.")
                return
            
            file_size = os.path.getsize(self.save_path)
            self.terminal_update.emit(f"MFT.bin created successfully (Size: {file_size:,} bytes)")
            
            # Step 2: Convert BIN to CSV
            self.status_update.emit("Converting MFT.bin to CSV...", 60)
            save_csv_folder = self.save_path.replace(".bin", "_csv")
            
            self.terminal_update.emit("Starting MFTECmd conversion...")
            self.terminal_update.emit(f"Output CSV folder: {save_csv_folder}")
            
            process = subprocess.run([
                mftecmd_path,
                "-f", self.save_path,
                "--csv", save_csv_folder
            ], capture_output=True, text=True)
            
            if process.returncode != 0:
                self.terminal_update.emit(f"MFTECmd stderr: {process.stderr}")
                raise subprocess.CalledProcessError(process.returncode, "MFTECmd")
            
            self.terminal_update.emit("MFTECmd completed successfully")
            self.status_update.emit("Locating generated CSV file...", 80)
            
            # Find the actual CSV file inside the folder
            csv_file = None
            if os.path.exists(save_csv_folder):
                for file in os.listdir(save_csv_folder):
                    if file.endswith(".csv"):
                        csv_file = os.path.join(save_csv_folder, file)
                        self.terminal_update.emit(f"Found CSV file: {csv_file}")
                        break
            
            if csv_file:
                csv_size = os.path.getsize(csv_file)
                self.terminal_update.emit(f"CSV file size: {csv_size:,} bytes")
                self.status_update.emit("MFT generation completed successfully!", 100)
                self.terminal_update.emit("=== MFT GENERATION COMPLETED SUCCESSFULLY ===")
                self.finished_success.emit(csv_file)
            else:
                self.terminal_update.emit("ERROR: CSV file not found in output folder")
                self.finished_error.emit("CSV file not found in output folder!")
                
        except subprocess.CalledProcessError as e:
            error_msg = f"Tool execution error: {e}"
            self.terminal_update.emit(f"ERROR: {error_msg}")
            self.finished_error.emit(error_msg)
        except Exception as e:
            error_msg = f"Unexpected error: {e}"
            self.terminal_update.emit(f"ERROR: {error_msg}")
            self.finished_error.emit(error_msg)


class GeneratePage(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.parent = parent  # Store reference to parent
        self.progress_dialog = None
        self.worker_thread = None

        layout = QVBoxLayout()

        # Title
        title = QLabel("Generate $MFT File")
        title.setStyleSheet("font-size: 16px; font-weight: bold; margin: 10px;")
        layout.addWidget(title)

        # Drive selection
        drive_label = QLabel("Select Drive:")
        layout.addWidget(drive_label)

        self.drive_combo = QComboBox()
        self.drive_combo.addItems(self.list_drives())
        layout.addWidget(self.drive_combo)

        # Save location
        self.save_path = None
        save_btn = QPushButton("Choose Save Location")
        save_btn.clicked.connect(self.choose_save_path)
        layout.addWidget(save_btn)

        # Run button
        self.run_btn = QPushButton("Generate MFT")
        self.run_btn.clicked.connect(self.generate_mft)
        layout.addWidget(self.run_btn)

        self.setLayout(layout)

    def list_drives(self):
        """List available drives on Windows"""
        drives = []
        for letter in string.ascii_uppercase:
            if os.path.exists(f"{letter}:\\"):
                drives.append(f"{letter}:")
        return drives

    def choose_save_path(self):
        """Ask user which folder to save MFT.bin"""
        folder = QFileDialog.getExistingDirectory(self, "Select Folder to Save MFT")
        if folder:
            self.save_path = os.path.join(folder, "MFT.bin")

    def generate_mft(self):
        """Start MFT generation process"""
        drive = self.drive_combo.currentText()
        save_bin = self.save_path

        if not drive:
            QMessageBox.critical(self, "Error", "Please select a drive.")
            return
        if not save_bin:
            QMessageBox.critical(self, "Error", "Please choose save location.")
            return

        # Disable the generate button during processing
        self.run_btn.setEnabled(False)
        self.run_btn.setText("Generating...")

        # Create and show progress dialog
        self.progress_dialog = ProgressDialog(self)
        self.progress_dialog.show()

        # Create worker thread
        self.worker_thread = MFTGeneratorThread(drive, save_bin)
        
        # Connect signals
        self.worker_thread.status_update.connect(self.progress_dialog.update_status)
        self.worker_thread.terminal_update.connect(self.progress_dialog.append_terminal)
        self.worker_thread.finished_success.connect(self.on_generation_success)
        self.worker_thread.finished_error.connect(self.on_generation_error)
        
        # Start the worker thread
        self.worker_thread.start()

    def on_generation_success(self, csv_file):
        """Handle successful MFT generation"""
        # Clean up
        self.cleanup_generation()

        # Show success message and wait for user to press OK
        QMessageBox.information(
            self,
            "Success",
            "MFT generated and converted successfully!"
        )

        # ✅ After user presses OK → exit program
        QApplication.quit()

    def on_generation_error(self, error_message):
        """Handle MFT generation error"""
        # Clean up
        self.cleanup_generation()
        
        # Show error message
        QMessageBox.critical(self, "Error", f"MFT generation failed:\n{error_message}")

    def cleanup_generation(self):
        """Clean up after generation (success or failure)"""
        # Close progress dialog
        if self.progress_dialog:
            self.progress_dialog.close()
            self.progress_dialog = None
        
        # Clean up worker thread
        if self.worker_thread:
            self.worker_thread.quit()
            self.worker_thread.wait()
            self.worker_thread = None
        
        # Re-enable the generate button
        self.run_btn.setEnabled(True)
        self.run_btn.setText("Generate MFT")

    def show_home_page(self):
        """Return to home page after generation"""
        if self.parent:
            if hasattr(self.parent, 'show_home_page'):
                self.parent.show_home_page()
            elif hasattr(self.parent, 'stacked_widget'):
                self.parent.stacked_widget.setCurrentIndex(0)
            self.parent.close()
        else:
            self.close()

    def close_dialog(self):
        """Close the generate dialog/window"""
        if self.worker_thread and self.worker_thread.isRunning():
            self.worker_thread.quit()
            self.worker_thread.wait()
        
        if self.progress_dialog:
            self.progress_dialog.close()
        
        if self.parent:
            self.parent.close()
        else:
            self.close()

    def closeEvent(self, event):
        """Handle window close event"""
        if self.worker_thread and self.worker_thread.isRunning():
            reply = QMessageBox.question(
                self, 
                "Confirm Close", 
                "MFT generation is in progress. Are you sure you want to close?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            
            if reply == QMessageBox.Yes:
                self.worker_thread.quit()
                self.worker_thread.wait()
                event.accept()
            else:
                event.ignore()
        else:
            event.accept()
