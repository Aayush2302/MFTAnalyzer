# app/main.py
import sys
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget,
    QVBoxLayout, QPushButton, QFileDialog, QMessageBox
)

from gui_generate import GeneratePage
from gui_dash import MFTAnalyzer as DashboardPage


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("MFT Analyzer - Start Menu")
        self.setGeometry(200, 200, 400, 200)

        # Central Widget
        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        layout = QVBoxLayout()

        # Button: Generate MFT
        btn_generate = QPushButton("Generate MFT File")
        btn_generate.setStyleSheet("font-size: 14px; padding: 8px;")
        btn_generate.clicked.connect(self.open_generate_page)
        layout.addWidget(btn_generate)

        # Button: Explore Existing CSV
        btn_explore = QPushButton("Explore Existing CSV")
        btn_explore.setStyleSheet("font-size: 14px; padding: 8px;")
        btn_explore.clicked.connect(self.open_existing_csv)
        layout.addWidget(btn_explore)

        central_widget.setLayout(layout)

    def open_generate_page(self):
        """Open the GeneratePage workflow"""
        self.generate_page = GeneratePage()
        self.setCentralWidget(self.generate_page)

    def open_existing_csv(self):
        """Select a CSV and open Dashboard directly"""
        file, _ = QFileDialog.getOpenFileName(self, "Open CSV File", "", "CSV Files (*.csv)")
        if file:
            self.dashboard = DashboardPage(csv_file=file)
            self.dashboard.show()
        else:
            QMessageBox.warning(self, "No File", "No CSV file selected.")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())
