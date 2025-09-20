# app/gui_explore.py
from PyQt5.QtWidgets import QWidget, QVBoxLayout, QLabel, QPushButton, QFileDialog, QMessageBox
from gui_dash import MFTAnalyzer as DashboardPage

class ExplorePage(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)

        layout = QVBoxLayout()

        # Title
        title = QLabel("Explore Existing MFT CSV")
        title.setStyleSheet("font-size: 16px; font-weight: bold; margin: 10px;")
        layout.addWidget(title)

        # Instruction
        instruction = QLabel("Select an existing CSV file to explore:")
        layout.addWidget(instruction)

        # Button to choose CSV
        choose_btn = QPushButton("Choose CSV File")
        choose_btn.clicked.connect(self.choose_csv)
        layout.addWidget(choose_btn)

        self.setLayout(layout)

    def choose_csv(self):
        csv_file, _ = QFileDialog.getOpenFileName(
            self,
            "Select MFT CSV File",
            "",
            "CSV Files (*.csv)"
        )

        if csv_file:
            try:
                dashboard = DashboardPage(csv_file)
                dashboard.show()
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Failed to load Dashboard:\n{e}")
