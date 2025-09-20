# app/gui_home.py
from PyQt5.QtWidgets import QWidget, QVBoxLayout, QLabel, QPushButton
import gui_generate
import gui_explore

class HomePage(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)

        layout = QVBoxLayout()

        # Title
        title = QLabel("MFT Tool")
        title.setStyleSheet("font-size: 18px; font-weight: bold; margin: 10px;")
        layout.addWidget(title)

        # Button: Generate MFT
        gen_btn = QPushButton("1. Generate $MFT File")
        gen_btn.clicked.connect(self.open_generate)
        layout.addWidget(gen_btn)

        # Button: Explore Existing
        explore_btn = QPushButton("2. Explore Existing MFT File")
        explore_btn.clicked.connect(self.open_explore)
        layout.addWidget(explore_btn)

        self.setLayout(layout)

    def open_generate(self):
        self.parent().setCentralWidget(gui_generate.GeneratePage(self.parent()))

    def open_explore(self):
        self.parent().setCentralWidget(gui_explore.ExplorePage(self.parent()))
