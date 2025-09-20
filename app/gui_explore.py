# app/gui_explore.py
import tkinter as tk
from tkinter import filedialog, messagebox
from gui_dash import MFTAnalyzer as DashboardPage

class ExplorePage:
    def __init__(self, root):
        self.root = root
        self.frame = tk.Frame(root, padx=20, pady=20)
        self.frame.pack(expand=True)

        # Title
        tk.Label(self.frame, text="Explore Existing MFT CSV", font=("Arial", 14, "bold")).pack(pady=10)

        # Instruction
        tk.Label(self.frame, text="Select an existing CSV file to explore:").pack(pady=5)

        # Button to choose CSV
        choose_btn = tk.Button(self.frame, text="Choose CSV File", command=self.choose_csv)
        choose_btn.pack(pady=10)

    def choose_csv(self):
        """Open file dialog to select an existing CSV and load into Dashboard"""
        csv_file = filedialog.askopenfilename(
            title="Select MFT CSV File",
            filetypes=[("CSV Files", "*.csv")]
        )
        if csv_file:
            try:
                # ✅ Open Dashboard with chosen CSV
                DashboardPage(csv_file)
            except Exception as e:
                messagebox.showerror("Error", f"Failed to load Dashboard:\n{e}")
