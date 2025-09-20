# app/gui_dashboard.py
import tkinter as tk
from tkinter import ttk, messagebox
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

class DashboardPage:
    def __init__(self, root, csv_path=None):
        self.root = root
        self.frame = tk.Toplevel(root)   # new window
        self.frame.title("MFT Dashboard")
        self.frame.geometry("800x600")

        tk.Label(self.frame, text="MFT Dashboard", font=("Arial", 16, "bold")).pack(pady=10)

        # Table
        self.tree = ttk.Treeview(self.frame, columns=("EntryNumber", "FileName", "ParentPath"), show="headings")
        self.tree.heading("EntryNumber", text="EntryNumber")
        self.tree.heading("FileName", text="FileName")
        self.tree.heading("ParentPath", text="ParentPath")
        self.tree.pack(fill="both", expand=True, pady=10)

        self.df = None

        # Auto-load CSV if provided
        if csv_path:
            self.load_csv(csv_path)

    def load_csv(self, path):
        try:
            self.df = pd.read_csv(path)
            self.populate_table()
            messagebox.showinfo("Success", f"Loaded {len(self.df)} records from:\n{path}")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load CSV:\n{e}")

    def populate_table(self):
        for row in self.tree.get_children():
            self.tree.delete(row)

        if self.df is not None:
            for _, row in self.df.head(100).iterrows():
                self.tree.insert("", "end", values=(row["EntryNumber"], row["FileName"], row["ParentPath"]))
