# app/gui_dashboard.py
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import pandas as pd
import os

class DashboardPage:
    def __init__(self, root, csv_file):
        self.root = root
        self.csv_file = csv_file
        self.frame = tk.Frame(root, padx=10, pady=10)
        self.frame.pack(fill="both", expand=True)

        # Title
        tk.Label(self.frame, text="MFT Dashboard", font=("Arial", 16, "bold")).pack(pady=10)

        # Load CSV into DataFrame
        try:
            self.df = pd.read_csv(csv_file, low_memory=False)
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load CSV:\n{e}")
            return

        # Search Bar
        search_frame = tk.Frame(self.frame)
        search_frame.pack(fill="x", pady=5)

        tk.Label(search_frame, text="Search FileName:").pack(side="left", padx=5)
        self.search_var = tk.StringVar()
        search_entry = tk.Entry(search_frame, textvariable=self.search_var, width=40)
        search_entry.pack(side="left", padx=5)
        tk.Button(search_frame, text="Search", command=self.search_file).pack(side="left", padx=5)
        tk.Button(search_frame, text="Show Deleted", command=self.show_deleted).pack(side="left", padx=5)
        tk.Button(search_frame, text="Export Filtered", command=self.export_filtered).pack(side="left", padx=5)

        # Treeview (Table)
        self.tree = ttk.Treeview(self.frame, show="headings")
        self.tree.pack(fill="both", expand=True, pady=10)

        # Scrollbars
        vsb = ttk.Scrollbar(self.frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(self.frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscroll=vsb.set, xscroll=hsb.set)
        vsb.pack(side="right", fill="y")
        hsb.pack(side="bottom", fill="x")

        # Choose essential columns
        essential_cols = [
            "RecordNumber", "ParentPath", "FileName", "SI Creation", 
            "SI Modified", "SI Accessed", "FN Created", "Size", "Deleted"
        ]
        self.columns = [col for col in essential_cols if col in self.df.columns]

        self.tree["columns"] = self.columns
        for col in self.columns:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=150, anchor="w")

        # Initially load all rows
        self.load_table(self.df)

    def load_table(self, dataframe):
        """Load dataframe into the Treeview"""
        self.tree.delete(*self.tree.get_children())  # clear table
        for _, row in dataframe.iterrows():
            values = [row.get(col, "") for col in self.columns]
            self.tree.insert("", "end", values=values)

    def search_file(self):
        """Search FileName in DataFrame"""
        keyword = self.search_var.get().lower()
        if not keyword:
            self.load_table(self.df)
            return
        filtered = self.df[self.df["FileName"].astype(str).str.lower().str.contains(keyword, na=False)]
        self.load_table(filtered)

    def show_deleted(self):
        """Show only deleted files"""
        if "Deleted" not in self.df.columns:
            messagebox.showerror("Error", "CSV does not contain 'Deleted' column")
            return
        filtered = self.df[self.df["Deleted"].astype(str).str.lower() == "true"]
        self.load_table(filtered)

    def export_filtered(self):
        """Export the currently shown table to CSV"""
        children = self.tree.get_children()
        if not children:
            messagebox.showerror("Error", "No data to export")
            return

        save_path = filedialog.asksaveasfilename(
            defaultextension=".csv", filetypes=[("CSV files", "*.csv")]
        )
        if not save_path:
            return

        data = []
        for child in children:
            data.append(self.tree.item(child)["values"])
        export_df = pd.DataFrame(data, columns=self.columns)
        export_df.to_csv(save_path, index=False)
        messagebox.showinfo("Success", f"Filtered data exported to:\n{save_path}")
