# app/gui_home.py
import tkinter as tk
from tkinter import messagebox
import gui_generate
import gui_explore

class HomePage:
    def __init__(self, root):
        self.root = root
        self.frame = tk.Frame(root, padx=20, pady=20)
        self.frame.pack(expand=True)

        # Title
        title = tk.Label(self.frame, text="MFT Tool", font=("Arial", 16, "bold"))
        title.pack(pady=10)

        # Button: Generate MFT
        gen_btn = tk.Button(self.frame, text="1. Generate $MFT File",
                            font=("Arial", 12), width=25, command=self.open_generate)
        gen_btn.pack(pady=10)

        # Button: Explore Existing
        explore_btn = tk.Button(self.frame, text="2. Explore Existing MFT File",
                                font=("Arial", 12), width=25, command=self.open_explore)
        explore_btn.pack(pady=10)

    def open_generate(self):
        self.frame.destroy()
        gui_generate.GeneratePage(self.root)

    def open_explore(self):
        self.frame.destroy()
        gui_explore.ExplorePage(self.root)
