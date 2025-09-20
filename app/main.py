# app/main.py
import tkinter as tk
# from app import gui_dash
from gui_home import HomePage
from gui_dash import MFTAnalyzer

def main():
    root = tk.Tk()
    dashboard = MFTAnalyzer()
    dashboard.show()
    root.title("MFT Tool")
    root.geometry("400x250")
    root.resizable(False, False)

    HomePage(root)  # Load HomePage
    root.mainloop()

if __name__ == "__main__":
    
    main()
