#!/usr/bin/env python3
"""
Minimal tkinter test - no ttk.
"""
import os
os.environ['TK_SILENCE_DEPRECATION'] = '1'

import tkinter as tk

print("Step 1: Creating root window...")
root = tk.Tk()
print("Step 2: Root window created")

root.title("Minimal Test")
root.geometry("300x200")

print("Step 3: Creating label...")
label = tk.Label(root, text="If you see this, it works!", bg="yellow", font=("Arial", 16))
label.pack(pady=50)

print("Step 4: All widgets created. Starting mainloop...")
print("The window should appear now. Close it to exit.")
root.mainloop()
print("Step 5: Mainloop ended.")
