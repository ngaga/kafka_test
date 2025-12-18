#!/usr/bin/env python3
"""
Test script to verify GUI rendering.
"""
import os
os.environ['TK_SILENCE_DEPRECATION'] = '1'

import tkinter as tk
from tkinter import ttk

root = tk.Tk()
root.title("Test GUI")
root.geometry("800x600")

# Test basic widgets
frame = ttk.LabelFrame(root, text="Test Frame", padding=10)
frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

label = ttk.Label(frame, text="If you see this, tkinter works!")
label.pack(pady=10)

button = ttk.Button(frame, text="Test Button", command=lambda: print("Button clicked!"))
button.pack(pady=10)

entry = ttk.Entry(frame, width=30)
entry.insert(0, "Test entry field")
entry.pack(pady=10)

print("GUI created. Starting mainloop...")
try:
    root.mainloop()
except Exception as e:
    print(f"Error in mainloop: {e}")
