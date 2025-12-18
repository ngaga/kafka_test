#!/usr/bin/env python3
"""
Ultra-simple tkinter test.
"""
import os
os.environ['TK_SILENCE_DEPRECATION'] = '1'

import tkinter as tk

print("Creating window...")
root = tk.Tk()
root.title("Simple Test")
root.geometry("400x300")

# Use basic tk widgets instead of ttk
label = tk.Label(root, text="Hello World!", font=("Arial", 20), bg="lightblue")
label.pack(pady=50)

button = tk.Button(root, text="Click Me", command=lambda: print("Clicked!"), font=("Arial", 14))
button.pack(pady=20)

print("Window created. Calling mainloop...")
root.update()
root.update_idletasks()
print("Starting mainloop...")
root.mainloop()
print("Mainloop ended.")
