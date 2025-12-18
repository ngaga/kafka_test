"""
Main entry point for Kafka Test GUI application.
"""
import os
import tkinter as tk
from gui import KafkaGUI

# Suppress Tk deprecation warning on macOS
os.environ['TK_SILENCE_DEPRECATION'] = '1'


def main():
    """Launch the Kafka GUI application."""
    try:
        root = tk.Tk()
        app = KafkaGUI(root)
        root.protocol("WM_DELETE_WINDOW", app.on_closing)
        root.update()
        root.mainloop()
    except Exception as e:
        print(f"Error launching GUI: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

