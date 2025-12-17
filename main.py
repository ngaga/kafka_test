"""
Main entry point for Kafka Test GUI application.
"""
import tkinter as tk
from gui import KafkaGUI


def main():
    """Launch the Kafka GUI application."""
    root = tk.Tk()
    app = KafkaGUI(root)
    root.protocol("WM_DELETE_WINDOW", app.on_closing)
    root.mainloop()


if __name__ == "__main__":
    main()
