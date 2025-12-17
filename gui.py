"""
GUI application for Kafka queue visualization.
"""
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import threading
import json
from datetime import datetime
from typing import Optional
from kafka_manager import KafkaManager


class KafkaGUI:
    """Main GUI application for Kafka testing."""
    
    def __init__(self, root):
        """
        Initialize the GUI.
        
        Args:
            root: Tkinter root window
        """
        self.root = root
        self.root.title("Kafka Queue Test GUI")
        self.root.geometry("1200x800")
        
        self.kafka_manager: Optional[KafkaManager] = None
        self.consuming = False
        self.consumer_thread: Optional[threading.Thread] = None
        
        self.setup_ui()
        
    def setup_ui(self):
        """Setup the user interface."""
        # Connection frame
        connection_frame = ttk.LabelFrame(self.root, text="Connection", padding=10)
        connection_frame.pack(fill=tk.X, padx=10, pady=5)
        
        ttk.Label(connection_frame, text="Bootstrap Servers:").grid(row=0, column=0, padx=5, pady=5)
        self.bootstrap_entry = ttk.Entry(connection_frame, width=30)
        self.bootstrap_entry.insert(0, "localhost:9092")
        self.bootstrap_entry.grid(row=0, column=1, padx=5, pady=5)
        
        self.connect_btn = ttk.Button(connection_frame, text="Connect", command=self.connect)
        self.connect_btn.grid(row=0, column=2, padx=5, pady=5)
        
        self.status_label = ttk.Label(connection_frame, text="Disconnected", foreground="red")
        self.status_label.grid(row=0, column=3, padx=10, pady=5)
        
        # Topic management frame
        topic_frame = ttk.LabelFrame(self.root, text="Topic Management", padding=10)
        topic_frame.pack(fill=tk.X, padx=10, pady=5)
        
        ttk.Label(topic_frame, text="Topic Name:").grid(row=0, column=0, padx=5, pady=5)
        self.topic_entry = ttk.Entry(topic_frame, width=30)
        self.topic_entry.insert(0, "test-topic")
        self.topic_entry.grid(row=0, column=1, padx=5, pady=5)
        
        ttk.Button(topic_frame, text="Create Topic", command=self.create_topic).grid(row=0, column=2, padx=5, pady=5)
        
        # Producer frame
        producer_frame = ttk.LabelFrame(self.root, text="Producer - Send Messages", padding=10)
        producer_frame.pack(fill=tk.X, padx=10, pady=5)
        
        ttk.Label(producer_frame, text="Topic:").grid(row=0, column=0, padx=5, pady=5)
        self.producer_topic_entry = ttk.Entry(producer_frame, width=20)
        self.producer_topic_entry.insert(0, "test-topic")
        self.producer_topic_entry.grid(row=0, column=1, padx=5, pady=5)
        
        ttk.Label(producer_frame, text="Key (optional):").grid(row=0, column=2, padx=5, pady=5)
        self.key_entry = ttk.Entry(producer_frame, width=20)
        self.key_entry.grid(row=0, column=3, padx=5, pady=5)
        
        ttk.Label(producer_frame, text="Message:").grid(row=1, column=0, padx=5, pady=5, sticky=tk.N)
        self.message_text = scrolledtext.ScrolledText(producer_frame, width=50, height=5)
        self.message_text.insert("1.0", '{"message": "Hello Kafka", "timestamp": ""}')
        self.message_text.grid(row=1, column=1, columnspan=3, padx=5, pady=5, sticky=tk.W)
        
        ttk.Button(producer_frame, text="Send Message", command=self.send_message).grid(row=2, column=0, padx=5, pady=5)
        ttk.Button(producer_frame, text="Send Multiple (10)", command=self.send_multiple).grid(row=2, column=1, padx=5, pady=5)
        
        # Consumer frame
        consumer_frame = ttk.LabelFrame(self.root, text="Consumer - Receive Messages", padding=10)
        consumer_frame.pack(fill=tk.X, padx=10, pady=5)
        
        ttk.Label(consumer_frame, text="Topics (comma-separated):").grid(row=0, column=0, padx=5, pady=5)
        self.consumer_topics_entry = ttk.Entry(consumer_frame, width=40)
        self.consumer_topics_entry.insert(0, "test-topic")
        self.consumer_topics_entry.grid(row=0, column=1, padx=5, pady=5)
        
        self.start_consumer_btn = ttk.Button(consumer_frame, text="Start Consumer", command=self.start_consumer)
        self.start_consumer_btn.grid(row=0, column=2, padx=5, pady=5)
        
        self.stop_consumer_btn = ttk.Button(consumer_frame, text="Stop Consumer", command=self.stop_consumer, state=tk.DISABLED)
        self.stop_consumer_btn.grid(row=0, column=3, padx=5, pady=5)
        
        # Messages display frame
        messages_frame = ttk.LabelFrame(self.root, text="Messages Log", padding=10)
        messages_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        # Statistics frame
        stats_frame = ttk.Frame(messages_frame)
        stats_frame.pack(fill=tk.X, pady=5)
        
        self.stats_label = ttk.Label(stats_frame, text="Messages received: 0 | Messages sent: 0")
        self.stats_label.pack(side=tk.LEFT)
        
        ttk.Button(stats_frame, text="Clear Log", command=self.clear_log).pack(side=tk.RIGHT, padx=5)
        
        self.messages_text = scrolledtext.ScrolledText(messages_frame, width=100, height=25)
        self.messages_text.pack(fill=tk.BOTH, expand=True)
        
        self.messages_sent = 0
        self.messages_received = 0
        
    def connect(self):
        """Connect to Kafka broker."""
        bootstrap_servers = self.bootstrap_entry.get().strip()
        if not bootstrap_servers:
            messagebox.showerror("Error", "Please enter bootstrap servers")
            return
        
        try:
            self.kafka_manager = KafkaManager(bootstrap_servers)
            success = self.kafka_manager.run_async(self.kafka_manager.connect_producer())
            if success:
                self.status_label.config(text="Connected", foreground="green")
                self.connect_btn.config(state=tk.DISABLED)
                self.log_message("SYSTEM", f"Connected to Kafka at {bootstrap_servers}")
            else:
                raise Exception("Failed to connect")
        except Exception as e:
            messagebox.showerror("Connection Error", f"Failed to connect to Kafka: {e}")
            self.status_label.config(text="Connection Failed", foreground="red")
    
    def create_topic(self):
        """Create a new Kafka topic."""
        if not self.kafka_manager:
            messagebox.showerror("Error", "Please connect to Kafka first")
            return
        
        topic_name = self.topic_entry.get().strip()
        if not topic_name:
            messagebox.showerror("Error", "Please enter a topic name")
            return
        
        try:
            success = self.kafka_manager.run_async(
                self.kafka_manager.create_topic(topic_name)
            )
            if success:
                messagebox.showinfo("Success", f"Topic '{topic_name}' created successfully")
                self.log_message("SYSTEM", f"Created topic: {topic_name}")
            else:
                messagebox.showerror("Error", f"Failed to create topic '{topic_name}'")
        except Exception as e:
            messagebox.showerror("Error", f"Error creating topic: {e}")
    
    def send_message(self):
        """Send a message to Kafka topic."""
        if not self.kafka_manager:
            messagebox.showerror("Error", "Please connect to Kafka first")
            return
        
        topic = self.producer_topic_entry.get().strip()
        if not topic:
            messagebox.showerror("Error", "Please enter a topic name")
            return
        
        message_text = self.message_text.get("1.0", tk.END).strip()
        if not message_text:
            messagebox.showerror("Error", "Please enter a message")
            return
        
        try:
            message = json.loads(message_text)
            message["timestamp"] = datetime.now().isoformat()
            
            key = self.key_entry.get().strip() or None
            
            success = self.kafka_manager.run_async(
                self.kafka_manager.send_message(topic, message, key)
            )
            if success:
                self.messages_sent += 1
                self.update_stats()
                self.log_message("PRODUCER", f"Sent to {topic}: {json.dumps(message)}")
            else:
                messagebox.showerror("Error", "Failed to send message")
        except json.JSONDecodeError:
            messagebox.showerror("Error", "Invalid JSON format in message")
        except Exception as e:
            messagebox.showerror("Error", f"Error sending message: {e}")
    
    def send_multiple(self):
        """Send multiple messages for testing."""
        if not self.kafka_manager:
            messagebox.showerror("Error", "Please connect to Kafka first")
            return
        
        topic = self.producer_topic_entry.get().strip()
        if not topic:
            messagebox.showerror("Error", "Please enter a topic name")
            return
        
        async def send_all():
            for i in range(10):
                message = {
                    "message": f"Test message {i+1}",
                    "sequence": i+1,
                    "timestamp": datetime.now().isoformat()
                }
                key = f"key-{i % 3}"  # Cycle through 3 keys
                
                success = await self.kafka_manager.send_message(topic, message, key)
                if success:
                    self.messages_sent += 1
                    self.root.after(0, self.log_message, "PRODUCER", 
                                  f"Sent to {topic}: {json.dumps(message)}")
            self.root.after(0, self.update_stats)
            self.root.after(0, lambda: messagebox.showinfo("Success", "Sent 10 messages successfully"))
        
        try:
            self.kafka_manager.run_async_in_thread(send_all())
        except Exception as e:
            messagebox.showerror("Error", f"Error sending messages: {e}")
    
    def start_consumer(self):
        """Start consuming messages from Kafka."""
        if not self.kafka_manager:
            messagebox.showerror("Error", "Please connect to Kafka first")
            return
        
        if self.consuming:
            messagebox.showwarning("Warning", "Consumer is already running")
            return
        
        topics_str = self.consumer_topics_entry.get().strip()
        if not topics_str:
            messagebox.showerror("Error", "Please enter topics to consume")
            return
        
        topics = [t.strip() for t in topics_str.split(",")]
        
        async def start_consume():
            success = await self.kafka_manager.connect_consumer(topics)
            if success:
                self.consuming = True
                self.root.after(0, lambda: self.start_consumer_btn.config(state=tk.DISABLED))
                self.root.after(0, lambda: self.stop_consumer_btn.config(state=tk.NORMAL))
                self.root.after(0, self.log_message, "SYSTEM", 
                             f"Started consuming from topics: {', '.join(topics)}")
                
                # Start consuming loop
                def message_callback(topic, partition, offset, key, value):
                    if self.consuming:
                        self.messages_received += 1
                        self.root.after(0, self.log_message, "CONSUMER", 
                                      f"[{topic}:{partition}:{offset}] Key: {key}, Value: {json.dumps(value)}")
                        self.root.after(0, self.update_stats)
                
                def should_continue():
                    return self.consuming
                
                try:
                    await self.kafka_manager.consume_messages(message_callback, should_continue=should_continue)
                except Exception as e:
                    if self.consuming:
                        self.root.after(0, lambda: self.log_message("ERROR", f"Consumer error: {e}"))
                        self.root.after(0, self.stop_consumer)
            else:
                self.root.after(0, messagebox.showerror, "Error", "Failed to start consumer")
        
        try:
            self.kafka_manager.run_async_in_thread(start_consume())
        except Exception as e:
            messagebox.showerror("Error", f"Error starting consumer: {e}")
    
    def stop_consumer(self):
        """Stop consuming messages."""
        self.consuming = False
        self.start_consumer_btn.config(state=tk.NORMAL)
        self.stop_consumer_btn.config(state=tk.DISABLED)
        self.log_message("SYSTEM", "Stopped consumer")
    
    def log_message(self, source: str, message: str):
        """
        Log a message to the messages display.
        
        Args:
            source: Source of the message (PRODUCER, CONSUMER, SYSTEM, ERROR)
            message: Message content
        """
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        log_entry = f"[{timestamp}] [{source}] {message}\n"
        
        self.messages_text.insert(tk.END, log_entry)
        self.messages_text.see(tk.END)
    
    def update_stats(self):
        """Update statistics display."""
        self.stats_label.config(text=f"Messages received: {self.messages_received} | Messages sent: {self.messages_sent}")
    
    def clear_log(self):
        """Clear the messages log."""
        self.messages_text.delete("1.0", tk.END)
    
    def on_closing(self):
        """Handle window closing."""
        if self.kafka_manager:
            self.consuming = False
            self.kafka_manager.run_async(self.kafka_manager.close())
        self.root.destroy()
