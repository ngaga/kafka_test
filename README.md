# Kafka Test GUI

A simple GUI application to test and visualize Kafka queue dynamics.

## Requirements

- Python 3.7+ (recommended: Python 3.11+ via Homebrew for better tkinter support on macOS)
- Kafka server running (default: localhost:9092)

### Note on macOS

If you experience issues with the GUI not displaying (empty window), it's likely due to the system Python's tkinter. Install Python via Homebrew:

```bash
brew install python@3.11
# Then use: /opt/homebrew/bin/python3.11 main.py
# Or create an alias
```

## Quick Start with Docker (Recommended)

Start everything (Kafka + Web Interface):

```bash
docker-compose up
```

Then open http://localhost:5000 in your browser.

The web interface is built with Streamlit and provides:
- ✅ Works perfectly in Docker
- ✅ Modern, responsive UI
- ✅ Real-time message updates

To run in background:
```bash
docker-compose up -d
```

## Starting Kafka Only

### Option 1: Docker

```bash
docker-compose up -d zookeeper kafka
```

### Option 2: Homebrew (macOS)

```bash
brew install kafka
brew services start zookeeper
brew services start kafka
```

### Verify Kafka is running

```bash
python3 check_kafka.py
```

## Installation

```bash
pip install -e .
```

Or with development dependencies:

```bash
pip install -e ".[dev]"
```

## Usage

### Option 1: Web Interface (Recommended - works in Docker)

```bash
docker-compose up
```

Then open http://localhost:5000 in your browser.

### Option 2: Desktop GUI (requires tkinter)

```bash
python3 main.py
```

**Note:** On macOS, the system Python's tkinter may have display issues. Use the web interface or install Python via Homebrew for better compatibility.

## Features

- Connect to Kafka broker
- Create and manage topics
- Send messages to topics
- Consume messages in real-time
- Visualize message flow and statistics

