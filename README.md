# Kafka Test GUI

A simple GUI application to test and visualize Kafka queue dynamics.

## Requirements

- Python 3.7+
- Kafka server running (default: localhost:9092)

## Starting Kafka

### Option 1: Docker (Recommended for quick start)

```bash
docker-compose up -d
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

```bash
python main.py
```

## Features

- Connect to Kafka broker
- Create and manage topics
- Send messages to topics
- Consume messages in real-time
- Visualize message flow and statistics
