#!/usr/bin/env python3
"""
Script to check if Kafka is running and accessible.
"""
import socket
import sys


def check_kafka_port(host='localhost', port=9092, timeout=2):
    """
    Check if Kafka is accessible on the given host and port.
    
    Args:
        host: Kafka host
        port: Kafka port
        timeout: Connection timeout in seconds
        
    Returns:
        True if accessible, False otherwise
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception as e:
        print(f"Error checking connection: {e}")
        return False


def main():
    """Main function."""
    host = sys.argv[1] if len(sys.argv) > 1 else 'localhost'
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 9092
    
    print(f"Checking Kafka connection to {host}:{port}...")
    
    if check_kafka_port(host, port):
        print(f"✅ Kafka is running and accessible on {host}:{port}")
        sys.exit(0)
    else:
        print(f"❌ Kafka is NOT accessible on {host}:{port}")
        print("\nTo start Kafka:")
        print("  - With Homebrew: brew services start kafka")
        print("  - With Docker: docker-compose up -d")
        print("  - Manually: Start Zookeeper first, then Kafka server")
        sys.exit(1)


if __name__ == "__main__":
    main()
