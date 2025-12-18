"""
Web-based Kafka Test GUI using Streamlit.
"""
import os
import streamlit as st
from kafka_manager import KafkaManager
import json
import asyncio
from datetime import datetime
import time

# Page config
st.set_page_config(
    page_title="Kafka Queue Test GUI",
    page_icon="📨",
    layout="wide"
)

# Initialize session state
if 'kafka_manager' not in st.session_state:
    st.session_state.kafka_manager = None
if 'consuming' not in st.session_state:
    st.session_state.consuming = False
if 'messages_log' not in st.session_state:
    st.session_state.messages_log = []
if 'messages_sent' not in st.session_state:
    st.session_state.messages_sent = 0
if 'messages_received' not in st.session_state:
    st.session_state.messages_received = 0

st.title("📨 Kafka Queue Test GUI")

# Sidebar for connection
with st.sidebar:
    st.header("Connection")
    bootstrap_servers = st.text_input(
        "Bootstrap Servers",
        value=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9093'),
        help="Use 'kafka:9093' when running in Docker, 'localhost:9092' when running locally"
    )
    
    if st.button("Connect", type="primary"):
        try:
            st.session_state.kafka_manager = KafkaManager(bootstrap_servers)
            success = st.session_state.kafka_manager.run_async(
                st.session_state.kafka_manager.connect_producer()
            )
            if success:
                st.success(f"Connected to Kafka at {bootstrap_servers}")
                st.session_state.messages_log.append({
                    'timestamp': datetime.now(),
                    'source': 'SYSTEM',
                    'message': f"Connected to Kafka at {bootstrap_servers}"
                })
            else:
                st.error("Failed to connect to Kafka")
        except Exception as e:
            st.error(f"Connection error: {e}")
    
    if st.session_state.kafka_manager:
        st.success("✅ Connected")
    else:
        st.warning("⚠️ Not connected")

# Main content
col1, col2 = st.columns(2)

with col1:
    st.header("📝 Topic Management")
    topic_name = st.text_input("Topic Name", value="test-topic", key="topic_name")
    if st.button("Create Topic"):
        if st.session_state.kafka_manager:
            try:
                success = st.session_state.kafka_manager.run_async(
                    st.session_state.kafka_manager.create_topic(topic_name)
                )
                if success:
                    st.success(f"Topic '{topic_name}' created successfully")
                    st.session_state.messages_log.append({
                        'timestamp': datetime.now(),
                        'source': 'SYSTEM',
                        'message': f"Created topic: {topic_name}"
                    })
                else:
                    st.error(f"Failed to create topic '{topic_name}'")
            except Exception as e:
                st.error(f"Error: {e}")
        else:
            st.warning("Please connect to Kafka first")

    st.header("📤 Producer - Send Messages")
    producer_topic = st.text_input("Topic", value="test-topic", key="producer_topic")
    message_key = st.text_input("Key (optional)", key="message_key")
    
    message_text = st.text_area(
        "Message (JSON)",
        value='{"message": "Hello Kafka", "timestamp": ""}',
        height=100,
        key="message_text"
    )
    
    col_send1, col_send2 = st.columns(2)
    with col_send1:
        if st.button("Send Message", type="primary"):
            if st.session_state.kafka_manager:
                try:
                    message = json.loads(message_text)
                    message["timestamp"] = datetime.now().isoformat()
                    success = st.session_state.kafka_manager.run_async(
                        st.session_state.kafka_manager.send_message(
                            producer_topic, message, message_key or None
                        )
                    )
                    if success:
                        st.session_state.messages_sent += 1
                        st.session_state.messages_log.append({
                            'timestamp': datetime.now(),
                            'source': 'PRODUCER',
                            'message': f"Sent to {producer_topic}: {json.dumps(message)}"
                        })
                        st.success("Message sent!")
                    else:
                        st.error("Failed to send message")
                except json.JSONDecodeError:
                    st.error("Invalid JSON format")
                except Exception as e:
                    st.error(f"Error: {e}")
            else:
                st.warning("Please connect to Kafka first")
    
    with col_send2:
        if st.button("Send Multiple (10)"):
            if st.session_state.kafka_manager:
                async def send_all():
                    for i in range(10):
                        message = {
                            "message": f"Test message {i+1}",
                            "sequence": i+1,
                            "timestamp": datetime.now().isoformat()
                        }
                        key = f"key-{i % 3}"
                        if await st.session_state.kafka_manager.send_message(producer_topic, message, key):
                            st.session_state.messages_sent += 1
                            st.session_state.messages_log.append({
                                'timestamp': datetime.now(),
                                'source': 'PRODUCER',
                                'message': f"Sent to {producer_topic}: {json.dumps(message)}"
                            })
                
                try:
                    st.session_state.kafka_manager.run_async_in_thread(send_all())
                    st.success("Sending 10 messages...")
                except Exception as e:
                    st.error(f"Error: {e}")
            else:
                st.warning("Please connect to Kafka first")

with col2:
    st.header("📥 Consumer - Receive Messages")
    consumer_topics = st.text_input(
        "Topics (comma-separated)",
        value="test-topic",
        key="consumer_topics"
    )
    
    col_cons1, col_cons2 = st.columns(2)
    with col_cons1:
        if st.button("Start Consumer", disabled=st.session_state.consuming):
            if st.session_state.kafka_manager:
                topics = [t.strip() for t in consumer_topics.split(",")]
                
                async def start_consume():
                    success = await st.session_state.kafka_manager.connect_consumer(topics)
                    if success:
                        st.session_state.consuming = True
                        st.session_state.messages_log.append({
                            'timestamp': datetime.now(),
                            'source': 'SYSTEM',
                            'message': f"Started consuming from topics: {', '.join(topics)}"
                        })
                        
                        def message_callback(topic, partition, offset, key, value):
                            if st.session_state.consuming:
                                st.session_state.messages_received += 1
                                st.session_state.messages_log.append({
                                    'timestamp': datetime.now(),
                                    'source': 'CONSUMER',
                                    'message': f"[{topic}:{partition}:{offset}] Key: {key}, Value: {json.dumps(value)}"
                                })
                        
                        def should_continue():
                            return st.session_state.consuming
                        
                        try:
                            await st.session_state.kafka_manager.consume_messages(
                                message_callback, should_continue=should_continue
                            )
                        except Exception as e:
                            if st.session_state.consuming:
                                st.session_state.messages_log.append({
                                    'timestamp': datetime.now(),
                                    'source': 'ERROR',
                                    'message': f"Consumer error: {e}"
                                })
                                st.session_state.consuming = False
                
                try:
                    st.session_state.kafka_manager.run_async_in_thread(start_consume())
                    st.success("Consumer started!")
                except Exception as e:
                    st.error(f"Error: {e}")
            else:
                st.warning("Please connect to Kafka first")
    
    with col_cons2:
        if st.button("Stop Consumer", disabled=not st.session_state.consuming):
            st.session_state.consuming = False
            st.session_state.messages_log.append({
                'timestamp': datetime.now(),
                'source': 'SYSTEM',
                'message': "Stopped consumer"
            })
            st.info("Consumer stopped")
    
    if st.session_state.consuming:
        st.info("🟢 Consumer is running...")

# Messages Log
st.header("📋 Messages Log")
col_stats1, col_stats2 = st.columns(2)
with col_stats1:
    st.metric("Messages Sent", st.session_state.messages_sent)
with col_stats2:
    st.metric("Messages Received", st.session_state.messages_received)

if st.button("Clear Log"):
    st.session_state.messages_log = []
    st.rerun()

# Display log
log_container = st.container()
with log_container:
    for msg in st.session_state.messages_log[-100:]:  # Show last 100 messages
        timestamp = msg['timestamp'].strftime("%H:%M:%S.%f")[:-3] if isinstance(msg['timestamp'], datetime) else str(msg['timestamp'])
        source = msg['source']
        message = msg['message']
        
        color_map = {
            'PRODUCER': '🟢',
            'CONSUMER': '🔵',
            'SYSTEM': '🟡',
            'ERROR': '🔴'
        }
        
        st.text(f"[{timestamp}] {color_map.get(source, '⚪')} [{source}] {message}")

# Auto-refresh when consumer is running
if st.session_state.consuming:
    time.sleep(0.5)
    st.rerun()
