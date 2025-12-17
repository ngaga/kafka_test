"""
Kafka manager for handling producer and consumer operations using aiokafka (async).
"""
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
import json
import asyncio
from typing import Optional, Callable, List


class KafkaManager:
    """Manages Kafka connections and operations using aiokafka (async/await)."""
    
    def __init__(self, bootstrap_servers: str = 'localhost:9092'):
        """
        Initialize Kafka manager.
        
        Args:
            bootstrap_servers: Kafka broker address
        """
        self.bootstrap_servers = bootstrap_servers
        self.producer: Optional[AIOKafkaProducer] = None
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.admin_client: Optional[AIOKafkaAdminClient] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        
    async def connect_producer(self) -> bool:
        """
        Connect to Kafka as producer.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            await self.producer.start()
            return True
        except Exception as e:
            print(f"Error connecting producer: {e}")
            return False
    
    async def connect_consumer(self, topics: List[str], group_id: str = 'test-group') -> bool:
        """
        Connect to Kafka as consumer.
        
        Args:
            topics: List of topics to consume from
            group_id: Consumer group ID
            
        Returns:
            True if connection successful, False otherwise
        """
        try:
            self.consumer = AIOKafkaConsumer(
                *topics,
                bootstrap_servers=self.bootstrap_servers,
                group_id=group_id,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                auto_offset_reset='earliest',
                enable_auto_commit=True
            )
            await self.consumer.start()
            return True
        except Exception as e:
            print(f"Error connecting consumer: {e}")
            return False
    
    async def create_topic(self, topic_name: str, num_partitions: int = 1, replication_factor: int = 1) -> bool:
        """
        Create a new Kafka topic.
        
        Args:
            topic_name: Name of the topic
            num_partitions: Number of partitions
            replication_factor: Replication factor
            
        Returns:
            True if topic created successfully, False otherwise
        """
        try:
            if not self.admin_client:
                self.admin_client = AIOKafkaAdminClient(
                    bootstrap_servers=self.bootstrap_servers
                )
                await self.admin_client.start()
            
            topic = NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor
            )
            
            await self.admin_client.create_topics([topic])
            return True
        except Exception as e:
            print(f"Error creating topic: {e}")
            return False
    
    async def send_message(self, topic: str, message: dict, key: Optional[str] = None) -> bool:
        """
        Send a message to a Kafka topic.
        
        Args:
            topic: Topic name
            message: Message content as dictionary
            key: Optional message key
            
        Returns:
            True if message sent successfully, False otherwise
        """
        if not self.producer:
            if not await self.connect_producer():
                return False
        
        try:
            await self.producer.send_and_wait(topic, value=message, key=key)
            return True
        except Exception as e:
            print(f"Error sending message: {e}")
            return False
    
    async def consume_messages(self, callback: Callable, should_continue: Optional[Callable[[], bool]] = None) -> None:
        """
        Consume messages and call callback for each message.
        
        Args:
            callback: Function to call with each message (topic, partition, offset, key, value)
            should_continue: Optional function that returns False to stop consuming
        """
        if not self.consumer:
            return
        
        try:
            async for msg in self.consumer:
                # Check if we should continue
                if should_continue and not should_continue():
                    break
                
                callback(
                    msg.topic,
                    msg.partition,
                    msg.offset,
                    msg.key,
                    msg.value
                )
        except Exception as e:
            print(f"Error consuming messages: {e}")
            raise
    
    async def close(self):
        """Close all Kafka connections."""
        if self.producer:
            await self.producer.stop()
            self.producer = None
        if self.consumer:
            await self.consumer.stop()
            self.consumer = None
        if self.admin_client:
            await self.admin_client.close()
            self.admin_client = None
    
    def run_async(self, coro):
        """
        Run async coroutine in a new event loop (for use with sync code).
        
        Args:
            coro: Coroutine to run
            
        Returns:
            Result of the coroutine
        """
        if self._loop is None or self._loop.is_closed():
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
        
        return self._loop.run_until_complete(coro)
    
    def run_async_in_thread(self, coro, callback=None):
        """
        Run async coroutine in a separate thread with its own event loop.
        
        Args:
            coro: Coroutine to run
            callback: Optional callback to call with result
        """
        def run_in_thread():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(coro)
                if callback:
                    callback(result)
            finally:
                loop.close()
        
        import threading
        thread = threading.Thread(target=run_in_thread, daemon=True)
        thread.start()
        return thread
