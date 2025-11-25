import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from utils.logger import setup_logger

logger = setup_logger(__name__)

class EventProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = 'ecommerce_events'
    
    def generate_event(self):
        """Generate random e-commerce event"""
        event_types = ['page_view', 'add_to_cart', 'purchase', 'search']
        
        event = {
            'event_id': str(random.randint(100000, 999999)),
            'event_type': random.choice(event_types),
            'user_id': f"user_{random.randint(1, 1000)}",
            'product_id': f"prod_{random.randint(1, 500)}",
            'timestamp': datetime.now().isoformat(),
            'value': round(random.uniform(10.0, 500.0), 2)
        }
        return event
    
    def start_streaming(self, rate=10):
        """Stream events at specified rate (events per second)"""
        logger.info(f"Starting event producer at {rate} events/sec")
        
        try:
            while True:
                event = self.generate_event()
                self.producer.send(self.topic, value=event)
                logger.info(f"Sent: {event}")
                time.sleep(1 / rate)
                
        except KeyboardInterrupt:
            logger.info("Stopping producer...")
        finally:
            self.producer.close()

if __name__ == '__main__':
    producer = EventProducer()
    producer.start_streaming(rate=10)  # 10 events per second
