from confluent_kafka import Producer
from faker import Faker
import time
import json,random

# Initialize Faker and Kafka Producer
fake = Faker()
producer = Producer({'bootstrap.servers': 'localhost:9092'})  # Replace with your Kafka server address

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
        print(msg)

def generate_fake_weather_data():
    return {
        'date': fake.date_this_year().strftime('%Y-%m-%d'),
            'temperature': round(random.uniform(-10.0, 40.0), 2),  # Random temperature between -10 and 40 Celsius
            'humidity': round(random.uniform(0.0, 100.0), 2),       # Random humidity percentage
            'precipitation': round(random.uniform(0.0, 50.0), 2),   # Random precipitation in mm
            'wind_speed': round(random.uniform(0.0, 20.0), 2)      
    }

def produce_messages(topic='weather_data', interval=5):
    while True:
        fake_data = generate_fake_weather_data()
        producer.produce(topic, key='key', value=json.dumps(fake_data), callback=delivery_report)
        producer.poll(0)  # Serve delivery reports
        time.sleep(1)  # Wait before sending the next message

if __name__ == "__main__":
    try:
        produce_messages()
    except KeyboardInterrupt:
        print("Producer interrupted")
    finally:
        producer.flush()
