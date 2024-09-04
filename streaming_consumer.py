import csv ,json,os
from confluent_kafka import Consumer, KafkaException

# Kafka Consumer configuration
conf = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka server address
    'group.id': 'weather_group',            # Consumer group ID
    'auto.offset.reset': 'earliest'         # Start reading from the earliest message
}

# Create a Kafka Consumer instance
consumer = Consumer(conf)

# Subscribe to the topic
topic = 'weather_data'
consumer.subscribe([topic])


file_path = 'weather_data.csv'

# Open CSV file in append mode
csv_file = open(file_path, mode='a', newline='')
csv_writer = csv.writer(csv_file)

# Check if file is empty and write headers if necessary
if os.stat(file_path).st_size == 0:
    csv_writer.writerow(['date', 'temperature', 'humidity', 'precipitation', 'wind_speed'])
def process_message(msg):
    message = msg.value().decode('utf-8')
    data = json.loads(message)
    csv_writer.writerow([data['date'], data['temperature'], data['humidity'], data['precipitation'], data['wind_speed']])
    csv_file.flush()  # Ensure data is written to disk

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                raise KafkaException(msg.error())
        
        process_message(msg)

except KeyboardInterrupt:
    print("Consumer interrupted")

finally:
    csv_file.close()  # Ensure the CSV file is properly closed
    consumer.close()