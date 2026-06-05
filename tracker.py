import json
import sys
from confluent_kafka import Consumer
from hdfs import InsecureClient

print("""
########################################
#          IOT HDFS ARCHIVER           #
#       [ KAFKA -> HDFS 9870 ]         #
########################################
""")

# Configuration for Hadoop 3.x
HDFS_URL = 'http://localhost:9870'
HDFS_USER = 'root'
HDFS_PATH = '/user/root/iot_data.json'

# Initialize HDFS Client
hdfs_client = InsecureClient(HDFS_URL, user=HDFS_USER)

# Ensure the parent directory exists in HDFS
try:
    if not hdfs_client.status('/user/root', strict=False):
        hdfs_client.makedirs('/user/root')
        print("[ SYSTEM ] Created HDFS directory: /user/root")
except Exception as e:
    print(f"[ ERROR ] Could not connect to HDFS: {e}")
    print("Check if Hadoop container is running and port 9870 is open.")
    sys.exit(1)

# Kafka Consumer Configuration
consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "iot-hdfs-group",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumer_config)
consumer.subscribe(["iot_sensor_data"])

try:
    while True:
        msg = consumer.poll(1.0)
        
        if msg is None:
            continue
        if msg.error():
            print(f"[ ERROR ] Kafka: {msg.error()}")
            continue

        # Process message
        raw_data = msg.value().decode("utf-8")
        data = json.loads(raw_data)
        
        # Append logic with fallback for new file creation
        try:
            # Check if file exists to decide between append or write
            if hdfs_client.status(HDFS_PATH, strict=False):
                with hdfs_client.write(HDFS_PATH, append=True) as writer:
                    writer.write(raw_data + "\n")
            else:
                with hdfs_client.write(HDFS_PATH) as writer:
                    writer.write(raw_data + "\n")
            
            print(f"[ STORED ] {data['sensor_id']} | {data['reading']} {data['unit']}")
            
        except Exception as e:
            print(f"[ ERROR ] HDFS Write Failed: {e}")
            print("HINT: Ensure 'hadoop-master' is in your /etc/hosts file.")

except KeyboardInterrupt:
    print("\n[ SHUTDOWN ] Closing consumer...")
finally:
    consumer.close()
