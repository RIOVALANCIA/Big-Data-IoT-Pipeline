import json
import time
import random
from datetime import datetime
from confluent_kafka import Producer

print("""
########################################
#          IOT SENSOR PRODUCER         #
#       [ STATUS: TRANSMITTING ]       #
########################################
""")

producer_config = {"bootstrap.servers": "localhost:9092"}
producer = Producer(producer_config)

def delivery_report(err, msg):
    if err:
        print(f"ERROR: {err}")

SENSORS = ["sensor_thermal_01", "sensor_thermal_02", "sensor_pressure_01"]

try:
    while True:
        for sensor_id in SENSORS:
            payload = {
                "sensor_id": sensor_id,
                "timestamp": datetime.now().isoformat(),
                "reading": round(random.uniform(20.0, 35.0), 2),
                "unit": "Celsius",
                "status": "OK" if random.random() > 0.1 else "WARNING"
            }

            producer.produce(
                topic="iot_sensor_data",
                value=json.dumps(payload).encode("utf-8"),
                callback=delivery_report
            )
        
        producer.flush()
        print(f"TX: {len(SENSORS)} packets at {datetime.now().strftime('%H:%M:%S')}")
        time.sleep(1)

except KeyboardInterrupt:
    print("\n[ SHUTDOWN ]")
