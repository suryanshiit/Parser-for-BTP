import paho.mqtt.client as mqtt
import time
from pymongo import MongoClient
from datetime import datetime

mongo_uri = 'mongodb://admin:mybtp@3.109.19.112:27017/'
client = MongoClient(mongo_uri)
db = client['sensor_data']  # Database name
collection = db['readings']  # Collection name

# MQTT broker details
broker_ip = '3.109.19.112'  # Replace with your MQTT broker IP
port = 1883  # Default MQTT port
subscribe_topic = 'sensor/reading'  # Topic to subscribe to
publish_topic = 'device/time_update'  # Topic to publish the time
#node_id = 1  # Define the node_id
#node_id = int(node_id)
# Callback when the client connects to the broker
def on_connect(client, userdata, flags, rc):
    print('Connected to MQTT broker!')
    client.subscribe(subscribe_topic)
    print(f'Subscribed to topic "{subscribe_topic}"')

# Callback when a message is received on a subscribed topic
from datetime import datetime

def on_message(client, userdata, message):
    print(f'Received message on topic "{message.topic}": "{message.payload.decode()}"')
    payload = message.payload.decode()
    
    # Assuming payload format is "battery_voltage solar pressure node_id"
    readings = payload.split()

    readings = payload.split()

    if len(readings) == 3:  # Updated to match the new payload format
        # Extract and map battery_voltage and pressure
        battery_voltage, pressure = map(float, readings[:2])
        
        # Assign solar as 0 (independent variable)
        solar = 0.0

        # Extract the numeric part of node_id (formatted as "N<id>")
        node_id = int(readings[2][1:])  # Skip the "N" and convert the remaining part to int

        # Print or process the extracted values
        print(f"Battery Voltage: {battery_voltage}, Pressure: {pressure}, Solar: {solar}, Node ID: {node_id}")
    else:
        print("Invalid payload format!")
        # Get the current timestamp
    timestamp = datetime.now()

    # Create a document to insert into MongoDB
    document = {
        "node_id": node_id,
        "battery_voltage": battery_voltage,
        "solar": solar,
        "pressure": pressure,
        "timestamp": timestamp
    }

    # Insert the document into MongoDB
    collection.insert_one(document)
    print(f'Inserted document: {document}')


# Callback when the client encounters an error
def on_log(client, userdata, level, buf):
    print(f'MQTT Client Log: {buf}')

# Create an MQTT client instance
client = mqtt.Client()

# Set the callbacks
client.on_connect = on_connect
client.on_message = on_message
client.on_log = on_log

# Connect to the MQTT broker
client.connect(broker_ip, port, 60)

# Start the network loop in a separate thread
client.loop_start()

try:
    while True:
        # # Get the current time as a string
        # current_time = str(2)

        # # Publish the current time to the topic
        # client.publish(publish_topic, current_time)
        # print(f'Sent time: {current_time} to topic "{publish_topic}"')

        # # Wait for a 5-second interval before sending the next time
        time.sleep(500000)

except KeyboardInterrupt:
    print('Disconnected from MQTT broker')
    client.loop_stop()
    client.disconnect()