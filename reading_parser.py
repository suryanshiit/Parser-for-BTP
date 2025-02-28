import paho.mqtt.client as mqtt
import time
from pymongo import MongoClient
from datetime import datetime

# MongoDB connection settings
mongo_uri = 'mongodb://admin:mybtp@3.109.19.112:27017/'

def connect_mongo():
    """Function to establish a MongoDB connection with retry logic."""
    while True:
        try:
            client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            print("Connected to MongoDB")
            return client
        except Exception as e:
            print(f"MongoDB connection failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)

# Initialize MongoDB client with retry logic
mongo_client = connect_mongo()
db = mongo_client['sensor_data']  # Database name
collection = db['readings']  # Collection name

# MQTT broker details
broker_ip = '3.109.19.112'  # Replace with your MQTT broker IP
port = 1883  # Default MQTT port
subscribe_topic = 'sensor/reading'  # Topic to subscribe to
publish_topic = 'device/time_update'  # Topic to publish the time

# Callback when the client connects to the broker
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print('Connected to MQTT broker!')
        client.subscribe(subscribe_topic)
        print(f'Subscribed to topic "{subscribe_topic}"')
    else:
        print(f"Failed to connect to MQTT broker, return code {rc}")

# Callback when a message is received on a subscribed topic
def on_message(client, userdata, message):
    global mongo_client, collection
    print(f'Received message on topic "{message.topic}": "{message.payload.decode()}"')
    payload = message.payload.decode()
    
    readings = payload.split()
    
    if len(readings) == 3:
        battery_voltage, pressure = map(float, readings[:2])
        solar = 0.0
        node_id = readings[2][1:]
        print(f"Battery Voltage: {battery_voltage}, Pressure: {pressure}, Solar: {solar}, Node ID: {node_id}")
    else:
        print("Invalid payload format!")
        return

    timestamp = datetime.now()
    document = {
        "node_id": node_id,
        "battery_voltage": battery_voltage,
        "solar": solar,
        "pressure": pressure,
        "timestamp": timestamp
    }
    
    try:
        collection.insert_one(document)
        print(f'Inserted document: {document}')
    except Exception as e:
        print(f"MongoDB insertion error: {e}. Reconnecting...")
        mongo_client = connect_mongo()
        db = mongo_client['sensor_data']
        collection = db['readings']
        collection.insert_one(document)
        print("Reconnected and inserted document.")

# Callback when the client encounters an error
def on_log(client, userdata, level, buf):
    print(f'MQTT Client Log: {buf}')

# Create an MQTT client instance
mqtt_client = mqtt.Client()

# Set the callbacks
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.on_log = on_log

# Connect to the MQTT broker
mqtt_client.connect(broker_ip, port, 60)

# Start the network loop in a separate thread
mqtt_client.loop_start()

try:
    while True:
        time.sleep(500000)
except KeyboardInterrupt:
    print('Disconnected from MQTT broker')
    mqtt_client.loop_stop()
    mqtt_client.disconnect()
