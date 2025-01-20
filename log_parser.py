import paho.mqtt.client as mqtt
import time
from pymongo import MongoClient
from datetime import datetime

mongo_uri = 'mongodb://admin:mybtp@3.109.19.112:27017/'
client = MongoClient(mongo_uri)
db = client['sensor_data']  # Database name
collection = db['logs']  # Collection name

# MQTT broker details
broker_ip = '3.109.19.112'  # Replace with your MQTT broker IP
port = 1883  # Default MQTT port
subscribe_topic = 'deviceLogs'  # Topic to subscribe to
publish_topic = 'device/time_update'  # Topic to publish the time

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
    
    # Split the payload by ":"
    readings = payload.split(": ")

    if len(readings) == 2:
        # Extract node_id and message
        node_id = int(readings[0][1:])  # Skip "N" and convert the numeric part to int
        message = readings[1]  # Extract the message part

        # Print or process the extracted values
        print(f"Node ID: {node_id}, Message: '{message}'")
    else:
        print("Invalid payload format!")

    timestamp = datetime.now()
    # Create a document to insert into MongoDB
    document = {
        "node_id": node_id,
        "message": message,
        "timestamp": timestamp
    }

    # Print the document (or insert it into MongoDB)
    print("Document to insert:", document)


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
        # Get the current time as a string
        current_time = str(2)

        # Publish the current time to the topic
        client.publish(publish_topic, current_time)
        print(f'Sent time: {current_time} to topic "{publish_topic}"')

        # Wait for a 5-second interval before sending the next time
        time.sleep(500000)

except KeyboardInterrupt:
    print('Disconnected from MQTT broker')
    client.loop_stop()
    client.disconnect()