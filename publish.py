import paho.mqtt.client as mqtt

# MQTT broker details
broker_ip = '3.109.19.112'  # Replace with your MQTT broker IP
port = 1883  # Default MQTT port
subscribe_topic = 'sensor/reading'  # Topic to subscribe to
publish_topic = 'deviceLogs'  # Topic to publish to

# Callback when the client receives a connection response from the broker
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker successfully!")
        # Subscribe to the topic
        client.subscribe(subscribe_topic)
        print(f"Subscribed to topic: {subscribe_topic}")
    else:
        print(f"Failed to connect, return code {rc}")

# Callback when a message is received on the subscribed topic
def on_message(client, userdata, msg):
    print(f"Received message: '{msg.payload.decode()}' on topic: '{msg.topic}'")

# Create an MQTT client instance
client = mqtt.Client()

# Assign callback functions
client.on_connect = on_connect
client.on_message = on_message

# Connect to the broker
print("Connecting to broker...")
client.connect(broker_ip, port, keepalive=60)

# Publish a test message
test_payload = "N4: Great Work Machake!"
client.publish(publish_topic, test_payload)
print(f"Published message: '{test_payload}' to topic: '{publish_topic}'")

# Start the loop to process network traffic and handle reconnects
client.loop_start()

try:
    # Keep the script running to receive messages
    while True:
        pass
except KeyboardInterrupt:
    print("\nDisconnecting...")
    client.loop_stop()
    client.disconnect()
