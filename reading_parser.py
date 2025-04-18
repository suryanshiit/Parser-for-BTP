import paho.mqtt.client as mqtt
import time
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime

# InfluxDB 2.x configuration
influx_url = "https://us-east-1-1.aws.cloud2.influxdata.com"
influx_token = "psFxR835CKkYwdTMJYcmT4pVpY4jcpfkfQSe8eRfO382LW-WYboRy3crsCSiHjYF7GYJgi5ScHyYm-q1e0EISQ=="
influx_org = "IIT KGP"
influx_bucket = "sensor_data"  # Ensure this bucket exists in your InfluxDB

# Function to connect to InfluxDB with retry logic
def connect_influx():
    while True:
        try:
            client = InfluxDBClient(
                url=influx_url,
                token=influx_token,
                org=influx_org,
                timeout=5000
            )
            client.ping()
            print("✅ Connected to InfluxDB")
            return client
        except Exception as e:
            print(f"❌ InfluxDB connection failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)

# Initialize InfluxDB client
influx_client = connect_influx()
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

# MQTT broker configuration
broker_ip = '3.109.19.112'
port = 1883
subscribe_topic = 'sensor/reading'

# MQTT on_connect callback
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print('✅ Connected to MQTT broker!')
        client.subscribe(subscribe_topic)
        print(f'📡 Subscribed to topic "{subscribe_topic}"')
    else:
        print(f"❌ Failed to connect to MQTT broker, return code {rc}")

# MQTT on_message callback
def on_message(client, userdata, message):
    global write_api
    print(f'📥 Received message on "{message.topic}": "{message.payload.decode()}"')
    payload = message.payload.decode()

    readings = payload.split()

    if len(readings) == 3:
        battery_voltage, pressure = map(float, readings[:2])
        solar = 0.0  # Placeholder; you can extract this too if needed
        node_id = readings[2][1:]  # remove prefix like "N12" → "12"
        print(f"🔋 Voltage: {battery_voltage}, 💧 Pressure: {pressure}, ☀ Solar: {solar}, 🆔 Node: {node_id}")
    else:
        print("❗ Invalid payload format!")
        return

    try:
        point = (
            Point("sensor_reading")
            .tag("node_id", node_id)
            .field("battery_voltage", battery_voltage)
            .field("pressure", pressure)
            .field("solar", solar)
            .time(datetime.utcnow())
        )
        write_api.write(bucket=influx_bucket, org=influx_org, record=point)
        print("✅ Data written to InfluxDB")
    except Exception as e:
        print(f"❌ InfluxDB write error: {e}. Reconnecting...")
        influx_client = connect_influx()
        write_api = influx_client.write_api(write_options=SYNCHRONOUS)

# MQTT log callback
def on_log(client, userdata, level, buf):
    print(f'🔍 MQTT Log: {buf}')

# Initialize and start MQTT client
mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.on_log = on_log

mqtt_client.connect(broker_ip, port, 60)
mqtt_client.loop_start()

# Keep running
try:
    while True:
        time.sleep(500000)
except KeyboardInterrupt:
    print('👋 Disconnected from MQTT broker')
    mqtt_client.loop_stop()
    mqtt_client.disconnect()