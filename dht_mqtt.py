import time
import adafruit_dht
import board
import os
from dotenv import load_dotenv
import paho.mqtt.client as mqtt
import json

load_dotenv()

dht_device = adafruit_dht.DHT11(board.D4)

def initiate_mqtt_connection():
    broker_address = os.getenv("BROKER_ADDRESS")
    broker_port = 8883

    cert_dir = os.getenv("CERTIFICATE_DIR")
    ca_cert = f"{cert_dir}/ca.crt"
    client_cert = f"{cert_dir}/client.crt"
    client_key = f"{cert_dir}/client.key"

    username = os.getenv("MQTT_USERNAME")
    password = os.getenv("MQTT_PASSWORD")

    client = mqtt.Client()
    client.tls_set(ca_certs=ca_cert, certfile=client_cert, keyfile=client_key)
    client.username_pw_set(username, password)
    client.connect(broker_address, broker_port)

    return client

def report_dht(client, temp, humid):
    topic = "smarthome/dht11"
    data = {
        "temperature": temp,
        "humidity": humid
    }
    msg = json.dumps(data)

    try:
        client.publish(topic, msg)
        print(f"Published message: {msg} to topic: {topic}")
    except err:
        print(err.args[0])

client = initiate_mqtt_connection()
try:
    while True:
        try:
            temperature_c = dht_device.temperature
            humidity = dht_device.humidity
            report_dht(client, temperature_c, humidity)
        except RuntimeError as err:
            print(err.args[0])

        time.sleep(2)
except KeyboardInterrupt:
    print("Program stopped by KeyboardInterrupt.")
    client.disconnect()
    

