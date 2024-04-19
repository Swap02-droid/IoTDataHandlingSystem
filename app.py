import paho.mqtt.client as mqtt
import pika
from pymongo import MongoClient

from dotenv import load_dotenv
import logging
import os


# Configure logging to write to a file
load_dotenv()
logging.basicConfig(level=logging.INFO, filename='mqtt_rabbitmq.log', format='%(asctime)s - %(levelname)s - %(message)s')

# MQTT Settings
MQTT_BROKER_HOST = os.getenv("MQTT_BROKER_HOST")
MQTT_BROKER_PORT = os.getenv("MQTT_BROKER_PORT")
MQTT_TOPIC = os.getenv("MQTT_TOPIC")

# RabbitMQ Settings
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = os.getenv("RABBITMQ_PORT")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE")

# MongoDB Settings
MONGO_DB_HOST = os.getenv("MONGO_DB_HOST")
MONGO_DB_PORT = os.getenv("MONGO_DB_PORT")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
MONGO_COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME")

def on_message(client, userdata, message):
    logging.info("Inside on_message function")
    try:
        # Connect to RabbitMQ
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT))
        channel = connection.channel()

        # Declare the queue and Publish the message to RabbitMQ
        channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
        channel.basic_publish(exchange='', routing_key=RABBITMQ_QUEUE, body=message.payload)

        # Close RabbitMQ connection
        connection.close()

        logging.info(f"Message received: {message.payload}")
    except Exception as e:
        logging.error(f"Error processing message: {e}")

def on_connect(client, userdata, flags, rc):
    logging.info("Inside on_connect function")
    logging.info("Connected with result code " + str(rc))
    client.subscribe(MQTT_TOPIC)

def on_disconnect(client, userdata, rc):
    logging.info("Inside on_disconnect function")
    logging.info("Disconnected with result code " + str(rc))

# Set up MQTT client
mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.on_disconnect = on_disconnect

# Connect to MQTT broker
mqtt_client.connect(MQTT_BROKER_HOST, int(MQTT_BROKER_PORT), 60)

# Start the MQTT loop
mqtt_client.loop_start()

# MongoDB client
mongo_client = MongoClient(MONGO_DB_HOST, int(MONGO_DB_PORT))
db = mongo_client[MONGO_DB_NAME]


# Set up RabbitMQ consumer
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, port=int(RABBITMQ_PORT)))
channel = connection.channel()
channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

def process_message(channel, method, properties, body):
    logging.info("Inside process_message function")
    try:
        # Store the message in MongoDB
        collection = db[MONGO_COLLECTION_NAME]
        collection.insert_one({"topic": method.routing_key, "payload": body.decode()})
    except Exception as e:
        logging.error(f"Error processing message: {e}")

    logging.info(f"Message processed: {body.decode()}")
channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=process_message, auto_ack=True)

# Start consuming messages
channel.start_consuming()
