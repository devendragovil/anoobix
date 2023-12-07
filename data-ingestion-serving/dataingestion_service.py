import time, os, random
import requests
from google.transit import gtfs_realtime_pb2

BART_LINK = 'https://api.bart.gov/gtfsrt/tripupdate.aspx'

from dotenv import load_dotenv

# Confluent Kafka client libraries for producing messages and managing Kafka topics
from confluent_kafka.admin import AdminClient, NewTopic  # Kafka administration (topics management)
from confluent_kafka import SerializingProducer  # Kafka producer with serialization capabilities
from confluent_kafka.serialization import StringSerializer  # Serializer for message keys
from confluent_kafka.schema_registry import SchemaRegistryClient  # For schema registry interactions
from confluent_kafka.schema_registry.avro import AvroSerializer  # Avro serializer for message values


import kafka_schemas  # Avro schema definitions for Kafka message serialization
from data_ingestion_schema import TripMessageModel

load_dotenv(verbose=True)

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

KAFKA_CONFIG = {
        "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS"),
        "security.protocol": os.getenv("SECURITY_PROTOCOL"),
        'sasl.mechanisms': os.environ['SASL_MECHANISMS'],
        'sasl.username': os.environ['SASL_USERNAME'],
        'sasl.password': os.environ['SASL_PASSWORD'],
}

admin_client = AdminClient(KAFKA_CONFIG)
topics = [
    NewTopic(topic=os.getenv("TOPIC_NAME"), num_partitions=3, replication_factor=3)
]
result = admin_client.create_topics(topics)

for topic, j in result.items():
    try:
        j.result()
        print(f"Successfully created the topic: {topic}")
    except TypeError as e_type:
        print(
            f"""
            Failed to create Kafka topics, there was a type error raised.

            More details:
            {e_type.__repr__()}

            Please investigate.
            """
        )
    except ValueError as e_val:
        print(
            f"""
            Failed to create Kafka topics, there was a value error raised.

            More details:
            {e_val.__repr__()}

            Please investigate.
            """
        )
    except Exception as e:
        if e.args[0].name() == 'TOPIC_ALREADY_EXISTS':
            print(f'Topic - {topic} already exists.')
        else:
            print(
                f"""
                Failed to create Kafka topics, there was an exception raised.

                More details:
                {e.__repr__()}

                Please investigate.
                """
            )

try:
    schema_registry_client = SchemaRegistryClient(
        {
            "url": os.getenv("schema.registry.url"),
            "basic.auth.user.info": os.getenv("basic.auth.user.info"),
        }
    )
    value_serializer = AvroSerializer(
        schema_str=kafka_schemas.trip_update_schema,
        schema_registry_client=schema_registry_client,
        to_dict= lambda x, ctx: x.dict(by_alias=True)
    )

    producer = SerializingProducer({
        **(KAFKA_CONFIG),
        "key.serializer": StringSerializer('utf_8'),
        "value.serializer": value_serializer,
        'acks': 'all'
    })
except Exception as e:
    print(
        f"""
        Failed to create producer, there was an exception raised.

        More details:
        {e.__repr__()}

        Please investigate.
        """
    )

class ProducerCallback:
    """
    Callback class for Kafka producer to handle message delivery reports.
    """
    def __call__(self, err, msg):
        if err is not None:
            logger.error(f"Delivery failed for record: {err}")
        else:
            logger.info(f"Record delivered to {msg.topic()}")
            logger.info(f"Message delivered with Key: {msg.key()}")

pc1 = ProducerCallback()

while True:
    time.sleep(5)
    feed = gtfs_realtime_pb2.FeedMessage()
    response = requests.get(BART_LINK)
    feed.ParseFromString(response.content)
    timestamp_id = str(feed.header.timestamp)
    message_string = feed.SerializeToString()

    trip_message = TripMessageModel(timestamp_id=timestamp_id, message=message_string)
    producer.produce(
        topic=os.getenv("TOPIC_NAME"),
        key=f'trip-details-{timestamp_id}',
        value=trip_message,
        on_delivery=pc1
    )
    producer.flush()