import logging, random, os, kafka_schemas, asyncio, time
from dotenv import load_dotenv
from data_ingestion_schema import TripMessageModel
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.error import KafkaError
from google.transit import gtfs_realtime_pb2
import boto3
from typing import List
import uuid
import json


load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

logger.info("Starting the script...")

KAFKA_CONFIG = {
        "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS"),
        "security.protocol": os.getenv("SECURITY_PROTOCOL"),
        'sasl.mechanisms': os.environ['SASL_MECHANISMS'],
        'sasl.username': os.environ['SASL_USERNAME'],
        'sasl.password': os.environ['SASL_PASSWORD'],
}

schema_registry_client = SchemaRegistryClient(
    {
        "url": os.getenv("schema.registry.url"),
        "basic.auth.user.info": os.getenv("basic.auth.user.info"),
    }
)
value_deserializer = AvroDeserializer(
    schema_str=kafka_schemas.trip_update_schema,
    schema_registry_client=schema_registry_client,
    from_dict=lambda data, ctx: TripMessageModel(**data)
)
consumer_config = {
    **(KAFKA_CONFIG),
    'key.deserializer': StringDeserializer('utf_8'),
    'value.deserializer': value_deserializer,
    'group.id': os.environ['CONSUMER_GROUP_ID'],
    'auto.offset.reset': 'latest'
}
consumer = DeserializingConsumer(consumer_config)

topic_initializing = os.getenv("TOPIC_NAME")
logger.info(f"Initializing Consumption for topic: {topic_initializing}")
consumer.subscribe([topic_initializing])

bt3s = boto3.Session(region_name='us-west-2',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
)
ddb = bt3s.client('dynamodb')

def processing_feed(feed: gtfs_realtime_pb2.FeedMessage) -> None:
    # Extracting timestamp
    timestamp_id = str(feed.header.timestamp)

    # Iterating over each entity
    for entity in feed.entity:
        trip_id = entity.id
        stop_time_update_dict = {
            'stop_id': [],
            'delay': [],
            'time_expected': []

        }

        # Iterating over each stop_time_update of each entity
        for stop_time_update in entity.trip_update.stop_time_update:
            # Extracting values for required fields
            stop_id = stop_time_update.stop_id
            delay = stop_time_update.arrival.delay
            time_expected = stop_time_update.arrival.time

            # Appending to dict for producing a single dict for an entity to speed up code
            stop_time_update_dict['delay'].append(delay)
            stop_time_update_dict['stop_id'].append(stop_id)
            stop_time_update_dict['time_expected'].append(time_expected)
        
        # Serialize the dictionary to a string representation for JSON.
        stop_time_update_dict_string = json.dumps(stop_time_update_dict)
        
        uuid_val = str(uuid.uuid4()) + str(uuid.uuid4())
        item = {
            'uuid_val': {
                'S': uuid_val
            },
            'timestamp_id': {
                'S': timestamp_id
            },
            'trip_id': {
                'S': trip_id
            },
            'stop_time_update_dict': {
                'S': stop_time_update_dict_string
            }
        }
        ddb.put_item(
            Item=item,
            TableName='trip-stream'
        )

logger.info("Starting consumption...")

try:
    while True:
        msg = consumer.poll(0.2)
        logger.info("Polling completed...")
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                logger.info('End of partition reached {0}/{1}'
                            .format(msg.topic(), msg.partition()))
            else:
                logger.error(msg.error())
            continue
        
        # Parsing message
        trip_data = msg.value().model_dump()
        timestamp_id = trip_data['timestamp_id']
        message_string = trip_data['message']

        # Parsing message string from the protobuf format
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(message_string)
        print(time.ctime(int(timestamp_id)))

        # Processing feed for insertion in DynamoDB
        processing_feed(feed=feed)
        logger.info(f'Processed Message : {timestamp_id}')
except KeyboardInterrupt:
    logger.info("Consumer interrupted by the user.")
finally:
    # Close down consumer to commit final offsets.
    consumer.close()
    logger.info("Consumer consuming the trip stream is closed.")