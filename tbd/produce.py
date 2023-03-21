from kafka import KafkaConsumer, KafkaProducer
import json
import uuid
import random
import time
import cProfile
import os
import pstats
from pstats import SortKey


def connect_kafka_producer(servers):
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=servers, api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer


def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def produce_xy(producer, topic_name):
    cwd = os.getcwd()
    with open("data.json") as file:
        data = json.load(file)
    n = 0
    while data:
        product = data.pop()
        message = json.dumps({"product": product})
        print("Published items: {}".format(n))
        publish_message(producer, topic_name, str(uuid.uuid4()), message)
        time.sleep(1)
        n += 1


if __name__ == "__main__":
    servers = ['broker1:9093', 'broker2:9094', 'broker3:9095']
    topic1 = "products"
    producer = connect_kafka_producer(servers)
    produce_xy(producer, topic1)

# producer = connect_kafka_producer(servers)

# cProfile.run("produce_xy(producer, topic1)", "produce_stats")

# p = pstats.Stats("produce_stats")
# p.sort_stats(SortKey.TIME)
# p.print_stats(10)