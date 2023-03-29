from kafka import KafkaConsumer, KafkaProducer
import atexit
import json
import uuid
import random
import time
import cProfile
import os
import pstats
from pstats import SortKey
import csv
time.sleep(10)


def connect_kafka_producer(servers):
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=servers, api_version=(0, 10))
    except Exception as e:
        print('Exception while connecting Kafka')
        print(str(e))
    finally:
        return _producer


def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as e:
        print('Exception in publishing message')
        print(str(e))


def produce_xy(producer, topic_name):
    with open("data.json") as file:
        data = json.load(file)
    n_item = 1
    while True:
        random_int = random.randint(0, 300)
        product = data[random_int]
        message = json.dumps({"product": product})
        publish_message(producer, topic_name, str(uuid.uuid4()), message)
        print("Published items so far: {}".format(n_item))
        time.sleep(1)
        n_item += 1


if __name__ == "__main__":
    servers = ['broker1:9093', 'broker2:9094', 'broker3:9095']
    topic = "products"
    producer = connect_kafka_producer(servers)
    consume_stats = [
        ["run", "loops", "time", "calls"]
    ]
    for run in range(0, 10):
        for loop in range(0, 10):
            cProfile.run("produce_xy(producer, topic)", 'data_generator_stats')
            stats = pstats.Stats('data_generator_stats')
            total_time = stats.total_tt
            ncalls = stats.total_calls
            consume_stats.append([run, loop, total_time, ncalls])

    with open('consume_stats.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerows(consume_stats)

