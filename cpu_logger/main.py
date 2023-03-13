import time
import random
import json
from producer import Producer
from data_processor import DataProcessor


def get_data_from_file(filepath, chunk_size):
    with open("file/to/path") as file:
        data = json.load(file)
        for item in data:
            yield item


server_1 = "broker1:9093"
topic_1 = "names"
kafka_1 = Producer(server_1)
kafka_1.connect()


def send_data_for_time(duration):
    start = time.time()
    while time.time() - start < duration:
        time.sleep(1)
        yield random.randint(0, 100)


print("Start Producing")
for data in send_data_for_time(10):
    kafka_1.produce(
        topic="name",
        value=data
    )


print("Start connect KafkaConsumer")
data_processor = DataProcessor()
consumer = data_processor.connect_consumer(topic_1, server_1)
print("Start consuming")
print("Type of consumer: {}".format(type(consumer)))
for msg in consumer:
    print(msg)
    print(msg.key.decode("utf-8"), msg.value)


