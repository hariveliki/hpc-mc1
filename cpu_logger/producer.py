from kafka import KafkaConsumer, KafkaProducer
import time
import json
import random


class Producer:


    def __init__(self, server: str) -> None:
        self.server = server


    def connect(self):
        producer = None
        try:
            producer = KafkaProducer(
                bootstrap_servers = self.server,
                api_version = (0, 10)
            )
            self.producer_instance = producer
            print("Successfully connected to Kafka Server")
        except Exception as e:
            print("At Producer.connect_kafka_producer(): {}".format(str(e)))


    def produce(
        self,
        topic: str,
        value = None
    ):
        if not value:
            value = random.randint(0, 100)
        message_value = "Marcus Aurelius {}".format(value)
        message = json.dumps({"name" : message_value})
        key = str(round(time.time()))
        self.publish(
            topic,
            key,
            message
        )
         

    def publish(
            self,
            topic: str,
            key: str,
            message: str
    ):
        try:
            key_bytes = bytes(key, encoding="utf-8")
            value_bytes = bytes(message, encoding="utf-8")
            self.producer_instance.send(
                topic,
                key=key_bytes,
                value=value_bytes
            )
            self.producer_instance.flush()
            print("Message published successfully")
        except Exception as e:
            print("At Producer.publish_message(): {}".format(str(e)))





        
