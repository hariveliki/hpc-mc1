from kafka import KafkaConsumer
import json

class DataProcessor:


    def __init__(self):
        pass


    def connect_consumer(self, topic, server):
        try:
            consumer = KafkaConsumer(
                topic,
                auto_offset_reset="earliest",
                bootstrap_servers=[server],
                api_version=(0, 10),
                value_deserializer=json.loads,
                consumer_timeout_ms=1000
            )
            print("Connected KafkaConsumer successfully")
            return consumer
        except Exception as e:
            print("At DataProcessor.connect_consumer(): {}".format(str(e)))


    def process(data):
        pass
         

    def save_to_file(data):
        pass
