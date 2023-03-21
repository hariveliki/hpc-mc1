from kafka import KafkaConsumer
import json

class DataProcessor:


    def __init__(self):
        pass


    def connect_consumer(self, topic, servers):
        try:
            consumer = KafkaConsumer(
                topic,
                auto_offset_reset="earliest",
                bootstrap_servers=servers,
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

    
    def consume(consumer):
        n = 1
        for msg in consumer:
            message = msg.value.get("product")
            print("Message consumed: {}".format(message))
            print("n message: {}".format(n))


if __name__ == "__main__":
    data_processor = DataProcessor()
    servers = ['broker1:9093', 'broker2:9094', 'broker3:9095']
    topic = "products"
    consumer = data_processor.connect_consumer(topic, servers)
    data_processor.consume(consumer)