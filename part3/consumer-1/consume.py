from kafka import KafkaConsumer
import json, time


def connect_consumer(topic, servers):
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
        print("At connect_consumer(): {}".format(str(e)))


def consume(consumer):
    n = 1
    print("Before looping trough consumer iterator")
    for msg in consumer:
        print("Start consuming")
        message = msg.value.get("product")
        print("Message consumed: {}".format(message.keys()))
        print("n message: {}".format(n))
        n += 1
        time.sleep(1)


if __name__ == "__main__":
    time.sleep(5)
    servers= ['broker1:9093', 'broker2:9094', 'broker3:9095']
    topic = "products"
    consumer = connect_consumer(topic, servers)
    consume(consumer)