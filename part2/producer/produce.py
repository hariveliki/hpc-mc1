import pika, time, os, sys, json, random
time.sleep(10)

def produce():
    with open("data.json") as f:
        data = json.load(f)

    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
    channel.queue_declare(queue='product')

    n = 0
    while True:
        n += 1
        random_int = random.randint(0, 300)
        product = str(data[random_int])
        channel.basic_publish(exchange='', routing_key='product', body=product)
        print(" [x] Sent {}".format(n))
        time.sleep(1)


if __name__ == "__main__":
    produce()
