import pika, sys, os, time, json
time.sleep(10)


def write_json_to_file(filepath, content):
    with open(filepath, "w") as file:
        json.dump(content, file)

def fibonacci(n):
    """Computes the nth-fibonacci number. It is used to create artificially longer runtime for the processor"""
    if n <= 1:
        return 1
    return fibonacci(n-1) + fibonacci(n-2)


def process_data():
    print("function called")
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
    channel.queue_declare(queue='product')

    global counter
    counter = 0
    file = open("/data/changed_data.txt", "a")
    def callback(ch, method, properties, body):
        global counter
        if counter >= 10:
            counter = 0
        product = eval(body.decode("utf-8"))
        keys = list(product.keys())
        for key in keys:
            product[key] = fibonacci(counter)
        file.write(str(product) + "\n")
        assert body
        print(" [x] Received body")
        counter += 1
    
    channel.basic_consume(queue='product', on_message_callback=callback, auto_ack=True)
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()
    connection.close()

    file.close()


if __name__ == '__main__':
    process_data()


