import pika

def create_channel(queue_name):
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
    channel =  connection.channel()
    channel.queue_declare(queue = queue_name)
    return channel

