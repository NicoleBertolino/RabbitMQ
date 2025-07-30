import pika
import json

class MessageBroker:
    def __init__(self, host='rabbitmq', username='Nicole', password='nicole123'):
        credentials = pika.PlainCredentials(username, password)
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host, credentials=credentials)
        )
        self.channel = self.connection.channel()

    def declare_queue(self, queue_name):
        self.channel.queue_declare(queue=queue_name, durable=True)

    def publish(self, queue_name, message):
        self.channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2)  # Make message persistent
        )

    def consume(self, queue_name, callback):
        def wrapped_callback(ch, method, properties, body):
            message = json.loads(body.decode())
            callback(message)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=queue_name, on_message_callback=wrapped_callback)
        self.channel.start_consuming()

    def close(self):
        self.connection.close()
