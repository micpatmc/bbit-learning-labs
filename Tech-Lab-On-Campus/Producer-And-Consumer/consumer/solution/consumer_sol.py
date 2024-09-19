from consumer_interface import mqConsumerInterface
import pika
import os

class mqConsumer(mqConsumerInterface):
    def __init__(self, binding_key: str, exchange_name: str, queue_name: str) -> None:
        # body of constructor
        self.binding_key = binding_key
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.setupRMQConnection()


    def setupRMQConnection(self) -> None:
        conParams = pika.URLParameters(os.environ['AMQP_URL'])
        connection = pika.BlockingConnection(parameters=conParams)
        channel = connection.channel()
        channel.queue_declare(queue=self.queue_name)
        channel.exchange_declare(exchange=self.exchange_name)
        channel.queue_bind(queue=self.queue_name, exchange=self.exchange_name, routing_key=self.binding_key)
        channel.basic_consume(queue=self.queue_name, on_message_callback=self.on_message_callback, auto_ack=True)

    def on_message_callback(
        self, channel, method_frame, header_frame, body
    ) -> None:
        # Acknowledge message
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)

        #Print message (The message is contained in the body parameter variable)
        print(body)

    def startConsuming(self) -> None:
        # Print " [*] Waiting for messages. To exit press CTRL+C"
        print(" [*] Waiting for messages. To exit press CTRL+C")
        # Start consuming messages
        conParams = pika.URLParameters(os.environ['AMQP_URL'])
        connection = pika.BlockingConnection(parameters=conParams)
        channel = connection.channel()
        channel.start_consuming()
    
    def __del__(self) -> None:
        # Print "Closing RMQ connection on destruction"
        print("Closing RMQ connection on destruction")
        conParams = pika.URLParameters(os.environ['AMQP_URL'])
        connection = pika.BlockingConnection(parameters=conParams)
        channel = connection.channel()
        # Close Channel
        channel.close()
        # Close Connection
        connection.close()