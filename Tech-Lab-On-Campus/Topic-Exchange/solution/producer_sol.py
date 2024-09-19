from producer_interface import mqProducerInterface
import pika
import os
import sys

class mqProducer(mqProducerInterface):
    routing_key = ""
    exchange_name = ""
    channel = ""

    def __init__(self, routing_key: str, exchange_name: str) -> None:
        # Save parameters to class variables
        self.routing_key = routing_key
        self.exchange_name = exchange_name

        # Call setupRMQConnection
        self.setupRMQConnection()

        pass

    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service
        connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))

        # Establish Channel
        self.channel = self.connection.channel()

        # Create the topic exchange if not already present
        exchange = self.channel.exchange_declare(exchange=self.exchange_name, exchange_type="topic")

        pass

    def publishOrder(self, message: str) -> None:
        # Basic Publish to Exchange
        # self.channel.basic_publish(
            # exchange=self.exchange_name,
            # routing_key=self.routing_key,
            # body="Hello World",
        # )

        # Create Appropiate Topic String

        # Send serialized message or String
        print("Name of the program using sys.argv[0]: ", sys.argv[0])
        print("Length of arguments given including program name: ", len(sys.argv))
        print("Argument list: ", sys.argv)
        print("Argument list type: ", type(sys.argv))
        print("Give the first argument (after program name): ", sys.argv[1])

        routing_key = sys.argv[1] if len(sys.argv) > 2 else 'anonymous.info'
        message = ' '.join(sys.argv[2:]) or 'Hello World!'
        channel.basic_publish(
            exchange='topic_logs', routing_key=routing_key, body=message)
        print(f" [x] Sent {routing_key}:{message}")

        # Close Channel
        self.channel.close()

        # Close Connection
        self.connection.close()
    
        pass