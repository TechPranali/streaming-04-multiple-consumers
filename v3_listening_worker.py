"""
Continuously listens for messages in a RabbitMQ queue and processes them. More workers
can be added by starting multiple instances of this script.

Author: Pranali Baban Dhobale
Date: 05/24/2024
"""

import pika
import time
import sys


def handle_message(ch, method, properties, body):
    """
    Callback function to process received messages.
    """
    task = body.decode()
    print(f"Received: {task}")
    time.sleep(task.count('.'))
    print("Task completed.")
    ch.basic_ack(delivery_tag=method.delivery_tag)

def setup_worker(host='localhost', queue='task_processing_queue'):
    """
    Set up the RabbitMQ consumer on a specified queue.
    This involves establishing a connection, setting up a channel, declaring the queue,
    and starting the message consumption process.
    """
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        channel = connection.channel()
        channel.queue_declare(queue=queue, durable=True)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=queue, on_message_callback=handle_message)
        print("Worker is ready for tasks. To exit press CTRL+C")
        channel.start_consuming()
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    setup_worker()