"""
Emits task messages to a RabbitMQ queue from a tasks.csv file.  

Author: Pranali Baban Dhobale
Date: 05/24/2024
"""

import csv
import pika
import sys
import webbrowser

def prompt_for_admin_panel():
    """
    Ask the user if they want to open the RabbitMQ admin panel.
    This is useful for monitoring queues and seeing how tasks are processed.
    """
    response = input("Open RabbitMQ admin panel to monitor queues? (y/n): ")
    if response.lower() == 'y':
        webbrowser.open("http://localhost:15672/#/queues")
        print("Admin panel opened.")

def publish_message(server_host, target_queue, content):
    """
    Connect to RabbitMQ server, send a single message to a specified queue,
    and then close the connection. 
    """
    connection = pika.BlockingConnection(pika.ConnectionParameters(server_host))
    channel = connection.channel()
    channel.queue_declare(queue=target_queue, durable=True)
    channel.basic_publish(exchange='', routing_key=target_queue, body=content)
    print(f"Sent: {content}")
    connection.close()

def process_and_send_tasks(csv_file, host, queue):
    """
    Read Messages from a tasks.csv file and send them to a RabbitMQ queue.
    Each task is read from the file and sent as a separate message.
    """
    with open(csv_file, mode='r', newline='') as file:
        tasks = csv.reader(file)
        for task in tasks:
            publish_message(host, queue, ' '.join(task))

if __name__ == "__main__":
    prompt_for_admin_panel()
    process_and_send_tasks('tasks.csv', 'localhost', 'task_processing_queue')