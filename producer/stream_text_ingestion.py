import os
import argparse
import pika

parser = argparse.ArgumentParser()
parser.add_argument(
    "-d",
    "--directory",
    default="texts",
    help="Directory containing text files to send to RabbitMQ",
)
parser.add_argument(
    "-b",
    "--rabbitmq_server",
    default="localhost",
    help="Where the RabbitMQ server is",
)
parser.add_argument(
    "-q",
    "--queue_name",
    default="text_queue",
    help="The name of the RabbitMQ queue to send text data to",
)

args = parser.parse_args()

def send_word(channel, queue_name, word):
    channel.basic_publish(
        exchange='',
        routing_key=queue_name,
        body=word,
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        ))
    print(f"Sent word: {word}")

def create_queue(channel, queue_name):
    channel.queue_declare(queue=queue_name, durable=True)

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=args.rabbitmq_server))
    channel = connection.channel()
    create_queue(channel, args.queue_name)

    for text_file in os.listdir(args.directory):
        text_path = os.path.join(args.directory, text_file)
        if os.path.isfile(text_path):
            with open(text_path, 'r') as f:
                text_data = f.read()
                words = text_data.split()
                for word in words:
                    send_word(channel, args.queue_name, word)

    connection.close()

if __name__ == "__main__":
    main()