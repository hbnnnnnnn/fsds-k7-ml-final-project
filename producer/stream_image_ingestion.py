import os
import argparse
import pika

parser = argparse.ArgumentParser()
parser.add_argument(
    "-d",
    "--directory",
    default="images",
    help="Directory containing image files to send to RabbitMQ",
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
    default="image_queue",
    help="The name of the RabbitMQ queue to send images to",
)

args = parser.parse_args()

def send_image(channel, queue_name, image_path):
    with open(image_path, 'rb') as f:
        image_data = f.read()
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=image_data,
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            ))
        print(f"Sent image: {image_path}")

def create_queue(channel, queue_name):
    channel.queue_declare(queue=queue_name, durable=True)

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=args.rabbitmq_server))
    channel = connection.channel()
    create_queue(channel, args.queue_name)

    for image_file in os.listdir(args.directory):
        image_path = os.path.join(args.directory, image_file)
        if os.path.isfile(image_path):
            send_image(channel, args.queue_name, image_path)

    connection.close()

if __name__ == "__main__":
    main()