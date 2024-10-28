import os
import argparse
import pika

parser = argparse.ArgumentParser()
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
    help="The name of the RabbitMQ queue to consume images from",
)
parser.add_argument(
    "-o",
    "--output_directory",
    default="received_images",
    help="Directory to save received image files",
)

args = parser.parse_args()

def callback(ch, method, properties, body):
    """
    Callback function to handle incoming messages from RabbitMQ.

    This function is triggered when a message is received. It saves the message
    body as an image file in the specified output directory.

    Args:
        ch: The channel object.
        method: The method frame with delivery information.
        properties: The properties of the message.
        body: The body of the message, which is expected to be the image data.

    Returns:
        None
    """
    image_name = f"image_{method.delivery_tag}.jpg"
    output_path = os.path.join(args.output_directory, image_name)
    with open(output_path, 'wb') as f:
        f.write(body)
    print(f"Received and saved image: {output_path}")

def main():
    """
    Main function to set up and start consuming messages from a RabbitMQ queue.

    This function performs the following steps:
    1. Checks if the output directory specified in the arguments exists, and creates it if it does not.
    2. Establishes a connection to the RabbitMQ server specified in the arguments.
    3. Declares a queue with the name specified in the arguments, ensuring it is durable.
    4. Sets up a consumer to listen for messages on the specified queue and processes them using the provided callback function.
    5. Starts consuming messages and prints a message indicating it is waiting for messages.

    Args:
        args (Namespace): A namespace object containing the following attributes:
            - output_directory (str): The directory where output files will be saved.
            - rabbitmq_server (str): The hostname of the RabbitMQ server.
            - queue_name (str): The name of the RabbitMQ queue to consume messages from.
            - callback (function): The callback function to process messages from the queue.
    """
    if not os.path.exists(args.output_directory):
        os.makedirs(args.output_directory)

    connection = pika.BlockingConnection(pika.ConnectionParameters(host=args.rabbitmq_server))
    channel = connection.channel()
    channel.queue_declare(queue=args.queue_name, durable=True)

    channel.basic_consume(
        queue=args.queue_name,
        on_message_callback=callback,
        auto_ack=True
    )

    print(f"Waiting for messages in queue: {args.queue_name}. To exit press CTRL+C")
    channel.start_consuming()

if __name__ == "__main__":
    main()