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
    default="text_queue",
    help="The name of the RabbitMQ queue to consume text data from",
)
parser.add_argument(
    "-o",
    "--output_directory",
    default="received_texts",
    help="Directory to save received text files",
)

args = parser.parse_args()

def callback(ch, method, properties, body):
    word = body.decode('utf-8')
    word_filename = f"word_{method.delivery_tag}.txt"
    output_path = os.path.join(args.output_directory, word_filename)
    with open(output_path, 'w') as f:
        f.write(word)
    print(f"Received and saved word: {word}")

def main():
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