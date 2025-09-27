import argparse
import json
import os
from datetime import datetime
from time import sleep
import pandas as pd
import pika


parser = argparse.ArgumentParser()
parser.add_argument(
    "-m",
    "--mode",
    default="setup",
    choices=["setup", "teardown"],
    help="Whether to setup or teardown a RabbitMQ queue with driver stats events.",
)
parser.add_argument(
    "-b",
    "--rabbitmq_server",
    default="localhost",
    help="Where the RabbitMQ server is",
)
parser.add_argument(
    "-u",
    "--user",
    default="guest",
    help="RabbitMQ username (default: guest)",
)
parser.add_argument(
    "-p",
    "--password",
    default="guest",
    help="RabbitMQ password (default: guest)",
)
parser.add_argument(
    "-q",
    "--queue_name",
    default="alicpp_records",
    help="The name of the queue to use",
)
parser.add_argument(
    "-n",
    "--num_records",
    type=int,
    default=0,
    help="Number of records to send (0 = unlimited, continuous streaming)",
)
parser.add_argument(
    "-i",
    "--interval",
    type=float,
    default=2.0,
    help="Interval between records in seconds (default: 2.0)",
)

args = parser.parse_args()

def load_demo_data():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    csv_paths = [
        os.path.join(script_dir, "credit_card_demo.csv"),
        "producer/credit_card_demo.csv"
    ]
    
    for csv_path in csv_paths:
        try:
            df = pd.read_csv(csv_path)
            print(f"Loaded {len(df)} demo records from {csv_path}")
            return df
        except FileNotFoundError:
            continue

    raise FileNotFoundError("No demo data file found.")


def create_queue(channel, queue_name):
    channel.queue_declare(queue=queue_name, durable=True)
    print(f"Queue {queue_name} is ready.")


def create_streams(server, queue_name, username, password):
    connection = None
    channel = None

    for attempt in range(10):
        try:
            creds = pika.PlainCredentials(username, password)
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=server, port=5672, credentials=creds)
            )
            channel = connection.channel()
            print("Connected to RabbitMQ")
            break
        except Exception as e:
            print(f"Connection attempt {attempt+1} failed: {e}")
            sleep(5)

    if not channel:
        raise RuntimeError("Could not connect to RabbitMQ after 10 attempts.")

    create_queue(channel, queue_name=queue_name)
    
    demo_df = load_demo_data()
    print("Producer ready to send credit card records for real-time prediction")

    try:
        record_count = 0
        max_records = args.num_records if args.num_records > 0 else float('inf')
        
        if args.num_records > 0:
            print(f"Will send {args.num_records} records with {args.interval}s intervals")
        else:
            print(f"Continuous streaming mode with {args.interval}s intervals")

        while record_count < max_records:
            # Sample a random record from the demo data
            record = demo_df.sample(1).iloc[0].to_dict()
            record["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            channel.basic_publish(
                exchange="",
                routing_key=queue_name,
                body=json.dumps(record, default=str),
                properties=pika.BasicProperties(delivery_mode=2),
            )
            
            record_count += 1
            print(f"Sent record {record_count}: LIMIT_BAL={record['LIMIT_BAL']}, AGE={record['AGE']}, SEX={record['SEX']}")
            
            if record_count >= max_records:
                print(f"Finished sending {record_count} records")
                break
                
            sleep(args.interval)
            
    except KeyboardInterrupt:
        print(f"\nStopped by user after sending {record_count} records")
    finally:
        connection.close()
        print("Connection closed.")


def teardown_queue(queue_name, server, username, password):
    try:
        creds = pika.PlainCredentials(username, password)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=server, port=5672, credentials=creds)
        )
        channel = connection.channel()
        channel.queue_delete(queue=queue_name)
        print(f"Queue {queue_name} deleted")
        connection.close()
    except Exception as e:
        print(f"Error deleting queue {queue_name}: {e}")


if __name__ == "__main__":
    mode = args.mode
    server = args.rabbitmq_server
    username = args.user
    password = args.password
    queue_name = args.queue_name

    if mode == "teardown":
        teardown_queue(queue_name, server, username, password)
    else:
        create_streams(server, queue_name, username, password)
