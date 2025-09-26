import argparse
import json
import os
from datetime import datetime
from time import sleep
import pickle
import numpy as np

import pandas as pd
import pika
from sklearn.preprocessing import OneHotEncoder, OrdinalEncoder, RobustScaler
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.base import BaseEstimator, TransformerMixin


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


# Data preprocessing pipeline components
class LogTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, columns=None):
        self.columns = columns

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        X = X.copy()
        if self.columns:
            for col in self.columns:
                X[col] = np.log1p(X[col].clip(lower=0))
        return X


def setup_preprocessing_pipeline():
    """Setup the data preprocessing pipeline matching the notebook"""
    # Define column types as in the notebook
    numerical_cols = ['LIMIT_BAL', 'AGE', 'BILL_AMT1', 'BILL_AMT2', 'BILL_AMT3', 
                     'BILL_AMT4', 'BILL_AMT5', 'BILL_AMT6', 'PAY_AMT1', 'PAY_AMT2', 
                     'PAY_AMT3', 'PAY_AMT4', 'PAY_AMT5', 'PAY_AMT6']
    
    no_order_categorical_cols = ['SEX', 'MARRIAGE', 'EDUCATION']
    order_categorical_cols = ['PAY_0'] + [f'PAY_{i}' for i in range(2, 7)]
    
    # Tree-based model preprocessor (as used in the notebook)
    tree_preprocessor = ColumnTransformer(
        transformers=[
            ('num', RobustScaler(), numerical_cols),
            ('onehot', OneHotEncoder(handle_unknown='ignore'), no_order_categorical_cols),
            ('ordinal', 'passthrough', order_categorical_cols)
        ])
    
    tree_pipeline = Pipeline(steps=[('preprocessor', tree_preprocessor)])
    return tree_pipeline, numerical_cols, no_order_categorical_cols, order_categorical_cols


def preprocess_record(record, pipeline):
    """Preprocess a single record using the fitted pipeline"""
    # Convert to DataFrame
    df = pd.DataFrame([record])
    
    # Apply preprocessing
    processed = pipeline.transform(df)
    
    return processed[0].tolist()  # Return as list


def create_queue(channel, queue_name):
    """Declare a queue if it does not exist"""
    channel.queue_declare(queue=queue_name, durable=True)
    print(f"Queue {queue_name} is ready.")


def create_streams(server, queue_name, username, password):
    """
    Connect to RabbitMQ and stream records from a CSV file into the queue.
    """
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

    # Load CSV
    try:
        # Get the directory of this script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(script_dir, "credit_card_sample.csv")
        df = pd.read_csv(csv_path)
        print(f"Loaded {len(df)} sample records from credit card dataset")
    except FileNotFoundError:
        raise RuntimeError(f"CSV file '{csv_path}' not found.")

    create_queue(channel, queue_name=queue_name)
    
    # Setup preprocessing pipeline
    pipeline, numerical_cols, no_order_categorical_cols, order_categorical_cols = setup_preprocessing_pipeline()
    
    # Load the full dataset to fit the pipeline (mimicking notebook training data)
    try:
        # Try different possible paths for the UCI dataset
        possible_paths = [
            "UCI_Credit_Card.csv",  # In current directory 
            "../UCI_Credit_Card.csv",  # One level up
            os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "UCI_Credit_Card.csv")  # Project root
        ]
        
        full_df = None
        for path in possible_paths:
            try:
                full_df = pd.read_csv(path)
                print(f"Found UCI dataset at: {path}")
                break
            except FileNotFoundError:
                continue
                
        if full_df is None:
            raise FileNotFoundError("UCI_Credit_Card.csv not found in any expected location")
            
        full_df = full_df.drop(columns=['ID'])  # Remove ID as in notebook
        X_full = full_df.drop(columns=['default.payment.next.month'])
        
        # Fit the pipeline on the full dataset
        pipeline.fit(X_full)
        print("Pipeline fitted on full dataset")
    except FileNotFoundError:
        print("Warning: Full UCI_Credit_Card.csv not found. Pipeline may not work correctly.")
        # Try to fit on sample data
        pipeline.fit(df)
        print("Pipeline fitted on sample data")

    try:
        record_count = 0
        max_records = args.num_records if args.num_records > 0 else float('inf')
        
        print(f"🚀 Starting to send records...")
        if args.num_records > 0:
            print(f"📊 Will send {args.num_records} records with {args.interval}s intervals")
        else:
            print(f"♾️  Continuous streaming mode with {args.interval}s intervals")
        
        while record_count < max_records:
            # Sample a record
            record = df.sample(1).to_dict(orient="records")[0]
            
            # Add metadata
            record["created"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            record["datetime"] = record["created"]
            
            # Preprocess the record
            try:
                processed_features = preprocess_record(record.copy(), pipeline)
                record["processed_features"] = processed_features
                record["raw_features"] = {k: v for k, v in record.items() 
                                        if k not in ["created", "datetime", "processed_features"]}
            except Exception as e:
                print(f"Error preprocessing record: {e}")
                record["processed_features"] = None
                record["raw_features"] = {k: v for k, v in record.items() 
                                        if k not in ["created", "datetime", "processed_features"]}

            channel.basic_publish(
                exchange="",
                routing_key=queue_name,
                body=json.dumps(record, default=str),  # default=str to handle numpy types
                properties=pika.BasicProperties(delivery_mode=2),  # persistent
            )
            
            record_count += 1
            print(f"➡️ Sent record {record_count} with {len(record.get('processed_features', []))} processed features")
            
            # Check if we've reached the limit
            if record_count >= max_records:
                print(f"✅ Finished sending {record_count} records")
                break
                
            sleep(args.interval)
            
    except KeyboardInterrupt:
        print(f"\n🛑 Stopped by user after sending {record_count} records")
    finally:
        connection.close()
        print("🔌 Connection closed.")


def teardown_queue(queue_name, server, username, password):
    """Delete the queue if it exists"""
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
