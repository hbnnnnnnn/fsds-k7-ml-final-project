import argparse
import pika
import json
import pickle
import numpy as np
import pandas as pd
from datetime import datetime
import os

parser = argparse.ArgumentParser()
parser.add_argument("-b", "--rabbitmq_server", default="localhost", 
                   help="RabbitMQ server hostname")
parser.add_argument("-q", "--queue_name", default="alicpp_records",
                   help="Queue name to consume from")
parser.add_argument("-m", "--model_path", default="credit_card_model.pkl",
                   help="Path to the trained model pickle file")
args = parser.parse_args()

model = None
prediction_count = 0
default_predictions = 0

def load_model(model_path):
    global model
    try:
        if os.path.exists(model_path):
            with open(model_path, 'rb') as f:
                model = pickle.load(f)
            print(f"Model loaded successfully from {model_path}")
            print(f"Model type: {type(model)}")
            return True
        else:
            print(f"Model file not found: {model_path}")
            print("Please make sure the model file exists or provide correct path with -m")
            return False
    except Exception as e:
        print(f"Error loading model: {e}")
        return False

def callback(ch, method, properties, body):
    global prediction_count, default_predictions
    
    try:
        message = json.loads(body)
        timestamp = message.get("timestamp", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        
        feature_names = ['LIMIT_BAL', 'SEX', 'EDUCATION', 'MARRIAGE', 'AGE', 
                        'PAY_0', 'PAY_2', 'PAY_3', 'PAY_4', 'PAY_5', 'PAY_6',
                        'BILL_AMT1', 'BILL_AMT2', 'BILL_AMT3', 'BILL_AMT4', 'BILL_AMT5', 'BILL_AMT6',
                        'PAY_AMT1', 'PAY_AMT2', 'PAY_AMT3', 'PAY_AMT4', 'PAY_AMT5', 'PAY_AMT6']
        
        feature_values = []
        for feature in feature_names:
            if feature in message:
                feature_values.append(float(message[feature]))
            else:
                print(f"Missing feature: {feature}")
                return
        
        X = pd.DataFrame([feature_values], columns=feature_names)
        
        if model is None:
            print("Model not loaded")
            return
        
        prediction = model.predict(X)[0]
        prediction_proba = model.predict_proba(X)[0]
        
        prediction_count += 1
        if prediction == 1:
            default_predictions += 1
        
        default_probability = prediction_proba[1]
        
        print(f"\nPrediction #{prediction_count} | {timestamp}")
        print(f"Input: LIMIT_BAL={message['LIMIT_BAL']}, AGE={message['AGE']}, SEX={message['SEX']}")
        print(f"Result: {prediction} ({'DEFAULT' if prediction == 1 else 'NO DEFAULT'})")
        print(f"Probability: {default_probability:.3f}")
        print(f"Stats: {default_predictions}/{prediction_count} defaults predicted")
        
    except Exception as e:
        print(f"Error processing message: {e}")

def consume_messages(server, queue_name):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=server))
        channel = connection.channel()
        channel.queue_declare(queue=queue_name, durable=True)
        
        print(f"Connected to RabbitMQ at {server}")
        print(f"Waiting for messages in queue: {queue_name}")
        print(f"To exit press CTRL+C")

        channel.basic_consume(
            queue=queue_name,
            on_message_callback=callback,
            auto_ack=True
        )
        
        channel.start_consuming()
        
    except KeyboardInterrupt:
        print("\nStopping consumer...")
        channel.stop_consuming()
        connection.close()
        print(f"Final stats: {prediction_count} predictions made, {default_predictions} defaults predicted")
    except Exception as e:
        print(f"Connection error: {e}")

if __name__ == "__main__":
    server = args.rabbitmq_server
    queue_name = args.queue_name
    model_path = args.model_path
    
    if load_model(model_path):
        consume_messages(server, queue_name)
    else:
        print("Exiting due to model loading failure")
