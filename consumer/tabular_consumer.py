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

# Global variables for model and stats
model = None
prediction_count = 0
default_predictions = 0

def load_model(model_path):
    """Load the trained LightGBM model"""
    global model
    try:
        if os.path.exists(model_path):
            with open(model_path, 'rb') as f:
                model = pickle.load(f)
            print(f"✅ Model loaded successfully from {model_path}")
            print(f"Model type: {type(model)}")
            return True
        else:
            print(f"❌ Model file not found: {model_path}")
            print("Please make sure the model file exists or provide correct path with -m")
            return False
    except Exception as e:
        print(f"❌ Error loading model: {e}")
        return False

def callback(ch, method, properties, body):
    """Process incoming messages and make predictions"""
    global prediction_count, default_predictions
    
    try:
        # Parse the message
        message = json.loads(body)
        print(f"\n📨 Received message at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Extract features and metadata
        processed_features = message.get("processed_features")
        raw_features = message.get("raw_features", {})
        created_time = message.get("created", "N/A")
        
        # Use raw features for prediction (model will handle preprocessing)
        if raw_features:
            # Convert raw features dict to ordered list matching training data
            feature_names = ['LIMIT_BAL', 'SEX', 'EDUCATION', 'MARRIAGE', 'AGE', 
                           'PAY_0', 'PAY_2', 'PAY_3', 'PAY_4', 'PAY_5', 'PAY_6',
                           'BILL_AMT1', 'BILL_AMT2', 'BILL_AMT3', 'BILL_AMT4', 'BILL_AMT5', 'BILL_AMT6',
                           'PAY_AMT1', 'PAY_AMT2', 'PAY_AMT3', 'PAY_AMT4', 'PAY_AMT5', 'PAY_AMT6']
            
            # Create feature array in correct order
            feature_values = []
            for feature in feature_names:
                if feature in raw_features:
                    feature_values.append(float(raw_features[feature]))
                else:
                    print(f"❌ Missing feature: {feature}")
                    return
            
            # Create DataFrame with proper column names (required by sklearn pipeline)
            X = pd.DataFrame([feature_values], columns=feature_names)
        elif processed_features:
            print("⚠️ Using processed features (may cause dimension mismatch)")
            X = np.array(processed_features).reshape(1, -1)
        else:
            print("❌ No features found in message")
            return
        
        if model is None:
            print("❌ Model not loaded")
            return
        
        # Make prediction
        prediction = model.predict(X)[0]
        prediction_proba = model.predict_proba(X)[0]
        
        # Update statistics
        prediction_count += 1
        if prediction == 1:
            default_predictions += 1
        
        # Calculate confidence (probability of predicted class)
        confidence = prediction_proba[prediction]
        default_probability = prediction_proba[1]  # Probability of default
        
        # Display results
        print(f"🎯 Credit Default Prediction Results:")
        print(f"   📊 Raw features: LIMIT_BAL={raw_features.get('LIMIT_BAL', 'N/A')}, "
              f"AGE={raw_features.get('AGE', 'N/A')}, SEX={raw_features.get('SEX', 'N/A')}")
        print(f"   🔮 Prediction: {'🚨 DEFAULT' if prediction == 1 else '✅ NO DEFAULT'}")
        print(f"   📈 Default Probability: {default_probability:.4f} ({default_probability*100:.2f}%)")
        print(f"   🎪 Confidence: {confidence:.4f} ({confidence*100:.2f}%)")
        print(f"   📅 Record created: {created_time}")
        print(f"   📊 Total predictions: {prediction_count}, Defaults predicted: {default_predictions}")
        
        # Store prediction results (you could save to database here)
        result = {
            "timestamp": datetime.now().isoformat(),
            "prediction": int(prediction),
            "default_probability": float(default_probability),
            "confidence": float(confidence),
            "raw_features": raw_features,
            "created_time": created_time
        }
        
        # Optional: Save results to file
        # with open("predictions.jsonl", "a") as f:
        #     f.write(json.dumps(result) + "\n")
        
    except json.JSONDecodeError as e:
        print(f"❌ JSON decode error: {e}")
    except Exception as e:
        print(f"❌ Error processing message: {e}")

def consume_messages(server, queue_name):
    """Connect to RabbitMQ and start consuming messages"""
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=server))
        channel = connection.channel()
        channel.queue_declare(queue=queue_name, durable=True)
        
        print(f"🔄 Connected to RabbitMQ at {server}")
        print(f"📬 Waiting for messages in queue: {queue_name}")
        print(f"🛑 To exit press CTRL+C")
        
        channel.basic_consume(
            queue=queue_name,
            on_message_callback=callback,
            auto_ack=True
        )
        
        channel.start_consuming()
        
    except KeyboardInterrupt:
        print("\n🛑 Stopping consumer...")
        channel.stop_consuming()
        connection.close()
        print(f"📊 Final stats: {prediction_count} predictions made, {default_predictions} defaults predicted")
    except Exception as e:
        print(f"❌ Connection error: {e}")

if __name__ == "__main__":
    server = args.rabbitmq_server
    queue_name = args.queue_name
    model_path = args.model_path
    
    # Load the model first
    if load_model(model_path):
        consume_messages(server, queue_name)
    else:
        print("Exiting due to model loading failure")
