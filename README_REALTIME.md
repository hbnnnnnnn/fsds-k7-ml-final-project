# Credit Card Default Prediction - Real-time ML Pipeline

This project implements a real-time machine learning pipeline for credit card default prediction using RabbitMQ, with a trained LightGBM model from the k7_ml.ipynb notebook.

## Architecture

```
[UCI Credit Card Data] → [Producer + Preprocessing] → [RabbitMQ] → [Consumer + ML Model] → [Predictions]
```

## Setup Instructions

### 1. Install Dependencies

```bash
pip install pandas numpy scikit-learn lightgbm pika
```

### 2. Start RabbitMQ

Using Docker:
```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

Or using docker-compose:
```bash
docker-compose up -d
```

### 3. Save the Trained Model

First, make sure you have run the k7_ml.ipynb notebook to completion. Then save the model:

```bash
python save_model.py
```

This will create `credit_card_model.pkl` with the trained LightGBM pipeline.

### 4. Run the Producer

The producer loads credit card sample data, applies preprocessing, and streams records to RabbitMQ.

**For testing (limited records):**
```bash
# Send 10 records with 1 second intervals
python producer/stream_tabular_ingestion.py --mode setup --rabbitmq_server localhost --queue_name alicpp_records --num_records 10 --interval 1

# Send 5 records with 0.5 second intervals  
python producer/stream_tabular_ingestion.py --mode setup --rabbitmq_server localhost --queue_name alicpp_records --num_records 5 --interval 0.5
```

**For continuous streaming:**
```bash
# Continuous streaming with 2 second intervals (default)
python producer/stream_tabular_ingestion.py --mode setup --rabbitmq_server localhost --queue_name alicpp_records

# Continuous streaming with custom interval
python producer/stream_tabular_ingestion.py --mode setup --rabbitmq_server localhost --queue_name alicpp_records --interval 5
```

**Producer Options:**
- `--num_records N`: Send N records then stop (0 = unlimited, default)
- `--interval X`: Wait X seconds between records (default: 2.0)
- `--queue_name NAME`: RabbitMQ queue name (default: alicpp_records)

### 5. Run the Consumer

The consumer receives preprocessed records and makes real-time predictions:

```bash
cd consumer
python tabular_consumer.py --rabbitmq_server localhost --queue_name credit_predictions --model_path ../credit_card_model.pkl
```

## Features

### Producer (`stream_tabular_ingestion.py`)
- ✅ Loads credit card sample data matching the UCI dataset structure
- ✅ Implements the exact preprocessing pipeline from k7_ml.ipynb:
  - RobustScaler for numerical features
  - OneHotEncoder for categorical features (SEX, MARRIAGE, EDUCATION)
  - Passthrough for ordinal features (PAY_0, PAY_2-6)
- ✅ Streams preprocessed records to RabbitMQ every 2 seconds
- ✅ Includes both raw and preprocessed features in messages

### Consumer (`tabular_consumer.py`)
- ✅ Loads the trained LightGBM model from k7_ml.ipynb
- ✅ Makes real-time predictions on incoming data
- ✅ Provides detailed prediction results:
  - Binary prediction (Default/No Default)
  - Default probability
  - Prediction confidence
  - Running statistics
- ✅ Rich console output with emojis and formatting

### Model (`save_model.py`)
- ✅ Extracts the trained LightGBM pipeline from the notebook
- ✅ Saves it as a pickle file for the consumer
- ✅ Includes the complete preprocessing pipeline

## Data Pipeline

The preprocessing follows the exact same steps as in k7_ml.ipynb:

1. **Numerical Features**: `LIMIT_BAL`, `AGE`, `BILL_AMT1-6`, `PAY_AMT1-6`
   - Scaled using RobustScaler (handles outliers better)

2. **Categorical Features**: `SEX`, `MARRIAGE`, `EDUCATION`
   - One-hot encoded

3. **Ordinal Features**: `PAY_0`, `PAY_2`, `PAY_3`, `PAY_4`, `PAY_5`, `PAY_6`
   - Passed through without encoding (tree models handle ordinal naturally)

## Sample Output

### Producer Output:
```
Pipeline fitted on full dataset
➡️ Sent record with 27 processed features
➡️ Sent record with 27 processed features
```

### Consumer Output:
```
✅ Model loaded successfully from credit_card_model.pkl
🎯 Credit Default Prediction Results:
   📊 Raw features: LIMIT_BAL=20000, AGE=24, SEX=2
   🔮 Prediction: 🚨 DEFAULT
   📈 Default Probability: 0.7234 (72.34%)
   🎪 Confidence: 0.7234 (72.34%)
   📊 Total predictions: 15, Defaults predicted: 8
```

## Files Structure

```
├── producer/
│   ├── stream_tabular_ingestion.py    # Data producer with preprocessing
│   └── credit_card_sample.csv         # Sample credit card data
├── consumer/
│   └── tabular_consumer.py            # ML model consumer
├── save_model.py                      # Model extraction script
├── credit_card_model.pkl              # Trained model (generated)
├── k7_ml.ipynb                        # Original ML notebook
└── UCI_Credit_Card.csv                # Full dataset
```

## Troubleshooting

1. **Model not found**: Make sure to run `python save_model.py` first
2. **RabbitMQ connection error**: Ensure RabbitMQ is running on localhost:5672
3. **Import errors**: Install all dependencies with pip
4. **CSV not found**: Ensure `credit_card_sample.csv` exists in producer/ directory

## Model Performance

Based on k7_ml.ipynb analysis:
- **Model**: LightGBM (best performing)
- **F1 Score**: ~0.45-0.50 (varies by run)
- **ROC AUC**: ~0.75-0.80
- **Features**: 27 after preprocessing (14 numerical + 7 one-hot + 6 ordinal)