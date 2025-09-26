import pickle
import pandas as pd
import numpy as np
from sklearn.preprocessing import OneHotEncoder, RobustScaler
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from lightgbm import LGBMClassifier

def create_and_save_model():
    df = pd.read_csv('UCI_Credit_Card.csv')
    
    X = df.drop(columns=['ID', 'default.payment.next.month'])
    y = df['default.payment.next.month']
    
    numerical_cols = ['LIMIT_BAL', 'AGE'] + [f'BILL_AMT{i}' for i in range(1, 7)] + [f'PAY_AMT{i}' for i in range(1, 7)]
    no_order_categorical_cols = ['SEX', 'MARRIAGE', 'EDUCATION']
    order_categorical_cols = ['PAY_0'] + [f'PAY_{i}' for i in range(2, 7)]
    
    tree_preprocessor = ColumnTransformer(
        transformers=[
            ('num', RobustScaler(), numerical_cols),
            ('onehot', OneHotEncoder(handle_unknown='ignore'), no_order_categorical_cols),
            ('ordinal', 'passthrough', order_categorical_cols)
        ])
    
    model = Pipeline([
        ('preprocessor', tree_preprocessor),
        ('classifier', LGBMClassifier(verbose=-1))
    ])
    
    model.fit(X, y)
    
    with open('credit_card_model.pkl', 'wb') as f:
        pickle.dump(model, f)
    
    sample_data = X.iloc[0:1] 
    prediction = model.predict(sample_data)
    probability = model.predict_proba(sample_data)
    print(f"Model test - Prediction: {prediction[0]}, Probability: {probability[0]}")
    
    return model

if __name__ == "__main__":
    create_and_save_model()