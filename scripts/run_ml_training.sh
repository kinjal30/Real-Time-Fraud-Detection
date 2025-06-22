#!/bin/bash

# Run ML model training

echo "Starting ML model training..."

# Run batch processing first to ensure we have processed data
echo "Running batch processing to prepare data..."
docker exec spark-master spark-submit --master spark://spark-master:7077 /opt/spark/jobs/batch_processing.py

# Wait for batch processing to complete
sleep 10

# Run ML training
echo "Running ML model training..."
docker exec spark-master spark-submit --master spark://spark-master:7077 /opt/spark/jobs/fraud_ml_training.py

echo "ML training completed!"
