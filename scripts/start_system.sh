#!/bin/bash

# Start the complete fraud detection system

echo "Starting Fraud Detection System..."

# Start transaction generator in background
echo "Starting transaction generator..."
python3 data-generator/transaction_producer.py &
GENERATOR_PID=$!

# Wait a bit for some data to be generated
sleep 10

# Submit Flink job
echo "Submitting Flink fraud detection job..."
docker exec flink-jobmanager flink run /opt/flink/jobs/flink-fraud-detection-1.0-SNAPSHOT.jar &

# Wait for some transactions to accumulate
sleep 30

# Run batch processing
echo "Running batch processing..."
docker exec spark-master spark-submit --master spark://spark-master:7077 /opt/spark/jobs/batch_processing.py

# Start alert monitoring
echo "Starting alert monitoring..."
python3 monitoring/alert_consumer.py &
MONITOR_PID=$!

echo "System started successfully!"
echo "Transaction Generator PID: $GENERATOR_PID"
echo "Alert Monitor PID: $MONITOR_PID"
echo ""
echo "To stop the system:"
echo "kill $GENERATOR_PID $MONITOR_PID"
echo "docker-compose down"
