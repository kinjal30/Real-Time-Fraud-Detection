#!/bin/bash

# Fraud Detection System Setup Script

echo "Setting up Fraud Detection System..."

# Create necessary directories
mkdir -p data/logs
mkdir -p data/models
mkdir -p data/warehouse
mkdir -p flink-jobs/target
mkdir -p spark-jobs

# Set permissions
chmod +x scripts/*.sh
chmod +x data-generator/*.py
chmod +x spark-jobs/*.py
chmod +x monitoring/*.py

# Start Docker services
echo "Starting Docker services..."
docker-compose up -d

# Wait for services to be ready
echo "Waiting for services to start..."
sleep 30

# Create Kafka topics
echo "Creating Kafka topics..."
docker exec kafka kafka-topics --create --topic transactions --bootstrap-server localhost:29092 --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --create --topic fraud-alerts --bootstrap-server localhost:29092 --partitions 3 --replication-factor 1

# Initialize HDFS
echo "Initializing HDFS..."
docker exec hadoop hdfs namenode -format -force

# Build Flink job
echo "Building Flink fraud detection job..."
cd flink-jobs
mvn clean package
cd ..

# Copy Flink job to container
docker cp flink-jobs/target/flink-fraud-detection-1.0-SNAPSHOT.jar flink-jobmanager:/opt/flink/jobs/

echo "Setup completed!"
echo ""
echo "Services available at:"
echo "- Kafka: localhost:9092"
echo "- Flink Dashboard: http://localhost:8081"
echo "- Spark Master: http://localhost:8080"
echo "- Hadoop NameNode: http://localhost:9870"
echo "- Hive Server: localhost:10000"
echo ""
echo "To start the system:"
echo "1. Run transaction generator: python3 data-generator/transaction_producer.py"
echo "2. Submit Flink job: docker exec flink-jobmanager flink run /opt/flink/jobs/flink-fraud-detection-1.0-SNAPSHOT.jar"
echo "3. Run batch processing: docker exec spark-master spark-submit /opt/spark/jobs/batch_processing.py"
echo "4. Start alert monitoring: python3 monitoring/alert_consumer.py"
