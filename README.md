# Real-Time Fraud Detection System

A comprehensive fraud detection system built with Apache Kafka, Flink, Spark, and Hive for real-time transaction monitoring and machine learning-based fraud detection.

## Architecture Overview

![image](https://github.com/user-attachments/assets/f31643ad-6f7a-4273-b59c-253adb5aae9c)

## Features

### Real-Time Processing (Apache Flink)
- **High Amount Detection**: Flags transactions over $5,000
- **Rapid Transaction Detection**: Identifies users with >3 transactions per minute
- **Unusual Location Detection**: Flags transactions from unknown locations
- **Unusual Hour Detection**: Detects large transactions during off-hours (2-5 AM)

### Batch Processing & ML (Apache Spark)
- **Feature Engineering**: Creates user behavior profiles and transaction features
- **Machine Learning Models**: Random Forest and Logistic Regression for fraud detection
- **Model Training**: Automated retraining on historical data
- **Batch Analytics**: Daily fraud reports and trend analysis

### Data Storage (Apache Hive)
- **Raw Transaction Storage**: Complete transaction history
- **Processed Data**: Feature-engineered datasets for ML
- **Fraud Alerts**: Historical alert data for analysis
- **Partitioned Tables**: Optimized for time-based queries

### Monitoring & Alerting
- **Real-time Alert Processing**: Immediate notification system
- **Severity-based Routing**: Different notification channels based on alert severity
- **Comprehensive Logging**: Detailed audit trails for compliance

## Quick Start

### Prerequisites
- Docker and Docker Compose
- Python 3.8+
- Java 11+
- Maven 3.6+

### Setup

1. **Clone and Setup**
   \`\`\`bash
   git clone <repository>
   cd fraud-detection-system
   chmod +x scripts/*.sh
   \`\`\`

2. **Start Infrastructure**
   \`\`\`bash
   ./scripts/setup.sh
   \`\`\`

3. **Start the System**
   \`\`\`bash
   ./scripts/start_system.sh
   \`\`\`

4. **Run ML Training**
   \`\`\`bash
   ./scripts/run_ml_training.sh
   \`\`\`

### Manual Setup Steps

1. **Start Services**
   \`\`\`bash
   docker-compose up -d
   \`\`\`

2. **Create Kafka Topics**
   \`\`\`bash
   docker exec kafka kafka-topics --create --topic transactions --bootstrap-server localhost:29092 --partitions 3 --replication-factor 1
   docker exec kafka kafka-topics --create --topic fraud-alerts --bootstrap-server localhost:29092 --partitions 3 --replication-factor 1
   \`\`\`

3. **Build Flink Job**
   \`\`\`bash
   cd flink-jobs
   mvn clean package
   docker cp target/flink-fraud-detection-1.0-SNAPSHOT.jar flink-jobmanager:/opt/flink/jobs/
   \`\`\`

4. **Start Components**
   \`\`\`bash
   # Start transaction generator
   python3 data-generator/transaction_producer.py &
   
   # Submit Flink job
   docker exec flink-jobmanager flink run /opt/flink/jobs/flink-fraud-detection-1.0-SNAPSHOT.jar &
   
   # Run batch processing
   docker exec spark-master spark-submit --master spark://spark-master:7077 /opt/spark/jobs/batch_processing.py
   
   # Start alert monitoring
   python3 monitoring/alert_consumer.py &
   \`\`\`

## System Components

### Data Generator
- Generates realistic transaction data with configurable fraud rate
- Simulates user behavior patterns and spending habits
- Produces both normal and fraudulent transactions

### Real-Time Processing (Flink)
- Processes transaction streams in real-time
- Applies multiple fraud detection rules
- Generates immediate alerts for suspicious activities

### Batch Processing (Spark)
- Performs feature engineering on historical data
- Trains and updates ML models
- Generates daily analytics reports

### Data Warehouse (Hive)
- Stores raw and processed transaction data
- Maintains fraud alert history
- Supports complex analytical queries

## Configuration

### Fraud Detection Rules

1. **High Amount Threshold**: $5,000 (configurable)
2. **Rapid Transaction Limit**: 3 transactions per minute
3. **Unusual Hours**: 2:00 AM - 5:00 AM for amounts > $1,000
4. **Location-based**: Transactions from "Unknown Location"

### Alert Severity Levels

- **CRITICAL**: Immediate response required (email + SMS + Slack)
- **HIGH**: High priority (email + Slack)
- **MEDIUM**: Medium priority (email only)
- **LOW**: Low priority (logged only)

## Monitoring

### Web Interfaces
- **Flink Dashboard**: http://localhost:8081
- **Spark Master**: http://localhost:8080
- **Hadoop NameNode**: http://localhost:9870

### Logs
- **Fraud Alerts**: `/opt/spark/data/logs/fraud_alerts.log`
- **System Logs**: Docker container logs

### Key Metrics
- Transaction throughput
- Fraud detection rate
- Alert response time
- Model accuracy metrics

## Analytics Queries

The system includes pre-built Hive queries for:
- Daily fraud statistics
- User behavior analysis
- Merchant fraud patterns
- Time-based fraud trends
- Location-based analysis

Example query:
\`\`\`sql
SELECT 
    processing_date,
    COUNT(*) as total_transactions,
    SUM(is_fraud) as fraud_transactions,
    ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) as fraud_rate_percent
FROM fraud_detection.transactions_processed
GROUP BY processing_date
ORDER BY processing_date DESC;
\`\`\`

## Machine Learning

### Features Used
- Transaction amount and log-transformed amount
- Hour of day and weekend indicator
- User historical behavior (average amount, transaction count)
- Merchant category and location
- Channel and card type

### Models
- **Random Forest**: Ensemble method for robust fraud detection
- **Logistic Regression**: Linear model for interpretable results
- **Model Selection**: Automatic selection based on AUC performance

### Training Pipeline
1. Data extraction from Hive
2. Feature engineering and scaling
3. Model training and evaluation
4. Model persistence and metadata storage

## Scaling Considerations

### Horizontal Scaling
- **Kafka**: Increase partitions for higher throughput
- **Flink**: Add more TaskManagers
- **Spark**: Add more worker nodes
- **Hive**: Distribute across multiple nodes

### Performance Optimization
- **Kafka**: Tune batch size and compression
- **Flink**: Optimize checkpointing and parallelism
- **Spark**: Configure memory and executor settings
- **Hive**: Use appropriate file formats (Parquet, ORC)

## Security Considerations

### Data Protection
- Encrypt sensitive transaction data
- Implement access controls for Hive tables
- Secure Kafka topics with authentication

### Compliance
- Maintain audit logs for all fraud decisions
- Implement data retention policies
- Ensure GDPR/PCI DSS compliance

## Troubleshooting

### Common Issues

1. **Kafka Connection Issues**
   \`\`\`bash
   # Check Kafka status
   docker exec kafka kafka-topics --list --bootstrap-server localhost:29092
   \`\`\`

2. **Flink Job Failures**
   \`\`\`bash
   # Check Flink logs
   docker logs flink-jobmanager
   docker logs flink-taskmanager
   \`\`\`

3. **Spark Job Issues**
   \`\`\`bash
   # Check Spark logs
   docker logs spark-master
   docker logs spark-worker
   \`\`\`

4. **Hive Connection Problems**
   \`\`\`bash
   # Test Hive connection
   docker exec hive-server beeline -u jdbc:hive2://localhost:10000
   \`\`\`

### Testing

Run system tests:
\`\`\`bash
python3 scripts/test_system.py
\`\`\`

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.

## Support

For issues and questions:
1. Check the troubleshooting section
2. Review system logs
3. Open an issue on GitHub
4. Contact the development team

---

**Note**: This is a demonstration system. For production use, additional security, monitoring, and compliance measures should be implemented.
