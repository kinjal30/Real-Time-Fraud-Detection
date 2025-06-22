#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.streaming import StreamingContext
import json
from datetime import datetime, timedelta

class BatchProcessor:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("FraudDetectionBatchProcessing") \
            .config("spark.sql.warehouse.dir", "/opt/spark/data/warehouse") \
            .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
            .enableHiveSupport() \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
    
    def setup_hive_tables(self):
        """Create Hive tables for storing processed data"""
        print("Setting up Hive tables...")
        
        # Create database
        self.spark.sql("CREATE DATABASE IF NOT EXISTS fraud_detection")
        
        # Raw transactions table
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS fraud_detection.transactions_raw (
                transaction_id STRING,
                user_id STRING,
                amount DOUBLE,
                merchant_id STRING,
                merchant_category STRING,
                location STRING,
                timestamp STRING,
                card_type STRING,
                channel STRING,
                is_weekend BOOLEAN,
                hour_of_day INT
            )
            STORED AS PARQUET
            LOCATION '/opt/spark/data/warehouse/fraud_detection.db/transactions_raw'
        """)
        
        # Processed transactions table
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS fraud_detection.transactions_processed (
                transaction_id STRING,
                user_id STRING,
                amount DOUBLE,
                merchant_id STRING,
                merchant_category STRING,
                location STRING,
                timestamp STRING,
                card_type STRING,
                channel STRING,
                is_weekend BOOLEAN,
                hour_of_day INT,
                amount_percentile DOUBLE,
                user_avg_amount DOUBLE,
                user_transaction_count BIGINT,
                is_unusual_amount BOOLEAN,
                is_unusual_location BOOLEAN,
                is_fraud INT,
                processing_date STRING
            )
            PARTITIONED BY (processing_date STRING)
            STORED AS PARQUET
            LOCATION '/opt/spark/data/warehouse/fraud_detection.db/transactions_processed'
        """)
        
        # Fraud alerts table
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS fraud_detection.fraud_alerts (
                alert_id STRING,
                alert_type STRING,
                transaction_id STRING,
                user_id STRING,
                amount DOUBLE,
                merchant_id STRING,
                location STRING,
                timestamp STRING,
                alert_timestamp STRING,
                severity STRING,
                processing_date STRING
            )
            PARTITIONED BY (processing_date STRING)
            STORED AS PARQUET
            LOCATION '/opt/spark/data/warehouse/fraud_detection.db/fraud_alerts'
        """)
        
        print("Hive tables created successfully!")
    
    def read_kafka_batch(self, topic="transactions"):
        """Read batch data from Kafka"""
        print(f"Reading batch data from Kafka topic: {topic}")
        
        # Define schema for transaction data
        transaction_schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("merchant_id", StringType(), True),
            StructField("merchant_category", StringType(), True),
            StructField("location", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("card_type", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("is_weekend", BooleanType(), True),
            StructField("hour_of_day", IntegerType(), True)
        ])
        
        # Read from Kafka
        df = self.spark \
            .read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .load()
        
        # Parse JSON data
        df = df.select(
            from_json(col("value").cast("string"), transaction_schema).alias("data")
        ).select("data.*")
        
        return df
    
    def feature_engineering(self, df):
        """Perform feature engineering on transaction data"""
        print("Performing feature engineering...")
        
        # Calculate user statistics
        user_stats = df.groupBy("user_id").agg(
            avg("amount").alias("user_avg_amount"),
            count("*").alias("user_transaction_count"),
            stddev("amount").alias("user_amount_stddev")
        )
        
        # Join user statistics back to main dataframe
        df = df.join(user_stats, "user_id", "left")
        
        # Calculate amount percentiles
        df = df.withColumn("amount_percentile", 
                          percent_rank().over(Window.orderBy("amount")))
        
        # Create fraud indicators
        df = df.withColumn("is_unusual_amount", 
                          (col("amount") > col("user_avg_amount") * 5).cast("boolean"))
        
        df = df.withColumn("is_unusual_location", 
                          (col("location") == "Unknown Location").cast("boolean"))
        
        # Simple fraud labeling (in real scenario, this would come from historical data)
        df = df.withColumn("is_fraud", 
                          when((col("amount") > 5000) | 
                               (col("is_unusual_location")) |
                               (col("hour_of_day").between(2, 5) & (col("amount") > 1000)), 1)
                          .otherwise(0))
        
        # Add processing date
        df = df.withColumn("processing_date", lit(datetime.now().strftime("%Y-%m-%d")))
        
        return df
    
    def save_to_hive(self, df, table_name):
        """Save processed data to Hive"""
        print(f"Saving data to Hive table: {table_name}")
        
        df.write \
            .mode("append") \
            .insertInto(f"fraud_detection.{table_name}")
        
        print(f"Data saved to {table_name} successfully!")
    
    def generate_batch_reports(self):
        """Generate batch analytics reports"""
        print("Generating batch reports...")
        
        # Daily fraud summary
        daily_summary = self.spark.sql("""
            SELECT 
                processing_date,
                COUNT(*) as total_transactions,
                SUM(is_fraud) as fraud_transactions,
                ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) as fraud_rate_percent,
                ROUND(AVG(amount), 2) as avg_transaction_amount,
                ROUND(SUM(CASE WHEN is_fraud = 1 THEN amount ELSE 0 END), 2) as total_fraud_amount
            FROM fraud_detection.transactions_processed
            GROUP BY processing_date
            ORDER BY processing_date DESC
        """)
        
        print("Daily Fraud Summary:")
        daily_summary.show()
        
        # Top merchants by fraud rate
        merchant_fraud = self.spark.sql("""
            SELECT 
                merchant_id,
                COUNT(*) as total_transactions,
                SUM(is_fraud) as fraud_transactions,
                ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) as fraud_rate_percent
            FROM fraud_detection.transactions_processed
            WHERE processing_date = CURRENT_DATE()
            GROUP BY merchant_id
            HAVING COUNT(*) >= 10
            ORDER BY fraud_rate_percent DESC
            LIMIT 10
        """)
        
        print("Top Merchants by Fraud Rate:")
        merchant_fraud.show()
        
        # Hourly fraud pattern
        hourly_pattern = self.spark.sql("""
            SELECT 
                hour_of_day,
                COUNT(*) as total_transactions,
                SUM(is_fraud) as fraud_transactions,
                ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) as fraud_rate_percent
            FROM fraud_detection.transactions_processed
            WHERE processing_date = CURRENT_DATE()
            GROUP BY hour_of_day
            ORDER BY hour_of_day
        """)
        
        print("Hourly Fraud Pattern:")
        hourly_pattern.show()
    
    def run_batch_processing(self):
        """Run the complete batch processing pipeline"""
        try:
            # Setup Hive tables
            self.setup_hive_tables()
            
            # Read data from Kafka
            raw_df = self.read_kafka_batch()
            
            if raw_df.count() == 0:
                print("No new data to process")
                return
            
            print(f"Processing {raw_df.count()} transactions")
            
            # Save raw data
            self.save_to_hive(raw_df, "transactions_raw")
            
            # Feature engineering
            processed_df = self.feature_engineering(raw_df)
            
            # Save processed data
            self.save_to_hive(processed_df, "transactions_processed")
            
            # Generate reports
            self.generate_batch_reports()
            
            print("Batch processing completed successfully!")
            
        except Exception as e:
            print(f"Error in batch processing: {str(e)}")
        finally:
            self.spark.stop()

if __name__ == "__main__":
    processor = BatchProcessor()
    processor.run_batch_processing()
