#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
import json

class FraudMLTraining:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("FraudDetectionMLTraining") \
            .config("spark.sql.warehouse.dir", "/opt/spark/data/warehouse") \
            .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
            .enableHiveSupport() \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
    
    def load_historical_data(self):
        """Load historical transaction data from Hive"""
        print("Loading historical transaction data...")
        
        # Read from Hive table (will be created by batch processing job)
        df = self.spark.sql("""
            SELECT 
                transaction_id,
                user_id,
                amount,
                merchant_category,
                location,
                hour_of_day,
                is_weekend,
                channel,
                card_type,
                is_fraud
            FROM fraud_detection.transactions_processed
            WHERE is_fraud IS NOT NULL
        """)
        
        return df
    
    def feature_engineering(self, df):
        """Create features for ML model"""
        print("Performing feature engineering...")
        
        # Create additional features
        df = df.withColumn("amount_log", log(col("amount") + 1))
        df = df.withColumn("is_high_amount", (col("amount") > 1000).cast("int"))
        df = df.withColumn("is_night_transaction", 
                          ((col("hour_of_day") >= 22) | (col("hour_of_day") <= 6)).cast("int"))
        
        # String indexers for categorical variables
        merchant_indexer = StringIndexer(inputCol="merchant_category", outputCol="merchant_category_idx")
        location_indexer = StringIndexer(inputCol="location", outputCol="location_idx")
        channel_indexer = StringIndexer(inputCol="channel", outputCol="channel_idx")
        card_type_indexer = StringIndexer(inputCol="card_type", outputCol="card_type_idx")
        
        # Apply indexers
        df = merchant_indexer.fit(df).transform(df)
        df = location_indexer.fit(df).transform(df)
        df = channel_indexer.fit(df).transform(df)
        df = card_type_indexer.fit(df).transform(df)
        
        # Feature vector assembly
        feature_cols = [
            "amount", "amount_log", "hour_of_day", "is_weekend", 
            "is_high_amount", "is_night_transaction",
            "merchant_category_idx", "location_idx", "channel_idx", "card_type_idx"
        ]
        
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
        df = assembler.transform(df)
        
        # Scale features
        scaler = StandardScaler(inputCol="features_raw", outputCol="features")
        scaler_model = scaler.fit(df)
        df = scaler_model.transform(df)
        
        return df, scaler_model
    
    def train_models(self, df):
        """Train multiple ML models"""
        print("Training ML models...")
        
        # Split data
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
        
        print(f"Training set size: {train_df.count()}")
        print(f"Test set size: {test_df.count()}")
        
        # Random Forest
        rf = RandomForestClassifier(
            featuresCol="features",
            labelCol="is_fraud",
            numTrees=100,
            maxDepth=10,
            seed=42
        )
        
        # Logistic Regression
        lr = LogisticRegression(
            featuresCol="features",
            labelCol="is_fraud",
            maxIter=100
        )
        
        # Train models
        print("Training Random Forest...")
        rf_model = rf.fit(train_df)
        
        print("Training Logistic Regression...")
        lr_model = lr.fit(train_df)
        
        # Evaluate models
        evaluator = BinaryClassificationEvaluator(labelCol="is_fraud", metricName="areaUnderROC")
        
        # Random Forest evaluation
        rf_predictions = rf_model.transform(test_df)
        rf_auc = evaluator.evaluate(rf_predictions)
        print(f"Random Forest AUC: {rf_auc:.4f}")
        
        # Logistic Regression evaluation
        lr_predictions = lr_model.transform(test_df)
        lr_auc = evaluator.evaluate(lr_predictions)
        print(f"Logistic Regression AUC: {lr_auc:.4f}")
        
        # Choose best model
        if rf_auc > lr_auc:
            best_model = rf_model
            best_model_name = "RandomForest"
            best_auc = rf_auc
        else:
            best_model = lr_model
            best_model_name = "LogisticRegression"
            best_auc = lr_auc
        
        print(f"Best model: {best_model_name} with AUC: {best_auc:.4f}")
        
        return best_model, best_model_name, best_auc
    
    def save_model(self, model, model_name):
        """Save the trained model"""
        model_path = f"/opt/spark/data/models/fraud_detection_{model_name.lower()}"
        print(f"Saving model to: {model_path}")
        model.write().overwrite().save(model_path)
        
        # Save model metadata
        metadata = {
            "model_name": model_name,
            "model_path": model_path,
            "training_timestamp": str(datetime.now()),
            "features": [
                "amount", "amount_log", "hour_of_day", "is_weekend", 
                "is_high_amount", "is_night_transaction",
                "merchant_category_idx", "location_idx", "channel_idx", "card_type_idx"
            ]
        }
        
        with open("/opt/spark/data/models/model_metadata.json", "w") as f:
            json.dump(metadata, f, indent=2)
    
    def run_training_pipeline(self):
        """Run the complete ML training pipeline"""
        try:
            # Load data
            df = self.load_historical_data()
            
            if df.count() == 0:
                print("No historical data found. Please run batch processing first.")
                return
            
            # Feature engineering
            df_features, scaler_model = self.feature_engineering(df)
            
            # Train models
            best_model, model_name, auc = self.train_models(df_features)
            
            # Save model
            self.save_model(best_model, model_name)
            
            print("ML training pipeline completed successfully!")
            
        except Exception as e:
            print(f"Error in ML training pipeline: {str(e)}")
        finally:
            self.spark.stop()

if __name__ == "__main__":
    from datetime import datetime
    
    trainer = FraudMLTraining()
    trainer.run_training_pipeline()
