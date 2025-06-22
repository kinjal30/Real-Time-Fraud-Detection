#!/usr/bin/env python3

import json
import time
import requests
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

def test_kafka_connectivity():
    """Test Kafka connectivity"""
    print("Testing Kafka connectivity...")
    
    try:
        # Test producer
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        test_message = {"test": "message", "timestamp": time.time()}
        producer.send('test-topic', test_message)
        producer.flush()
        producer.close()
        
        print("✓ Kafka producer test passed")
        
        # Test consumer
        consumer = KafkaConsumer(
            'test-topic',
            bootstrap_servers='localhost:9092',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000
        )
        
        messages = []
        for message in consumer:
            messages.append(message.value)
            break
        
        consumer.close()
        
        if messages:
            print("✓ Kafka consumer test passed")
        else:
            print("✗ Kafka consumer test failed")
            
    except Exception as e:
        print(f"✗ Kafka test failed: {str(e)}")

def test_flink_dashboard():
    """Test Flink dashboard accessibility"""
    print("Testing Flink dashboard...")
    
    try:
        response = requests.get('http://localhost:8081/overview', timeout=10)
        if response.status_code == 200:
            print("✓ Flink dashboard accessible")
        else:
            print(f"✗ Flink dashboard returned status: {response.status_code}")
    except Exception as e:
        print(f"✗ Flink dashboard test failed: {str(e)}")

def test_spark_master():
    """Test Spark master accessibility"""
    print("Testing Spark master...")
    
    try:
        response = requests.get('http://localhost:8080', timeout=10)
        if response.status_code == 200:
            print("✓ Spark master accessible")
        else:
            print(f"✗ Spark master returned status: {response.status_code}")
    except Exception as e:
        print(f"✗ Spark master test failed: {str(e)}")

def test_hadoop_namenode():
    """Test Hadoop NameNode accessibility"""
    print("Testing Hadoop NameNode...")
    
    try:
        response = requests.get('http://localhost:9870', timeout=10)
        if response.status_code == 200:
            print("✓ Hadoop NameNode accessible")
        else:
            print(f"✗ Hadoop NameNode returned status: {response.status_code}")
    except Exception as e:
        print(f"✗ Hadoop NameNode test failed: {str(e)}")

def main():
    """Run all system tests"""
    print("Running Fraud Detection System Tests...")
    print("=" * 50)
    
    test_kafka_connectivity()
    test_flink_dashboard()
    test_spark_master()
    test_hadoop_namenode()
    
    print("=" * 50)
    print("System tests completed!")

if __name__ == "__main__":
    main()
