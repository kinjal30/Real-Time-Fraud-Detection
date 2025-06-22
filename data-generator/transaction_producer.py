#!/usr/bin/env python3

import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
import uuid

class TransactionGenerator:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        
        # Sample data for realistic transactions
        self.merchants = [
            'Amazon', 'Walmart', 'Target', 'Starbucks', 'McDonalds',
            'Shell', 'Exxon', 'Home Depot', 'Best Buy', 'Costco',
            'ATM_Withdrawal', 'Online_Transfer', 'Gas_Station', 'Grocery_Store'
        ]
        
        self.locations = [
            'New York, NY', 'Los Angeles, CA', 'Chicago, IL', 'Houston, TX',
            'Phoenix, AZ', 'Philadelphia, PA', 'San Antonio, TX', 'San Diego, CA',
            'Dallas, TX', 'San Jose, CA', 'Austin, TX', 'Jacksonville, FL'
        ]
        
        self.user_profiles = self._generate_user_profiles()
        
    def _generate_user_profiles(self):
        """Generate user profiles with spending patterns"""
        profiles = {}
        for i in range(1000):  # 1000 users
            user_id = f"user_{i:04d}"
            profiles[user_id] = {
                'avg_transaction_amount': random.uniform(20, 500),
                'preferred_merchants': random.sample(self.merchants, random.randint(3, 7)),
                'home_location': random.choice(self.locations),
                'risk_score': random.uniform(0.1, 0.9)
            }
        return profiles
    
    def generate_normal_transaction(self):
        """Generate a normal transaction"""
        user_id = random.choice(list(self.user_profiles.keys()))
        profile = self.user_profiles[user_id]
        
        # Normal transaction characteristics
        amount = max(1.0, random.gauss(profile['avg_transaction_amount'], 
                                     profile['avg_transaction_amount'] * 0.3))
        merchant = random.choice(profile['preferred_merchants'])
        location = profile['home_location'] if random.random() < 0.8 else random.choice(self.locations)
        
        transaction = {
            'transaction_id': str(uuid.uuid4()),
            'user_id': user_id,
            'amount': round(amount, 2),
            'merchant_id': merchant,
            'merchant_category': self._get_merchant_category(merchant),
            'location': location,
            'timestamp': datetime.now().isoformat(),
            'card_type': random.choice(['credit', 'debit']),
            'channel': random.choice(['online', 'pos', 'atm', 'mobile']),
            'is_weekend': datetime.now().weekday() >= 5,
            'hour_of_day': datetime.now().hour
        }
        
        return transaction
    
    def generate_fraudulent_transaction(self):
        """Generate a potentially fraudulent transaction"""
        user_id = random.choice(list(self.user_profiles.keys()))
        profile = self.user_profiles[user_id]
        
        fraud_type = random.choice(['high_amount', 'unusual_location', 'rapid_transactions', 'unusual_merchant'])
        
        if fraud_type == 'high_amount':
            # Unusually high amount
            amount = profile['avg_transaction_amount'] * random.uniform(5, 20)
        elif fraud_type == 'unusual_location':
            # Transaction from unusual location
            amount = random.uniform(50, 300)
        elif fraud_type == 'rapid_transactions':
            # Part of rapid transaction sequence
            amount = random.uniform(20, 100)
        else:  # unusual_merchant
            # Transaction at unusual merchant
            amount = random.uniform(100, 1000)
        
        # Choose unusual characteristics
        merchant = random.choice(self.merchants) if fraud_type != 'unusual_merchant' else 'UNKNOWN_MERCHANT'
        location = random.choice(self.locations) if fraud_type != 'unusual_location' else 'Unknown Location'
        
        transaction = {
            'transaction_id': str(uuid.uuid4()),
            'user_id': user_id,
            'amount': round(amount, 2),
            'merchant_id': merchant,
            'merchant_category': self._get_merchant_category(merchant),
            'location': location,
            'timestamp': datetime.now().isoformat(),
            'card_type': random.choice(['credit', 'debit']),
            'channel': random.choice(['online', 'pos', 'atm', 'mobile']),
            'is_weekend': datetime.now().weekday() >= 5,
            'hour_of_day': datetime.now().hour,
            'fraud_type': fraud_type  # This would not be available in real scenario
        }
        
        return transaction
    
    def _get_merchant_category(self, merchant):
        """Get merchant category"""
        categories = {
            'Amazon': 'retail',
            'Walmart': 'retail',
            'Target': 'retail',
            'Starbucks': 'food',
            'McDonalds': 'food',
            'Shell': 'gas',
            'Exxon': 'gas',
            'Home Depot': 'retail',
            'Best Buy': 'electronics',
            'Costco': 'retail',
            'ATM_Withdrawal': 'cash',
            'Online_Transfer': 'transfer',
            'Gas_Station': 'gas',
            'Grocery_Store': 'grocery'
        }
        return categories.get(merchant, 'other')
    
    def start_streaming(self, topic='transactions', fraud_rate=0.05):
        """Start streaming transactions to Kafka"""
        print(f"Starting transaction stream to topic: {topic}")
        print(f"Fraud rate: {fraud_rate * 100}%")
        
        try:
            while True:
                # Decide if this should be a fraudulent transaction
                if random.random() < fraud_rate:
                    transaction = self.generate_fraudulent_transaction()
                    print(f"Generated fraudulent transaction: {transaction['transaction_id']}")
                else:
                    transaction = self.generate_normal_transaction()
                
                # Send to Kafka
                self.producer.send(
                    topic,
                    key=transaction['user_id'],
                    value=transaction
                )
                
                # Random delay between transactions
                time.sleep(random.uniform(0.1, 2.0))
                
        except KeyboardInterrupt:
            print("Stopping transaction generator...")
        finally:
            self.producer.close()

if __name__ == "__main__":
    generator = TransactionGenerator()
    generator.start_streaming()
