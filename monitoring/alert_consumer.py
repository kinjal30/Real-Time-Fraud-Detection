#!/usr/bin/env python3

import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from kafka import KafkaConsumer
import logging
from datetime import datetime

class AlertConsumer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.consumer = KafkaConsumer(
            'fraud-alerts',
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='alert-monitoring-group',
            auto_offset_reset='latest'
        )
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('/opt/spark/data/logs/fraud_alerts.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Alert thresholds
        self.alert_thresholds = {
            'CRITICAL': {'email': True, 'sms': True, 'slack': True},
            'HIGH': {'email': True, 'sms': False, 'slack': True},
            'MEDIUM': {'email': True, 'sms': False, 'slack': False},
            'LOW': {'email': False, 'sms': False, 'slack': False}
        }
    
    def process_alert(self, alert):
        """Process incoming fraud alert"""
        alert_type = alert.get('alert_type', 'UNKNOWN')
        severity = alert.get('severity', 'LOW')
        user_id = alert.get('user_id', 'UNKNOWN')
        amount = alert.get('amount', 0)
        
        self.logger.info(f"Processing {severity} alert: {alert_type} for user {user_id}, amount: ${amount}")
        
        # Log alert to file
        self.log_alert(alert)
        
        # Send notifications based on severity
        if self.alert_thresholds[severity]['email']:
            self.send_email_alert(alert)
        
        if self.alert_thresholds[severity]['slack']:
            self.send_slack_alert(alert)
        
        # For critical alerts, also trigger immediate response
        if severity == 'CRITICAL':
            self.trigger_immediate_response(alert)
    
    def log_alert(self, alert):
        """Log alert details"""
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'alert_id': alert.get('alert_id'),
            'alert_type': alert.get('alert_type'),
            'severity': alert.get('severity'),
            'user_id': alert.get('user_id'),
            'transaction_id': alert.get('transaction_id'),
            'amount': alert.get('amount'),
            'location': alert.get('location'),
            'merchant_id': alert.get('merchant_id')
        }
        
        self.logger.info(f"FRAUD_ALERT: {json.dumps(log_entry)}")
    
    def send_email_alert(self, alert):
        """Send email notification (mock implementation)"""
        self.logger.info(f"EMAIL ALERT: {alert['alert_type']} - User: {alert['user_id']}")
        
        # In a real implementation, you would configure SMTP settings
        # and send actual emails to the fraud investigation team
        
    def send_slack_alert(self, alert):
        """Send Slack notification (mock implementation)"""
        self.logger.info(f"SLACK ALERT: {alert['alert_type']} - User: {alert['user_id']}")
        
        # In a real implementation, you would use Slack webhooks
        # to send alerts to a dedicated fraud monitoring channel
        
    def trigger_immediate_response(self, alert):
        """Trigger immediate response for critical alerts"""
        self.logger.warning(f"CRITICAL ALERT - IMMEDIATE RESPONSE REQUIRED: {alert}")
        
        # In a real implementation, this could:
        # 1. Automatically freeze the user's account
        # 2. Block the transaction
        # 3. Trigger a phone call to the user
        # 4. Escalate to fraud investigation team
        
    def start_monitoring(self):
        """Start monitoring fraud alerts"""
        self.logger.info("Starting fraud alert monitoring...")
        
        try:
            for message in self.consumer:
                alert = message.value
                self.process_alert(alert)
                
        except KeyboardInterrupt:
            self.logger.info("Stopping fraud alert monitoring...")
        except Exception as e:
            self.logger.error(f"Error in alert monitoring: {str(e)}")
        finally:
            self.consumer.close()

if __name__ == "__main__":
    monitor = AlertConsumer()
    monitor.start_monitoring()
