package com.frauddetection.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class FraudDetectionJob {
    
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Kafka source configuration
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:29092")
                .setTopics("transactions")
                .setGroupId("fraud-detection-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        
        // Kafka sink for alerts
        KafkaSink<String> alertSink = KafkaSink.<String>builder()
                .setBootstrapServers("kafka:29092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("fraud-alerts")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();
        
        // Create data stream from Kafka
        DataStream<String> transactions = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        
        // Parse JSON and detect fraud
        DataStream<Transaction> parsedTransactions = transactions
                .map(new TransactionParser());
        
        // Rule 1: High amount transactions (> $5000)
        DataStream<String> highAmountAlerts = parsedTransactions
                .filter(new HighAmountFilter())
                .map(new AlertGenerator("HIGH_AMOUNT"));
        
        // Rule 2: Rapid transactions (more than 3 in 1 minute window)
        DataStream<String> rapidTransactionAlerts = parsedTransactions
                .keyBy(Transaction::getUserId)
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .process(new RapidTransactionDetector());
        
        // Rule 3: Unusual location (simple rule - transactions from "Unknown Location")
        DataStream<String> unusualLocationAlerts = parsedTransactions
                .filter(new UnusualLocationFilter())
                .map(new AlertGenerator("UNUSUAL_LOCATION"));
        
        // Rule 4: Large amount at unusual hours (> $1000 between 2 AM and 5 AM)
        DataStream<String> unusualHourAlerts = parsedTransactions
                .filter(new UnusualHourFilter())
                .map(new AlertGenerator("UNUSUAL_HOUR"));
        
        // Combine all alerts
        DataStream<String> allAlerts = highAmountAlerts
                .union(rapidTransactionAlerts)
                .union(unusualLocationAlerts)
                .union(unusualHourAlerts);
        
        // Send alerts to Kafka
        allAlerts.sinkTo(alertSink);
        
        // Print alerts to console for monitoring
        allAlerts.print("FRAUD ALERT");
        
        env.execute("Real-time Fraud Detection");
    }
    
    // Transaction POJO
    public static class Transaction {
        public String transactionId;
        public String userId;
        public double amount;
        public String merchantId;
        public String location;
        public String timestamp;
        public int hourOfDay;
        
        // Constructors, getters, setters
        public Transaction() {}
        
        public String getTransactionId() { return transactionId; }
        public String getUserId() { return userId; }
        public double getAmount() { return amount; }
        public String getMerchantId() { return merchantId; }
        public String getLocation() { return location; }
        public String getTimestamp() { return timestamp; }
        public int getHourOfDay() { return hourOfDay; }
    }
    
    // JSON Parser
    public static class TransactionParser implements MapFunction<String, Transaction> {
        private ObjectMapper objectMapper = new ObjectMapper();
        
        @Override
        public Transaction map(String value) throws Exception {
            JsonNode json = objectMapper.readTree(value);
            Transaction transaction = new Transaction();
            transaction.transactionId = json.get("transaction_id").asText();
            transaction.userId = json.get("user_id").asText();
            transaction.amount = json.get("amount").asDouble();
            transaction.merchantId = json.get("merchant_id").asText();
            transaction.location = json.get("location").asText();
            transaction.timestamp = json.get("timestamp").asText();
            transaction.hourOfDay = json.get("hour_of_day").asInt();
            return transaction;
        }
    }
    
    // High amount filter
    public static class HighAmountFilter implements FilterFunction<Transaction> {
        @Override
        public boolean filter(Transaction transaction) throws Exception {
            return transaction.getAmount() > 5000.0;
        }
    }
    
    // Unusual location filter
    public static class UnusualLocationFilter implements FilterFunction<Transaction> {
        @Override
        public boolean filter(Transaction transaction) throws Exception {
            return "Unknown Location".equals(transaction.getLocation());
        }
    }
    
    // Unusual hour filter
    public static class UnusualHourFilter implements FilterFunction<Transaction> {
        @Override
        public boolean filter(Transaction transaction) throws Exception {
            int hour = transaction.getHourOfDay();
            return transaction.getAmount() > 1000.0 && (hour >= 2 && hour <= 5);
        }
    }
    
    // Alert generator
    public static class AlertGenerator implements MapFunction<Transaction, String> {
        private String alertType;
        private ObjectMapper objectMapper = new ObjectMapper();
        
        public AlertGenerator(String alertType) {
            this.alertType = alertType;
        }
        
        @Override
        public String map(Transaction transaction) throws Exception {
            Map<String, Object> alert = new HashMap<>();
            alert.put("alert_id", java.util.UUID.randomUUID().toString());
            alert.put("alert_type", alertType);
            alert.put("transaction_id", transaction.getTransactionId());
            alert.put("user_id", transaction.getUserId());
            alert.put("amount", transaction.getAmount());
            alert.put("merchant_id", transaction.getMerchantId());
            alert.put("location", transaction.getLocation());
            alert.put("timestamp", transaction.getTimestamp());
            alert.put("alert_timestamp", java.time.Instant.now().toString());
            alert.put("severity", getSeverity(alertType, transaction.getAmount()));
            
            return objectMapper.writeValueAsString(alert);
        }
        
        private String getSeverity(String alertType, double amount) {
            if ("HIGH_AMOUNT".equals(alertType) && amount > 10000) return "CRITICAL";
            if ("RAPID_TRANSACTIONS".equals(alertType)) return "HIGH";
            if ("UNUSUAL_LOCATION".equals(alertType)) return "MEDIUM";
            if ("UNUSUAL_HOUR".equals(alertType)) return "MEDIUM";
            return "LOW";
        }
    }
    
    // Rapid transaction detector
    public static class RapidTransactionDetector 
            extends ProcessWindowFunction<Transaction, String, String, TimeWindow> {
        
        private ObjectMapper objectMapper = new ObjectMapper();
        
        @Override
        public void process(String userId, Context context, 
                          Iterable<Transaction> elements, Collector<String> out) throws Exception {
            
            int count = 0;
            Transaction lastTransaction = null;
            
            for (Transaction transaction : elements) {
                count++;
                lastTransaction = transaction;
            }
            
            // Alert if more than 3 transactions in 1 minute
            if (count > 3 && lastTransaction != null) {
                Map<String, Object> alert = new HashMap<>();
                alert.put("alert_id", java.util.UUID.randomUUID().toString());
                alert.put("alert_type", "RAPID_TRANSACTIONS");
                alert.put("user_id", userId);
                alert.put("transaction_count", count);
                alert.put("time_window", "1_minute");
                alert.put("last_transaction_id", lastTransaction.getTransactionId());
                alert.put("alert_timestamp", java.time.Instant.now().toString());
                alert.put("severity", "HIGH");
                
                out.collect(objectMapper.writeValueAsString(alert));
            }
        }
    }
}
