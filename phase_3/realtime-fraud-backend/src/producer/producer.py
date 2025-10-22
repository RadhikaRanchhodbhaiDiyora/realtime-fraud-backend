import os
import time
import pandas as pd
from confluent_kafka import Producer
import json
from hashlib import sha256
import sys

# Configuration
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'kafka:9092')
TOPIC = 'raw_transactions'
# File path is relative to the /app folder inside the container
FILE_PATH = 'data/fraudTrain.csv' 

# Kafka Producer setup
p = Producer({'bootstrap.servers': KAFKA_BROKER})

def mask_pii(record):
    """Masks sensitive PII fields using SHA-256 hashing."""
    
    # Hashing PII fields for anonymization/data protection
    # We convert the number/string to bytes using .encode() before hashing
    record['cc_num_hash'] = sha256(str(record['cc_num']).encode()).hexdigest()
    record['trans_num_hash'] = sha256(str(record['trans_num']).encode()).hexdigest()

    # Drop original PII and other unused columns (Data Governance)
    for col in ['cc_num', 'trans_num', 'first', 'last', 'street', 'dob', 'trans_date_trans_time']:
        if col in record:
            del record[col]
            
    return record

def produce_data_stream():
    """Reads CSV, simulates real-time stream, and produces to Kafka."""
    print("Producer Service: Starting data ingestion and real-time simulation...")
    
    try:
        # Load dataset
        df = pd.read_csv(FILE_PATH)
    except FileNotFoundError:
        print(f"ERROR: Dataset not found at {FILE_PATH}. Check your 'data' folder mapping.")
        sys.exit(1)
        
    # Ensure data is sorted by time for correct simulation
    df = df.sort_values(by='unix_time').reset_index(drop=True)
    
    last_time = None
    
    for index, row in df.iterrows():
        record = row.to_dict()
        
        # 1. PII Masking and Data Transformation
        processed_record = mask_pii(record)
        
        current_time = processed_record['unix_time']
        
        # 2. Real-Time Simulation (Calculates delay between transactions)
        if last_time is not None:
            time_difference = current_time - last_time
            # For testing, you can scale this down (e.g., time_difference / 100)
            # We will use the actual difference for the required "real-time" simulation.
            time.sleep(time_difference)

        last_time = current_time

        # 3. Produce to Kafka
        try:
            # Poll must be called to handle any previous events
            p.poll(0) 
            # Use merchant as the key for partitioning (Scalability)
            p.produce(TOPIC, 
                      key=str(processed_record['merchant']).encode('utf-8'),
                      value=json.dumps(processed_record).encode('utf-8'))
        except Exception as e:
            print(f"Producer ERROR: Failed to produce message: {e}")
            
    p.flush() # Ensure all messages are sent before exiting
    print("Producer Service: Finished producing data stream.")

if __name__ == '__main__':
    # Give Kafka a moment to start up before connecting (Reliability)
    time.sleep(10) 
    produce_data_stream()