# Import Statements
from faker import Faker
import json
import csv
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
from fastavro import parse_schema, writer
import pandas as pd
import sqlite3
import logging
from prometheus_client import start_http_server, Counter

# generating sample data
def generate_ad_impressions(num_records):
    ad_impressions = []
    for _ in range(num_records):
        impression = {
            "ad_creative_id": random.randint(1, 100),
            "user_id": random.randint(1000, 5000),
            "timestamp": fake.date_time_between(start_date='-1y', end_date='now').isoformat(),
            "website": fake.url()
        }
        ad_impressions.append(impression)
    return ad_impressions

def generate_clicks_conversions(num_records):
    clicks_conversions = []
    for _ in range(num_records):
        event_timestamp = fake.date_time_between(start_date='-1y', end_date='now').isoformat()
        click_conversion = {
            "event_timestamp": event_timestamp,
            "user_id": random.randint(1000, 5000),
            "ad_campaign_id": random.randint(1, 10),
            "conversion_type": random.choice(["signup", "purchase", "other"])
        }
        clicks_conversions.append(click_conversion)
    return clicks_conversions

def generate_bid_requests(num_records):
    bid_requests = []
    for _ in range(num_records):
        auction_details = {
            "auction_id": fake.uuid4(),
            "bid_amount": random.uniform(0.1, 10.0)
        }
        bid_request = {
            "user_id": random.randint(1000, 5000),
            "timestamp": fake.date_time_between(start_date='-1y', end_date='now').isoformat(),
            "auction_details": auction_details,
            "ad_targeting_criteria": fake.words(nb=3)
        }
        bid_requests.append(bid_request)
    return bid_requests

def save_json_data(data, file_name):
    with open(file_name, 'w') as f:
        json.dump(data, f, indent=2)

def save_csv_data(data, file_name, fieldnames):
    with open(file_name, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

# Data Ingestion
def ingest_ad_impressions(json_data):
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    producer.send('ad_impressions_topic', json_data.encode('utf-8'))

def ingest_clicks_conversions(csv_data):
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    producer.send('clicks_conversions_topic', csv_data.encode('utf-8'))

def ingest_bid_requests(avro_data):
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    producer.send('bid_requests_topic', avro_data)

# Data Processing
def process_ad_impressions(ad_impressions):
    ad_impressions_df = pd.DataFrame(ad_impressions)
    ad_impressions_df = ad_impressions_df.drop_duplicates(subset=['ad_creative_id', 'user_id', 'timestamp'])
    return ad_impressions_df.to_dict(orient='records')

def correlate_data(ad_impressions, clicks_conversions):
    ad_impressions_df = pd.DataFrame(ad_impressions)
    clicks_conversions_df = pd.DataFrame(clicks_conversions)
    correlated_data = pd.merge(ad_impressions_df, clicks_conversions_df, on='user_id', how='inner')
    return correlated_data.to_dict(orient='records')

# Data Storage
def create_database():
    connection = sqlite3.connect('advertising_data.db')
    cursor = connection.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ad_impressions (
            ad_creative_id INTEGER,
            user_id INTEGER,
            timestamp TEXT,
            website TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS clicks_conversions (
            event_timestamp TEXT,
            user_id INTEGER,
            ad_campaign_id INTEGER,
            conversion_type TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS correlated_data (
            ad_creative_id INTEGER,
            user_id INTEGER,
            timestamp TEXT,
            website TEXT,
            event_timestamp TEXT,
            ad_campaign_id INTEGER,
            conversion_type TEXT
        )
    ''')

    connection.commit()
    connection.close()

def insert_into_database(table_name, data):
    connection = sqlite3.connect('advertising_data.db')
    cursor = connection.cursor()

    cursor.executemany(f'INSERT INTO {table_name} VALUES ({",".join(["?"] * len(data[0]))})', data)

    connection.commit()
    connection.close()

# Error Handling and Monitoring
class ErrorHandlingUtil:
    def __init__(self, log_file_path='error_log.txt'):
        self.log_file_path = log_file_path
        logging.basicConfig(filename=log_file_path, level=logging.ERROR)

    def log_error(self, error_message):
        logging.error(error_message)

class PrometheusMonitoring:
    def __init__(self, port=9090):
        self.port = port
        start_http_server(port)

        #  Prometheus metrics
        self.ad_impressions_counter = Counter('ad_impressions_count', 'Count of ad impressions')
        self.clicks_conversions_counter = Counter('clicks_conversions_count', 'Count of clicks and conversions')
        self.correlation_errors_counter = Counter('correlation_errors_count', 'Count of correlation errors')

    def increment_ad_impressions(self):
        self.ad_impressions_counter.inc()

    def increment_clicks_conversions(self):
        self.clicks_conversions_counter.inc()

    def increment_correlation_errors(self):
        self.correlation_errors_counter.inc()

#  Execution
if __name__ == "__main__":
    fake = Faker()

    num_ad_impressions = 1000
    num_clicks_conversions = 800
    num_bid_requests = 1200

    ad_impressions_data = generate_ad_impressions(num_ad_impressions)
    clicks_conversions_data = generate_clicks_conversions(num_clicks_conversions)
    bid_requests_data = generate_bid_requests(num_bid_requests)

    error_handler = ErrorHandlingUtil()
    prometheus_monitoring = PrometheusMonitoring(port=9090)

    # Ingest data
    json_impressions = json.dumps(ad_impressions_data)
    csv_clicks_conversions = 'event_timestamp,user_id,ad_campaign_id,conversion_type\n'
    csv_clicks_conversions += '\n'.join([f'{item["event_timestamp"]},{item["user_id"]},{item["ad_campaign_id"]},{item["conversion_type"]}' for item in clicks_conversions_data])
    
    #  Avro schema for bid requests
    avro_schema = {
        "type": "record",
        "name": "BidRequest",
        "fields": [
            {"name": "user_id", "type": "int"},
            {"name": "timestamp", "type": "string"},
            {"name": "auction_details", "type": {"type": "record", "name": "AuctionDetails", "fields": [
                {"name": "auction_id", "type": "string"},
                {"name": "bid_amount", "type": "double"}
            ]}},
            {"name": "ad_targeting_criteria", "type": {"type": "array", "items": "string"}}
        ]
    }

    avro_bid_requests = [writer(avro_data, parse_schema(avro_schema)) for avro_data in bid_requests_data]

    try:
        ingest_ad_impressions(json_impressions)
        prometheus_monitoring.increment_ad_impressions()
    except Exception as e:
        error_handler.log_error(f"Error ingesting ad impressions: {str(e)}")

    try:
        ingest_clicks_conversions(csv_clicks_conversions)
        prometheus_monitoring.increment_clicks_conversions()
    except Exception as e:
        error_handler.log_error(f"Error ingesting clicks and conversions: {str(e)}")

    for avro_data in avro_bid_requests:
        try:
            ingest_bid_requests(avro_data)
        except Exception as e:
            error_handler.log_error(f"Error ingesting bid requests: {str(e)}")

    # Processing data
    try:
        processed_ad_impressions = process_ad_impressions(ad_impressions_data)
    except Exception as e:
        error_handler.log_error(f"Error processing ad impressions: {str(e)}")

    try:
        correlated_data = correlate_data(processed_ad_impressions, clicks_conversions_data)
        prometheus_monitoring.increment_correlation_errors()
    except Exception as e:
        error_handler.log_error(f"Error correlating data: {str(e)}")

    # Store data in the database
    try:
        create_database()
        insert_into_database('ad_impressions', [(item['ad_creative_id'], item['user_id'], item['timestamp'], item['website']) for item in ad_impressions_data])
        insert_into_database('clicks_conversions', [(item['event_timestamp'], item['user_id'], item['ad_campaign_id'], item['conversion_type']) for item in clicks_conversions_data])
        insert_into_database('correlated_data', [(item['ad_creative_id'], item['user_id'], item['timestamp'], item['website'], item['event_timestamp'], item['ad_campaign_id'], item['conversion_type']) for item in correlated_data])
    except Exception as e:
        error_handler.log_error(f"Error storing data in the database: {str(e)}")
