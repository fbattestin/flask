from flask import Flask, request, jsonify
import requests
import boto3
import json
from confluent_kafka import Producer
from abc import ABC, abstractmethod

# Define the abstract SinkClient interface
class SinkClient(ABC):
    @abstractmethod
    def __init__(self, **kwargs):
        pass

    @abstractmethod
    def send_message(self, message, key):
        pass

# Implement S3 Client
class S3Client(SinkClient):
    def __init__(self, **kwargs):
        self.s3 = boto3.client('s3')
        self.bucket_name = kwargs.get('bucket_name')

    def send_message(self, message, key):
        response = self.s3.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=json.dumps(message)
        )
        return response

# Implement Kafka Client
class KafkaClient(SinkClient):
    def __init__(self, **kwargs):
        self.producer = Producer({'bootstrap.servers': kwargs.get('brokers')})
        self.topic = kwargs.get('topic')

    def send_message(self, message, key):
        self.producer.produce(self.topic, key=key, value=json.dumps(message))
        self.producer.flush()

# Define the dispatch table for sink clients
def get_sink_client(sink_type, **kwargs):
    dispatch_table = {
        's3': S3Client,
        'kafka': KafkaClient
    }
    if sink_type not in dispatch_table:
        raise ValueError(f"Invalid SINK_TYPE: {sink_type}. Choose 's3' or 'kafka'.")
    return dispatch_table[sink_type](**kwargs)

# Configuration
SINK_TYPE = 'kafka'  # or 's3'
SINK_CONFIG = {
    's3': {'bucket_name': 'your-bucket-name'},
    'kafka': {'brokers': 'your-kafka-broker', 'topic': 'your-topic'}
}

sink_client = get_sink_client(SINK_TYPE, **SINK_CONFIG[SINK_TYPE])

# Flask app setup
app = Flask(__name__)

def confirm_subscription(subscribe_url):
    response = requests.get(subscribe_url)
    if response.status_code == 200:
        print("Subscription confirmed successfully.")
    else:
        print(f"Failed to confirm subscription: {response.status_code}")

@app.route('/sns-endpoint', methods=['POST'])
def sns_endpoint():
    sns_message_type = request.headers.get('x-amz-sns-message-type')
    
    if sns_message_type == 'SubscriptionConfirmation':
        message = request.json
        subscribe_url = message['SubscribeURL']
        print(f"Subscription confirmation URL: {subscribe_url}")
        confirm_subscription(subscribe_url)
    elif sns_message_type == 'Notification':
        message = request.json
        notification_message = message['Message']
        print(f"Received message: {notification_message}")
        
        # Store the message using the selected sink client
        key = f"notifications/{message['MessageId']}.json"
        response = sink_client.send_message(message, key)
        print(f"Message stored with response: {response}")
    
    return jsonify({'status': 'success'}), 200

if __name__ == '__main__':
    app.run(port=5000)
