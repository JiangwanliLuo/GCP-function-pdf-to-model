from google.cloud import pubsub_v1
import json
import os


# credential_path = ""
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

# Set the Pub/Sub topic name
PROJECT_ID = 'cc-doc-ai-data-extract'
TOPIC_NAME = 'azure_model_result_transfer'  # Replace with your Pub/Sub topic

def publish_message(data):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)

    # Convert the data to JSON format and publish to Pub/Sub
    data_str = json.dumps(data)
    future = publisher.publish(topic_path, data_str.encode('utf-8'))
    return future.result()

def gcs_function(result):
    # Your logic to get the data (e.g., from Cloud Storage or another source)
    # Publish data to Pub/Sub
    publish_message(result)
    print(f"Published message: {result}")

