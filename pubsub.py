import json
import time
import random
from datetime import datetime, timezone
from google.cloud import pubsub_v1, storage
import os

# Configuration
PROJECT_ID = "data-lake-seminar"
TOPIC_ID = "log-events"
SUBSCRIPTION_ID = "log-events-sub"
BUCKET_NAME = "hospital-data-lake"

# Initialize clients
publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
storage_client = storage.Client(project=PROJECT_ID)

topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

def generate_log_entry():
    """Generate a simulated log entry"""
    log_levels = ["INFO", "WARNING", "ERROR", "DEBUG"]
    services = ["auth-service", "payment-service", "user-service", "api-gateway"]
    
    log_entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "level": random.choice(log_levels),
        "service": random.choice(services),
        "message": f"Processing request {random.randint(1000, 9999)}",
        "user_id": f"user_{random.randint(1, 100)}",
        "response_time_ms": random.randint(50, 500),
        "status_code": random.choice([200, 201, 400, 404, 500])
    }
    return log_entry

def publish_logs():
    print(f"Starting log publisher to topic: {topic_path}")
    
    while True:
        # Generate batch of logs (5-10 logs per minute)
        num_logs = random.randint(5, 10)
        logs = [generate_log_entry() for _ in range(num_logs)]
        
        # Publish each log
        for log in logs:
            data = json.dumps(log).encode("utf-8")
            future = publisher.publish(topic_path, data)
            print(f"Published log: {log['level']} - {log['service']}")
        
        print(f"Published {num_logs} logs. Waiting 60 seconds...")
        time.sleep(60)

def upload_to_gcs(logs, filename):
    """Upload logs to GCS bucket"""
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"logs/{filename}")
    
    # Convert logs to newline-delimited JSON
    log_data = "\n".join([json.dumps(log) for log in logs])
    blob.upload_from_string(log_data, content_type="application/json")
    print(f"Uploaded {filename} to gs://{BUCKET_NAME}/logs/")

def callback(message):
    """Process received messages and batch upload to GCS"""
    log_entry = json.loads(message.data.decode("utf-8"))
    
    # Store in memory buffer (in production, use Redis or similar)
    if not hasattr(callback, "buffer"):
        callback.buffer = []
        callback.last_upload = time.time()
    
    callback.buffer.append(log_entry)
    
    # Upload every minute or when buffer reaches 50 logs
    current_time = time.time()
    if (current_time - callback.last_upload >= 60) or (len(callback.buffer) >= 50):
        #timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        timestamp = datetime.now(timezone.utc)

        filename = f"logs_{timestamp}.jsonl"
        
        upload_to_gcs(callback.buffer, filename)
        
        # Reset buffer
        callback.buffer = []
        callback.last_upload = current_time
    
    message.ack()

def subscribe_and_upload():
    """Subscribe to messages and upload to GCS"""
    print(f"Listening for messages on {subscription_path}")
    
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        print("\nSubscriber stopped.")

def setup_pubsub():
    """Create topic and subscription if they don't exist"""
    try:
        publisher.create_topic(request={"name": topic_path})
        print(f"Created topic: {topic_path}")
    except Exception as e:
        print(f"Topic already exists or error: {e}")
    
    try:
        subscriber.create_subscription(
            request={"name": subscription_path, "topic": topic_path}
        )
        print(f"Created subscription: {subscription_path}")
    except Exception as e:
        print(f"Subscription already exists or error: {e}")

if __name__ == "__main__":
    import sys
    
    # Setup
    setup_pubsub()
    
    if len(sys.argv) > 1 and sys.argv[1] == "publisher":
        # Run as publisher
        publish_logs()
    elif len(sys.argv) > 1 and sys.argv[1] == "subscriber":
        # Run as subscriber
        subscribe_and_upload()
    else:
        print("Usage:")
        print("  python pubsub.py publisher   # Start publishing logs")
        print("  python pubsub.py subscriber  # Start subscriber and upload to GCS")
