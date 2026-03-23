from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import google.auth
from datetime import datetime
from Database import Database

# Authenticate with Google Cloud
credentials, project = google.auth.default()
print("Authenticated project:", project)

# -------------------------------
# CONNECT TO CLOUD SQL (MySQL)
# -------------------------------
db = Database(
    host="130.211.227.149",   # Your Cloud SQL public IP
    user="root",              # Your MySQL username
    password="123456", # <-- Replace this with your actual password
    database="Pubsub"         # The database you created in Cloud SQL
)

# Create table if it doesn't exist
db.create_table()

# -------------------------------
# PUB/SUB CONFIG
# -------------------------------
project_id = "project-a85a075d-91d4-41d4-bc0"
subscription_id = "my-sub"
timeout = 60.0  # Listen for 60 seconds

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

# -------------------------------
# CALLBACK FUNCTION
# -------------------------------
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    print("\n--- New Message Received ---")

    # Extract message data
    data = message.data.decode("utf-8")
    message_id = message.message_id
    publish_time = message.publish_time.replace(tzinfo=None)
    receive_time = datetime.utcnow()

    print(f"Message ID: {message_id}")
    print(f"Content: {data}")
    print(f"Published at: {publish_time}")
    print(f"Received at: {receive_time}")

    # Save to Cloud SQL
    saved = db.insert_message(message_id, data, publish_time, receive_time)

    if saved:
        print("Status: Saved to Cloud SQL")
    else:
        print("Status: Duplicate or Insert Error")

    # Acknowledge the message
    message.ack()
    print("Message ACKed")

# -------------------------------
# START LISTENING
# -------------------------------
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}...\n")

with subscriber:
    try:
        streaming_pull_future.result(timeout=timeout)
    except TimeoutError:
        streaming_pull_future.cancel()
        streaming_pull_future.result()
