import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from google.cloud import pubsub_v1
import google.auth
import json
from datetime import datetime
from config import DB_CONFIG
from storage.database import Database

# ---------------- AUTH ----------------
credentials, project = google.auth.default()
print("Authenticated project:", project)

# ---------------- PUB/SUB ----------------
project_id = "project-a85a075d-91d4-41d4-bc0"
subscription_id = "my-sub-2"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

print(f"Listening for messages on {subscription_path}...")

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    db = None
    receive_time = datetime.now()

    try:
        payload = json.loads(message.data.decode("utf-8"))

        message_id = payload["message_id"]
        publish_time = datetime.fromisoformat(payload["event_timestamp"])
        content = payload["content"]

        db = Database(
            **DB_CONFIG,
            database="subscriber_db"
        )

        db.create_second_subscriber_table()

        saved = db.insert_second_received_message(
            message_id,
            content,
            publish_time,
            receive_time,
        )

        if saved:
            print(f"Saved message {message_id} to second subscriber table")
            message.ack()
            print("Message ACKed\n")
        else:
            print("Insert error")
            message.nack()

    except Exception as e:
        print(f"Error processing message: {e}")
        message.nack()

    finally:
        if db:
            db.close()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

with subscriber:
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        print("Second subscriber worker stopped.")