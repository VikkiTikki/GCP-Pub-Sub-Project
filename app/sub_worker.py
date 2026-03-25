from google.cloud import pubsub_v1
import google.auth
import json
from datetime import datetime
from database import Database

# ---------------- AUTH ----------------
credentials, project = google.auth.default()
print("Authenticated project:", project)

# ---------------- PUB/SUB ----------------
project_id = "project-a85a075d-91d4-41d4-bc0"
subscription_id = "my-sub"

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

        # NEW: open a fresh DB connection for this message
        db = Database(
            host="130.211.227.149",
            user="user",
            password="123456",
            database="subscriber_db"
        )

        if not db.conn:
            print("Database connection failed inside callback.")
            message.nack()
            return

        db.create_subscriber_table()

        is_duplicate = db.message_exists(message_id)

        saved = db.insert_received_message(
            message_id,
            content,
            publish_time,
            receive_time,
            is_duplicate
        )

        if saved:
            print(f"Saved message {message_id} | Duplicate: {'Yes' if is_duplicate else 'No'}")
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
        print("Subscriber worker stopped.")