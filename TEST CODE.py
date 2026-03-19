# This code is a subscriber for Google Cloud Pub/Sub that listens to messages on a specified subscription, processes them, and calculates latency and time between messages. It uses thread-safe operations to ensure accurate calculations in a concurrent environment.
from concurrent.futures import TimeoutError
from email import message
from google.cloud import pubsub_v1
import google.auth
import json
from datetime import datetime
from threading import Lock # For thread-safe operations

credentials, project = google.auth.default()
print("Authenticated project:", project)

project_id = "project-a85a075d-91d4-41d4-bc0"
subscription_id = "my-sub"
# Number of seconds the subscriber should listen for messages
timeout = 10.0


subscriber = pubsub_v1.SubscriberClient()
# The `subscription_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/subscriptions/{subscription_id}`
subscription_path = subscriber.subscription_path(project_id, subscription_id)

# Thread-Safe Structures
last_receive_time = None
lock = Lock()

# Callback function to process incoming messages
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    global last_receive_time

    receive_time = datetime.now()
    with lock:
        payload = json.loads(message.data.decode("utf-8"))

        # Extracting message details, define types for clarity
        message_id = payload["message_id"]
        event_timestamp = datetime.fromisoformat(payload["event_timestamp"])
        content = payload["content"]

        receive_time = datetime.now()
        # Latency Calculation
        latency = (receive_time - event_timestamp).total_seconds()
        
        # Time Between Messages Calculation
        time_between = None
        if last_receive_time:
                time_between = (receive_time - last_receive_time).total_seconds()

        last_receive_time = receive_time

        # Atomic Print Block
        output = f"""
        --- Message Analytics ---
        Message ID: {message_id}
        Content: {content}
        Latency (seconds): {latency:.4f}
        Time Between Messages: {time_between}
        -------------------------
        """
        print(output)

    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")

# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        streaming_pull_future.result(timeout=timeout)
    except TimeoutError:
        streaming_pull_future.cancel()
        streaming_pull_future.result()