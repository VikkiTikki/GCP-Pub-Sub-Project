# This script publishes messages to a Google Cloud Pub/Sub topic.
from google.cloud import pubsub_v1 # Pub/Sub client library
import json # For JSON serialization
import uuid # For generating unique message IDs
from datetime import datetime # For timestamping messages

project_id = "project-a85a075d-91d4-41d4-bc0"
topic_id = "my-topic"

publisher = pubsub_v1.PublisherClient()
# The `topic_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/topics/{topic_id}`
topic_path = publisher.topic_path(project_id, topic_id)

for n in range(1, 10):

    message_payload = {
        "message_id": str(uuid.uuid4()),
        "event_timestamp": datetime.now().isoformat(),
        "content": f"Order #{n} is ready!",
        }
    
    # Data must be a bytestring
    data = json.dumps(message_payload).encode("utf-8")
   
    # When you publish a message, the client returns a future.
    future = publisher.publish(topic_path, data)
    print(f"Published message ID: {future.result()}")

print(f"Published messages to {topic_path}.")
