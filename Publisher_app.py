from google.cloud import pubsub_v1
import json
import uuid
from datetime import datetime
import streamlit as st
import sys
import io

# ---------------- STREAMLIT UI ----------------
st.set_page_config(page_title="Pub/Sub Publisher", layout="wide")
st.title("📤 Pub/Sub Message Publisher")

if "logs" not in st.session_state:
    st.session_state.logs = []

# Capture print output
class StreamCapture(io.StringIO):
    def write(self, txt):
        if txt.strip():
            st.session_state.logs.append(txt)
        super().write(txt)

def run_publisher():
    captured_output = StreamCapture()
    sys.stdout = captured_output

    # ---------------- YOUR ORIGINAL CODE (UNCHANGED) ----------------
    project_id = "project-a85a075d-91d4-41d4-bc0"
    topic_id = "my-topic"

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    for n in range(1, 10):

        message_payload = {
            "message_id": str(uuid.uuid4()),
            "event_timestamp": datetime.now().isoformat(),
            "content": f"Order #{n} is ready!",
        }

        data = json.dumps(message_payload).encode("utf-8")

        future = publisher.publish(topic_path, data)
        print(f"Published message ID: {future.result()}")

    print(f"Published messages to {topic_path}.")
    # ---------------------------------------------------------------

    sys.stdout = sys.__stdout__

# ---------------- UI BUTTON ----------------
if st.button("📤 Run Publisher"):
    run_publisher()

# ---------------- LOG DISPLAY ----------------
st.subheader("Output")
st.text("\n".join(st.session_state.logs[-100:]))
