from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import google.auth
import json
from datetime import datetime
from threading import Lock
import streamlit as st
import threading
import sys
import io

# ---------------- STREAMLIT SETUP ----------------
st.set_page_config(page_title="Pub/Sub Consumer", layout="wide")
st.title("📩 Pub/Sub Message Consumer")

if "running" not in st.session_state:
    st.session_state.running = False
if "logs" not in st.session_state:
    st.session_state.logs = []

# Capture print output
class StreamCapture(io.StringIO):
    def write(self, txt):
        if txt.strip():
            st.session_state.logs.append(txt)
        super().write(txt)

# ---------------- ORIGINAL CODE (UNCHANGED LOGIC) ----------------
def run_consumer():
    captured_output = StreamCapture()
    sys.stdout = captured_output

    credentials, project = google.auth.default()
    print("Authenticated project:", project)

    project_id = "project-a85a075d-91d4-41d4-bc0"
    subscription_id = "my-sub"
    timeout = 10.0

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    last_receive_time = None
    lock = Lock()

    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        nonlocal last_receive_time

        receive_time = datetime.now()
        payload = json.loads(message.data.decode("utf-8"))

        message_id = payload["message_id"]
        event_timestamp = datetime.fromisoformat(payload["event_timestamp"])
        content = payload["content"]

        with lock:
            latency = (receive_time - event_timestamp).total_seconds()

            time_between = None
            if last_receive_time:
                time_between = (receive_time - last_receive_time).total_seconds()

            last_receive_time = receive_time

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

    with subscriber:
        try:
            streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
            streaming_pull_future.cancel()
            streaming_pull_future.result()

    sys.stdout = sys.__stdout__
    st.session_state.running = False

# ---------------- UI CONTROLS ----------------
col1, col2 = st.columns(2)

with col1:
    if st.button("▶️ Start Listening"):
        if not st.session_state.running:
            st.session_state.running = True
            thread = threading.Thread(target=run_consumer, daemon=True)
            thread.start()

with col2:
    if st.button("⏹ Stop"):
        st.session_state.logs.append("Stop requested...")
        st.session_state.running = False

# ---------------- STATUS ----------------
st.subheader("Status")
st.write("🟢 Listening" if st.session_state.running else "🔴 Stopped")

# ---------------- LOG DISPLAY ----------------
st.subheader("Message Output")
st.text("\n".join(st.session_state.logs[-100:]))

# Auto refresh
st.experimental_rerun()
