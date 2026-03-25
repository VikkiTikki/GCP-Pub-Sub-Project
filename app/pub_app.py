from google.cloud import pubsub_v1
import json
import uuid
from datetime import datetime
import streamlit as st
import sys
import io
import pandas as pd
from database import Database

# ---------------- STREAMLIT UI ----------------
st.set_page_config(page_title="Pub/Sub Publisher", layout="wide")
st.title("📤 Pub/Sub Message Publisher")

if "logs" not in st.session_state:
    st.session_state.logs = []

if "last_message_id" not in st.session_state:
    st.session_state.last_message_id = None

# ---------------- USER INPUT ----------------
message_content = st.text_area(
    "Enter message content:",
    placeholder="Type your message here..."
)

reuse_previous_id = st.checkbox("Reuse previous message ID (simulate duplicate)")

# Capture print output
class StreamCapture(io.StringIO):
    def write(self, txt):
        if txt.strip():
            st.session_state.logs.append(txt)
        super().write(txt)

def run_publisher(user_message, reuse_previous=False):
    captured_output = StreamCapture()
    sys.stdout = captured_output

    db = None

    try:
        # ---------------- DATABASE CONNECTION ----------------
        db = Database(
            host="130.211.227.149",
            user="user",
            password="123456",
            database="publisher_db"
        )
        db.create_publisher_table()

        # ---------------- PUB/SUB SETUP ----------------
        project_id = "project-a85a075d-91d4-41d4-bc0"
        topic_id = "my-topic"

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_id)

        # ---------------- MESSAGE ID LOGIC ----------------
        if reuse_previous and st.session_state.last_message_id:
            message_id = st.session_state.last_message_id
            print(f"Reusing previous message_id for duplicate test: {message_id}")
        else:
            message_id = str(uuid.uuid4())
            st.session_state.last_message_id = message_id
            print(f"Generated new message_id: {message_id}")

        publish_time = datetime.now()

        message_payload = {
            "message_id": message_id,
            "event_timestamp": publish_time.isoformat(),
            "content": user_message,
        }

        data = json.dumps(message_payload).encode("utf-8")

        future = publisher.publish(topic_path, data)
        pubsub_message_id = future.result()

        print(f"Published Pub/Sub message ID: {pubsub_message_id}")
        print("Payload sent:")
        print(json.dumps(message_payload, indent=2))

        # ---------------- SAVE TO PUBLISHER DB ----------------
        inserted = db.insert_published_message(
            message_id=message_id,
            content=user_message,
            publish_time=publish_time
        )

        if inserted:
            print("Saved to publisher_db successfully.")
        else:
            print("Failed to save to publisher_db.")

    except Exception as e:
        print(f"Error publishing message: {e}")

    finally:
        sys.stdout = sys.__stdout__
        if db:
            db.close()

# ---------------- UI BUTTON ----------------
if st.button("📤 Publish Message"):
    if message_content.strip():
        if reuse_previous_id and not st.session_state.last_message_id:
            st.warning("No previous message exists yet to reuse.")
        else:
            run_publisher(message_content, reuse_previous_id)
    else:
        st.warning("Please enter message content before publishing.")

# ---------------- LOG DISPLAY ----------------
# print (st.subheader("Publisher Output")
# st.text("\n".join(st.session_state.logs[-100:]))

# ---------------- SHOW PUBLISHER DATABASE ----------------
st.subheader("Publisher Database Records")

try:
    db_view = Database(
        host="130.211.227.149",
        user="user",
        password="123456",
        database="publisher_db"
    )
    db_view.create_publisher_table()
    published_rows = db_view.fetch_published_messages()

    if published_rows:
        st.dataframe(pd.DataFrame(published_rows), width="stretch")
    else:
        st.info("No published messages stored yet.")

    db_view.close()

except Exception as e:
    st.error(f"Could not load publisher database records: {e}")

