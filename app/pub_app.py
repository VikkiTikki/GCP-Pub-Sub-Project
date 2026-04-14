from google.cloud import pubsub_v1
import json
import uuid
from datetime import datetime
import streamlit as st
from streamlit_option_menu import option_menu
import sys
import io
import pandas as pd
from database import Database
import base64
import os
from config import DB_CONFIG

st.set_page_config(page_title="PawPrint Grooming Co.", page_icon="🐾",layout="wide")

def get_base64(file_path):
    with open(file_path, "rb") as f:
        return base64.b64encode(f.read()).decode()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
img_path = os.path.join(BASE_DIR, "..", "assets", "dogs.jpg")
img_base64 = get_base64(img_path)

st.markdown(
    f"""
    <style>
    [data-testid="stSidebar"] {{
        background: linear-gradient(
            rgba(0, 0, 0, 0.6),
            rgba(0, 0, 0, 0.6)
        ),
        url("data:image/jpeg;base64,{img_base64}");
        background-size: cover;
        background-position: center;
        filter: grayscale(60%);
        backdrop-filter: blur(4px);
    }}
    </style>
    """,
    unsafe_allow_html=True
)

st.markdown("""
<style>
/* Fade the testing checkbox */
div[data-testid="stCheckbox"] {
    opacity: 0.6;
}

/* On hover, make it clearer */
div[data-testid="stCheckbox"]:hover {
    opacity: 1;
}
</style>
""", unsafe_allow_html=True)

# ---------------- SIDEBAR ----------------
with st.sidebar:
    st.markdown(
        "<h4 style='text-align: center;'>Welcome! Choose an option below.</h4>",
        unsafe_allow_html=True
    )

    category = option_menu(
        menu_title="Main Menu",
        options=["Send Pickup Update", "View Messages"],
        icons=["send", "eye"],
        default_index=0,
    )

# ---------------- SESSION STATE ----------------
if "logs" not in st.session_state:
    st.session_state.logs = []

if "last_message_id" not in st.session_state:
    st.session_state.last_message_id = None

# ---------------- LOG CAPTURE ----------------
class StreamCapture(io.StringIO):
    def write(self, txt):
        if txt.strip():
            st.session_state.logs.append(txt)
        super().write(txt)

# ---------------- PUBLISHER FUNCTION ----------------
def run_publisher(user_message, reuse_previous=False):
    captured_output = StreamCapture()
    sys.stdout = captured_output
    db = None
    publish_success = False

    try:
        db = Database(
            **DB_CONFIG,
            database="subscriber_db"
)
        db.create_publisher_table()

        project_id = "project-a85a075d-91d4-41d4-bc0"
        topic_id = "my-topic"

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_id)

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

        inserted = db.insert_published_message(
            message_id=message_id,
            content=user_message,
            publish_time=publish_time
        )

        if inserted:
            print("Saved to publisher_db successfully.")
        else:
            print("Failed to save to publisher_db.")

        publish_success = True

    except Exception as e:
        print(f"Error publishing message: {e}")

    finally:
        sys.stdout = sys.__stdout__
        if db:
            db.close()

    return publish_success

# ---------------- PAGE CONTENT ----------------
if category == "Send Pickup Update":
    st.markdown(
    """
    <h1 style='font-size: 42px; margin-bottom: 0;'> PawPrint Grooming Co.</h1>
    <p style='color: gray; margin-top: 0;'>Send a quick pickup update when a pet is ready.</p>
    """,
    unsafe_allow_html=True
    )

    message_content = st.text_area(
        "Pickup message",
        placeholder="Example: Molly is ready for pickup!"
    )

    reuse_previous_id = st.checkbox(
    "Simulate Duplicate Message"
)
    if st.button("Submit"):
        if message_content.strip():
            if reuse_previous_id and not st.session_state.last_message_id:
                st.warning("No previous message exists yet to reuse.")
            else:
                success = run_publisher(message_content, reuse_previous_id)
                if success:
                    st.success("Pickup update sent successfully.")
                else:
                    st.error("The update could not be sent. Please try again.")
        else:
            st.warning("Please enter a pickup message before sending.")

elif category == "View Messages":
    st.title("Message Records")

    try:
        db_view = Database(
            host="127.0.0.1",
            user="user",
            port=3307,
            password="123456",
            database="publisher_db"
        )
        db_view.create_publisher_table()
        published_rows = db_view.fetch_published_messages()

        if published_rows:
            st.dataframe(pd.DataFrame(published_rows), width="stretch")
        else:
            st.info("No messages stored yet.")

        db_view.close()

    except Exception as e:
        st.error(f"Could not load message records: {e}")