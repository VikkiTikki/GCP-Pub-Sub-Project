import streamlit as st
import pandas as pd
from streamlit_autorefresh import st_autorefresh
from database import Database

st.set_page_config(page_title="Pub/Sub Consumer Dashboard", layout="wide")
st.title("📩 Pub/Sub Consumer Dashboard")

# st_autorefresh(interval=5000, key="subscriber_refresh")
if st.button("Refresh Data"):
    st.rerun()

st.subheader("Subscriber Database Records")

try:
    db = Database(
        host="130.211.227.149",
        user="user",
        password="123456",
        database="subscriber_db"
    )

    if db.conn:
        db.create_subscriber_table()
        rows = db.fetch_received_messages()

        if rows:
            df = pd.DataFrame(rows)

            if "is_duplicate" in df.columns:
                df["is_duplicate"] = df["is_duplicate"].map({
                    1: "Yes",
                    0: "No",
                    True: "Yes",
                    False: "No"
                })

            st.dataframe(df, width="stretch")
        else:
            st.info("No subscriber messages stored yet.")

        db.close()
    else:
        st.error("Could not connect to subscriber_db.")

except Exception as e:
    st.error(f"Error loading subscriber data: {e}")