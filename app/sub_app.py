import streamlit as st
import pandas as pd
from database import Database
from datetime import datetime

def reset_filters(min_time, max_time):
    st.session_state["selected_message_id"] = "All"
    st.session_state["duplicate_filter"] = "All messages"
    st.session_state["start_receive_date"] = min_time.date()
    st.session_state["start_receive_time"] = min_time.time().replace(microsecond=0)
    st.session_state["end_receive_date"] = max_time.date()
    st.session_state["end_receive_time"] = max_time.time().replace(microsecond=0)
    st.session_state["apply_time_filter"] = False
    st.session_state["sort_option"] = "Newest records first"

st.set_page_config(page_title="Pub/Sub Consumer Dashboard", layout="wide")
st.title("📩 Pub/Sub Consumer Dashboard")

if st.button("Refresh Data"):
    st.rerun()

st.subheader("Subscriber Database Records")

try:
    db = Database(
        host="127.0.0.1",
        user="user",
        port=3307,
        password="123456",
        database="subscriber_db"
    )

    if db.conn:
        db.create_subscriber_table()
        rows = db.fetch_received_messages()

        if rows:
            df = pd.DataFrame(rows)

            if not df.empty:
                # ---------------- CLEANUP ----------------
                df["publish_time"] = pd.to_datetime(df["publish_time"])
                df["receive_time"] = pd.to_datetime(df["receive_time"])
                df["message_id"] = df["message_id"].astype(str).str.strip()

                display_df = df.copy()

                message_ids = ["All"] + sorted(
                    display_df["message_id"].dropna().unique().tolist()
                )

                min_time = display_df["receive_time"].min().to_pydatetime()
                max_time = display_df["receive_time"].max().to_pydatetime()

                # ---------------- SESSION STATE ----------------
                if "selected_message_id" not in st.session_state:
                    st.session_state["selected_message_id"] = "All"

                if "duplicate_filter" not in st.session_state:
                    st.session_state["duplicate_filter"] = "All messages"

                if "start_receive_date" not in st.session_state:
                    st.session_state["start_receive_date"] = min_time.date()

                if "start_receive_time" not in st.session_state:
                    st.session_state["start_receive_time"] = min_time.time().replace(microsecond=0)

                if "end_receive_date" not in st.session_state:
                    st.session_state["end_receive_date"] = max_time.date()

                if "end_receive_time" not in st.session_state:
                    st.session_state["end_receive_time"] = max_time.time().replace(microsecond=0)

                if "apply_time_filter" not in st.session_state:
                    st.session_state["apply_time_filter"] = False

                if "sort_option" not in st.session_state:
                    st.session_state["sort_option"] = "Newest records first"

                # ---------------- FILTER + SORT PANEL ----------------
                with st.expander("Filter and Sort Messages", expanded=False):
                    st.selectbox(
                        "Message ID",
                        options=message_ids,
                        key="selected_message_id"
                    )

                    st.selectbox(
                        "Duplicate Filter",
                        options=[
                            "All messages",
                            "Duplicates only",
                            "Non-duplicates only"
                        ],
                        key="duplicate_filter"
                    )

                    st.markdown("**Time Range**")
                    time_col1, time_col2 = st.columns(2)

                    with time_col1:
                        st.date_input("Start Date", key="start_receive_date")
                        st.time_input("Start Time", key="start_receive_time")

                    with time_col2:
                        st.date_input("End Date", key="end_receive_date")
                        st.time_input("End Time", key="end_receive_time")

                    st.checkbox("Apply Time Range", key="apply_time_filter")

                    st.selectbox(
                        "Sort By",
                        options=[
                            "Newest records first",
                            "Oldest records first",
                            "Highest delivery attempt",
                            "Duplicate messages first",
                            "Longest retry gap",
                            "Longest duplicate delay",
                            "Highest latency"
                        ],
                        key="sort_option"
                    )

                    st.button(
                        "Reset Filters",
                        on_click=reset_filters,
                        args=(min_time, max_time)
                    )

                # ---------------- APPLY FILTERS ----------------
                filtered_df = display_df.copy()

                # Duplicate filter
                if st.session_state["duplicate_filter"] == "Duplicates only":
                    filtered_df = filtered_df[
                        filtered_df["is_duplicate"].isin([1, True])
                    ]
                elif st.session_state["duplicate_filter"] == "Non-duplicates only":
                    filtered_df = filtered_df[
                        filtered_df["is_duplicate"].isin([0, False])
                    ]

                # Message ID filter
                if st.session_state["selected_message_id"] != "All":
                    filtered_df = filtered_df[
                        filtered_df["message_id"] == st.session_state["selected_message_id"]
                    ]

                # Time filter
                if st.session_state["apply_time_filter"]:
                    start_dt = pd.to_datetime(
                        datetime.combine(
                            st.session_state["start_receive_date"],
                            st.session_state["start_receive_time"]
                        )
                    )
                    end_dt = pd.to_datetime(
                        datetime.combine(
                            st.session_state["end_receive_date"],
                            st.session_state["end_receive_time"]
                        )
                    )

                    filtered_df = filtered_df[
                        (filtered_df["receive_time"] >= start_dt) &
                        (filtered_df["receive_time"] <= end_dt)
                    ]

                # ---------------- APPLY SORTING ----------------
                sort_option = st.session_state["sort_option"]

                if sort_option == "Newest records first":
                    filtered_df = filtered_df.sort_values(
                        by="receive_time",
                        ascending=False
                    )

                elif sort_option == "Oldest records first":
                    filtered_df = filtered_df.sort_values(
                        by="receive_time",
                        ascending=True
                    )

                elif sort_option == "Highest delivery attempt":
                    filtered_df = filtered_df.sort_values(
                        by=["delivery_attempt", "receive_time"],
                        ascending=[False, False]
                    )

                elif sort_option == "Duplicate messages first":
                    filtered_df = filtered_df.sort_values(
                        by=["is_duplicate", "receive_time"],
                        ascending=[False, False]
                    )


                elif sort_option == "Longest retry gap":
                    if "time_since_previous_attempt_ms" in filtered_df.columns:
                        filtered_df = filtered_df.sort_values(
                            by=["time_since_previous_attempt_ms", "receive_time"],
                            ascending=[False, False]
                        )

                elif sort_option == "Longest duplicate delay":
                    filtered_df = filtered_df.sort_values(
                        by=["duplicate_delay_ms", "receive_time"],
                        ascending=[False, False]
                    )

                elif sort_option == "Highest latency":
                    filtered_df = filtered_df.sort_values(
                        by=["latency_ms", "receive_time"],
                        ascending=[False, False]
                    )

                # ---------------- DISPLAY FORMATTING ----------------
                filtered_df["is_duplicate"] = filtered_df["is_duplicate"].map({
                    1: "Yes",
                    0: "No",
                    True: "Yes",
                    False: "No"
                })

                filtered_df["latency_sec"] = (
                    filtered_df["latency_ms"] / 1000
                ).round(3)

                filtered_df["duplicate_delay_sec"] = (
                    filtered_df["duplicate_delay_ms"] / 1000
                ).round(3)

                if "time_since_previous_attempt_ms" in filtered_df.columns:
                    filtered_df["time_since_previous_attempt_sec"] = (
                        filtered_df["time_since_previous_attempt_ms"] / 1000
                    ).round(3)

                filtered_df = filtered_df.rename(columns={
                    "id": "Arrival #",
                    "message_id": "Message ID",
                    "publish_time": "Published At",
                    "receive_time": "Received At",
                    "content": "Content",
                    "is_duplicate": "Is Duplicate",
                    "duplicate_delay_sec": "Duplicate Delay (sec)",
                    "delivery_attempt": "Attempt #",
                    "time_since_previous_attempt_sec": "Retry Gap (sec)",
                    "latency_sec": "Latency (sec)",
                })

                total_records = len(display_df)
                filtered_count = len(filtered_df)

                if filtered_count != total_records:
                    st.caption(f"Showing {filtered_count} of {total_records} records (filtered)")
                else:
                    st.caption(f"Showing {total_records} records")

                columns_to_drop = [
                    "latency_ms",
                    "duplicate_delay_ms",
                    "time_since_previous_attempt_ms"
                ]

                filtered_df = filtered_df.drop(
                    columns=[col for col in columns_to_drop if col in filtered_df.columns]
                )

                st.dataframe(filtered_df, width="stretch", hide_index=True)

            else:
                st.info("No subscriber messages stored yet.")
        else:
            st.info("No subscriber messages stored yet.")
    else:
        st.error("Could not connect to subscriber_db.")

except Exception as e:
    st.error(f"Error loading subscriber data: {e}")

finally:
    if "db" in locals() and db:
        db.close()