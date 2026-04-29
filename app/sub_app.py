import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import streamlit as st
import pandas as pd
import altair as alt
from storage.database import Database
from datetime import datetime
from config import DB_CONFIG

def reset_filters(min_time, max_time):
    st.session_state["selected_message_id"] = "All"
    st.session_state["duplicate_filter"] = "All"
    st.session_state["start_receive_date"] = min_time.date()
    st.session_state["start_receive_time"] = min_time.time().replace(microsecond=0)
    st.session_state["end_receive_date"] = max_time.date()
    st.session_state["end_receive_time"] = max_time.time().replace(microsecond=0)
    st.session_state["apply_time_filter"] = False
    st.session_state["sort_option"] = "Newest first"

def highlight_duplicate_cell(val):
    if val == "Yes":
        return "background-color: rgba(185, 28, 28, 0.15); color: #FCA5A5;"
    return ""

st.set_page_config(
    page_title="Pub/Sub Consumer Dashboard",
    layout="wide",
    initial_sidebar_state="collapsed"
)

title_col, button_col = st.columns([6, 1])

with title_col:
    st.title("Message Delivery Dashboard")

with button_col:
    st.write("")
    st.write("")
    if st.button("Refresh Data"):
        st.rerun()

try:
    db = Database(
    **DB_CONFIG,
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
                    st.session_state["duplicate_filter"] = "All"

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
                    st.session_state["sort_option"] = "Newest first"

                # ---------------- SIDEBAR FILTERS ----------------
                with st.sidebar:
                    st.header("Filters & Sorting")

                    st.selectbox(
                        "Message ID",
                        options=message_ids,
                        key="selected_message_id"
                    )

                    st.selectbox(
                        "Duplicate Status",
                        options=[
                            "All",
                            "Duplicates",
                            "Non-duplicates"
                        ],
                        key="duplicate_filter"
                    )

                    st.selectbox(
                        "Sort By",
                        options=[
                            "Newest first",
                            "Oldest first",
                            "Highest attempt",
                            "Duplicate first",
                            "Longest retry gap",
                            "Longest duplicate delay",
                            "Highest latency"
                        ],
                        key="sort_option"
                    )

                    st.markdown("### Received Time Range")

                    st.date_input("Start Date", key="start_receive_date")
                    st.time_input("Start Time", key="start_receive_time")

                    st.date_input("End Date", key="end_receive_date")
                    st.time_input("End Time", key="end_receive_time")

                    st.checkbox("Filter by Time Range", key="apply_time_filter")

                    st.button(
                        "Reset Filters",
                        on_click=reset_filters,
                        args=(min_time, max_time)
                    )
                # ---------------- APPLY FILTERS ----------------
                filtered_df = display_df.copy()

                # Duplicate filter
                if st.session_state["duplicate_filter"] == "Duplicates":
                    filtered_df = filtered_df[filtered_df["is_duplicate"].isin([1, True])]

                elif st.session_state["duplicate_filter"] == "Non-duplicates":
                    filtered_df = filtered_df[filtered_df["is_duplicate"].isin([0, False])]

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

                    if start_dt > end_dt:
                            st.warning("Start time must be before end time.")
                    else:
                        filtered_df = filtered_df[
                            (filtered_df["receive_time"] >= start_dt) &
                            (filtered_df["receive_time"] <= end_dt)
                        ]
                # ---------------- APPLY SORTING ----------------
                sort_option = st.session_state["sort_option"]

                if sort_option == "Newest first":
                    filtered_df = filtered_df.sort_values(by="receive_time", ascending=False)

                elif sort_option == "Oldest first":
                    filtered_df = filtered_df.sort_values(by="receive_time", ascending=True)

                elif sort_option == "Highest attempt":
                    filtered_df = filtered_df.sort_values(
                        by=["delivery_attempt", "receive_time"],
                        ascending=[False, False]
                    )

                elif sort_option == "Duplicate first":
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

                filtered_df["latency_sec"] = filtered_df["latency_ms"] / 1000

                filtered_df["duplicate_delay_sec"] = (
                    filtered_df["duplicate_delay_ms"] / 1000
                )

                filtered_df["time_since_previous_attempt_sec"] = (
                    filtered_df["time_since_previous_attempt_ms"] / 1000
                )

                filtered_df = filtered_df.rename(columns={
                    "id": "Record ID",
                    "message_id": "Message ID",
                    "publish_time": "Published At",
                    "receive_time": "Received At",
                    "content": "Message Content",
                    "is_duplicate": "Duplicate",
                    "delivery_attempt": "Attempt",
                    "duplicate_delay_sec": "Time Since First (sec)",
                    "time_since_previous_attempt_sec": "Time Since Previous (sec)",
                    "latency_sec": "Delivery Latency (sec)",
                })

                total_records = len(display_df)
                filtered_count = len(filtered_df)

                # ---------------- SUMMARY METRICS ----------------
                duplicate_count = (filtered_df["Duplicate"] == "Yes").sum()
                duplicate_rate = (duplicate_count / len(filtered_df) * 100) if len(filtered_df) > 0 else 0

                avg_latency = pd.to_numeric(filtered_df["Delivery Latency (sec)"], errors="coerce").mean()
                max_retry_gap = pd.to_numeric(filtered_df["Time Since Previous (sec)"], errors="coerce").max()

                metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)

                max_attempt = filtered_df["Attempt"].max() if not filtered_df.empty else 0

                metric_col1.metric("Total Records", len(filtered_df))
                metric_col2.metric("Duplicate Rate", f"{duplicate_rate:.1f}%")
                metric_col3.metric("Avg Delivery Latency", f"{avg_latency:.2f}s")
                if max_attempt <= 1:
                    metric_col4.metric("Max Delivery Attempt", max_attempt, delta="No retries")
                else:
                    metric_col4.metric("Max Delivery Attempt", max_attempt)

                # ---------------- METADATA VISUALS ---------------
                chart_col1, chart_col2 = st.columns(2)

                # --- Color Palette ---
                PRIMARY_BLUE = "#60A5FA"
                SECONDARY_PURPLE = "#A78BFA"
                HIGHLIGHT_AMBER = "#F59E0B"
                ALERT_RED = "#EF4444"

                with chart_col1:
                    st.markdown("**Attempt Distribution**")

                    attempt_counts = (
                        filtered_df["Attempt"]
                        .value_counts()
                        .sort_index()
                        .reset_index()
                    )

                    attempt_counts.columns = ["Attempt", "Record Count"]

                    # Convert to string so it's categorical (clean x-axis)
                    attempt_counts["Attempt"] = attempt_counts["Attempt"].astype(str)

                    # Add color logic based on attempt number
                    def assign_color(attempt):
                        attempt = int(attempt)
                        if attempt == 1:
                            return PRIMARY_BLUE
                        elif attempt == 2:
                            return SECONDARY_PURPLE
                        else:
                            return HIGHLIGHT_AMBER

                    attempt_counts["color"] = attempt_counts["Attempt"].apply(assign_color)

                    bar_chart = (
                        alt.Chart(attempt_counts)
                        .mark_bar()
                        .encode(
                            x=alt.X("Attempt:N", title="Delivery Attempt Distribution"),
                            y=alt.Y("Record Count:Q", title="Records"),
                            color=alt.Color("color:N", scale=None),  # uses exact colors
                            tooltip=["Attempt", "Record Count"]
                        )
                    )

                    st.altair_chart(bar_chart, width='stretch')

                with chart_col2:
                    st.markdown("**Avg Delivery Latency by Attempt**")

                    latency_by_attempt = (
                        filtered_df
                        .groupby("Attempt")["Delivery Latency (sec)"]
                        .mean()
                        .sort_index()
                        .reset_index()
                    )

                    latency_by_attempt.columns = ["Attempt", "Avg Latency (sec)"]
                    latency_by_attempt["Attempt"] = latency_by_attempt["Attempt"].astype(str)

                    line_chart = (
                        alt.Chart(latency_by_attempt)
                        .mark_line(color=PRIMARY_BLUE, point=True)
                        .encode(
                            x=alt.X("Attempt:N", title="Delivery Attempt"),
                            y=alt.Y("Avg Latency (sec):Q", title="Avg Latency (sec)"),
                            tooltip=[
                                "Attempt",
                                alt.Tooltip("Avg Latency (sec):Q", format=".3f")
                            ]
                        )
                    )

                    st.altair_chart(line_chart, width='stretch')

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

                styled_df = (
                    filtered_df.style
                    .map(highlight_duplicate_cell, subset=["Duplicate"])
                    .format({
                        "Delivery Latency (sec)": "{:.3f}",
                        "Time Since First (sec)": lambda x: "—" if x == 0 else f"{x:.3f}",
                        "Time Since Previous (sec)": lambda x: "—" if x == 0 else f"{x:.3f}",
                    })
                )
                st.dataframe(styled_df, width="stretch", hide_index=True)

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