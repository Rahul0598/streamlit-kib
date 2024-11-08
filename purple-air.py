import io
import os
import time
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import requests
import streamlit as st


def get_uts(date_string):
    dt = datetime.strptime(date_string, "%m/%d/%Y %H:%M:%S")
    return int(dt.timestamp())


def generate_filename(start_date, end_date, sensor_id):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    return f"sensor_{sensor_id}_{start_str}_to_{end_str}.csv"


def file_exists(start_date, end_date, sensor_id):
    filename = generate_filename(start_date, end_date, sensor_id)
    return os.path.exists(filename)


def fetch_sensor_data(sensor_id, api_key, start_date, end_date, fields):
    end_timestamp = get_uts(end_date.strftime("%m/%d/%Y %H:%M:%S"))
    start_timestamp = get_uts(start_date.strftime("%m/%d/%Y %H:%M:%S"))

    url = f"https://api.purpleair.com/v1/sensors/{sensor_id}/history/csv"

    headers = {"X-API-Key": api_key}

    params = {
        "start_timestamp": start_timestamp,
        "end_timestamp": end_timestamp,
        "fields": fields,
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        return pd.read_csv(io.StringIO(response.text))
    else:
        st.error(f"Error getting data for sensor {sensor_id}: {response.status_code}")
        return None


def process_sensor_data(combined_df, selected_field):
    # Convert timestamp and set as index
    combined_df["time_stamp"] = pd.to_datetime(combined_df["time_stamp"], unit="s")
    combined_df.set_index("time_stamp", inplace=True)

    # Map sensor IDs to location names
    combined_df["location"] = combined_df["sensor_id"].map(sensor_ids)

    # Create hourly averages for each location
    hourly_data = (
        combined_df.groupby(["location", pd.Grouper(freq="h")])[selected_field]
        .mean()
        .reset_index()
    )

    # Create plot
    fig = px.line(
        hourly_data,
        x="time_stamp",
        y=selected_field,
        color="location",
        title=f"Hourly Average {selected_field.upper()} Levels Over Time",
        template="plotly_white",
        labels={
            "time_stamp": "Time",
            "location": "Location",
            selected_field: f"{selected_field.upper()} Level",
        },
    )

    # Customize layout
    fig.update_layout(
        xaxis_title="Time",
        yaxis_title=f"{selected_field.upper()} Level",
        legend_title="Location",
        hovermode="x unified",
    )

    return fig, hourly_data


# Streamlit app layout
st.title("Purple Air Data for Arlington Woods")

# Sensor mapping
sensor_ids = {
    220759: "East 33rd Street",
    220757: "East 30th Street",
    220755: "North Catherwood Avenue",
}

api_key = st.secrets["API_KEY"]

# Date range selection
col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("Start Date", datetime.now() - timedelta(days=10))
with col2:
    end_date = st.date_input("End Date", datetime.now())

# Fields selection
selected_field = st.selectbox(
    "Select Metric to Display",
    ["pm2.5_alt", "humidity", "temperature"],
    format_func=lambda x: {
        "pm2.5_alt": "PM2.5",
        "humidity": "Humidity",
        "temperature": "Temperature",
    }[x],
)

if st.button("Fetch Data"):
    if not api_key:
        st.error("Please enter an API key")
    else:
        start_datetime = datetime.combine(start_date, datetime.min.time())
        end_datetime = datetime.combine(end_date, datetime.max.time())

        combined_df = pd.DataFrame()
        need_fetch = False

        for sensor_id in sensor_ids.keys():
            filename = generate_filename(start_datetime, end_datetime, sensor_id)

            if file_exists(start_datetime, end_datetime, sensor_id):
                # Read existing file
                df = pd.read_csv(filename)
                df["sensor_id"] = sensor_id
                combined_df = pd.concat([combined_df, df], ignore_index=True)
            else:
                need_fetch = True
                break

        if need_fetch:
            with st.spinner("Fetching new data..."):
                combined_df = pd.DataFrame()
                for sensor_id in sensor_ids.keys():
                    # Convert dates to datetime
                    start_datetime = datetime.combine(start_date, datetime.min.time())
                    end_datetime = datetime.combine(end_date, datetime.max.time())

                    # Fetch data
                    df = fetch_sensor_data(
                        sensor_id,
                        api_key,
                        start_datetime,
                        end_datetime,
                        "temperature,humidity,pm2.5_alt",
                    )

                    if df is not None:
                        df["sensor_id"] = sensor_id
                        combined_df = pd.concat([combined_df, df], ignore_index=True)

                        # Save to CSV
                        filename = generate_filename(
                            start_datetime, end_datetime, sensor_id
                        )
                        df.to_csv(filename, index=False)

        if not combined_df.empty:
            fig, hourly_data = process_sensor_data(combined_df, selected_field)
            st.plotly_chart(fig, use_container_width=True)

            # Show hourly averaged data
            st.subheader("Hourly Averaged Data")
            st.dataframe(hourly_data)

            # Download button for hourly data
            csv = hourly_data.to_csv(index=False)
            st.download_button(
                label="Download Hourly Data as CSV",
                data=csv,
                file_name=f"hourly_averages_{datetime.now().strftime('%Y-%m-%d')}.csv",
                mime="text/csv",
            )
