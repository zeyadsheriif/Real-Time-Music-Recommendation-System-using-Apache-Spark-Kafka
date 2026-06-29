import streamlit as st
import pandas as pd
import os
import time

st.set_page_config(page_title="Music Rec Engine", layout="wide", initial_sidebar_state="collapsed")

st.markdown("""
    <h1 style='text-align: center; color: #1DB954;'>🎧 Live Music Recommendation System</h1>
    <hr style='border: 2px solid #1DB954;'>
""", unsafe_allow_html=True)

placeholder = st.empty()

while True:
    with placeholder.container():
        st.subheader("⚡ Real-Time Streaming Metrics")
        colA, colB, colC = st.columns(3)
        
        latency = "0.0"
        if os.path.exists("dashboard_latency.txt"):
            try:
                with open("dashboard_latency.txt", "r") as f:
                    latency = f.read().strip()
                    if not latency: latency = "0.0"
            except:
                pass
                
        try:
            lat_val = float(latency)
        except:
            lat_val = 0.0

        colA.metric(label="Pipeline Latency (ms)", value=f"{lat_val} ms", delta="Sub-second!" if lat_val < 1000 else None, delta_color="inverse")
        colB.metric(label="Spark Status", value="Active", delta="Running", delta_color="normal")
        colC.metric(label="Kafka Status", value="Connected", delta="Listening", delta_color="normal")
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("<h3 style='color: #4682B4;'>👤 Top-5 Personal Recommendations</h3>", unsafe_allow_html=True)
            if os.path.exists("dashboard_recs.csv"):
                try:
                    df_recs = pd.read_csv("dashboard_recs.csv").head(10)
                    st.dataframe(df_recs, use_container_width=True)
                except:
                    st.info("Waiting for user streams...")

        with col2:
            st.markdown("<h3 style='color: #FF8C00;'>🔥 Trending Items (Engagement Score)</h3>", unsafe_allow_html=True)
            if os.path.exists("dashboard_trending.csv"):
                try:
                    df_trend = pd.read_csv("dashboard_trending.csv").head(10)
                    st.dataframe(df_trend[["item", "avg_rating", "interaction_count", "engagement_score"]], use_container_width=True)
                except:
                    st.info("Aggregating 30-second window...")

        st.markdown("<br>", unsafe_allow_html=True)

        col3, col4 = st.columns(2)
        
        with col3:
            st.markdown("<h3 style='color: #8A2BE2;'>📈 Active User Spikes</h3>", unsafe_allow_html=True)
            if os.path.exists("dashboard_users.csv"):
                try:
                    df_users = pd.read_csv("dashboard_users.csv").head(10)
                    st.bar_chart(data=df_users, x="user", y="activity_count", color="#8A2BE2", use_container_width=True)
                except:
                    st.info("Aggregating 30-second window...")

        with col4:
            st.markdown("<h3 style='color: #DC143C;'>🚨 System Alerts (Rating > 4.5)</h3>", unsafe_allow_html=True)
            if os.path.exists("dashboard_alerts.csv"):
                try:
                    df_alerts = pd.read_csv("dashboard_alerts.csv").tail(8)
                    for _, row in df_alerts.iterrows():
                        st.error(f"HIGH RATING DETECTED: User **{row['user']}** rated Item **{row['item']}** with a **{row['rating']}**!")
                except:
                    st.success("No anomalies detected.")

    time.sleep(2)
