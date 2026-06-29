# Real-Time-Music-Recommendation-System-using-Apache-Spark-Kafka

A real-time recommendation system built using Apache Spark, Kafka, and Spark Structured Streaming that delivers personalized music recommendations while continuously processing live user interactions.

---

## Project Overview

This project demonstrates a complete Big Data pipeline for real-time recommendation systems.

Historical user-rating data is used to train an ALS collaborative filtering model, while live user interactions are streamed through Apache Kafka and processed using Spark Structured Streaming to generate personalized recommendations in real time.

A Streamlit dashboard visualizes recommendations, trending songs, user activity, alerts, and pipeline latency.

---

## Features

* Real-time music recommendation engine
* Apache Spark MLlib ALS collaborative filtering
* Kafka event streaming
* Spark Structured Streaming
* Personalized recommendations
* Trending item detection
* User activity analytics
* Real-time monitoring dashboard
* Cold-start user handling
* Low-latency streaming pipeline

---

## System Architecture

```text
Amazon Music Ratings Dataset
            │
            ▼
     Spark MLlib (ALS)
            │
      Trained ALS Model
            │
            ▼
     Apache Kafka Stream
            │
            ▼
 Spark Structured Streaming
            │
     ├── Personalized Recommendations
     ├── Trending Items
     ├── User Activity
     ├── Alerts
     └── Latency Monitoring
            │
            ▼
     Streamlit Dashboard
```

---

## Technologies

* Python
* Apache Spark
* Spark MLlib
* Spark Structured Streaming
* Apache Kafka
* Streamlit
* Pandas

---

## Machine Learning

The recommendation engine uses **Alternating Least Squares (ALS)** collaborative filtering from Spark MLlib.

Key features include:

* User and item indexing
* RMSE evaluation
* Automatic hyperparameter tuning
* Cold-start handling
* Model persistence

---

## Streaming Pipeline

The streaming system processes live user events using Apache Kafka and Spark Structured Streaming.

Streaming analytics include:

* Personalized recommendations
* Trending music
* User activity monitoring
* High-rating alerts
* Pipeline latency tracking

---

## Dashboard

The Streamlit dashboard provides live visualization of:

* Pipeline latency
* Spark and Kafka status
* Top-5 personalized recommendations
* Trending songs
* Active users
* System alerts

---

## Results

The system successfully combines batch machine learning with real-time streaming analytics.

Key achievements include:

* Processing over **836,000 music rating records**
* Real-time recommendation generation
* Pipeline latency below **5 seconds**
* Continuous streaming analytics
* Interactive monitoring dashboard

---

## Learning Outcomes

* Big Data Analytics
* Recommendation Systems
* Apache Spark
* Spark Structured Streaming
* Apache Kafka
* Collaborative Filtering
* Real-Time Machine Learning
* Stream Processing
* Streamlit Dashboard Development

---

## Future Improvements

* Deploy on a distributed Spark cluster
* Integrate deep learning recommendation models
* Add implicit feedback recommendations
* Support real-time user personalization at larger scale
* Deploy using Docker and Kubernetes

---

## Author

**Zeyad Sherif**
