# 📩 Message Delivery Dashboard (Pub/Sub Project)
## Duplicate Simulation & Delivery Analytics

This project implements an event-driven messaging system using Google Cloud Pub/Sub, with a focus on analyzing message delivery behavior through controlled duplicate simulation.

## 🧠 Key Concept

Unlike typical systems that rely on unpredictable retry behavior, this project:

👉 intentionally simulates duplicate messages
👉 detects duplicates in the consumer layer before persistence
👉 analyzes delivery patterns using stored metadata

##🏗️ Architecture
Publisher UI → Pub/Sub Topic → Subscriber Workers → MySQL → Dashboard
                         ├── Worker 1 → received_messages (duplicate-aware)
                         └── Worker 2 → second_received_messages (no duplicate logic)
### Components
Publisher UI (Streamlit)
Sends messages and allows controlled duplicate simulation

Subscriber Worker 1 (Python)
Detects duplicates before saving and computes delivery metadata

Subscriber Worker 2 (Python) (Optional Objective)
Processes the same messages independently and stores them without duplicate checks

MySQL (Local)
Stores all messages and enables analysis of delivery behavior

Dashboard UI (Streamlit)
Displays message records and delivery metrics (latency, retries, duplicates)

## ⚙️ Setup Instructions

Follow all steps in order.

### 0️⃣ Start MySQL

Ensure your MySQL server is running locally before starting the app.

### 1️⃣ Clone the Repository

git clone <repo-url>
cd GCP-Pub-Sub-Project

### 2️⃣ Create Virtual Environment
python -m venv env

Activate it:

Windows
.\env\Scripts\activate

Mac/Linux
source env/bin/activate

### 3️⃣ Install Dependencies

pip install -r requirements.txt

### 4️⃣ Install Google Cloud SDK

https://cloud.google.com/sdk/docs/install

Then initialize:

gcloud init

### 5️⃣ Authenticate (Required for Pub/Sub)
gcloud auth application-default login

### 6️⃣ Set Project ID
gcloud config set project project-a85a075d-91d4-41d4-bc0

### 7️⃣ Create Pub/Sub Resources
gcloud pubsub topics create my-topic
gcloud pubsub subscriptions create my-sub --topic=my-topic
gcloud pubsub subscriptions create my-sub-2 --topic=my-topic

### ▶️ Running the Application

Step 1 – Start Subscriber Worker 1
python app/sub_worker.py

Step 2 – Start Subscriber Worker 2 (Optional Objective)
python app/sub_worker_2.py

Step 3 – Run Publisher UI
streamlit run app/pub_app.py

Step 4 – Run Dashboard UI
streamlit run app/sub_app.py

## 🧠 Recommended Testing Order
Start both subscriber workers
Run publisher UI
Send messages
Use Simulate Duplicate
View results in dashboard

## 🗄️ Database Behavior
Databases are automatically created
Tables are automatically created
No manual SQL setup required
Tables
received_messages → duplicate-aware processing
second_received_messages → independent subscriber (no duplicate logic)

## 📊 Features
Duplicate detection using message_id
Controlled duplicate simulation
Latency calculation
Retry gap analysis
Delivery attempt tracking
Multi-subscriber architecture (fan-out)
Filtering and sorting dashboard

## 🎯 Project Focus

### This project demonstrates:

Event-driven architecture
At-least-once delivery behavior
Controlled duplicate testing
Data-driven message analysis
Multi-subscriber message processing

## 📌 Notes

Duplicate messages are intentionally generated
Duplicate detection occurs in the Python consumer layer
MySQL is used for storage and lookup, not detection

## 🚀 Future Improvements
Cloud deployment (Cloud Run / Cloud SQL)
Enhanced dashboard visualizations
Automated Pub/Sub resource creation

## 👩‍💻 Author

Victoria D
