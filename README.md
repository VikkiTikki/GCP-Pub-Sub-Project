# GCP-Pub-Sub-Project
## Setup Instructions

1. Clone repo
2. Create virtual environment
3. Activate environment
4. Install dependencies
5. Authenticate with GCP
6. Set project ID
7. Run Producer and Consumer

Google Pub/Sub Python Quickstart:
https://cloud.google.com/pubsub/docs/publish-receive-messages-client-library

Application Default Credentials:
https://cloud.google.com/docs/authentication/provide-credentials-adc

Google Cloud SDK install:
https://cloud.google.com/sdk/docs/install

This document explains how to properly set up and run the Pub/Sub project locally.

Please follow all steps in order.

## 🚀 1. Clone the Repository
git clone <YOUR-REPO-URL>
cd GCP-Pub-Sub-Project

## 🐍 2. Create a Virtual Environment (Required)

Inside the project folder:

python -m venv env

Activate it:

Windows
.\env\Scripts\activate
Mac/Linux
source env/bin/activate

You should now see:

(env)

in your terminal.

## 📦 3. Install Project Dependencies

Install required packages:

pip install -r requirements.txt

If requirements.txt does not exist, contact the repository owner.

## ☁️ 4. Install Google Cloud SDK

Download and install:

https://cloud.google.com/sdk/docs/install

After installation, initialize:

gcloud init

## 🔐 5. Authenticate Application Default Credentials (ADC)

This step is required for Pub/Sub to work:

gcloud auth application-default login

A browser window will open. Log in with your Google account.

## 🎯 6. Set the Correct GCP Project
gcloud config set project project-a85a075d-91d4-41d4-bc0

Verify:

gcloud config list

Make sure the project matches:

project-a85a075d-91d4-41d4-bc0

## 📡 7. Ensure Topic and Subscription Exist

If they do not already exist, run:

gcloud pubsub topics create my-topic
gcloud pubsub subscriptions create my-sub --topic=my-topic

## ▶️ 8. Running the Application
IMPORTANT:

Do NOT use the VS Code Code Runner extension.

Use one of the following methods:

Option A – Recommended (VS Code)

Right-click the file and select:

Run Python File in Terminal
Option B – Manual Terminal

Make sure virtual environment is active:

.\env\Scripts\activate

Then run:

python Producer.py
python Consumer.py
⚠️ Common Issues
❌ ModuleNotFoundError: No module named 'google.cloud'

You are not using the virtual environment.

Fix:

Activate env

Or select the correct interpreter in VS Code:

Ctrl + Shift + P

Python: Select Interpreter

Choose env\Scripts\python.exe

❌ Subscriber receives no messages

Make sure:

Subscription exists before publishing

Subscriber is running before Producer

You did not manually pull and acknowledge messages in the GCP console

🧠 Pub/Sub Testing Order

Correct sequence:

Create Topic

Create Subscription

Run Subscriber

Run Producer

📊 Project Focus

This project evaluates:

Message frequency

Latency between publish and receive

Duplicate message detection

At-least-once delivery behavior

✅ Final Checklist

Before running:

 Virtual environment created

 Environment activated

 Dependencies installed

 GCloud SDK installed

 ADC authenticated

 Correct project set

 Topic + subscription created

If setup fails, verify:

python --version
which python  (Mac/Linux)
where python  (Windows)

It must point to:

env/Scripts/python.exe
