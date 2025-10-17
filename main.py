# main.py
from fastapi import FastAPI
from utils import fetch_and_store_movies
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
import time
import threading

app = FastAPI()

# --------------------------------------
# 1️⃣ Manual endpoint (still available)
# --------------------------------------
@app.get("/movies")
def manual_trigger():
    """
    Manual trigger for fetching and storing movies (for testing).
    """
    fetch_and_store_movies()
    return {"status": "Triggered manually!"}

# --------------------------------------
# 2️⃣ Background Scheduler Setup
# --------------------------------------

scheduler = BackgroundScheduler()
job_running = False
last_failure_time = None

def daily_job():
    global job_running, last_failure_time
    if job_running:
        print("⚙️ Job already running, skipping duplicate trigger.")
        return

    job_running = True
    print(f"⏰ Job started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    try:
        fetch_and_store_movies()
        print(f"✅ Job completed successfully at {datetime.now().strftime('%H:%M:%S')}")
    except Exception as e:
        print(f"❌ Error during job: {e}")
        # Check if it's due to all API keys exhausted
        if "exhausted" in str(e).lower():
            last_failure_time = datetime.now()
            print("🕓 All API keys exhausted. Will retry after 24 hours.")
        else:
            print("⚠️ Job failed due to another error.")
    finally:
        job_running = False


# --------------------------------------
# 3️⃣ Schedule Job
# --------------------------------------

def start_scheduler():
    scheduler.add_job(daily_job, "cron", hour=12, minute=45)
    scheduler.start()
    print("🗓️ Scheduler started: job runs every 24 hours.")


@app.on_event("startup")
def on_startup():
    # Start background scheduler when FastAPI starts
    thread = threading.Thread(target=start_scheduler)
    thread.start()
    print("🚀 FastAPI app + scheduler initialized successfully.")


@app.on_event("shutdown")
def on_shutdown():
    scheduler.shutdown()
    print("🛑 Scheduler stopped.")
