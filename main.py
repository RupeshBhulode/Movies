# main.py
from fastapi import FastAPI
from utils import fetch_and_store_movies

app = FastAPI()

# --------------------------------------
# ✅ Manual endpoint only
# --------------------------------------
@app.get("/movies")
def manual_trigger():
    """
    Manual trigger for fetching and storing movies (for testing).
    """
    try:
        fetch_and_store_movies()
        return {"status": "Triggered manually!", "success": True}
    except Exception as e:
        return {"status": "Error occurred", "error": str(e), "success": False}

