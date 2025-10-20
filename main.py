# main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from utils import fetch_and_store_movies

app = FastAPI()

# --------------------------------------
# 🌐 Enable CORS
# --------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # or restrict to specific origin e.g. ["http://127.0.0.1:5500"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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

