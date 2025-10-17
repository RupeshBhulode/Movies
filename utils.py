# utils.py
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import isodate
import time
import firebase_admin
from firebase_admin import credentials, firestore
import os

# =====================================
# 🔑 CONFIGURATION
# =====================================
API_KEYS = [
    "AIzaSyCbfIO4dLgSdZaPrTS7P0lvMnlTX-u9_48",
    "AIzaSyDYjOKHNOf-fY55p_MQA93Eaj13Uvv4puY",
    "AIzaSyA0s8gvFZJBQHgSlYdF4lG78FM0YLb4wm0",
    "AIzaSyCTFcusoP1DJb_nwSMwftESGyFchn_Kdgo"
]

CATEGORIES = [
    "Hindi Animated",
    "Hindi Family",
    "Hindi Comedy",
    "Hindi Horror",
    "Hindi Suspense Thriller",
    "Hindi Action"
]

current_key_index = 0

# =====================================
# 🔁 YOUTUBE API KEY ROTATION
# =====================================
def get_youtube_client():
    global current_key_index
    key = API_KEYS[current_key_index]
    print(f"✅ Using API key #{current_key_index + 1}")
    return build("youtube", "v3", developerKey=key)

youtube = get_youtube_client()

def safe_api_call(func, *args, **kwargs):
    global current_key_index, youtube
    while True:
        try:
            return func(*args, **kwargs).execute()
        except HttpError as e:
            if "quotaExceeded" in str(e):
                current_key_index += 1
                if current_key_index >= len(API_KEYS):
                    raise Exception("🚫 All API keys exhausted for today.")
                print(f"⚠️ Quota exhausted. Switching to API key #{current_key_index + 1}")
                youtube = get_youtube_client()
                time.sleep(0.5)
                continue
            raise

# =====================================
# ⏱ UTILS
# =====================================
def get_video_duration_seconds(iso_duration: str) -> int:
    try:
        duration = isodate.parse_duration(iso_duration)
        return int(duration.total_seconds())
    except Exception:
        return 0

# =====================================
# 🔥 FIRESTORE SETUP
# =====================================
import json

firebase_json = os.getenv("FIREBASE_SERVICE_ACCOUNT_JSON")

if not firebase_admin._apps:
    if firebase_json:
        cred_dict = json.loads(firebase_json)
        cred = credentials.Certificate(cred_dict)
    else:
        cred = credentials.Certificate("serviceAccountKey.json")
    firebase_admin.initialize_app(cred)


db = firestore.client()

# =====================================
# 🎥 FETCH AND STORE LOGIC
# =====================================
def fetch_and_store_movies():
    """
    Fetch 5 latest (>45min) free movies for each Hindi category
    and store directly to Firestore (unique by videoId).
    """
    for category in CATEGORIES:
        print(f"🎬 Fetching movies for: {category}")

        # Step 1: Fetch latest videos for this category
        search_resp = safe_api_call(
            youtube.search().list,
            q=category + " full movie free",
            part="id",
            type="video",
            order="date",          # ✅ newest first
            maxResults=10,         # fetch 10 then filter
            videoDuration="long",
            safeSearch="none"
        )

        video_ids = [
            item["id"]["videoId"]
            for item in search_resp.get("items", [])
            if item.get("id", {}).get("videoId")
        ]

        if not video_ids:
            continue

        # Step 2: Get video details
        details_resp = safe_api_call(
            youtube.videos().list,
            part="snippet,contentDetails,liveStreamingDetails",
            id=",".join(video_ids)
        )

        count = 0
        for item in details_resp.get("items", []):
            if count >= 5:   # ✅ only 5 per category
                break

            video_id = item.get("id")
            snippet = item.get("snippet", {}) or {}
            content = item.get("contentDetails", {}) or {}

            if not video_id or not snippet:
                continue
            if "liveStreamingDetails" in item:
                continue

            duration_sec = get_video_duration_seconds(content.get("duration", ""))
            if duration_sec < 2700:
                continue

            thumbnails = snippet.get("thumbnails", {}) or {}
            thumb_url = ""
            for k in ("maxres", "standard", "high", "medium", "default"):
                if k in thumbnails and thumbnails[k].get("url"):
                    thumb_url = thumbnails[k]["url"]
                    break

            doc = {
                "title": snippet.get("title", ""),
                "videoId": video_id,
                "url": f"https://www.youtube.com/embed/{video_id}",
                "thumbnail": thumb_url,
                "durationSeconds": duration_sec,
                "publishedAt": snippet.get("publishedAt", "")
            }

            # Step 3: Store in Firestore (unique only)
            doc_ref = db.collection(category).document(video_id)
            if doc_ref.get().exists:
                continue

            doc_ref.set(doc)
            count += 1
            print(f"✅ Stored: {doc['title'][:60]}... in {category}")
