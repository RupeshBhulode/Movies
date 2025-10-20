# utils.py
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import isodate
import time
import firebase_admin
from firebase_admin import credentials, firestore
import os, json

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
    retries = 3
    while retries > 0:
        try:
            return func(*args, **kwargs).execute()
        except HttpError as e:
            if "quotaExceeded" in str(e):
                current_key_index += 1
                if current_key_index >= len(API_KEYS):
                    raise Exception("🚫 All API keys exhausted for today.")
                print(f"⚠️ Quota exhausted. Switching to API key #{current_key_index + 1}")
                youtube = get_youtube_client()
                time.sleep(1)
                continue
            else:
                print(f"⚠️ YouTube API Error: {e}")
                retries -= 1
                time.sleep(2)
        except Exception as e:
            print(f"❌ Network or Unknown Error: {e}")
            retries -= 1
            time.sleep(2)
    raise Exception("❌ API call failed after retries.")

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
    Fetch as many valid long movies as possible for each category and store all unique ones.
    """
    for category in CATEGORIES:
        print(f"\n🎬 Fetching movies for: {category}")

        total_stored = 0
        page_token = None
        max_pages = 10   # YouTube API allows up to ~500 results (10 pages × 50)

        for page_num in range(1, max_pages + 1):
            print(f"🔹 Page {page_num}...")

            try:
                search_resp = safe_api_call(
                    youtube.search().list,
                    q=f"{category} full movie free",
                    part="id",
                    type="video",
                    order="date",
                    maxResults=50,
                    videoDuration="long",
                    safeSearch="none",
                    pageToken=page_token
                )
            except Exception as e:
                print(f"⚠️ Error fetching page: {e}")
                break

            video_ids = [
                item["id"]["videoId"]
                for item in search_resp.get("items", [])
                if item.get("id", {}).get("videoId")
            ]

            if not video_ids:
                print("⚠️ No videos found on this page.")
                break

            # Fetch detailed info for these videos
            details_resp = safe_api_call(
                youtube.videos().list,
                part="snippet,contentDetails,liveStreamingDetails",
                id=",".join(video_ids)
            )

            batch = db.batch()
            batch_count = 0

            for item in details_resp.get("items", []):
                video_id = item.get("id")
                snippet = item.get("snippet", {})
                content = item.get("contentDetails", {})

                if not video_id or not snippet or "liveStreamingDetails" in item:
                    continue

                # Filter: only full-length movies (45+ min)
                duration_sec = get_video_duration_seconds(content.get("duration", ""))
                if duration_sec < 2700:
                    continue

                # Pick best thumbnail available
                thumbnails = snippet.get("thumbnails", {})
                thumb_url = ""
                for k in ("maxres", "standard", "high", "medium", "default"):
                    if k in thumbnails and thumbnails[k].get("url"):
                        thumb_url = thumbnails[k]["url"]
                        break

                doc_ref = db.collection(category).document(video_id)
                if doc_ref.get().exists:
                    continue  # skip duplicates

                data = {
                    "title": snippet.get("title", ""),
                    "videoId": video_id,
                    "url": f"https://www.youtube.com/embed/{video_id}",
                    "thumbnail": thumb_url,
                    "durationSeconds": duration_sec,
                    "publishedAt": snippet.get("publishedAt", ""),
                    "fetchedAt": firestore.SERVER_TIMESTAMP
                }

                batch.set(doc_ref, data)
                batch_count += 1
                total_stored += 1

                # Firestore batch limit = 500 writes per batch
                if batch_count == 500:
                    batch.commit()
                    batch = db.batch()
                    batch_count = 0
                    print(f"💾 Committed 500 movies (partial batch) in {category}")

            # Commit remaining batch for this page
            if batch_count > 0:
                batch.commit()
                print(f"💾 Committed {batch_count} new movies in {category}")

            # Move to next page
            page_token = search_resp.get("nextPageToken")
            if not page_token:
                print("🚫 No more pages available.")
                break

        print(f"📦 Total new movies stored in {category}: {total_stored}")
