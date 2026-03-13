
import json
import feedparser
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from time import mktime
import concurrent.futures
import re
import os
import random
import threading
import googlenewsdecoder
import urllib3
import binascii
import urllib.parse
from urllib.parse import urljoin, urlparse
from google.oauth2 import service_account
import google.auth.transport.requests

# تعطيل تحذيرات SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

print("--- 🚀 FULL FB-SHIELD NEWS BOT + (JSON PER ID) + INDEX + CLOUDFLARE 🚀 ---", flush=True)

# مسارات التشغيل المباشرة
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FOLDER = os.path.join(BASE_DIR, "News_Output")

SOURCES_FILE = os.path.join(BASE_DIR, "sources.txt")
COOKIE_FOLDER = os.path.join(BASE_DIR, "fb_cookies") 
PROXIES_FILE = os.path.join(BASE_DIR, "proxies.txt")

# إعدادات الفلتر الزمني
DAYS_TO_KEEP = 7
CUTOFF_SECONDS = DAYS_TO_KEEP * 24 * 60 * 60

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Mobile Safari/537.36'
]

BANNED_DOMAINS = ["elwatannews.com", "dostor.org", "elaosboa.com", "ahlmasrnews.com", "baladnaelyoum.com", "cairo24.com"]

thread_local = threading.local()
seen_ids_lock = threading.Lock()

processed_count = 0
total_to_process = 0
processed_lock = threading.Lock()
cf_workers_list = []

def load_workers():
    print(f"🌍 جاري تحميل الوركرز من ملف {PROXIES_FILE}...", flush=True)
    if not os.path.exists(PROXIES_FILE):
        print("⚠️ ملف proxies.txt غير موجود، سيتم الاتصال المباشر.", flush=True)
        return []
    try:
        with open(PROXIES_FILE, "r", encoding="utf-8") as f:
            workers = [line.strip() for line in f if line.strip() and line.startswith("http")]
            print(f"✅ تم تحميل {len(workers)} حساب وركر.", flush=True)
            return workers
    except Exception as e:
        print(f"❌ خطأ في تحميل الوركرز: {e}", flush=True)
        return []

def get_safe_firebase_id(url):
    if not url: 
        return "unknown_news"
    return re.sub(r'[.#$\[\]/:]', '_', url)

def get_fcm_v1_auth():
    print("🔑 جاري محاولة جلب توكن FCM...", flush=True)
    fcm_json = os.environ.get('FCM_SERVICE_ACCOUNT')
    if not fcm_json:
        print("⚠️ FCM_SERVICE_ACCOUNT Secret is missing!", flush=True)
        return None, None
    try:
        info = json.loads(fcm_json)
        scopes = ['https://www.googleapis.com/auth/firebase.messaging']
        creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
        auth_request = google.auth.transport.requests.Request()
        creds.refresh(auth_request)
        print("✅ تم جلب توكن FCM بنجاح!", flush=True)
        return creds.token, info['project_id']
    except Exception as e:
        print(f"❌ FCM Auth Error: {e}", flush=True)
        return None, None

def clean_unicode(text):
    if not text: 
        return ""
    text = re.sub(r'[\u200e\u200f\u202a-\u202e\u200b\ufeff]', '', str(text))
    return text.strip()

def format_topic_for_fcm(tag_type, value):
    if not value: 
        return None
    try:
        hex_val = binascii.hexlify(value.strip().encode('utf-8')).decode('ascii')
        return f"{tag_type}_{hex_val}"
    except Exception:
        return None

def send_fcm_notification_v1(news_item, token, project_id):
    if not token or not project_id: 
        return
    topics = [
        format_topic_for_fcm("topic", news_item.get('topic')),
        format_topic_for_fcm("lang", news_item.get('lang')),
        format_topic_for_fcm("category", news_item.get('category')),
        format_topic_for_fcm("extra", news_item.get('extra'))
    ]
    url = f'https://fcm.googleapis.com/v1/projects/{project_id}/messages:send'
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    for topic in topics:
        if not topic: 
            continue
        payload = {
            "message": {
                "topic": topic, 
                "data": {
                    "title": news_item['title'], 
                    "description": news_item['description'], 
                    "image": news_item['image'], 
                    "url": news_item['url']
                }
            }
        }
        try:
            requests.post(url, json=payload, headers=headers, timeout=5)
        except Exception: 
            pass 

def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=500, pool_maxsize=500, max_retries=2)
        thread_local.session.mount('http://', adapter)
        thread_local.session.mount('https://', adapter)
    return thread_local.session

def make_request(target_url, headers=None, cookies=None, timeout=10, verify=False):
    if not cf_workers_list:
        return get_session().get(target_url, headers=headers, cookies=cookies, timeout=timeout, verify=verify)
    worker_base = random.choice(cf_workers_list)
    encoded_url = urllib.parse.quote(target_url, safe='')
    proxy_url = f"{worker_base}{encoded_url}" if "?url=" in worker_base else f"{worker_base.rstrip('/')}/?url={encoded_url}"
    return get_session().get(proxy_url, headers=headers, cookies=cookies, timeout=timeout, verify=True)

def get_random_cookies():
    try:
        if not os.path.exists(COOKIE_FOLDER): 
            return None
        files = [f for f in os.listdir(COOKIE_FOLDER) if f.endswith('.json')]
        if not files: 
            return None
        random_file = random.choice(files)
        with open(os.path.join(COOKIE_FOLDER, random_file), 'r') as f:
            return {c['name']: c['value'] for c in json.load(f)}
    except Exception: 
        return None

def load_feeds():
    print(f"📂 جاري تحميل الروابط من ملف {SOURCES_FILE}...", flush=True)
    if not os.path.exists(SOURCES_FILE): 
        print(f"❌ مصيبة: الملف {SOURCES_FILE} مش موجود أصلاً في الفولدر!", flush=True)
        return []
    feeds = []
    try:
        with open(SOURCES_FILE, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    parts = line.strip().split('|')
                    if len(parts) >= 6:
                        feeds.append({
                            "rssid": parts[0].strip(), 
                            "topic": parts[1].strip(), 
                            "lang": parts[2].strip(), 
                            "category": parts[3].strip(), 
                            "extra": parts[4].strip(), 
                            "url": parts[5].strip()
                        })
        print(f"✅ تم تحميل {len(feeds)} رابط.", flush=True)
    except Exception as e: 
        print(f"❌ خطأ: {e}", flush=True)
    return feeds

def fetch_feed_quick(args):
    index, feed_data, existing_google_urls = args
    url = feed_data['url']
    try:
        headers = {'User-Agent': random.choice(USER_AGENTS)}
        response = make_request(url, headers=headers, timeout=10)
        d = feedparser.parse(response.content)
        entries = []
        if not d.entries:
            print(f"❌ الرابط {index} مفهوش اخبار | الرابط: {url}", flush=True)
            return []
        for i, e in enumerate(d.entries[:20], 1):
            if e.link in existing_google_urls:
                continue
            e['rssid_tag'] = feed_data.get('rssid', '')
            e['topic_tag'] = feed_data['topic']
            e['lang_tag'] = feed_data['lang']
            e['category_tag'] = feed_data['category']
            e['extra_tag'] = feed_data['extra']
            entries.append(e)
            print(f"✔️ خبر {i}/رابط {index} تم استلام الخبر | الرابط: {e.link}", flush=True)
        return entries
    except Exception: 
        print(f"❌ الرابط {index} مفهوش اخبار (خطأ اتصال) | الرابط: {url}", flush=True)
        return []

def resolve_url_fast(url):
    if "news.google.com" in url:
        try:
            decoded = googlenewsdecoder.new_decoderv1(url)
            if decoded.get("status"): 
                return decoded["decoded_url"]
        except Exception: 
            pass
    return url

def validate_article_and_get_image(url):
    try:
        headers = {'User-Agent': random.choice(USER_AGENTS)}
        cookies = get_random_cookies()
        response = make_request(url, headers=headers, cookies=cookies, timeout=6, verify=False)
        if response.status_code != 200: 
            return None
        soup = BeautifulSoup(response.content, 'lxml')
        meta_og = soup.find("meta", property="og:image")
        if meta_og and meta_og.get("content"):
            return urljoin(url, meta_og.get("content"))
    except Exception: 
        pass
    return None

def process_full_article(entry, token, project_id, global_seen_urls):
    global processed_count, total_to_process
    try:
        try:
            real_url = resolve_url_fast(entry.link)
            if any(bd in real_url for bd in BANNED_DOMAINS): 
                return None
            
            with seen_ids_lock:
                if real_url in global_seen_urls: 
                    return None
                global_seen_urls.add(real_url)

            image_url = validate_article_and_get_image(real_url)
            if not image_url: 
                return None

            type_ = "video" if any(x in real_url for x in ["youtube.com", "youtu.be", "tiktok.com"]) else "article"
            title = clean_unicode(entry.title.rsplit(' - ', 1)[0] if hasattr(entry, 'title') else "بدون عنوان")
            source = clean_unicode(entry.source.title if hasattr(entry, 'source') and hasattr(entry.source, 'title') else "مصدر")
            desc = clean_unicode(BeautifulSoup(entry.description if hasattr(entry, 'description') else "", "lxml").text.replace("قراءة المزيد", ""))
            if len(desc) > 200: 
                desc = desc[:197] + "..."
            
            try: 
                ts = int(mktime(entry.published_parsed) * 1000)
            except Exception: 
                ts = int(datetime.now().timestamp() * 1000)
                
            news_item = {
                "id": real_url, "title": title, "description": desc, "url": real_url, "image": image_url,
                "timestamp": ts, "source": source, "publisher_id": urlparse(real_url).netloc,
                "type": type_, "google_url": entry.link,
                "rssid": clean_unicode(entry.get('rssid_tag', '')), "topic": clean_unicode(entry.get('topic_tag', '')), 
                "lang": clean_unicode(entry.get('lang_tag', '')), "category": clean_unicode(entry.get('category_tag', '')), 
                "extra": clean_unicode(entry.get('extra_tag', ''))
            }
            send_fcm_notification_v1(news_item, token, project_id)
            return news_item
        except Exception:
            return None
    finally:
        with processed_lock:
            processed_count += 1
            if processed_count % 50 == 0 or processed_count == total_to_process:
                print(f"🔄 جاري معالجة الروابط... تم الإنتهاء من ({processed_count}/{total_to_process}) خبر.", flush=True)

def main():
    print(f"🔍 تنبيه هام: السكربت شغال دلوقتي في المسار ده:\n{BASE_DIR}\n", flush=True)

    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)
        print(f"📁 تم إنشاء مجلد News_Output بنجاح.", flush=True)

    global cf_workers_list, processed_count, total_to_process
    cf_workers_list = load_workers()
    print("▶️ بدء تشغيل البوت...", flush=True)
    token, project_id = get_fcm_v1_auth()
    
    start_time = datetime.now()
    cutoff_ts_ms = (start_time.timestamp() - CUTOFF_SECONDS) * 1000

    feeds = load_feeds()
    if not feeds: 
        print("❌ الكود هيقف لأن مفيش أي روابط يشتغل عليها!", flush=True)
        return

    global_seen_google_urls = set()
    global_seen_urls = set()
    existing_news_by_rssid = {}
    
    print("📚 جاري قراءة ملفات الجيسون القديمة...", flush=True)
    for feed in feeds:
        rssid = feed.get('rssid') or "unknown"
        file_path = os.path.join(OUTPUT_FOLDER, f"{rssid}.json")
        existing_news_by_rssid[rssid] = []
        
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    old_data = json.load(f)
                    valid_old_data = [n for n in old_data if n.get("timestamp", 0) > cutoff_ts_ms]
                    existing_news_by_rssid[rssid] = valid_old_data
                    for n in valid_old_data:
                        if "url" in n: global_seen_urls.add(n["url"])
                        if "google_url" in n: global_seen_google_urls.add(n["google_url"])
            except Exception: pass

    indexed_feeds_with_cache = [(i, f, global_seen_google_urls) for i, f in enumerate(feeds, 1)]
    
    all_raw = []
    print("⚡ جاري سحب الأخبار من كافة الروابط...", flush=True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=200) as executor:
        results = list(executor.map(fetch_feed_quick, indexed_feeds_with_cache))
        for res in results: all_raw.extend(res)
    
    valid_entries = []
    for x in all_raw:
        try:
            ts = mktime(x.published_parsed) if x.get('published_parsed') else start_time.timestamp()
            if ts > (start_time.timestamp() - CUTOFF_SECONDS):
                valid_entries.append(x)
        except Exception: continue
            
    total_to_process = len(valid_entries)
    processed_count = 0
    print(f"\n🎯 الأخبار الجديدة الصالحة زمنياً للتحليل: {total_to_process} خبر.", flush=True)

    new_news_json = []
    if total_to_process > 0:
        with concurrent.futures.ThreadPoolExecutor(max_workers=300) as executor:
            futures = [executor.submit(process_full_article, entry, token, project_id, global_seen_urls) for entry in valid_entries]
            for future in concurrent.futures.as_completed(futures):
                try:
                    res = future.result()
                    if res: new_news_json.append(res)
                except Exception: pass

    print(f"\n✨ تم الانتهاء من المعالجة. تم جلب {len(new_news_json)} خبر جديد سليم.", flush=True)

    news_to_save_by_rssid = existing_news_by_rssid.copy()
    for news in new_news_json:
        rssid = news.get("rssid") or "unknown"
        if rssid not in news_to_save_by_rssid:
            news_to_save_by_rssid[rssid] = []
        news_to_save_by_rssid[rssid].append(news)

    FIREBASE_URL = "https://androidnewsapp-8e382-default-rtdb.europe-west1.firebasedatabase.app/news_interactions.json"
    try:
        interactions_resp = requests.get(FIREBASE_URL, timeout=10)
        interactions = interactions_resp.json() if interactions_resp.status_code == 200 else {}
    except Exception: interactions = {}

    print("\n🗂️ جاري تحديث وحفظ ملفات الجيسون الخاصة بكل ID...", flush=True)
    try:
        saved_files_count = 0
        for rssid, news_list in news_to_save_by_rssid.items():
            for news in news_list:
                safe_id = get_safe_firebase_id(news.get("url", ""))
                item_data = interactions.get(safe_id, {})
                news["likes"] = item_data.get("likesCount", 0)
                news["comments"] = item_data.get("commentsCount", 0)

            news_list.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
            
            save_path = os.path.join(OUTPUT_FOLDER, f"{rssid}.json")
            with open(save_path, "w", encoding='utf-8') as f:
                json.dump(news_list, f, ensure_ascii=False, indent=2)
            
            saved_files_count += 1
            
        print(f"\n✅ DONE! تمت عملية الحفظ لـ {saved_files_count} ملف.", flush=True)

        print("\n📝 جاري إنشاء ملف الفهرس (index.txt)...", flush=True)
        index_path = os.path.join(OUTPUT_FOLDER, "index.txt")
        json_files = [f for f in os.listdir(OUTPUT_FOLDER) if f.endswith('.json')]
        with open(index_path, "w", encoding='utf-8') as f:
            for jf in json_files:
                f.write(f"{jf}\n")
        print(f"✅ تم إنشاء الفهرس! بيحتوي على {len(json_files)} ملف.", flush=True)

    except Exception as e:
        print(f"\n❌ خطأ في الحفظ: {e}", flush=True)

    # 🛑 اللوج النهائي لإثبات وجود الملفات قبل ما السيرفر يقفل 🛑
    print("\n📂 --- لوج إثبات وجود الملفات جوه سيرفر جيتهاب ---", flush=True)
    if os.path.exists(OUTPUT_FOLDER):
        final_files = os.listdir(OUTPUT_FOLDER)
        print(f"✅ الفولدر News_Output جواه حالياً {len(final_files)} ملفات وهم: {final_files}", flush=True)
    else:
        print("❌ الفولدر مش موجود أصلاً!", flush=True)
    print("---------------------------------------------------------", flush=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n🔥 خطأ قاضي في الكود الرئيسي: {e}", flush=True)
