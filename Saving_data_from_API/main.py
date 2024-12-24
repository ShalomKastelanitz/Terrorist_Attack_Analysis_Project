import time
import requests
from opencage.geocoder import OpenCageGeocode
from db_utils import session,get_or_create_date,get_or_create_location,get_or_create_group,get_or_create_attack_type,insert_fact_event


NEWS_API_KEY = "f3ba4f42-1e14-4ec6-aca2-426648340bf8"
OPENCAGE_API_KEY = "db7788d5483845a4a9396ad2e6104cfd"
geocoder = OpenCageGeocode(OPENCAGE_API_KEY)

def fetch_news_articles(page=1):
    url = "https://eventregistry.org/api/v1/article/getArticles"
    payload = {
        "action": "getArticles",
        "keyword": "terror attack",
        "articlesPage": page,
        "articlesCount": 100,
        "apiKey": NEWS_API_KEY,
    }
    response = requests.post(url, json=payload)
    response.raise_for_status()
    print(f"[DEBUG] API Response Status Code: {response.status_code}")
    print(f"[DEBUG] API Response Content: {response.text[:500]}...")  # Limit output for readability
    return response.json().get("articles", {}).get("results", [])

def process_news_articles():
    page = 1
    while True:
        try:
            print(f"[INFO] Starting processing for page {page}")
            articles = fetch_news_articles(page=page)

            for article in articles:
                article_id = int(article.get("uri"))
                date_str = article.get("date", "")
                location_name = article.get("source", {}).get("title", "Unknown")

                if not date_str:
                    print(f"[WARNING] Skipping article {article_id} due to missing date.")
                    continue

                date_id = get_or_create_date(date_str)

                geo_result = geocoder.geocode(location_name)
                if geo_result:
                    lat = geo_result[0]["geometry"]["lat"]
                    lon = geo_result[0]["geometry"]["lng"]
                else:
                    lat, lon = None, None

                location_id = get_or_create_location(
                    region="Unknown",
                    country="Unknown",
                    city=location_name,
                    latitude=lat,
                    longitude=lon
                )

                attack_type_id = get_or_create_attack_type("General")
                group_id = get_or_create_group("Unknown")

                insert_fact_event(
                    date_id=date_id,
                    location_id=location_id,
                    attack_type_id=attack_type_id,
                    group_id=group_id,
                    nkill=0,
                    nwound=0,
                    article_id=article_id
                )

            page += 1
            session.commit()
            print(f"[INFO] Completed processing for page {page - 1}")
            time.sleep(120)  # המתנה של 2 דקות בין קריאות

        except Exception as e:
            session.rollback()
            print(f"[ERROR] Error occurred: {e}")

process_news_articles()
