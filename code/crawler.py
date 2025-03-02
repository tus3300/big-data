from kafka import KafkaProducer
import requests
from bs4 import BeautifulSoup
import json
import time

KAFKA_BROKER = 'kafka:9092'
TOPIC_NAME = 'stock_news'

def check_kafka_connection(retries=5, delay=5):
    for attempt in range(retries):
        try:
            producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
            producer.close()
            print("✅ Kafka is reachable!")
            return True
        except Exception as e:
            print(f"❌ Kafka connection failed (attempt {attempt + 1}/{retries}): {e}")
            time.sleep(delay)
    return False

if not check_kafka_connection():
    print("❌ Could not connect to Kafka after retries. Exiting.")
    exit(1)

producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def crawl_news(url):
    try:
        response = requests.get(url, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        articles = soup.find_all('article') or soup.find_all('div', class_='title')
        news_data = []
        for article in articles:
            title_tag = article.find('h2') or article.find('a')
            if title_tag and title_tag.text.strip():
                news_data.append({'title': title_tag.text.strip(), 'timestamp': time.time()})
        return news_data
    except Exception as e:
        print(f"❌ Error crawling {url}: {e}")
        return []

sources = [
    'https://vietstock.vn/chung-khoan.htm',
    'https://cafef.vn/thi-truong-chung-khoan.chn',
    'https://vnexpress.net/kinh-doanh/chung-khoan'
]

while True:
    for source in sources:
        news = crawl_news(source)
        for article in news:
            try:
                producer.send(TOPIC_NAME, article)
                print(f"✅ Sent: {article['title']}")
            except Exception as e:
                print(f"❌ Kafka send error: {e}")
        time.sleep(60)