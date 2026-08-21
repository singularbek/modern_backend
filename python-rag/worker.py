import json
import time
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from sentence_transformers import SentenceTransformer

print("⏳ Initializing Embedding Model...")
embedder = SentenceTransformer('all-MiniLM-L6-v2')

print("Connecting to ElasticSearch...")
es = Elasticsearch("http://localhost:9200")
INDEX_NAME = "catalog_context"

def init_es():
    if not es.indices.exists(index=INDEX_NAME):
        mapping = {
            "mappings": {
                "properties": {
                    "merchant_id": {"type": "keyword"},
                    "draft_id": {"type": "keyword"},
                    "title": {"type": "text"},
                    "price_etb": {"type": "float"},
                    "title_vector": {
                        "type": "dense_vector",
                        "dims": 384,
                        "index": True,
                        "similarity": "cosine"
                    }
                }
            }
        }
        es.indices.create(index=INDEX_NAME, body=mapping)
        print("🔍 [Python RAG] Created Elasticsearch Vector Index.")

def main():
    init_es()
    
    # Wait for Kafka readiness
    time.sleep(2)
    consumer = KafkaConsumer(
        'commerce.catalog.v1',
        bootstrap_servers=['127.0.0.1:9092'],
        group_id='rag-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest'
    )

    print("🤖 [Python Worker] Listening to Kafka topic 'commerce.catalog.v1'...")

    for message in consumer:
        event = message.value
        print(f"📥 [Python Worker] Consumed Kafka Event: {event['title']}")

        # 1. Generate Dense Vector Embedding
        vector = embedder.encode(event["title"]).tolist()

        # 2. Index into Elasticsearch
        doc = {
            "merchant_id": event["merchant_id"],
            "draft_id": event["draft_id"],
            "title": event["title"],
            "price_etb": event["price_etb"],
            "title_vector": vector
        }
        es.index(index=INDEX_NAME, id=event["draft_id"], body=doc)
        print(f"✅ [Python Worker] Successfully Indexed '{event['title']}' into Elasticsearch Vector DB!\n")

if __name__ == "__main__":
    main()