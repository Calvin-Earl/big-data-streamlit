import json
from pymongo import MongoClient
from kafka import KafkaConsumer

# -------------------------
# CONFIGURATION
# -------------------------
KAFKA_BROKER = 'localhost:9092'        
TOPIC = 'weather_topic'                
MONGO_URI = 'mongodb+srv://qeresanjuan:nosdeyar@cluster0.sgf8rsf.mongodb.net/?appName=Cluster0'  # MongoDB URI
DB_NAME = 'weather_db'                 
COLLECTION_NAME = 'weather_data'      

# MongoDB setup
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Kafka consumer setup
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True
)

print("Consumer started, storing data in MongoDB...")

# Consume data from Kafka and store it in MongoDB
for message in consumer:
    data = message.value
    # Insert the data into MongoDB
    collection.insert_one(data)
    print("Saved to MongoDB:", data)
