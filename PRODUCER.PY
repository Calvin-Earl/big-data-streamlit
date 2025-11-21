import json
import time
from datetime import datetime
import requests
from kafka import KafkaProducer

# -------------------------
# CONFIGURATION
# -------------------------
KAFKA_BROKER = 'localhost:9092'    
TOPIC = 'weather_topic'           
CITY = 'Quezon City'              
API_KEY = 'b4d082fbc43a4c42b7c114050252111'  
API_URL = f"https://api.weatherapi.com/v1/current.json?key={API_KEY}&q={CITY}&aqi=no"

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"Producer started, streaming weather data for {CITY}...")

# Function to fetch weather data
def fetch_weather_data():
    try:
        response = requests.get(API_URL)
        data = response.json()
        main = data.get('current', {})
        if not main:
            return None

        temperature = main.get("temp_c", 0)  
        humidity = main.get("humidity", 0)
        pressure = main.get("pressure_mb", 0)

        message = {
            "timestamp": datetime.utcnow().isoformat() + "Z",  
            "temperature": temperature,
            "humidity": humidity,
            "pressure": pressure,
            "city": CITY
        }
        return message
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

# Produce data continuously
while True:
    data = fetch_weather_data()
    if data:
        # Send data to Kafka
        producer.send(TOPIC, value=data)
        producer.flush()
        print("Sent:", data)
    time.sleep(15)  
