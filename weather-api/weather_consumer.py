from kafka import KafkaConsumer
import json
import sys
import time
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from datetime import datetime


CASSANDRA_HOST = 'cassandra' 
CASSANDRA_PORT = 9042
KEYSPACE = 'weather_data'
KAFKA_BROKER = 'kafka:9092'

def init_cassandra(retries=5):
    for attempt in range(1, retries + 1):
        try:
            cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
            session = cluster.connect()
            
            # Create Schema if not exists
            session.execute(f"CREATE KEYSPACE IF NOT EXISTS {KEYSPACE} WITH replication = {{'class':'SimpleStrategy', 'replication_factor':1}}")
            session.set_keyspace(KEYSPACE)
            
            session.execute("""
                CREATE TABLE IF NOT EXISTS weather_logs (
                    city_id int,
                    ts timestamp,
                    city_name text,
                    latitude float,
                    longitude float,
                    temp float,
                    weather_main text,
                    weather_desc text,
                    PRIMARY KEY (city_id, ts)
                ) WITH CLUSTERING ORDER BY (ts DESC)
            """)

            insert_ps = session.prepare("""
                INSERT INTO weather_logs (
                    city_id, ts, city_name, latitude, longitude, 
                    temp, weather_main, weather_desc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """)
            
            print("Successfully connected to Cassandra and verified Schema.")
            return cluster, session, insert_ps
        except Exception as e:
            print(f"Waiting for Cassandra... ({attempt}/{retries}): {e}")
            time.sleep(5)
    sys.exit(1)

def main():
    cluster, session, insert_ps = init_cassandra()

    consumer = KafkaConsumer(
        'weather-api-topic',
        bootstrap_servers=KAFKA_BROKER,
        group_id='weather-processor-v1',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Listening for weather data...")

    try:
        for msg in consumer:
            data = msg.value
            # print(f"Received data: {data}")
            
            # Parsing the flattened data from the JS producer
            try:
                # Convert ISO string to Python datetime
                ts = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
                
                session.execute(insert_ps, (
                    int(data['city_id']),
                    ts,
                    data['city_name'],
                    float(data['latitude']),
                    float(data['longitude']),
                    float(data['temp']),
                    data['weather_main'],
                    data['weather_desc']
                ))
                print(f"Stored data for: {data['city_name']} at {ts}")
            except Exception as e:
                print(f"Failed to process message: {e}")

    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        consumer.close()
        cluster.shutdown()

if __name__ == "__main__":
    main()