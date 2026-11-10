import os
import json
import snowflake.connector
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    authenticator=os.getenv("SNOWFLAKE_AUTHENTICATOR"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
    role=os.getenv("SNOWFLAKE_ROLE")
)
cursor = conn.cursor()
print("Successfully connected to Snowflake")

consumer = KafkaConsumer(
    "reefer-events",
    bootstrap_servers="localhost:9092",
    group_id="snowflake-consumer-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=False
)
print("Kafka connected!")

try:
    for message in consumer:
        event = message.value
        event_json = json.dumps(event)
        cursor.execute(
            """
            INSERT INTO CONTAINERS_DATABASE.PUBLIC.CONTAINER_EVENTS_RAW
            (EVENT_DATA)
            SELECT PARSE_JSON(%s)
            """,
            (event_json,)
        )
        conn.commit()
        consumer.commit()
        print("Inserted:", event.get("event_id"))
except Exception as e:
    print("Error during ingestion:", e)
finally:
    consumer.close()
    cursor.close()
    conn.close()
    print("Kafka and Snowflake connections closed.")