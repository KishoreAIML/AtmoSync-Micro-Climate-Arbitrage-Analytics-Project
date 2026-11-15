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

insert_query = """
    INSERT INTO CONTAINERS_DATABASE.PUBLIC.CONTAINER_EVENTS (
        EVENT_ID, MARKET_ID, SCHEMA_VERSION, TIMESTAMP, CONTAINER_ID,
        CARGO_NAME, ORIGINAL_VALUE, LATITUDE, LONGITUDE,
        DEST_LAT, DEST_LON, SPEED_KNOTS, COMPRESSOR_FAILED,
        HOURS_IN_TRANSIT, TARGET_TEMP_C, AMBIENT_TEMP_C,
        SUPPLY_TEMP_C, RETURN_TEMP_C, PULP_TEMP_C,
        TEMPERATURE_DRIFT_C, TEMPERATURE_DRIFT_FLAG,
        HUMIDITY_PCT, DEW_POINT_C, OXYGEN_PCT,
        CARBON_DIOXIDE_PCT, ETHYLENE_PPM, SHOCK_X_G,
        SHOCK_Y_G, SHOCK_Z_G, VIBRATION_HZ, LIGHT_LUX,
        DOOR_OPEN, PRESSURE_HPA
    )
    VALUES (
        %s,%s, %s, TO_TIMESTAMP_TZ(%s)::TIMESTAMP_NTZ,
        %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s
    )
"""

try:
    for message in consumer:
        event = message.value
        try:
            event_values = (
                event.get("event_id"),
                event.get("market_id"),
                event.get("schema_version"),
                event.get("timestamp"),
                event.get("container_id"),
                event.get("cargo_name"),
                event.get("original_value"),
                event.get("latitude"),
                event.get("longitude"),
                event.get("dest_lat"),
                event.get("dest_lon"),
                event.get("speed_knots"),
                event.get("compressor_failed"),
                event.get("hours_in_transit"),
                event.get("target_temp_c"),
                event.get("ambient_temp_c"),
                event.get("supply_temp_c"),
                event.get("return_temp_c"),
                event.get("pulp_temp_c"),
                event.get("temperature_drift_c"),
                event.get("temperature_drift_flag"),
                event.get("humidity_pct"),
                event.get("dew_point_c"),
                event.get("oxygen_pct"),
                event.get("carbon_dioxide_pct"),
                event.get("ethylene_ppm"),
                event.get("shock_x_g"),
                event.get("shock_y_g"),
                event.get("shock_z_g"),
                event.get("vibration_hz"),
                event.get("light_lux"),
                event.get("door_open"),
                event.get("pressure_hpa")
            )

            cursor.execute(insert_query,event_values)
            conn.commit()
            consumer.commit()
            print(
                "Successfully inserted:",event.get("event_id"))
        except Exception as e:
            conn.rollback()
            print("Event insertion failed:", e)

except Exception as e:
    print("Error during ingestion:", e)
finally:
    consumer.close()
    cursor.close()
    conn.close()
    print("Kafka and Snowflake connections closed.")