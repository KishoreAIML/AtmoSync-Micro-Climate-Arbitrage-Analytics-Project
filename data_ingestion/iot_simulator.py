import json
import random
import time
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer
from commodity_pricing_data import markets

class ContainerSimulator:
    def __init__(self, container_id, cargo_name, original_value, target_temp_c, drift_threshold_c, dest_latitude, dest_longitude, market_ids):
        self.container_id = container_id
        self.market_id = random.choice(market_ids)
        self.cargo_name = cargo_name
        self.original_value = original_value

        self.schema_version = "1.0"
        self.event_counter = 0

        self.latitude = 12.0588
        self.longitude = 70.0582

        self.dest_lat = dest_latitude
        self.dest_lon = dest_longitude

        self.speed_knots = 18.0

        self.target_temp_c = target_temp_c
        self.drift_threshold_c = drift_threshold_c

        self.ambient_temp_c = 2.4
        self.supply_temp_c = 1.0
        self.return_temp_c = 2.5
        self.pulp_temp_c = 1.0
        self.humidity_pct = 88.0

        self.oxygen_pct = 3.5
        self.carbon_dioxide_pct = 4.5
        self.ethylene_ppm = 0.5

        self.shock_x_g = 0.0
        self.shock_y_g = 0.0
        self.shock_z_g = 1.0
        self.vibration_hz = 2.0

        self.light_lux = 0.0
        self.door_open = False

        self.pressure_hpa = 1013.0

        self.compressor_failed = False
        self.hours_in_transit = 0

    def trigger_compressor_failure(self):
        self.compressor_failed = True
        print("ALERT: Compressor failure detected!")

    def calculate_dew_point(self):
        a = 17.27
        b = 237.7
        humidity = max(1.0, min(self.humidity_pct, 100.0))
        alpha = (
            (a * self.ambient_temp_c) / (b + self.ambient_temp_c)
            + __import__("math").log(humidity / 100.0)
        )
        dew_point = (b * alpha) / (a - alpha)
        return round(dew_point, 2)

    def generate_sensor_readings(self):
        if self.compressor_failed:
            self.supply_temp_c += random.uniform(0.8, 1.5)
            self.return_temp_c += random.uniform(1.0, 2.0)
            self.pulp_temp_c += random.uniform(0.6, 1.2)
            self.ambient_temp_c += random.uniform(0.2, 0.6)
            self.humidity_pct = max(20.0,self.humidity_pct - random.uniform(0.5, 2.0))
    
            self.oxygen_pct += random.uniform(0.01, 0.05)
            self.carbon_dioxide_pct += random.uniform(0.05, 0.15)
            self.ethylene_ppm += random.uniform(2.5, 5.0)
    
        else:
            self.supply_temp_c = random.normalvariate(
                self.target_temp_c, 0.1
            )
            self.return_temp_c = random.normalvariate(2.5, 0.2)
            self.pulp_temp_c = random.normalvariate(
                self.target_temp_c, 0.2
            )
    
            self.ambient_temp_c = random.normalvariate(2.4, 0.2)
            self.humidity_pct = random.normalvariate(88.0, 1.0)
    
            self.oxygen_pct = random.normalvariate(3.5, 0.1)
            self.carbon_dioxide_pct = random.normalvariate(4.5, 0.1)
            self.ethylene_ppm = random.normalvariate(0.5, 0.05)
        self.shock_x_g = random.uniform(-0.5, 0.5)
        self.shock_y_g = random.uniform(-0.5, 0.5)
        self.shock_z_g = random.uniform(0.8, 1.5)
    
        if random.random() < 0.05:
            self.shock_z_g += random.uniform(1.5, 3.0)
        
        self.vibration_hz = random.uniform(1.0, 10.0)
        self.door_open = random.random() < 0.03
        
        if self.door_open:
            self.light_lux = random.uniform(300.0, 500.0)
        else:
            self.light_lux = random.uniform(0.0, 5.0)

        self.pressure_hpa += random.uniform(-0.5, 0.5)
        
        self.latitude += 0.04
        self.longitude += 0.04
        
        self.hours_in_transit += 1

    def generate_telemetry_event(self):
        self.event_counter += 1
        self.generate_sensor_readings()
        temperature_drift = (
            self.pulp_temp_c - self.target_temp_c
        )
    
        temperature_drift_flag = (
            abs(temperature_drift) > self.drift_threshold_c
        )
    
        ist = timezone(timedelta(hours=5, minutes=30))
        timestamp = datetime.now(ist).isoformat(timespec="seconds")
        event = {
            "event_id": f"evt-{self.event_counter:06d}",
            "schema_version": self.schema_version,
            "timestamp": timestamp,
            "container_id": self.container_id,
            "market_id": self.market_id,
            "cargo_name": self.cargo_name,
            "original_value": self.original_value,
            "latitude": round(self.latitude, 4),
            "longitude": round(self.longitude, 4),
            "dest_lat": self.dest_lat,
            "dest_lon": self.dest_lon,
            "speed_knots": round(self.speed_knots, 2),
            "compressor_failed": self.compressor_failed,
            "hours_in_transit": self.hours_in_transit,
            "target_temp_c": round(self.target_temp_c, 2),
            "ambient_temp_c": round(self.ambient_temp_c, 2),
            "supply_temp_c": round(self.supply_temp_c, 2),
            "return_temp_c": round(self.return_temp_c, 2),
            "pulp_temp_c": round(self.pulp_temp_c, 2),
            "temperature_drift_c": round(temperature_drift, 2),
            "temperature_drift_flag": temperature_drift_flag,
            "humidity_pct": round(self.humidity_pct, 2),
            "dew_point_c": self.calculate_dew_point(),
            "oxygen_pct": round(self.oxygen_pct, 2),
            "carbon_dioxide_pct": round(self.carbon_dioxide_pct, 2),
            "ethylene_ppm": round(self.ethylene_ppm, 2),
            "shock_x_g": round(self.shock_x_g, 2),
            "shock_y_g": round(self.shock_y_g, 2),
            "shock_z_g": round(self.shock_z_g, 2),
            "vibration_hz": round(self.vibration_hz, 2),
            "light_lux": round(self.light_lux, 2),
            "door_open": self.door_open,
            "pressure_hpa": round(self.pressure_hpa, 2)
        }
    
        return event

    def run_simulation(self, duration_minutes, interval_seconds=10, producer=None, topic_name=None):
        total_duration = duration_minutes * 60
        start_time = time.monotonic()
        next_event_time = start_time

        try:
            while True:
                current_time = time.monotonic()
                elapsed_time = current_time - start_time

                if elapsed_time >= total_duration:
                    break
                event = self.generate_telemetry_event()
                json_event = json.dumps(event, indent=4)
                print(json_event)

                if producer is not None and topic_name is not None:
                    producer.send(topic_name, value=event)
                    print(f"Event {event['event_id']} sent to Kafka")

                next_event_time += interval_seconds
                remaining_time = next_event_time - time.monotonic()

                if remaining_time > 0:
                    time.sleep(remaining_time)

        except KeyboardInterrupt:
            print("\nSimulation stopped manually.")



if __name__ == "__main__":

    market_ids = [market[0] for market in markets]

    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    topic_name = "reefer-events"

    simulator = ContainerSimulator(
        container_id="DEMO-CN-909",
        cargo_name="Demo Cargo",
        original_value=15000.0,
        target_temp_c=1.0,
        drift_threshold_c=1.0,
        dest_latitude=19.0760,
        dest_longitude=72.8777,
        market_ids=market_ids
    )

    DURATION_MINUTES = 5
    EVENT_INTERVAL_SECONDS = 1

    try:
        simulator.run_simulation(
            duration_minutes=DURATION_MINUTES,
            interval_seconds=EVENT_INTERVAL_SECONDS,
            producer=producer,
            topic_name=topic_name
        )

        producer.flush()

    finally:
        producer.close()

