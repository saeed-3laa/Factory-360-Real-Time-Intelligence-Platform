import time
import json
import random
import datetime
from azure.eventhub import EventHubProducerClient, EventData

# --- Configuration ---
FACILITIES = [
    {"id": 9188, "name": "Ras Gas Plant"},
    {"id": 7382, "name": "West Nile Delta"},
    {"id": 8734, "name": "Zohr Gas Plant"}
]

SENSOR_CATALOG = {
    "Gas Detector": ["CH4", "H2S", "CO2", "VOCs"],
    "Temperature": [None],
    "Pressure": [None],
    "Wind": [None]
}

# --- Event Hub Configuration ---
CONNECTION_STR = "Endpoint=sb://siceventhub2.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=fHbBI35PqhGjXadqVvFxpJdoH8YW+JugZ+AEhNdLz0k="
EVENTHUB_NAME = "gas-detection"               

producer = EventHubProducerClient.from_connection_string(
    conn_str=CONNECTION_STR,
    eventhub_name=EVENTHUB_NAME
)

# --- Data Generator ---
def generate_reading():
    facility = random.choice(FACILITIES)
    sensor_type = random.choice(list(SENSOR_CATALOG.keys()))
    
    if sensor_type == "Gas Detector":
        gas_type = random.choice(SENSOR_CATALOG["Gas Detector"])
    else:
        gas_type = None

    status = random.choices(["Running", "Maintenance", "Shutdown", "Idle"], weights=[0.7, 0.1, 0.1, 0.1])[0]
    
    if status == "Running":
        temp_base = random.uniform(80, 100)
        pressure_base = random.uniform(50, 110)
    elif status == "Shutdown":
        temp_base = random.uniform(20, 30)
        pressure_base = random.uniform(0, 5)
    else:
        temp_base = random.uniform(40, 60)
        pressure_base = random.uniform(10, 30)

    gas_ppm = 0.0
    leak_detected = False
    
    if sensor_type == "Gas Detector":
        if random.random() < 0.3:
            gas_ppm = random.uniform(100, 600)
        else:
            gas_ppm = random.uniform(0, 20)
            
        if gas_type == "CH4" and gas_ppm > 500:
            leak_detected = True

    record = {
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "facility_id": facility["id"],
        "facility_name": facility["name"],
        "sensor_id": random.randint(100, 999), 
        "sensor_type": sensor_type,
        "gas_type": gas_type,
        "gas_concentration_ppm": round(gas_ppm, 2),
        "emission_rate_kg_h": round(gas_ppm * 0.5, 2),
        "methane_leak_detected": leak_detected,
        "h2s_alert_level": 3 if (gas_type == "H2S" and gas_ppm > 30) else 0,
        "temperature_celsius": round(temp_base + random.uniform(-2, 2), 2),
        "pressure_bar": round(pressure_base + random.uniform(-1, 1), 2) if sensor_type == "Pressure" else 0.0,
        "unit_status": status,
        "maintenance_required": status == "Maintenance",
        "power_consumption_kw": round(random.uniform(1000, 2000), 2) if status == "Running" else 100.0,
        "wind_speed_m_s": round(random.uniform(0, 15), 2),
        "wind_direction_deg": round(random.uniform(0, 360), 2),
        "ambient_temperature_celsius": round(random.uniform(15, 35), 2),
        "ambient_humidity_percent": round(random.uniform(30, 80), 2),
        "safety_threshold_exceeded": gas_ppm > 500 or (sensor_type == "Pressure" and pressure_base > 95)
    }
    
    return record

# --- Send to Event Hub ---
def send_to_eventhub(record):
    event_data = EventData(json.dumps(record))
    event_data.properties = {"partition_key": str(record["facility_id"])}
    event_data_batch = producer.create_batch()
    event_data_batch.add(event_data)
    producer.send_batch(event_data_batch)

# --- Main Loop ---
if __name__ == "__main__":
    print("Starting Logic-Aware Realtime Generator...")
    print("Press Ctrl+C to stop")
    try:
        while True:
            data = generate_reading()
            send_to_eventhub(data)
            time.sleep(5)
    except KeyboardInterrupt:
        print("\nGenerator Stopped.")
