import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer

class DispatcherTelemetryProducer:
    def __init__(self, broker: str, topic: str):
        self.topic = topic
        # Initialize native Kafka client targeting local Docker container mesh
        self.producer = KafkaProducer(
            bootstrap_servers=[broker],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks=1,                  # Durable validation: require cluster leader confirmation
            compression_type='gzip'  # Low-latency optimization: compress packet payloads
        )
        print(f"📡 DispatcherAI Kafka Producer streaming out live on topic: {self.topic}")

    def generate_event(self) -> dict:
        """Simulates dynamic real-time city infrastructure disruptions and atmospheric loads"""
        # Select an event track targeting our 36,759 seeded PostGIS street edges
        target_edge_id = random.randint(1, 36759)
        event_type = random.choice(["TELEMETRY_UPDATE", "INCIDENT_ALERT"])
        
        base_payload = {
            "edge_id": target_edge_id,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "event_type": event_type
        }

        if event_type == "TELEMETRY_UPDATE":
            # Tracking wind vectors, downpours, and stop-and-go scaling factors
            base_payload.update({
                "congestion_index": round(random.uniform(1.0, 3.8), 2),  # 1.0 = clear flow, 3.8 = complete gridlock
                "precipitation_mm_hr": round(random.uniform(0.0, 15.0), 2), # Rain impact on brake-drag friction
                "wind_speed_kph": round(random.uniform(0.0, 75.0), 2),      # Aerodynamic engine strain load
                "wind_bearing_deg": round(random.uniform(0.0, 360.0), 1)
            })
        else:
            # Event-driven localized asset drops causing instant alternative rerouting triggers
            incident_type = random.choice(["ACCIDENT", "CONSTRUCTION"])
            severity = random.choice(["MINOR", "MAJOR", "CRITICAL"])
            
            # Map structural delay penalty windows
            delay_mapping = {"MINOR": 5, "MAJOR": 20, "CRITICAL": 55}
            
            base_payload.update({
                "incident_type": incident_type,
                "severity": severity,
                "delay_minutes": delay_mapping[severity],
                # Generate random close coordinates over Metro Vancouver zone bounds for incident geography placement
                "latitude": round(random.uniform(49.1500, 49.3300), 6),
                "longitude": round(random.uniform(-123.2500, -122.8000), 6)
            })
            
        return base_payload

    def run_stream(self, events_per_second: int = 10):
        """Pipes constant stress telemetry logs straight into the distributed cluster broker"""
        print(f"🚀 Firing up environmental stress simulation lane ({events_per_second} events/sec)...")
        delay = 1.0 / events_per_second
        counter = 0
        
        try:
            while True:
                payload = self.generate_event()
                self.producer.send(self.topic, value=payload)
                counter += 1
                
                if counter % (events_per_second * 5) == 0:
                    print(f"📊 [PRODUCER METRIC] Pipeline healthy. Dispatched {counter} hazard events to cluster.")
                
                time.sleep(delay)
        except KeyboardInterrupt:
            print("\n🛑 Local telemetry generator pipeline paused by operator input.")
        finally:
            self.producer.flush()
            self.producer.close()

if __name__ == "__main__":
    KAFKA_BROKER = "localhost:9092"
    KAFKA_TOPIC = "dispatcher_environmental_events"
    
    # Initialize streamer core node
    streamer = DispatcherTelemetryProducer(broker=KAFKA_BROKER, topic=KAFKA_TOPIC)
    # Stream a sustained baseline of 20 telemetry entries every second
    streamer.run_stream(events_per_second=20)