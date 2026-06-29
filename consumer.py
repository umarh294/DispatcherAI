import json
import psycopg2
from psycopg2.extras import execute_batch
from kafka import KafkaConsumer

class DispatcherTelemetryConsumer:
    def __init__(self, broker: str, topic: str, batch_size: int = 50):
        self.batch_size = batch_size
        
        # 1. Initialize Distributed Kafka Consumer Channel
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[broker],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest', # Focus on real-time stream state parity
            enable_auto_commit=True,
            group_id='dispatcher_processor_nodes'
        )
        
        # 2. Establish High-Speed Transaction Pipeline to PostGIS / TimescaleDB
        self.conn = psycopg2.connect(
            host="localhost",
            database="dispatcher_ai_db",
            user="postgres",
            password="vancouver_dispatch_2026"
        )
        self.cursor = self.conn.cursor()
        print(f"📥 Consumer listening on Kafka topic '{topic}' and connected to Database Engine.")

    def flush_telemetry_batch(self, telemetry_rows: list):
        """Executes fast atomic bulk insert transactions into the TimescaleDB hypertable"""
        sql = """
            INSERT INTO live_segment_telemetry 
            (edge_id, timestamp, congestion_index, precipitation_mm_hr, wind_speed_kph, wind_bearing_deg)
            VALUES (%s, %s, %s, %s, %s, %s);
        """
        try:
            # execute_batch drops query-parsing roundtrip times by up to 90%
            execute_batch(self.cursor, sql, telemetry_rows)
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print(f"⚠️  Database flush rollback on telemetry segment: {e}")

    def insert_incident(self, row: dict):
        """Spatially projects event-driven incidents right into the PostGIS layer"""
        sql = """
            INSERT INTO active_incidents (incident_type, severity, delay_minutes, geom)
            VALUES (%s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326));
        """
        try:
            self.cursor.execute(sql, (
                row["incident_type"], 
                row["severity"], 
                row["delay_minutes"], 
                row["longitude"], # PostGIS coordinate pairs require Longitude first (X axis)
                row["latitude"]   # followed by Latitude (Y axis)
            ))
            self.conn.commit()
            print(f"🚨 [GEO-INCIDENT REGISTERED] Spatially indexed an active {row['severity']} {row['incident_type']} on map network.")
        except Exception as e:
            self.conn.rollback()
            print(f"⚠️  Database flush rollback on spatial incident entry: {e}")

    def start_processing(self):
        """Main event-processing runtime engine loop"""
        print(f"⚡ Ingestion processing engine engaged. Micro-batch window threshold set to: {self.batch_size} packets.")
        telemetry_buffer = []
        
        try:
            for message in self.consumer:
                event = message.value
                
                # Routing path A: Time-series operational metrics arrays
                if event.get("event_type") == "TELEMETRY_UPDATE":
                    row_tuple = (
                        event["edge_id"],
                        event["timestamp"],
                        event["congestion_index"],
                        event["precipitation_mm_hr"],
                        event["wind_speed_kph"],
                        event["wind_bearing_deg"]
                    )
                    telemetry_buffer.append(row_tuple)
                    
                    # When buffer window hits the watermark cap, flush instantly
                    if len(telemetry_buffer) >= self.batch_size:
                        self.flush_telemetry_batch(telemetry_buffer)
                        print(f"📊 [DATABASE BATCH METRIC] Successfully flushed {len(telemetry_buffer)} records to live_segment_telemetry hypertable.")
                        telemetry_buffer.clear()
                
                # Routing path B: Event-driven critical geographic hazards
                elif event.get("event_type") == "INCIDENT_ALERT":
                    self.insert_incident(event)
                    
        except KeyboardInterrupt:
            print("\n🛑 Ingestion Consumer processor safely shut down.")
        finally:
            if telemetry_buffer:
                self.flush_telemetry_batch(telemetry_buffer)
            self.cursor.close()
            self.conn.close()

if __name__ == "__main__":
    KAFKA_BROKER = "localhost:9092"
    KAFKA_TOPIC = "dispatcher_environmental_events"
    
    processor = DispatcherTelemetryConsumer(broker=KAFKA_BROKER, topic=KAFKA_TOPIC, batch_size=100)
    processor.start_processing()