-- Enable PostGIS spatial metrics
CREATE EXTENSION IF NOT EXISTS postgis;

-- 1. Static Graph Nodes (Intersections across Metro Vancouver)
CREATE TABLE network_nodes (
    node_id BIGINT PRIMARY KEY,
    elevation_meters NUMERIC(6,2), 
    geom GEOMETRY(Point, 4326) NOT NULL
);

-- 2. Static Graph Edges (Street segments connecting intersections)
CREATE TABLE network_edges (
    edge_id SERIAL PRIMARY KEY,
    u_node BIGINT REFERENCES network_nodes(node_id), 
    v_node BIGINT REFERENCES network_nodes(node_id), 
    city_name VARCHAR(50) NOT NULL, 
    street_name VARCHAR(150),
    length_meters NUMERIC(10,2) NOT NULL,
    max_speed_kph NUMERIC(5,2) NOT NULL,
    grade_slope NUMERIC(5,2) DEFAULT 0.0, 
    geom GEOMETRY(LineString, 4326) NOT NULL
);

-- 3. Dynamic Hazard & Live Incident Tracker
CREATE TABLE active_incidents (
    incident_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    incident_type VARCHAR(30) NOT NULL, 
    severity VARCHAR(10) NOT NULL,      
    delay_minutes INT NOT NULL,
    geom GEOMETRY(Point, 4326) NOT NULL,
    reported_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 4. Time-Series Hypertable: Ingests environmental telemetry streams
CREATE TABLE live_segment_telemetry (
    edge_id INT NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    congestion_index NUMERIC(3,2) NOT NULL, 
    precipitation_mm_hr NUMERIC(4,2) DEFAULT 0.0, 
    wind_speed_kph NUMERIC(5,2) DEFAULT 0.0,
    wind_bearing_deg NUMERIC(4,1) DEFAULT 0.0  
);

-- Convert to a hypertable partitioned natively by time windows
SELECT create_hypertable('live_segment_telemetry', 'timestamp');

-- Generate high-performance Geospatial GiST indices
CREATE INDEX idx_nodes_spatial ON network_nodes USING GIST(geom);
CREATE INDEX idx_edges_spatial ON network_edges USING GIST(geom);
CREATE INDEX idx_incidents_spatial ON active_incidents USING GIST(geom);