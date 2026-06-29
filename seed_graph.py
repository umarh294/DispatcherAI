import osmnx as ox
import psycopg2
from shapely.geometry import LineString

def seed_metro_dispatch_graph():
    # Setup connection strings directly matching our compose variables
    conn = psycopg2.connect(
        host="localhost",
        database="dispatcher_ai_db",
        user="postgres",
        password="vancouver_dispatch_2026"
    )
    cursor = conn.cursor()
    
    # Establish regional target parameters
    places = [
        "Vancouver, BC, Canada", 
        "Burnaby, BC, Canada", 
        "Surrey, BC, Canada", 
        "West Vancouver, BC, Canada"
    ]
    print("🗺️  Downloading topographic maps via OpenStreetMap Overpass Engine (OSMnx v2.0+)...")
    
    G = ox.graph_from_place(places, network_type="drive", simplify=True)
    print(f"✅ Map raw asset pulled cleanly. Nodes: {len(G.nodes)}, Edges: {len(G.edges)}")
    
    print("🚀 Deconstructing network arrays into spatial frames...")
    nodes_gdf, edges_gdf = ox.graph_to_gdfs(G)

    print("📥 Seeding network intersections (Nodes)...")
    for node_id, row in nodes_gdf.iterrows():
        point_wkt = f"SRID=4326;POINT({row['x']} {row['y']})"
        cursor.execute(
            """
            INSERT INTO network_nodes (node_id, elevation_meters, geom)
            VALUES (%s, %s, ST_GeomFromEWKT(%s))
            ON CONFLICT (node_id) DO NOTHING;
            """,
            (int(node_id), None, point_wkt)
        )
    conn.commit()

    print("📥 Seeding physical transit lines (Edges) into PostGIS relational schema...")
    edge_count = 0
    
    for (u, v, key), row in edges_gdf.iterrows():
        if 'geometry' in row and isinstance(row['geometry'], LineString):
            linestring_wkt = f"SRID=4326;{row['geometry'].wkt}"
        else:
            u_node_data = nodes_gdf.loc[u]
            v_node_data = nodes_gdf.loc[v]
            linestring_wkt = f"SRID=4326;LINESTRING({u_node_data['x']} {u_node_data['y']}, {v_node_data['x']} {v_node_data['y']})"

        street_name = row.get('name', 'Unknown Segment')
        if isinstance(street_name, list):
            street_name = street_name[0]

        # REVISED: Bulletproof OpenStreetMap speed parsing engine
        max_speed = 50.0  # Default local city speed limit
        if 'maxspeed' in row:
            speed_val = row['maxspeed']
            if isinstance(speed_val, list):
                speed_val = speed_val[0]
            
            # Handle delimited strings like "50; 60" or "90 km/h" safely by isolating the first value token
            first_token = str(speed_val).split(';')[0].split()[0]
            try:
                parsed_digits = ''.join(filter(str.isdigit, first_token))
                if parsed_digits:
                    max_speed = float(parsed_digits)
                    
                    # Sane guardrail check (BC high-speed limits cap out below 130 km/h)
                    # If data is corrupted or returns an absurd layout, fall back to municipal baseline
                    if max_speed > 140.0:
                        max_speed = 50.0
            except ValueError:
                pass

        lat = row['geometry'].coords[0][1] if 'geometry' in row else nodes_gdf.loc[u]['y']
        lon = row['geometry'].coords[0][0] if 'geometry' in row else nodes_gdf.loc[u]['x']
        
        if lat > 49.3: city = "West Vancouver"
        elif lon < -123.02: city = "Vancouver"
        elif lon > -122.92: city = "Surrey"
        else: city = "Burnaby"

        cursor.execute(
            """
            INSERT INTO network_edges (u_node, v_node, city_name, street_name, length_meters, max_speed_kph, geom)
            VALUES (%s, %s, %s, %s, %s, %s, ST_GeomFromEWKT(%s));
            """,
            (int(u), int(v), city, street_name, float(row['length']), float(max_speed), linestring_wkt)
        )
        edge_count += 1
        if edge_count % 5000 == 0:
            print(f"📊 Progress: Seeded {edge_count} active street edges across corridors.")
            conn.commit()

    conn.commit()
    cursor.close()
    conn.close()
    #Successful completion message
    print("🏆 Success! 'DispatcherAI' topological road map database is fully seeded!")

if __name__ == "__main__":
    seed_metro_dispatch_graph()