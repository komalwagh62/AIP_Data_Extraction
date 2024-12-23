import json
from model import Controlled, session

def fetch_data(json_data):
    fetched_data = []
    for feature in json_data['features']:
        properties = feature.get('properties', {})
        geometry = feature.get('geometry', {})
        
        item_data = {
            'id': properties.get('id', None),
            'name': properties.get('Name', None),
            'type': geometry.get('type', None),
            'geometry': geometry.get('coordinates', None),
        }
        fetched_data.append(item_data)
    return fetched_data

def insert_data(data):
    for item in data:
        coordinates = item['geometry']
        type = item['type']

        if coordinates and type == 'MultiPolygon':
            try:
                # Create WKT for MultiPolygon
                multipolygons = []
                for polygon in coordinates:  # Each polygon in MultiPolygon
                    rings = []
                    for ring in polygon:  # Outer ring and possible inner rings
                        ring_coords = ", ".join(f"{pt[0]} {pt[1]}" for pt in ring)
                        rings.append(f"({ring_coords})")
                    multipolygons.append(f"({', '.join(rings)})")
                wkt_geometry = f"SRID=4326;MULTIPOLYGON({', '.join(multipolygons)})"

                restrict_data = Controlled(
                    name=item['name'],
                    type=item['type'],
                    geometry=json.dumps(item['geometry']),  # Store raw GeoJSON geometry as string
                    geom=wkt_geometry
                )
                session.add(restrict_data)
            except Exception as e:
                print(f"Error processing geometry for item ID {item['id']}: {e}")

    # Commit session
    try:
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Error committing to database: {e}")

# Main Execution
if __name__ == "__main__":
    geojson_file = r'C:\Users\LENOVO\Desktop\ANS_Register_Extraction\AIP_Data_Extraction\ENR2.1_Final.geojson'
    
    with open(geojson_file, 'r') as file:
        data = json.load(file)
    
    # Fetch data
    result = fetch_data(data)
    
    # Insert data into database
    insert_data(result)
