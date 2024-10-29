import json
from model import Restricted, session
from sqlalchemy import func

def fetch_data(json_data):
    fetched_data = []
    for feature in json_data['features']:
        properties = feature['properties']
        # Fetch 'designation', 'name', 'type', 'fir_ids', and 'geometry'
#  "Airspace_name": "Chirala", "airspace_id": 184, "geometry_type": "ST_Polygon"}, "geometry": 
        item_data = {
            'id':properties['id'],
            'designation': properties['designation'],
            'name': properties['name'],
            'fir': properties['fir'],
            'type': properties['type'],
            'designator': properties['designator'],
            'Airspace_name': properties['Airspace_name'],
            'airspace_id': properties['airspace_id'],
            'geometry_type': properties['geometry_type'],
           'geometry': feature['geometry']['coordinates'] 
        }
        fetched_data.append(item_data)
    return fetched_data

# Read JSON data from file
with open(r'C:\Users\LENOVO\Desktop\ANS_Register_Extraction\AIP_Data_Extraction\RestrictedAirspace(7B86E25E).geojson', 'r') as file:
    data = json.load(file)

# Fetch specific fields from JSON data
result = fetch_data(data)

# Function to insert data into the Control table

def insert_data(data):
    for item in data:
        coordinates = item['geometry'][0]
        print("Coordinates:", coordinates)  # Debugging print
        print("Type of coordinates:", type(coordinates))  # Debugging print
        
        # Ensure coordinates is always a list of lists
        if isinstance(coordinates, list):
            if coordinates:  # Check if coordinates list is not empty
                # Check if all elements in coordinates are lists
                if all(isinstance(coord, list) for coord in coordinates):
                    # It's a valid polygon or list of coordinates
                    if isinstance(coordinates[0], list):
                        # Check if it's a polygon
                        coordinates = coordinates + [coordinates[0]]  # Close the polygon
                        geom_type = 'POLYGON'
                    else:
                        # It's a list of coordinates
                        geom_type = 'LINESTRING'
                else:
                    # Convert single coordinate to a list
                    coordinates = [[coordinates[0], coordinates[1]]]
                    geom_type = 'POINT'
                
                # Convert coordinates to a string representation
                coords_string = ','.join([f'{coord[0]} {coord[1]}' for coord in coordinates])
                # Construct the full WKT geometry string
                geometry = f'SRID=4326;{geom_type}(({coords_string}))'
                print(geometry)
                    
                restrict_data = Restricted(
                    restrict_id=item['id'],
                    designation=item['designation'],
                    name=item['name'],
                    fir=item['fir'],
                    type=item['type'],
                    designator=item['designator'],
                    Airspace_name=item['Airspace_name'],
                    airspace_id=item['airspace_id'],
                    geometry_type=item['geometry_type'],
                    geometry=item['geometry'],
                    geom=geometry
                )
                session.add(restrict_data)
            else:
                print("Empty coordinates list encountered.")
        else:
            print("Invalid coordinates format. Skipping this entry.")
    
    session.commit()  # Commit changes after inserting all data
 # Commit changes after inserting all data

# Insert the fetched data into the Control table
insert_data(result)
