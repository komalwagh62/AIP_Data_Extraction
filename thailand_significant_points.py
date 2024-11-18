from model import SignificantPoints, session
from sqlalchemy.exc import IntegrityError
import requests
from bs4 import BeautifulSoup
import re
from shapely import wkt
# from shapely.geometry import Point


# Define the URL of the webpage containing the data
url = 'https://aip.caat.or.th/2024-10-31-AIRAC/html/eAIP/VT-ENR-4.4-en-GB.html'

# Send an HTTP GET request to the URL
response = requests.get(url, verify=False)

# Function to convert DMS to Decimal Degrees
def conversionDMStoDD(coord):
    if not coord:
        return None
    direction = {"N": 1, "S": -1, "E": 1, "W": -1}
    dir_part = coord[-1]
    num_part = coord[:-1]

    if dir_part in ["N", "S"]:
        lat_degrees = int(num_part[:2])
        lat_minutes = int(num_part[2:4])
        lat_seconds = float(num_part[4:])
        lat_dd = (lat_degrees + lat_minutes / 60 + lat_seconds / 3600) * direction[dir_part]
        return round(lat_dd, 6)

    if dir_part in ["E", "W"]:
        lon_degrees = int(num_part[:3])
        lon_minutes = int(num_part[3:5])
        lon_seconds = float(num_part[5:])
        lon_dd = (lon_degrees + lon_minutes / 60 + lon_seconds / 3600) * direction[dir_part]
        return round(lon_dd, 6)



# Check if the request was successful
if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')
    tables = soup.find_all('table', class_='ENR-table')

    for table in tables:
        rows = table.find_all('tr')[2:]  # Skip the header rows

        for row in rows:
            cells = row.find_all('td')
            if len(cells) >= 3:
                try:
                    # Extract the first span data for the name-code designator
                    name_code_span = cells[0].find('span', class_='SD')
                    name_code = name_code_span.text.strip() if name_code_span else None

                    # Extract latitude and longitude
                    lat_p_tag = cells[1].find('p', {'id': re.compile('ID_[0-9]+')})
                    lng_p_tag = lat_p_tag.find_next('p', {'id': re.compile('ID_[0-9]+')})

                    latitude_span = lat_p_tag.find('span', class_='SD') if lat_p_tag else None
                    longitude_span = lng_p_tag.find('span', class_='SD') if lng_p_tag else None

                    latitude = latitude_span.text.strip() if latitude_span else None
                    longitude = longitude_span.text.strip() if longitude_span else None

                    # Convert latitude and longitude to Decimal Degrees
                    latitude_dd = conversionDMStoDD(latitude)
                    longitude_dd = conversionDMStoDD(longitude)
                    coordinates_dd = f"{longitude_dd} {latitude_dd}"

                    if latitude_dd is None or longitude_dd is None:
                        print(f"Skipping invalid coordinates for {name_code}")
                        continue

                    geometry = wkt.loads(f"POINT({longitude_dd} {latitude_dd})")
                    ewkb_geometry = geometry.wkb_hex

                    # Extract multiple route data from the cell
                    route_cell = cells[2]
                    route_spans = route_cell.find_all('span', class_='SD')
                    routes = [route.text.strip() for route in route_spans]

                    # Join all extracted routes with a comma
                    route_data = ", ".join(routes)

                    # Insert into SignificantPoints table
                    new_waypoint = SignificantPoints(
                        waypoints=name_code,
                        coordinates_dd=coordinates_dd,
                        geom=ewkb_geometry,
                        name_of_routes=route_data
                    )
                    session.add(new_waypoint)
                    session.commit()
                    print(f"Inserted {name_code} into SignificantPoints table.")

                except IntegrityError:
                    session.rollback()
                    print(f"Duplicate entry for {name_code}, skipping insertion.")
                except Exception as e:
                    session.rollback()
                    print(f"Error processing row for {name_code}: {e}")

    session.close()
else:
    print("Failed to retrieve data from the URL.")
