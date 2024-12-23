from model import Route, SignificantPoints, AiracData, session
from sqlalchemy.exc import IntegrityError
import requests
from bs4 import BeautifulSoup
from shapely import wkt
from sqlalchemy import or_, and_  # Import and_ for combined conditions
 # Import `or_` for combining conditions
import os
import re
from url_extraction import (
    find_eaip_url,
    fetch_and_parse_frameset,
    fetch_and_parse_navigation_frame,
    search_and_print_waypoint_links
)

# Conversion function from DMS to Decimal Degrees
def conversionDMStoDD(coord):
    direction = {"N": 1, "S": -1, "E": 1, "W": -1}
    dir_part = coord[-1]
    num_part = coord[:-1]
    if dir_part in ["N", "S"]:
        lat_degrees = int(num_part[:2])
        lat_minutes = int(num_part[2:4])
        lat_seconds = float(num_part[4:])
        return (lat_degrees + lat_minutes / 60 + lat_seconds / 3600) * direction[dir_part]
    elif dir_part in ["E", "W"]:
        lon_degrees = int(num_part[:3])
        lon_minutes = int(num_part[3:5])
        lon_seconds = float(num_part[5:])
        return (lon_degrees + lon_minutes / 60 + lon_seconds / 3600) * direction[dir_part]

# Function to get the active process_id from AiracData table
def get_active_process_id():
    # Query the AiracData table for the most recent active record
    active_record = session.query(AiracData).filter(AiracData.status == True).order_by(AiracData.created_At.desc()).first()
    if active_record:
        return active_record.id  # Assuming process_name is the desired process_id
    else:
        print("No active AIRAC record found.")
        return None

# Function to process waypoints and store them in the database
def process_waypoints(urls):
    process_id = get_active_process_id()
    for url in urls:
        try:
            response = requests.get(url, verify=False)
            response.raise_for_status()  # Raise an error for failed requests
        except requests.RequestException as e:
            print(f"Error fetching URL {url}: {e}")
            continue

        soup = BeautifulSoup(response.content, 'html.parser')
        tables = soup.find_all('table', class_='AmdtTable')
        
        for table in tables:
            rows = table.find_all('tr')
            for row in rows[1:]:  # Skip header row
                cells = row.find_all('td')
                if len(cells) >= 3:  # Ensure there are enough cells
                    waypoints = cells[0].get_text(strip=True)
                    coordinates = cells[1].get_text(strip=True)
                    name_of_routes = cells[2].get_text(strip=True)
                    
                    # Query the route table for both conv and non_conv types
                    conv_route = session.query(Route).filter(
                        and_(or_(Route.start_point == waypoints, Route.end_point == waypoints), Route.type == 'Conv')
                    ).first()

                    non_conv_route = session.query(Route).filter(
                        and_(or_(Route.start_point == waypoints, Route.end_point == waypoints), Route.type == 'Non Conv')
                    ).first()

                    # Determine the route type
                    if conv_route and non_conv_route:
                        # print(f"Waypoint '{waypoints}' found in both 'conv' and 'non_conv' routes. Type: non_conv")
                        route_type = 'Non Conv'
                    elif conv_route:
                        # print(f"Waypoint '{waypoints}' found in 'conv' routes. Type: conv")
                        route_type = 'Conv'
                    elif non_conv_route:
                        # print(f"Waypoint '{waypoints}' found in 'non_conv' routes. Type: non_conv")
                        route_type = 'Non Conv'
                    else:
                        # print(f"Waypoint '{waypoints}' not found in either 'conv' or 'non_conv' routes.")
                        route_type = None

                    # Regex to match coordinate format
                    match = re.match(r"(\d{4,6}[NS])\s*(\d{5,7}[EW])", coordinates)
                    if match:
                        try:
                            latitude_dd = conversionDMStoDD(match.group(1))
                            longitude_dd = conversionDMStoDD(match.group(2))
                            coordinates_dd = f"{longitude_dd} {latitude_dd}"
                            geometry = wkt.loads(f"POINT({longitude_dd} {latitude_dd})")
                            ewkb_geometry = geometry.wkb_hex

                            # Create a new SignificantPoints object
                            new_waypoint = SignificantPoints(
                                waypoints=waypoints,
                                coordinates_dd=coordinates_dd,
                                geom=ewkb_geometry,
                                name_of_routes=name_of_routes,
                                type=route_type,  # Use determined route_type
                                process_id=process_id  # Add process_id to the waypoint
                            )
                            session.add(new_waypoint)
                        except ValueError as ve:
                            print(f"Error converting coordinates for waypoint '{waypoints}': {ve}")
                            continue  # Skip invalid entries

        # Attempt to commit session for each URL processed
        try:
            session.commit()
            print(f"Committed waypoints from URL: {url}")
        except IntegrityError:
            session.rollback()
            print(f"Integrity error occurred for URL: {url}, rolled back changes.")

# Main function to drive the URL extraction and processing flow
def main():
    # processed_urls_file = "waypoint.url.txt"
    # eaip_url = find_eaip_url()
    # if eaip_url:
    #     base_frame_url = fetch_and_parse_frameset(eaip_url)
    #     if base_frame_url:
    #         navigation_url = fetch_and_parse_navigation_frame(base_frame_url)
    #         if navigation_url:
    #             enr_4_4_urls = search_and_print_waypoint_links(navigation_url, processed_urls_file)
                enr_4_4_urls = ["https://aim-india.aai.aero/eaip-v2-07-2024/eAIP/IN-ENR%204.4-en-GB.html"]
                process_waypoints(enr_4_4_urls)

if __name__ == "__main__":
    main()
