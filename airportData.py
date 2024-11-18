import time
import requests
from bs4 import BeautifulSoup
import re
from model import AirportData, session
from shapely import wkt
from url_extraction import (
    find_eaip_url,
    fetch_and_parse_frameset,
    fetch_and_parse_navigation_frame,
    fetch_and_print_airports
)

  
def conversionDMStoDD(coord):
    direction = {"N": 1, "S": -1, "E": 1, "W": -1}
    # Extract the direction (N/S or E/W) and numeric part
    dir_part = coord[-1]
    num_part = coord[:-1]
    # Handle latitude (N/S)
    if dir_part in ["N", "S"]:
        # Latitude degrees are up to two digits, minutes are the next two digits, and seconds are the rest
        lat_degrees = int(num_part[:2])
        lat_minutes = int(num_part[2:4])
        lat_seconds = float(num_part[4:])
        lat_dd = (lat_degrees + lat_minutes / 60 + lat_seconds / 3600) * direction[dir_part]
        return lat_dd
    # Handle longitude (E/W)
    if dir_part in ["E", "W"]:
        # Longitude degrees are up to three digits, minutes are the next two digits, and seconds are the rest
        lon_degrees = int(num_part[:3])
        lon_minutes = int(num_part[3:5])
        lon_seconds = float(num_part[5:])
        lon_dd = (lon_degrees + lon_minutes / 60 + lon_seconds / 3600) * direction[dir_part]
        return lon_dd
      

def fetch_airports_details(airport_url):
    # Fetch the content from the given airport URL
    response = requests.get(airport_url, verify=False)
    
    # Check for successful response
    if response.status_code != 200:
        print(f"Failed to fetch the navigation content. Status code: {response.status_code}")
        return

    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find all tables with the class 'AmdtTable'
    tables = soup.find_all('table', class_='AmdtTable')

    # Initialize flags and counters
    in_ad_2_2 = False
    data_count = 0  # Counter for the number of printed data points
    icao_code, airport_name = None, None  # Initialize ICAO code and airport name
    
    # Loop through each table found
    for table in tables:
       
        # Check for the Aerodrome Location Indicator and Name section
        if (table.find('p', class_='Undertitle-centered') or 
        table.find('p', class_='Undertitle') or 
        table.find('p', class_='Undertitle-text-centered') or
        table.find('h6', class_='Undertitle-centered') or
        table.find('h6', class_='Undertitle-text-centered')) and 'AD 2.1 AERODROME LOCATION INDICATOR AND NAME' in table.get_text():
        
            rows = table.find_all('tr')  # Get all rows in the table
            for row in rows:
                cells = row.find_all('td')  # Get all cells in the row
                
                for cell in cells:
                    # Try to find the <p> tag with class 'Undertitle'
                    p_tag = (cell.find('p', class_='Undertitle-text-centered') or 
                       cell.find('p', class_='Undertitle-centered') or 
                       cell.find('p', class_='Undertitle') or
                       cell.find('h6', class_='Undertitle-text-centered') or 
                       cell.find('h6', class_='Undertitle-centered'))
                    if p_tag:
                        aerodrome_name = p_tag.get_text(strip=True)
                        # Skip if 'AD 2.1' or 'NOTE' is in the aerodrome name
                        if 'AD 2.1' not in aerodrome_name or not aerodrome_name.startswith('NOTE'):
                            # Split the aerodrome name into ICAO code and airport name
                            parts = aerodrome_name.split(' - ', 1)
                            if len(parts) == 2:
                                icao_code = parts[0].strip()
                                airport_name = parts[1].strip()
                                print(f"ICAO Code: {icao_code}")
                                print(f"Airport Name: {airport_name}")

        # Check for the Aerodrome Data section (AD 2.2)
        if (table.find('h6', class_='Title-center') and 'AD 2.2' in table.get_text()) or (table.find('p', class_='Undertitle') and 'AD 2.2' in table.get_text()):
            in_ad_2_2 = True  # Set flag to True when entering AD 2.2
            
        # If we are in AD 2.2, process data points
        if in_ad_2_2:
            rows = table.find_all('tr')  # Get all rows in the table
            for row in rows:
                cells = row.find_all('td')  # Get all cells in the row
                
                # Process cells for relevant data fields
                if icao_code and airport_name:  # Ensure ICAO code and airport name are defined
                    # coordinates, distance, aerodrome_elevation, magnetic_variation = None, None, None, None
                    for i in range(len(cells) - 1):
                        # Field extraction logic for fields 1-4
                        
                        # Extract coordinates (field 1)
                        p_tag_centered = cells[i].find('p', class_='Undertitle-text-centered')
                        h6_tag_centered = cells[i].find('h6', class_='Undertitle-text-centered')
                        if (p_tag_centered and p_tag_centered.get_text(strip=True) == '1') or (h6_tag_centered and h6_tag_centered.get_text(strip=True) == '1'):
                            coordinates = cells[i + 2].find('p', class_='Paragraph-text').get_text(strip=True)
                            print(coordinates)  # Print coordinates for debug
                            match = re.match(r"(\d+)([NS])\s*(\d+)([EW])", coordinates)
                            if match:
                                latitude_dd = conversionDMStoDD(match.group(1) + match.group(2))
                                longitude_dd = conversionDMStoDD(match.group(3) + match.group(4))
                                coordinates_dd = f"{longitude_dd} {latitude_dd}"
                                geometry = wkt.loads(f"POINT({longitude_dd} {latitude_dd})")
                                ewkb_geometry = geometry.wkb_hex
                        
                        # Extract distance (field 2)
                        if (p_tag_centered and p_tag_centered.get_text(strip=True) == '2') or (h6_tag_centered and h6_tag_centered.get_text(strip=True) == '2'):
                            distance = cells[i + 2].find('p', class_='Paragraph-text').get_text(strip=True)
                            print(distance)  # Print distance for debug
                        
                        # Extract aerodrome elevation (field 3)
                        if (p_tag_centered and p_tag_centered.get_text(strip=True) == '3') or (h6_tag_centered and h6_tag_centered.get_text(strip=True) == '3'):
                            aerodrome_elevation = cells[i + 2].find('p', class_='Paragraph-text').get_text(strip=True)
                            print(aerodrome_elevation)  # Print elevation for debug
                         
                        # Extract magnetic variation (field 4)
                        if (p_tag_centered and p_tag_centered.get_text(strip=True) == '4') or (h6_tag_centered and h6_tag_centered.get_text(strip=True) == '4'):
                            magnetic_variation = cells[i + 2].find('p', class_='Paragraph-text').get_text(strip=True)
                            print(magnetic_variation)  # Print magnetic variation for debug

                            # Add to database if all data points are available
                    
                            print("yghbjn")
                            airport_data = AirportData(
                                    ICAOCode=icao_code,
                                    airport_name=airport_name,
                                    coordinate=coordinates,
                                    coordinates_dd = coordinates_dd,
                                    geom=ewkb_geometry,
                                    distance=distance,
                                    aerodrome_elevation=aerodrome_elevation,
                                    magnetic_variation=magnetic_variation
                                )
                            session.add(airport_data)
                            print(airport_data)
                            session.commit()
                            return  # Exit after saving data

                    data_count += 1
                    if data_count >= 15:
                        return  # Exit after printing the required data

        # Exit AD 2.2 section on reaching AD 2.3
        if (table.find('h6', class_='Title-center') and 'AD 2.3' in table.get_text()) or (table.find('p', class_='Undertitle') and 'AD 2.3' in table.get_text()):
            in_ad_2_2 = False
            break

    # Close the session after all operations
    session.close()
    
def main():
    eaip_url = find_eaip_url()
    if eaip_url:
        # Fetch and process the frame content
        base_frame_url = fetch_and_parse_frameset(eaip_url)
        if base_frame_url:
            navigation_frame_url = fetch_and_parse_navigation_frame(base_frame_url)
            if navigation_frame_url:
                airport_urls = fetch_and_print_airports(navigation_frame_url)
                for airport_url in airport_urls:
                    fetch_airports_details(airport_url)
    else:
        print("No EAIP URL found.")


if __name__ == "__main__":
    starttime = time.time()
    main()
    endtime = time.time()
    print(endtime-starttime,"time")
