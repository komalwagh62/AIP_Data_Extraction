import requests
from bs4 import BeautifulSoup
import urllib3
import re  # Import regular expression module
from model import ThailandConvLineData, session
from sqlalchemy.sql import text as sql_text


# Disable InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Define the URL of the webpage
url = 'https://aip.caat.or.th/2024-05-16-AIRAC/html/eAIP/VT-ENR-3.3-en-GB.html'

# Send an HTTP GET request to the URL
response = requests.get(url, verify=False)

def conversionDMStoDD(coord):
    direction = {"N": 1, "S": -1, "E": 1, "W": -1}
    dir_part = coord[-1]
    num_part = coord[:-1]
    if dir_part in ["N", "S"]:
        lat_degrees = int(num_part[:2])
        lat_minutes = int(num_part[2:4])
        lat_seconds = float(num_part[4:])
        lat_dd = (lat_degrees + lat_minutes / 60 + lat_seconds / 3600) * direction[dir_part]
        return lat_dd
    elif dir_part in ["E", "W"]:
        lon_degrees = int(num_part[:3])
        lon_minutes = int(num_part[3:5])
        lon_seconds = float(num_part[5:])
        lon_dd = (lon_degrees + lon_minutes / 60 + lon_seconds / 3600) * direction[dir_part]
        return lon_dd
    else:
        return None
    
def draw_line_between_coordinates(session, coord1, coord2):
    sql_query = sql_text(
        f"SELECT ST_MakeLine(ST_GeomFromText('POINT({coord1[1]} {coord1[0]})'), ST_GeomFromText('POINT({coord2[1]} {coord2[0]})'))"
    )
    result = session.execute(sql_query)
    line_geometry = result.scalar()
    return line_geometry

# Check if the request was successful
if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find all tables with the class 'ENR-table'
    tables = soup.find_all('table', class_='ENR-table')

    # Loop through each table
    for table in tables:
        # Extract Route Designator (Airway ID)
        route_designator = table.find('p', class_='Route-designator line')
        if route_designator:
            airway_id_span = route_designator.find('span', class_='SD')
            if airway_id_span:
                airway_id = airway_id_span.text.strip()

        # Extract Start Point and relevant span tags from the second row (Table-row-type-2)
        start_point_rows = table.find_all('tr', class_='Table-row-type-2')
        start_point_data = []  # List to hold the start points and coordinates in pairs
        
        
        remark_sections = table.find_all('td', colspan='13')
        # print(remark_sections)
        # Loop through each remark section found
        for section in remark_sections:
            # Find all <span> elements with class "Remark-title"
            remark_titles = section.find_all('p', class_='line')
    
            # Iterate through each remark title found
            for remark_title in remark_titles:
                # Extract and print the title of the remark
                title_text = remark_title.text.strip()
        
        # Extract all start points and their coordinates
        for start_point_row in start_point_rows:
            span_tags = start_point_row.find_all('span', class_='SD')

            # Temporary list to hold the start point data for this row
            current_start_point = []
            current_coordinates = []

            for span in span_tags:
                text = span.text.strip()

                # Use regex to check if the text matches a valid coordinate format (numbers followed by N, S, E, or W)
                if re.match(r'\d+[NSWE]', text):  # Matches numbers followed by N, S, E, or W
                    current_coordinates.append(text)
                else:
                    # Otherwise, it's a start point name
                    current_start_point.append(text)

            if current_start_point:
                start_point_data.append((' '.join(current_start_point), ' '.join(current_coordinates)))

        start_point_rows1 = table.find_all('tr', class_='Table-row-type-3')

        for row in start_point_rows1:
            # Extract Track MAG (first <span class="SD">)
            track_mag_span = row.find('span', class_='SD')
            if track_mag_span:
                track_mag = track_mag_span.text.strip()
            else:
                track_mag = 'N/A'

            # Extract Reverse Track MAG (second <span class="SD">)
            reverse_track_mag_spans = row.find_all('span', class_='SD')
            if len(reverse_track_mag_spans) > 1:  # Ensure there are at least 2 spans
                reverse_track_mag = reverse_track_mag_spans[1].text.strip()
            else:
                reverse_track_mag = 'N/A'

            # Extract Distance (third and fourth <span class="SD">)
            dist_spans = row.find_all('span', class_='SD')[2:4]  # The third and fourth spans
            if dist_spans:
                dist_values = ' '.join([span.text.strip() for span in dist_spans])
            else:
                dist_values = 'N/A'
                
            upper_limits = row.find_all('td',class_='Upper')
            for upper_td in upper_limits:
                # Find all span elements with class 'SD'
                span_elements = upper_td.find_all('span', class_='SD')
                # Check if there are at least 2 spans to get the second and third span
                if len(span_elements) == 2:
                    # print(f"All span elements: {span_elements}")
                    # Extract and print the values from both spans
                    first_span_text = span_elements[0].text.strip()
                    second_span_text = span_elements[1].text.strip()
                    upper_limit_value = f"{first_span_text} {second_span_text}"
                    
                    
            lower_limits = row.find_all('td',class_='Lower')
            for lower_td in lower_limits:
                # Find all span elements with class 'SD'
                span_elements = lower_td.find_all('span', class_='SD')
                # Check if there are at least 2 spans to get the second and third span
                if len(span_elements) == 3:
                    # Extract and print the values from both spans
                    first_span_text = span_elements[0].text.strip()
                    second_span_text = span_elements[1].text.strip()
                    lower_limit_value = f"{first_span_text} {second_span_text}"
                    
            min_flight_alitude = row.find_all('p',class_='line')
            for min_td in min_flight_alitude:
                # Find all span elements with class 'SD'
                span_elements = min_td.find_all('span', class_='SD')
                # Check if there are at least 2 spans to get the second and third span
                if len(span_elements) == 2:
                    # Extract and print the values from both spans
                    first_span_text = span_elements[0].text.strip()
                    second_span_text = span_elements[1].text.strip()
                    min_flight_value = f"{first_span_text} {second_span_text}"
                    
           
            spans = row.find_all('span', class_='SD')
            for i, span in enumerate(spans):
                if span.next_sibling and 'VAL_WID_LEFT' in span.next_sibling.get_text(strip=True):
                    lateral_limits = span.get_text(strip=True)
                    
            
            # Find all acronyms within the row
            acronyms = row.find_all('acronym', class_='Note')
            
            direction1 = None
            direction2 = None
            direction = None
            # Extract direction information from the acronyms
            for i, acronym in enumerate(acronyms):
                parent_span = acronym.find_parent('span')
                if parent_span:
                    # Get the text content of the parent span
                    parent_text = parent_span.text.strip()
                    
                    # Assign direction1 and direction2 based on the order of appearance
                    if i == 0:
                        direction1 = parent_text
                    elif i == 1:
                        direction2 = parent_text
            
           
            # Determine the type of direction based on direction1 and direction2
            if direction1 and direction2:
                if direction1 == "Even(1)" and direction2 == "Odd(1)":
                    direction = "Both"
                elif (direction1 == "Odd(1)" and direction2 == "Even(1)") or (direction1 == "Even(1)" and direction2 == "Odd(1)"):
                    direction = "Both"
                elif direction1 == "Odd(1)" and direction2 == "Odd(1)":
                    direction = "Forward"
                elif direction1 == "Even(1)" and direction2 == "Even(1)":
                    direction = "Backward"
            elif direction1:
                if direction1 == "Odd(1)":
                    direction = "Forward"
                elif direction1 == "Even(1)":
                    direction = "Backward"
            elif direction2:
                if direction2 == "Odd(1)":
                    direction = "Forward"
                elif direction2 == "Even(1)":
                    direction = "Backward"
                    
            
        # Print the start and end points properly paired, excluding N/A data and only printing if both start and end point exist
        for i in range(len(start_point_data) - 1):  # Iterate till second last index
            start_point_info, start_coordinates = start_point_data[i]
            if isinstance(start_coordinates, str):
                # Split the string by space to separate latitude and longitude
                coordinates = start_coordinates.split()
                start_latitude = coordinates[0]
                start_longitude = coordinates[1]
                start_geom = f"{start_latitude} {start_longitude}"
                match = re.match(r"(\d+\d+)([NS])\s*(\d+\d+)([EW])", start_geom)
                if match:
                    latitude_dd = conversionDMStoDD(match.group(1) + match.group(2))
                    longitude_dd = conversionDMStoDD(match.group(3) + match.group(4))
                    start_geom = (latitude_dd, longitude_dd)
    
            end_point_info, end_coordinates = start_point_data[i + 1]
            if isinstance(end_coordinates, str):
                # Split the string by space to separate latitude and longitude
                coordinates = end_coordinates.split()
                end_latitude = coordinates[0]
                end_longitude = coordinates[1]
                end_geom = f"{end_latitude} {end_longitude}"
                match = re.match(r"(\d+\d+)([NS])\s*(\d+\d+)([EW])", end_geom)
                if match:
                        latitude_dd = conversionDMStoDD(match.group(1) + match.group(2))
                        longitude_dd = conversionDMStoDD(match.group(3) + match.group(4))
                        end_geom = (latitude_dd, longitude_dd)
                        line_geometry = draw_line_between_coordinates(session, start_geom, end_geom)
            
            airway_id = airway_id
            start_point = start_point_info
            start_coordinates = start_coordinates
            end_point = end_point_info
            end_coordinates = end_coordinates
            track_mag = track_mag
            reverse_track_mag = reverse_track_mag
            dist_values = dist_values
            upper_limit_value = upper_limit_value
            lower_limit_value = lower_limit_value
            min_flight_value = min_flight_value
            lateral_limits = lateral_limits
            direction = direction
            title_text = title_text
            
            route = ThailandConvLineData(
                airway_id = airway_id,
                start_point=start_point,
                start_point_geom=start_geom,
                end_point=end_point,
                end_point_geom=end_geom,
                track_magnetic=track_mag if isinstance(track_mag, str) else track_mag.get_text(strip=True),
                reverse_magnetic=reverse_track_mag if isinstance(reverse_track_mag, str) else reverse_track_mag.get_text(strip=True),
                radial_distance = dist_values,
                upper_limit = upper_limit_value,
                lower_limit = lower_limit_value,
                min_flight_altitude = min_flight_value,
                lateral_limits=lateral_limits if isinstance(lateral_limits, str) else lateral_limits.get_text(strip=True),
                direction_of_cruising_levels = direction,
                geomcolumn=line_geometry,
                type = 'Class [A]',
                remarks = title_text if isinstance(title_text, str) else title_text.get_text(strip=True) if title_text else None
 
                            )
            session.add(route)
    session.commit()
                
            
            
            
            
            
            