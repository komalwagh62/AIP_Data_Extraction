import os
from sqlalchemy import func, text
from model import LineSegment,AiracData, Route,ConvLineData,nonConvLineData, session
from sqlalchemy.exc import IntegrityError
import requests
from bs4 import BeautifulSoup
import re
from shapely import wkt
from urllib.parse import urljoin, urlparse
import time


from url_extraction import (
    find_eaip_url,
    fetch_and_parse_frameset,
    fetch_and_parse_navigation_frame,
    search_and_print_enr_links
)

# Print the name of the file that is currently running
print(f"Running script: {os.path.basename(__file__)}")
# Function to get the active process_id from AiracData table
def get_active_process_id():
    # Query the AiracData table for the most recent active record
    active_record = session.query(AiracData).filter(AiracData.status == True).order_by(AiracData.created_At.desc()).first()
    if active_record:
        return active_record.id  # Assuming process_name is the desired process_id
    else:
        print("No active AIRAC record found.")
        return None
# Function to extract and process routes from URLs
def process_routes(urls,type):
            process_id = get_active_process_id()
            for url in urls:
                # Send an HTTP GET request to the URL
                response = requests.get(url, verify=False)
        
                # Check if the request was successful (status code 200)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.content, 'html.parser')
        
                    # Get the Title
                    div_element = soup.find('h3', class_="Title")
                    data1 = div_element.get_text(strip=True) if div_element else "Unknown Title"
                    data = ""  # Initialize to avoid UnboundLocalError
        
                    # Find all table elements with class 'AmdtTable'
                    tables = soup.find_all('table', class_='AmdtTable')
                    for index, table in enumerate(tables):
                        rows = table.find_all('tr')
                        found_remarks = False
        
                        # Process the next table for remarks
                        if index + 1 < len(tables):
                            next_table = tables[index + 1]
                            for row in next_table.find_all('tr'):
                                cells = row.find_all('td')
                                for cell in cells:
                                    p_tag = cell.find('p', class_='Paragraph-text')
                                    if p_tag:
                                        data = cell.get_text(strip=True)
        
                        # Iterate through the rows, starting from the fourth row
                        for row in rows[3:]:
                            cells = row.find_all('td')
                            
                            # Remove unnecessary tags from the soup
                            for del_tag in soup.find_all("del"):
                                del_tag.decompose()
                            for hidden_tag in soup.find_all(class_="AmdtDeletedAIRAC"):
                                hidden_tag.decompose()
        
                            # Iterate through the cells
                            for cell in cells:
                                # Find and decompose unnecessary tags
                                for del_tag in cell.find_all("del"):
                                    del_tag.decompose()
                                for hidden_tag in cell.find_all(class_="AmdtDeletedAIRAC"):
                                    hidden_tag.decompose()
        
                                # Find <em> elements with class 'Emphasis' for route information
                                p_tag = cell.find('em', class_='Emphasis')
        
                                if p_tag:
                                    route_info = p_tag.get_text(strip=True)
        
                                    # Skip processing if route_info is None
                                    if not route_info:
                                        continue
        
                                    # Match pattern with and without RNP type
                                    match = re.match(r"([^\[\(]+)\s*(?:\[([^\]]*)\]|\(([^\)]*)\))\s*\(([^)]+)\)", route_info)
                                    if match:
                                        route_designator = match.group(1).strip()
                                        rnp_type = match.group(2) or match.group(3)
                                        rnp_type = rnp_type.strip() if rnp_type else ""
                                        start_end_point = match.group(4).strip().split("-")
                                    else:
                                        match = re.match(r"([^(]+)\((.*)\)", route_info)
                                        if match:
                                            route_designator = match.group(1).strip()
                                            start_end_point = match.group(2).strip().split("-")
                                            rnp_type = ""
        
                                    # Clean up route_designator to remove unnecessary brackets
                                    route_designator = re.sub(r'\[.*?\]', '', route_designator).strip()
        
                                    # Split the route_designator to get only the first string
                                    first_string = route_designator.split()[0]
        
                                    # Handle the start and end points if they exist
                                    if len(start_end_point) > 1:
                                        start_point = start_end_point[0].strip()
                                        end_point = start_end_point[1].strip()
                                    else:
                                        start_point = ""
                                        end_point = ""
        
                                    # Print the extracted details
                                    
                                    # Create the Route object and add to the session
                                    route = Route(
                                        airway_name=data1,
                                        route_desginator=first_string,
                                        rnp_type=rnp_type,
                                        start_point=start_point,
                                        end_point=end_point,
                                        remarks=data,
                                        type = type,
                                        process_id=process_id
                                    )
                                    session.add(route) 
                                    
                    session.commit()
   

   
    
    
def draw_line_between_coordinates(session, coord1, coord2):
    query = text(
        f"SELECT ST_MakeLine(ST_GeomFromText('POINT({coord1[1]} {coord1[0]})'), ST_GeomFromText('POINT({coord2[1]} {coord2[0]})'))"
    )
    result = session.execute(query)
    line_geometry = result.scalar()
    return line_geometry


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
    if dir_part in ["E", "W"]:
        lon_degrees = int(num_part[:3])
        lon_minutes = int(num_part[3:5])
        lon_seconds = float(num_part[5:])
        lon_dd = (lon_degrees + lon_minutes / 60 + lon_seconds / 3600) * direction[dir_part]
        return lon_dd
 
                    
def process_lines(urls):
 previous_coordinates = None  # Initialize here
 line_geometry = None  # Initialize line_geometry here as well
 process_id = get_active_process_id()
 for url in urls:
    response = requests.get(url, verify=False)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        div_element = soup.find('h3', class_="Title")
        if div_element:
            data = div_element.get_text(strip=True)
            # print(data)
            tables = soup.find_all('table', class_='AmdtTable')
            for index, table in enumerate(tables):
                rows = table.find_all('tr')
                # print(rows)
                
                for row in rows[3:]:
                    cells = row.find_all('td')
                    for del_tag in soup.find_all("del"):
                        del_tag.decompose()
                    for hidden_tag in soup.find_all(class_="AmdtDeletedAIRAC"):
                        hidden_tag.decompose()
                    p_tags = cells[1].find_all('p')
                    
                    
                        
                    
                    if len(p_tags) >= 3:
                        name_of_significant_point = p_tags[0].get_text(strip=True) + p_tags[1].get_text(strip=True)
                        # print(name_of_significant_point)
                        geom = p_tags[2].get_text(strip=True)
                        match = re.match(r"(\d+\d+)([NS])\s*(\d+\d+)([EW])", geom)
                        # print(match)
                        if match:
                            latitude_dd = conversionDMStoDD(match.group(1) + match.group(2))
                            # print(latitude_dd)
                            longitude_dd = conversionDMStoDD(match.group(3) + match.group(4))
                            # print(longitude_dd)
                            lon_lat = (latitude_dd, longitude_dd)
                            if previous_coordinates:
                                # Calculate line geometry between current and previous coordinates
                                line_geometry = draw_line_between_coordinates(session, previous_coordinates, lon_lat)
                                # print(line_geometry)
                            previous_coordinates = lon_lat
                            # print(previous_coordinates)
                            
                            # Query Route table for a route with the same start_point
                    if len(cells) >= 3:
                        track_magnetic = None
                        reverse_magnetic = None
                        radial_distance = None
                        upper_limit = None
                        lower_limit = None
                        airspace = None
                        mea = None
                        lateral_limits = None
                        direction_of_cruising_levels = None
                        third_td = cells[2].find_all('p')
                        if len(third_td) >= 2:  # Check if any <p> tags are found
                            first_p_text = third_td[0].get_text(strip=True)
                            if '/' in first_p_text:  # If '/' is found, split into track/reverse
                             radial_distance_parts = first_p_text.split('/')
                             track_magnetic = radial_distance_parts[0].strip() if len(radial_distance_parts) >= 1 else None
                             reverse_magnetic = radial_distance_parts[1].strip() if len(radial_distance_parts) >= 2 else None
                            else:
                            # If no '/' is found, both track and reverse should be None
                             track_magnetic = None
                             reverse_magnetic = None

                        # Handle radial distance
                            second_p_text = third_td[1].get_text(strip=True) if len(third_td) > 1 else None
                        
                            if second_p_text and 'NM' in second_p_text:
                             radial_distance = second_p_text.strip()  # Extract radial distance containing NM
                            elif first_p_text and 'NM' in first_p_text:
                                radial_distance = first_p_text.strip()  # In case radial distance is in the first <p> tag
                            else:
                             radial_distance = None
                        else:
                         print("Insufficient <p> tags to extract data.")
                        limit_text = cells[3].find_all('p')
                        if len(limit_text) >= 2:
                            upper_limit = limit_text[0].get_text(strip=True)
                            lower_limit = limit_text[1].get_text(strip=True)
                        if len(limit_text) >= 4:
                            airspace = limit_text[3].get_text(strip=True)
                        if len(limit_text) >= 6:
                            mea = limit_text[5].get_text(strip=True)
                        
                        # print(airspace)
                        lateral_limits = cells[4].get_text(strip=True)
                        # print(lateral_limits)
                        direction_of_cruising_levelsws = None
                        if cells[3] is  not None:
                         direction_of_cruising_levels1 = cells[5].get_text(strip=True)
                        # print(direction_of_cruising_levels1)
                        
                         direction_of_cruising_levels2 = cells[6].get_text(strip=True)
                        # print(direction_of_cruising_levels2)

                         if direction_of_cruising_levels1 == "↑" and direction_of_cruising_levels2 == "↓":
                            direction_of_cruising_levelsws = "Both"
                         elif direction_of_cruising_levels1 == "↓" and direction_of_cruising_levels2 == "↑":
                            direction_of_cruising_levelsws = "Both"
                         elif direction_of_cruising_levels1 == "↓" and direction_of_cruising_levels2 == "↓":
                            direction_of_cruising_levelsws = "Forward"
                         elif direction_of_cruising_levels1 == "↓" and direction_of_cruising_levels2 == "":
                            direction_of_cruising_levelsws = "Forward"
                         elif direction_of_cruising_levels1 == "" and direction_of_cruising_levels2 == "↓":
                            direction_of_cruising_levelsws = "Forward"
                         elif direction_of_cruising_levels1 == "↑" and direction_of_cruising_levels2 == "":
                            direction_of_cruising_levelsws = "Backward"
                         elif direction_of_cruising_levels1 == "" and direction_of_cruising_levels2 == "":
                            direction_of_cruising_levelsws = "Both"
                         elif direction_of_cruising_levels1 == "↑" and direction_of_cruising_levels2 == "↑":
                            direction_of_cruising_levelsws = "Backward"
                    
                    # Skip adding the segment if any required field is None
                        if any(field is None for field in [name_of_significant_point]):
                         continue
                        route_obj = None
                        # print(data)
                        route_obj = (
                                session.query(Route)
                                .filter_by(airway_name=data)
                                .first()
                            )
                        # print(route_obj)
                
                        if route_obj:        # If a matching route is found, use its id as Route_id
                         linesegment = LineSegment(
                                    name_of_significant_point=name_of_significant_point,
                                    route_name = data,
                                    geom=line_geometry,
                                    track_magnetic=track_magnetic,
                                    reverse_magnetic=reverse_magnetic,
                                    radial_distance=radial_distance,
                                    upper_limit=upper_limit,
                                    lower_limit=lower_limit,
                                    airspace=airspace,
                                    mea=mea,
                                    lateral_limits=lateral_limits,
                                    direction_of_cruising_levels=direction_of_cruising_levelsws,
                                    route_id=route_obj.id,
                                    process_id=process_id
                                )
                        # print(airways)
                         session.add(linesegment)
        session.commit()
        
    
def conversionDMStoDD1(coord):
    direction = {"N": 1, "S": -1, "E": 1, "W": -1}
    dir_part = coord[-1]
    num_part = coord[:-1]
    if dir_part in ["N", "S"]:
        lat_degrees = int(num_part[:2])
        lat_minutes = int(num_part[2:4])
        lat_seconds = float(num_part[4:])
        lat_dd = (lat_degrees + lat_minutes / 60 + lat_seconds / 3600) * direction[dir_part]
        return lat_dd
    if dir_part in ["E", "W"]:
        lon_degrees = int(num_part[:3])
        lon_minutes = int(num_part[3:5])
        lon_seconds = float(num_part[5:])
        lon_dd = (lon_degrees + lon_minutes / 60 + lon_seconds / 3600) * direction[dir_part]
        return lon_dd

# Initialize a list to store all significant points along with their route designator
all_significant_points = []

def process_convline(urls):
 process_id = get_active_process_id()
 for url in urls:
    response = requests.get(url, verify=False)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        div_element = soup.find('h3', class_="Title")
        if div_element:
            route_designator = div_element.get_text(strip=True)

            # Decompose unwanted tags first
            for del_tag in soup.find_all("del"):
                del_tag.decompose()
            for hidden_tag in soup.find_all(class_="AmdtDeletedAIRAC"):
                hidden_tag.decompose()

            tables = soup.find_all('table', class_='AmdtTable')
            for index, table in enumerate(tables):
                rows = table.find_all('tr')
                coordinate_pattern = r"(\d{6})([NS])\s*(\d{7})([EW])"

                for row in rows[3:]:
                    cells = row.find_all('td')
                    p_tags = cells[1].find_all('p')
    
                    if len(p_tags) >= 3:
                        name_of_significant_point = p_tags[0].get_text(strip=True) + p_tags[1].get_text(strip=True)
                        # print(name_of_significant_point, "edfrg")
        
        # Try to get coordinates from p_tags[2] first
                        geom = None
                        if len(p_tags) > 2:  # If p_tags[2] exists, check it
                            geom = p_tags[2].get_text(strip=True)
                            # Match the coordinates in p_tags[2]
                            if re.match(coordinate_pattern, geom):
                                print(f"Found valid coordinates in p_tags[2]: {geom}")
                            else:
                                geom = None  # If invalid, reset geom and move to p_tags[3]
        
                            # If no valid coordinates were found in p_tags[2], check p_tags[3]
                        if geom is None and len(p_tags) > 3:  # If p_tags[3] exists, check it
                            geom = p_tags[3].get_text(strip=True)
            # Match the coordinates in p_tags[3]
                            if re.match(coordinate_pattern, geom):
                                print(f"Found valid coordinates in p_tags[3]: {geom}")
                            else:
                                print(f"No valid coordinates found for {name_of_significant_point}")

                        if geom:
            # If valid coordinates were found, convert to Decimal Degrees
                            match = re.match(coordinate_pattern, geom)
                            latitude_dd = conversionDMStoDD1(match.group(1) + match.group(2))
                            longitude_dd = conversionDMStoDD1(match.group(3) + match.group(4))
                            point = (longitude_dd, latitude_dd)
                            print(f"Matched coordinates: {point}")
                        else:
                            # If no valid coordinates were found, set point to None
                            point = None
        
        # Append the significant point with or without valid geometry
                        all_significant_points.append((route_designator, name_of_significant_point, point))
# Insert data into LineData table in pairs
 for i in range(len(all_significant_points)-1):
    print(all_significant_points)
    current_route, start_point_name, start_point_geom = all_significant_points[i]
    next_route, end_point_name, end_point_geom = all_significant_points[i + 1]
    
    if current_route == next_route:
        # Filter for the line segment where the fields are not None or empty strings
        line_segment = session.query(LineSegment).filter(
             (LineSegment.name_of_significant_point == start_point_name) and (LineSegment.name_of_significant_point == end_point_name),
            LineSegment.route_name == current_route ,
            LineSegment.upper_limit != None,
            LineSegment.lower_limit != None,
            LineSegment.lateral_limits != None
        ).first()

        
        route = session.query(Route).filter(Route.airway_name == current_route).first()

        if line_segment and route:
            track_magnetic = line_segment.track_magnetic
            reverse_magnetic = line_segment.reverse_magnetic
            radial_distance = line_segment.radial_distance
            upper_limit = line_segment.upper_limit
            lower_limit = line_segment.lower_limit
            airspace = line_segment.airspace
            mea = line_segment.mea
            lateral_limits = line_segment.lateral_limits
            direction_of_cruising_levels = line_segment.direction_of_cruising_levels
            remarks = route.remarks if route.remarks else ""

            conv_line_data = ConvLineData(
                airway_id=current_route,
                start_point=start_point_name,
                start_point_geom=start_point_geom,
                end_point=end_point_name,
                end_point_geom=end_point_geom,
                track_magnetic=track_magnetic,
                reverse_magnetic=reverse_magnetic,
                radial_distance=radial_distance,
                upper_limit=upper_limit,
                lower_limit=lower_limit,
                airspace=airspace,
                mea=mea,
                lateral_limits=lateral_limits,
                direction_of_cruising_levels=direction_of_cruising_levels,
                type="Conv",
                remarks=remarks,
                process_id=process_id
            )
            conv_line_data.geomcolumn = func.ST_MakeLine(
                func.ST_MakePoint(start_point_geom[0], start_point_geom[1]),
                func.ST_MakePoint(end_point_geom[0], end_point_geom[1])
            )

            session.add(conv_line_data)

# Commit the transaction to save changes in the database
    session.commit()

# Initialize a list to store all significant points along with their route designator
all_significant_points1 = []


def process_nonconv(urls):
 process_id = get_active_process_id()
 for url in urls:
    response = requests.get(url, verify=False)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        div_element = soup.find('h3', class_="Title")
        if div_element:
            route_designator = div_element.get_text(strip=True)
            for del_tag in soup.find_all("del"):
                del_tag.decompose()
            for hidden_tag in soup.find_all(class_="AmdtDeletedAIRAC"):
                hidden_tag.decompose()

            tables = soup.find_all('table', class_='AmdtTable')
            for index, table in enumerate(tables):
                rows = table.find_all('tr')
                for row in rows[3:]:
                    cells = row.find_all('td')
                    p_tags = cells[1].find_all('p')
                    if len(p_tags) >= 3:
                        name_of_significant_point = p_tags[0].get_text(strip=True) + p_tags[1].get_text(strip=True)
                        geom = p_tags[2].get_text(strip=True)
                        match = re.match(r"(\d+\d+)([NS])\s*(\d+\d+)([EW])", geom)
                        if match:
                            latitude_dd = conversionDMStoDD(match.group(1) + match.group(2))
                            longitude_dd = conversionDMStoDD(match.group(3) + match.group(4))
                            point = (longitude_dd, latitude_dd)
                            all_significant_points1.append((route_designator, name_of_significant_point, point))

 # Insert data into LineData table in pairs
 for i in range(len(all_significant_points1) - 1):
    current_route, start_point_name, start_point_geom = all_significant_points1[i]
    next_route, end_point_name, end_point_geom = all_significant_points1[i + 1]

    if current_route == next_route:
        # Filter for the line segment where the fields are not None or empty strings
        line_segment = session.query(LineSegment).filter(
             (LineSegment.name_of_significant_point == start_point_name) and (LineSegment.name_of_significant_point == end_point_name),
            LineSegment.route_name == current_route ,
            LineSegment.upper_limit != None,
            LineSegment.lower_limit != None,
            LineSegment.lateral_limits != None,
        ).first()

       

        route = session.query(Route).filter(Route.airway_name == current_route).first()

        if line_segment and route:
            track_magnetic = line_segment.track_magnetic
            reverse_magnetic = line_segment.reverse_magnetic
            radial_distance = line_segment.radial_distance
            upper_limit = line_segment.upper_limit
            lower_limit = line_segment.lower_limit
            airspace = line_segment.airspace
            mea = line_segment.mea
            lateral_limits = line_segment.lateral_limits
            direction_of_cruising_levels = line_segment.direction_of_cruising_levels
            rnp_type = route.rnp_type if route.rnp_type else ""
            remarks = route.remarks if route.remarks else ""
            

            conv_line_data = nonConvLineData(
                airway_id=current_route,
                rnp_type = rnp_type,
                start_point=start_point_name,
                start_point_geom=start_point_geom,
                end_point=end_point_name,
                end_point_geom=end_point_geom,
                track_magnetic=track_magnetic,
                reverse_magnetic=reverse_magnetic,
                radial_distance=radial_distance,
                upper_limit=upper_limit,
                lower_limit=lower_limit,
                airspace=airspace,
                mea=mea,
                lateral_limits=lateral_limits,
                direction_of_cruising_levels=direction_of_cruising_levels,
                type="Non Conv",
                remarks=remarks,
                process_id=process_id
            )
            conv_line_data.geomcolumn = func.ST_MakeLine(
                func.ST_MakePoint(start_point_geom[0], start_point_geom[1]),
                func.ST_MakePoint(end_point_geom[0], end_point_geom[1])
            )

            session.add(conv_line_data)

 # Commit the transaction to save changes in the database
 session.commit()


def main():
    processed_urls_file = "processed_urls.txt"
    eaip_url = find_eaip_url()
    if eaip_url:
        base_frame_url = fetch_and_parse_frameset(eaip_url)
        if base_frame_url:
            navigation_url = fetch_and_parse_navigation_frame(base_frame_url)
            if navigation_url:
                enr_3_1_urls, enr_3_2_urls = search_and_print_enr_links(navigation_url, processed_urls_file)
    
                # Here you would process ENR 3.1 and 3.2 URLs
                process_routes(enr_3_1_urls,"Conv")  # Process ENR 3.1 links
                process_routes(enr_3_2_urls,"Non Conv")  # Process ENR 3.2 links
                # process_lines(enr_3_1_urls)
                # process_lines(enr_3_2_urls)
                # process_convline(enr_3_1_urls)
                # process_nonconv(enr_3_2_urls)

        
if __name__ == "__main__":
    starttime = time.time()
    main()
    endtime = time.time()
    print(f"Execution time: {endtime - starttime:.2f} seconds")
        
            
