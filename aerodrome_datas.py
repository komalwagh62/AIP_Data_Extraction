import re
from model import Aerodrome_Obstacle,AiracData, AirTrafficServiceAirspace, AirTrafficServicesCommunicationFacilities, ApproachAndRunwayLighting, Navaids, RunwayCharacterstics,DeclaredDistances, session
from sqlalchemy.exc import IntegrityError
import requests
from bs4 import BeautifulSoup
from shapely.geometry import Point
from shapely import wkt
from url_extraction import (
    find_eaip_url,
    fetch_and_parse_frameset,
    fetch_and_parse_navigation_frame,
    fetch_and_print_ad_data
)
import time

# Function to convert DMS to DD
def conversionDMStoDD(coord):
    direction = {"N": 1, "S": -1, "E": 1, "W": -1}
    dir_part = coord[-1]
    num_part = coord[:-1]
    if dir_part in ["N", "S"]:
        degrees = int(num_part[:2])
        minutes = int(num_part[2:4])
        seconds = float(num_part[4:])
        dd = (degrees + minutes / 60 + seconds / 3600) * direction[dir_part]
    else:
        degrees = int(num_part[:3])
        minutes = int(num_part[3:5])
        seconds = float(num_part[5:])
        dd = (degrees + minutes / 60 + seconds / 3600) * direction[dir_part]
    return dd

# Function to get the active process_id from AiracData table
def get_active_process_id():
    # Query the AiracData table for the most recent active record
    active_record = session.query(AiracData).filter(AiracData.status == True).order_by(AiracData.created_At.desc()).first()
    if active_record:
        return active_record.id  # Assuming process_name is the desired process_id
    else:
        print("No active AIRAC record found.")
        return None

def process_ad_data(urls):
 process_id = get_active_process_id()
 for url in urls:
  icao_match = re.search(r"AD 2\.1([A-Z]+)", url)
  if not icao_match:
    print(f"Could not find ICAO code in URL: {url}")
    continue

        # Use the extracted ICAO code
  AIRPORT_ICAO = icao_match.group(1)
                # Send an HTTP GET request to the URL
  response = requests.get(url, verify=False)
  if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')
    tables = soup.find_all('table', class_='AmdtTable')
    
    # Extract Navaid Data
    table1_data = []
    table2_data = []
    print_content = False
    for i, table in enumerate(tables):
        if table.find('p', class_='Undertitle') and 'AD 2.19' in table.get_text():  # here 'p' is html tag
            print_content = True

        if print_content:
            if i + 1 < len(tables):
                next_table = tables[i + 1]
                rows = next_table.find_all('tr')[2:]  # Skip the first two rows
                for row in rows:
                    cells = row.find_all('td')
                    row_data = [cell.get_text(strip=True) for cell in cells]
                    table1_data.append(row_data)

                if i + 2 < len(tables):
                    subsequent_table = tables[i + 2]
                    rows = subsequent_table.find_all('tr')[2:]  # Skip the first two rows
                    for row in rows:
                        cells = row.find_all('td')
                        row_data = [cell.get_text(strip=True) for cell in cells]
                        table2_data.append(row_data)

                    combined_data = []
                    for data1, data2 in zip(table1_data, table2_data):
                        combined_data.append(data1 + data2)
                        
                    for data in combined_data:
                        match = re.match(r"(\d+\.\d+)([NS])\s*(\d+\.\d+)([EW])", data[4])
                        if match:
                            latitude_dd = conversionDMStoDD(match.group(1) + match.group(2))
                            longitude_dd = conversionDMStoDD(match.group(3) + match.group(4))
                            coordinates_dd = f"{longitude_dd} {latitude_dd}"
                            geometry = wkt.loads(f"POINT({longitude_dd} {latitude_dd})")
                            ewkb_geometry = geometry.wkb_hex

                            navaid = Navaids(
                                airport_icao = AIRPORT_ICAO,# Example ICAO code, replace with actual airport code
                                navaid_information=data[0],
                                identification=data[1],
                                frequency_and_channel=data[2],
                                hours_of_operation=data[3],
                                coordinates_dd = coordinates_dd,
                                geom=ewkb_geometry,
                                elevation=data[5],
                                service_volume_radius=data[6],
                                remarks=data[7],
                                process_id=process_id
                            )
                            session.add(navaid)
            break

        if table.find('p', class_='Undertitle') and 'AD 2.20' in table.get_text():
            break
    
    # Extract Obstacle Data
    for table in tables:
        if table.find('p', class_='Undertitle') and 'AD 2.10' in table.get_text():
            print_content = True
        
        if print_content:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 6:
                    del_tags = cells[2].find_all("del")
                    # print(del_tags)
                    if len(del_tags) == 0:  # If there is exactly one <del> tag
                     coordinates_text = ' '.join(p_tag.get_text(strip=True) for p_tag in cells[2].find_all('p'))
                     area_affected_text = ' '.join(p_tag.get_text(strip=True) for p_tag in cells[0].find_all('p'))
                     obstacle_type = cells[1].get_text(strip=True)
                     coordinates_text = ' '.join(p_tag.get_text(strip=True) for p_tag in cells[2].find_all('p'))
                     match = re.match(r"(\d+\.\d+[NS])\s+(\d+\.\d+[EW])", coordinates_text)
                    
                    
                     lat_lon = match.groups()
                     lat = conversionDMStoDD(lat_lon[0])
                     lon = conversionDMStoDD(lat_lon[1])
                     coordinates_dd = f"{lon} {lat}"
                     geometry = wkt.loads(f"POINT({lon} {lat})")


                     ewkb_geometry = geometry.wkb_hex
                    
                     elevation = cells[3].get_text(strip=True)
                     marking_lgt = cells[4].get_text(strip=True)
                     remarks = cells[5].get_text(strip=True)
                    
                    # Create an Aerodrome_Obstacle object and add it to the session
                     aerodrome_obstacle = Aerodrome_Obstacle(
                        airport_icao=AIRPORT_ICAO,
                        area_affected=area_affected_text,
                        obstacle_type=obstacle_type,
                        coordinates_dd = coordinates_dd,
                        geom=ewkb_geometry,
                        elevation=elevation,
                        marking_lgt=marking_lgt,
                        remarks=remarks,
                        process_id=process_id
                    )
                     session.add(aerodrome_obstacle)
                    else:
                        for del_tag in soup.find_all("del"):
                         del_tag.decompose()
                         for hidden_tag in soup.find_all(class_="AmdtDeletedAIRAC"):
                          hidden_tag.decompose()
                        row_data = [cell.get_text(strip=True) for cell in cells]
                    
                   
                        match = re.match(r"(\d+\.\d+)([NS])\s*(\d+\.\d+)([EW])", row_data[2])
                        #  print(match)
                        if match:
                            latitude_dd = conversionDMStoDD(match.group(1) + match.group(2))
                            longitude_dd = conversionDMStoDD(match.group(3) + match.group(4))
                            # print("Latitude in Decimal Degrees:", latitude_dd)
                            # print("Longitude in Decimal Degrees:", longitude_dd)
                            coordinates_dd = f"{longitude_dd} {latitude_dd}"
                            geometry = wkt.loads(f"POINT({longitude_dd} {latitude_dd})")
                            # print(geometry)
                            ewkb_geometry = geometry.wkb_hex
                    
                    # print(ewkb_geometry)
                    
                    # Create an Aerodrome_Obstacle object and add it to the session
                            aerodrome_obstacle = Aerodrome_Obstacle(
                        airport_icao = AIRPORT_ICAO,
                        area_affected=row_data[0],
                        obstacle_type=row_data[1],
                        coordinates_dd = coordinates_dd,
                        geom=ewkb_geometry,
                        elevation=row_data[3],
                        marking_lgt=row_data[4],
                        remarks=row_data[5],
                        process_id=process_id
                    )
                            session.add(aerodrome_obstacle)
                    
                    
        if table.find('p', class_='Undertitle') and 'AD 2.11' in table.get_text():
            break
        
        
   

  
    
    # Extract Runway Characteristics Data
    print_content = False
    table5_data = []
    table6_data = []
    table7_data = []
    
    for i, table in enumerate(tables):
        if table.find('p', class_='Undertitle') and 'AD 2.12' in table.get_text():
            print_content = True
   
        if print_content:
            processed_data = []
            if i + 1 < len(tables):
                next_table = tables[i + 1]
                if table.find('p', class_='Undertitle') and 'AD 2.13' in table.get_text():
                    break
                rows = next_table.find_all('tr')[2:]  # Skip the first two rows
                for row in rows:
                    
                    cells = row.find_all('td')
                    
                    row_data = [cell.get_text(strip=True) for cell in cells[:5]]  # Limit to first 5 columns
                    table5_data.append(row_data)
                
                for data in table5_data:
                    designation = data[0]
                    true_bearing = data[1]
                    dimensions = data[2]
                    geo_coordinates = data[4]
                    # print(geo_coordinates)
                    if len(cells) > 4:
                        p_tags = cells[3].find_all('p')
                        
                        strength_pavement = p_tags[0].get_text(strip=True) if len(p_tags) > 0 else 'None'
                        associated_data = p_tags[1].get_text(strip=True) or p_tags[2].get_text(strip=True) if len(p_tags) > 2 else 'None'
                        print(associated_data)
                        surface_of_runway = p_tags[4].get_text(strip=True) if len(p_tags) > 4 else 'None'
                        associated_stopways = p_tags[5].get_text(strip=True) if len(p_tags) > 5 else 'None'
                    
                    match_thr = re.search(r'THR:(\d+[\.\d+]*[NS])(\d+[\.\d+]*[EW])', geo_coordinates)
                    match_rwy_end = re.search(r'RWY END:(\d+[\.\d+]*[NS])(\d+[\.\d+]*[EW])', geo_coordinates)

                    thr_lat = match_thr.group(1) if match_thr else None
                    thr_lon = match_thr.group(2) if match_thr else None
                    rwy_end_lat = match_rwy_end.group(1) if match_rwy_end else None
                    rwy_end_lon = match_rwy_end.group(2) if match_rwy_end else None

                    ewkb_geometry = None
                    if thr_lat and thr_lon:
                            thr_lat_dd = conversionDMStoDD(thr_lat)
                            thr_lon_dd = conversionDMStoDD(thr_lon)
                            coordinates_geom_threshold_dd =  f"{thr_lon_dd} {thr_lat_dd}"
                            geometry = wkt.loads(f"POINT({thr_lon_dd} {thr_lat_dd})")
                            ewkb_geometry = geometry.wkb_hex

                    ewkb_geometry1 = None
                    if rwy_end_lat and rwy_end_lon:
                            rwy_end_lat_dd = conversionDMStoDD(rwy_end_lat)
                            rwy_end_lon_dd = conversionDMStoDD(rwy_end_lon)
                            coordinates_geom_runway_end_dd = f"{rwy_end_lon_dd} {rwy_end_lat_dd}"
                            geometry = wkt.loads(f"POINT({rwy_end_lon_dd} {rwy_end_lat_dd})")
                            ewkb_geometry1 = geometry.wkb_hex
                    else:
                        ewkb_geometry1 = None
                    
                    # Use the same coordinates as threshold if end coordinates are missing
                    processed_data.append((designation, true_bearing, dimensions, strength_pavement,associated_data,surface_of_runway,associated_stopways,coordinates_geom_threshold_dd,ewkb_geometry,coordinates_geom_runway_end_dd, ewkb_geometry1))
                    
            second_table = []
            printed_rows = set()
            if i + 2 < len(tables):
                        subsequent_table = tables[i + 2]
                        rows = subsequent_table.find_all('tr')[2:]  # Skip the first two rows
                        for row in rows:
                            cells = row.find_all('td')
                            row_data = [cell.get_text(strip=True) for cell in cells[:5]]  # Limit to first 5 columns
                            table6_data.append(row_data)
                            printed_rows.add(tuple(row_data))
                    
                        for data in table6_data:
                         if len(data) >= 5:  # Check if data has at least 5 elements
                            slope_of_runway = data[1]
                            dimension_of_stopway = data[2]
                            dimension_of_clearway = data[3]
                            dimension_of_strips = data[4]
                            # print(dimension_of_strips,"gbqswdef")

                            if len(cells) > 4:
                                # print(cells)
                                p_tags = cells[0].find_all('p')
                                # print(p_tags)
                                thr_elevation = p_tags[0].get_text(strip=True) if len(p_tags) > 0 else 'None'
                                tdz_of_precision = p_tags[1].get_text(strip=True) if len(p_tags) > 1 else 'None'
                            # print(data)
                            second_table.append((thr_elevation,tdz_of_precision,slope_of_runway,dimension_of_stopway,dimension_of_clearway,dimension_of_strips))
                            # print(thr_elevation)
            third_table = []
            if i + 3 < len(tables):
                next_table = tables[i + 3]
                rows = next_table.find_all('tr')[2:]  # Skip the first two rows
                for row in rows:
                    cells = row.find_all('td')
                    row_data = [cell.get_text(strip=True) for cell in cells[:4]]  # Limit to first 4 columns
                    table7_data.append(row_data)

                for data in table7_data:
                    dimension_of_runway = data[0]
                    existence_of_obstacle = data[2]
                    remarks = data[3]

                    location = 'None'
                    description_of_arresting_system = 'None'
                    if len(cells) > 2:
                        p_tags = cells[1].find_all('p')
                        location = p_tags[0].get_text(strip=True) if len(p_tags) > 0 else 'None'
                        description_of_arresting_system = p_tags[1].get_text(strip=True) if len(p_tags) > 1 else 'None'
                    
                    third_table.append((dimension_of_runway, location, description_of_arresting_system, existence_of_obstacle, remarks))

            
            combined_data =[]            
                            
            for data1, data2,data3 in zip(processed_data,second_table,third_table):
                        combined_data.append(data1 + data2 + data3)
            for data in combined_data:
                

                print("edrtg")
                runway_char = RunwayCharacterstics(
                        airport_icao = AIRPORT_ICAO,
                        designation=data[0],
                        true_bearing=data[1],
                        dimensions_of_rwy=data[2],
                        strength_pavement=data[3] if data[3] != 'None' else None,
                        associated_data=data[4] if data[4] != 'None' else '',
                        surface_of_runway=data[5] ,
                        associated_stopways=data[6],
                        coordinates_geom_threshold_dd = data[7] if data[7] != 'None' else None,
                        geom_threshold=data[8] if data[8] != 'None' else None,
                        coordinates_geom_runway_end_dd = data[9] if data[9] != 'None' else None,
                        geom_runway_end=data[10] if data[10] != 'None' else None,
                        thr_elevation=data[11] if data[11] != 'None' else None,
                        tdz_of_precision=data[12] if data[12] != 'None' else None,
                        slope_of_runway=data[13] if data[13] != 'None' else None,
                        dimension_of_stopway=data[14] if data[14] != 'None' else None,
                        dimension_of_clearway=data[15] if data[15] != 'None' else None,
                        dimension_of_strips=data[16] if data[16] != 'None' else None,
                        dimension_of_runway=data[17] if data[17] != 'None' else None,
                        location=data[18] if data[18] != 'None' else None,
                        description_of_arresting_system=data[19],
                        existence_of_obstacle=data[20] if data[20] != 'None' else None,
                        remarks=data[21] if data[21] != 'None' else None,
                        process_id=process_id
                    )
                    
                session.add(runway_char)   
            break
      
      
    # Declared Distance      
    print_content = False  # Reset print_content flag
    for table in tables:
        if table.find('p', class_='Undertitle') and 'AD 2.13' in table.get_text():
            print_content = True

        if print_content:
            rows = table.find_all('tr')
            for row in rows[1:]:
                cells = row.find_all('td')
                if cells:
                    row_data = [cell.get_text(strip=True) for cell in cells]
                    
                    declared_distances = DeclaredDistances(
                        airport_icao = AIRPORT_ICAO,
                        rwy_designator = row_data[0],
                        tora = row_data[1],
                        toda = row_data[2],
                        asda = row_data[3],
                        lda = row_data[4],
                        remarks = row_data[5],
                        process_id=process_id
                        )
                    
                    session.add(declared_distances)
                        
        if table.find('p', class_='Undertitle') and 'AD 2.14' in table.get_text():
            break
        
    
    #APPROACH AND RUNWAY LIGHTING
    print_content = False
    table8_data = []
    table9_data = []

    for i, table in enumerate(tables):
        if table.find('p', class_='Undertitle') and 'AD 2.14' in table.get_text():
            print_content = True

        if print_content:
            first_table = []
            if i + 1 < len(tables):
                next_table = tables[i + 1]
                if next_table.find('p', class_='Undertitle') and 'AD 2.15' in next_table.get_text():
                    break
                rows = next_table.find_all('tr')[2:]  # Skip the first two rows
                for row in rows:
                    cells = row.find_all('td')
                    row_data = [cell.get_text(strip=True) for cell in cells[:5]]  # Limit to first 5 columns
                    table8_data.append(row_data)

                    # Process and print Table 8 data
                    if len(cells) > 4:
                        # print(cells)
                        runway_designation = row_data[0]
                        type_of_visual_slope_indicator = row_data[3]
                        length_of_runway_touchdown_zone_lights = row_data[4]

                        # Handle the second cell
                        cell_1_content = cells[1].decode_contents()
                        if cell_1_content.strip() != "":
                            p_tags1 = BeautifulSoup(cell_1_content, 'html.parser').find_all('p')
                            # print(p_tags1)
                            type_of_approach_lighting_system = p_tags1[0].get_text(strip=True) if len(p_tags1) > 0 else cell_1_content
                            length_of_approach_lighting_system = p_tags1[1].get_text(strip=True) if len(p_tags1) > 1 else 'None'
                            intensity_of_approach_lighting_system = p_tags1[2].get_text(strip=True) if len(p_tags1) > 2 else 'None'
                        else:
                            type_of_approach_lighting_system = 'None'
                            length_of_approach_lighting_system = 'None'
                            intensity_of_approach_lighting_system = 'None'

                        # Handle the third cell
                        cell_2_content = cells[2].decode_contents()
                        if cell_2_content.strip() != "":
                            p_tags2 = BeautifulSoup(cell_2_content, 'html.parser').find_all('p')
                            # print(p_tags2)
                            runway_threshold_lights = p_tags2[0].get_text(strip=True) if len(p_tags2) > 0 else cell_2_content
                            runway_threshold_colour = p_tags2[1].get_text(strip=True) if len(p_tags2) > 1 else 'None'
                            runway_threshold_wing_bars = p_tags2[2].get_text(strip=True) if len(p_tags2) > 2 else 'None'
                        else:
                            runway_threshold_lights = 'None'
                            runway_threshold_colour = 'None'
                            runway_threshold_wing_bars = 'None'

                        first_table.append((
                            runway_designation,
                            type_of_approach_lighting_system,
                            length_of_approach_lighting_system,
                            intensity_of_approach_lighting_system,
                            runway_threshold_lights,
                            runway_threshold_colour,
                            runway_threshold_wing_bars,
                            type_of_visual_slope_indicator,
                            length_of_runway_touchdown_zone_lights
                        ))

                # print("Table 8 Data:")
                # print(first_table)

            if i + 2 < len(tables):
                second_table =[]
                subsequent_table = tables[i + 2]
                rows = subsequent_table.find_all('tr')[2:]  # Skip the first two rows
                for row in rows:
                    cells = row.find_all('td')
                    row_data = [cell.get_text(strip=True) for cell in cells[:5]]  # Limit to first 5 columns
                    table9_data.append(row_data)

                    # Process and print Table 9 data
                    if len(cells) > 4:
                        # Handle the second cell
                        cell_1_content = cells[0].decode_contents()
                        if cell_1_content.strip() != "":
                            p_tags1 = BeautifulSoup(cell_1_content, 'html.parser').find_all('p')
                            length_of_runway_centeral_line_lights = p_tags1[0].get_text(strip=True) if len(p_tags1) > 0 else cell_1_content
                            spacing_of_runway_centeral_line_lights = p_tags1[1].get_text(strip=True) if len(p_tags1) > 1 else 'None'
                            colour_of_runway_centeral_line_lights = p_tags1[2].get_text(strip=True) if len(p_tags1) > 2 else 'None'
                            intensity_of_runway_centeral_line_lights = p_tags1[3].get_text(strip=True) if len(p_tags1) > 3 else 'None'
                        
                        # Handle the second cell
                        cell_2_content = cells[1].decode_contents()
                        if cell_2_content.strip() != "":
                            
                            p_tags2 = BeautifulSoup(cell_2_content, 'html.parser').find_all('p')
                            length_of_runway_edge_lights = p_tags2[0].get_text(strip=True) if len(p_tags2) > 0 else 'None'
                            spacing_of_runway_edge_lights = p_tags2[1].get_text(strip=True) if len(p_tags2) > 1 else 'None'
                            colour_of_runway_edge_lights = p_tags2[2].get_text(strip=True) if len(p_tags2) > 2 else 'None'
                            intensity_of_runway_edge_lights = p_tags2[3].get_text(strip=True) if len(p_tags2) > 3 else 'None'
                        
                        # Handle the third cell
                        cell_3_content = cells[2].decode_contents()
                        if cell_3_content.strip() != "":
                            p_tags3 = BeautifulSoup(cell_3_content, 'html.parser').find_all('p')
                            colour_of_runway_end_lights = p_tags3[0].get_text(strip=True) if len(p_tags3) > 0 else 'None'
                            wing_bar = p_tags3[1].get_text(strip=True) if len(p_tags3) > 1 else 'None'
                        
                        # Handle the fourth cell
                        cell_4_content = cells[3].decode_contents()
                        if cell_4_content.strip() != "":
                            p_tags4 = BeautifulSoup(cell_4_content, 'html.parser').find_all('p')
                            length_of_stopway_lights = p_tags4[0].get_text(strip=True) if len(p_tags4) > 0 else 'None'
                            colour_of_stopway_lights = p_tags4[1].get_text(strip=True) if len(p_tags4) > 1 else 'None'

                        remarks = row_data[4]
                        # print(remarks)
                        second_table.append((
                            length_of_runway_centeral_line_lights,
                            spacing_of_runway_centeral_line_lights,
                            colour_of_runway_centeral_line_lights,
                            intensity_of_runway_centeral_line_lights,
                            length_of_runway_edge_lights,
                            spacing_of_runway_edge_lights,
                            colour_of_runway_edge_lights,
                            intensity_of_runway_edge_lights,
                            colour_of_runway_end_lights,
                            wing_bar,
                            length_of_stopway_lights,
                            colour_of_stopway_lights,
                            remarks
                        ))
                        

            combined_data =[]            
                            
            for data1, data2 in zip(first_table, second_table):
                combined_data.append(data1 + data2)

            for data in combined_data:
                    approach_and_runway_lighting = ApproachAndRunwayLighting(
                        airport_icao=AIRPORT_ICAO,
                        runway_desginator=data[0],
                        type_of_approach_lighting_system=data[1],
                        length_of_approach_lighting_system=data[2],
                        intensity_of_approach_lighting_system=data[3],
                        runway_threshold_lights=data[4],
                        runway_threshold__colour=data[5],
                        runway_threshold_wing_bars=data[6],  # Set default value for None
                        type_of_visual_slope_indicator=data[7],
                        length_of_runway_touchdown_zone_lights = data[8],
                        length_of_runway_centeral_line_lights=data[9],
                        spacing_of_runway_centeral_line_lights=data[10],
                        colour_of_runway_centeral_line_lights=data[11],
                        intensity_of_runway_centeral_line_lights=data[12],
                        length_of_runway_edge_lights=data[13],
                        spacing_of_runway_edge_lights=data[14],
                        colour_of_runway_edge_lights=data[15],
                        intensity_of_runway_edge_lights=data[16],
                        colour_of_runway_end_lights=data[17],
                        wing_bar=data[18],  # Set default value for None
                        length_of_stopway_lights=data[19],
                        colour_of_stopway_lights=data[20],
                        remarks=data[21],
                        process_id=process_id
    )
                    
                    session.add(approach_and_runway_lighting)   

            # session.commit()



    #AIR TRAFFIC SERVICE AIRSPACE
    print_content = False  # Reset print_content flag
    collected_rows = []
    for table in tables:
     if table.find('p', class_='Undertitle') and 'AD 2.17' in table.get_text():
        print_content = True

     if print_content:
        rows = table.find_all('tr')
        for row in rows:
            cells = row.find_all('td')
            row_data = [cell.get_text(strip=True) for cell in cells]
            collected_rows.append(row_data)
    
    # Stop collecting rows once 'AD 2.18' section is reached
     if table.find('p', class_='Undertitle') and 'AD 2.18' in table.get_text():
        break

    # Remove the first and last row if collected_rows is not empty
    if collected_rows:
     collected_rows = collected_rows[1:-1]

    # Define headers for data mapping
    headers = [
    "Airspace designation",
    "Geographical coordinates and lateral limits",
    "Vertical limits",
    "Airspace classification",
    "Call sign",
    "Language",
    "Transition altitude",
    "Hours of applicability",
    "Remarks"
]

    # Initialize mapped_data dictionary
    mapped_data = {}

    # Extract and map data to headers
    for index, row_data in enumerate(collected_rows):
     if index == 0:
        # Extract and split data for Airspace designation and Geographical coordinates
        full_text = row_data[2]
        if ': ' in full_text:
            airspace_designation, geographical_coordinates = full_text.split(': ', 1)
            mapped_data[headers[0]] = airspace_designation.strip()
            mapped_data[headers[1]] = geographical_coordinates.strip()
     elif index == 1:
        # Extract Vertical limits
        mapped_data[headers[2]] = row_data[2].strip()
     elif index == 2:
        # Extract Airspace classification
        mapped_data[headers[3]] = row_data[2].strip()
     elif index == 3:
        # Extract and split data for Call sign and Language
        call_sign_text = row_data[2]
        if ', ' in call_sign_text:
            call_sign, language = call_sign_text.split(', ', 1)
            mapped_data[headers[4]] = call_sign.strip()
            mapped_data[headers[5]] = language.strip()
     elif index == 4:
        # Extract Transition altitude
        mapped_data[headers[6]] = row_data[2].strip()
     elif index == 5:
        # Extract Hours of applicability
        mapped_data[headers[7]] = row_data[2].strip()
     elif index == 6:
        # Extract Remarks
        mapped_data[headers[8]] = row_data[2].strip()

# Create an instance of AirTrafficServiceAirspace with mapped data
    airspace_entry = AirTrafficServiceAirspace(
    airport_icao=AIRPORT_ICAO,  # Replace with your actual airport ICAO code
    airspace_designation=mapped_data.get(headers[0], 'N/A'),
    geographical_coordinates=mapped_data.get(headers[1], 'N/A'),
    vertical_limits=mapped_data.get(headers[2], 'N/A'),
    airspace_classification=mapped_data.get(headers[3], 'N/A'),
    call_sign=mapped_data.get(headers[4], 'N/A'),
    language_of_air_traffic_service=mapped_data.get(headers[5], 'N/A'),
    transition_altitude=mapped_data.get(headers[6], 'N/A'),
    hours_of_applicability=mapped_data.get(headers[7], 'N/A'),
    remarks=mapped_data.get(headers[8], 'N/A'),
    process_id=process_id
)
    session.add(airspace_entry)
    # session.commit()
    
    
    
    
    
    # AIR TRAFFIC SERVICES COMMUNICATION FACILITIES
    
    print_content = False
    table11_data = []
    table12_data = []

    for i, table in enumerate(tables):
     if table.find('p', class_='Undertitle') and 'AD 2.18' in table.get_text():
        print_content = True

     if print_content:
        if i + 1 < len(tables):
            next_table = tables[i + 1]
            rows = next_table.find_all('tr')[2:]  # Skip the first two rows
            for row in rows:
                cells = row.find_all('td')
                row_data = [cell.get_text(strip=True) for cell in cells]
                table11_data.append(row_data)

            if i + 2 < len(tables):
                subsequent_table = tables[i + 2]
                rows = subsequent_table.find_all('tr')[2:]  # Skip the first two rows
                for row in rows:
                    cells = row.find_all('td')
                    row_data = [cell.get_text(strip=True) for cell in cells]
                    table12_data.append(row_data)

                combined_data = []
                for data1, data2 in zip(table11_data, table12_data):
                    combined_data.append(data1 + data2)

                for data in combined_data:
                    # print(data)
                    air_traffic_service_communication_facilities = AirTrafficServicesCommunicationFacilities(
                            
                            airport_icao=AIRPORT_ICAO,  # Example ICAO code, replace with actual airport code
                            service_designation = data[0],
                            call_sign = data[1],
                            channel = data[2],
                            satvoice_number = data[3],
                            logon_address = data[4],
                            hours_of_operation = data[5],
                            remarks = data[6],
                            process_id=process_id
                        )
                    session.add(air_traffic_service_communication_facilities)
                session.commit()
                    

            break
        
        
def main():
    processed_urls_file = "AD_urls.txt"
    eaip_url = find_eaip_url()
    if eaip_url:
        base_frame_url = fetch_and_parse_frameset(eaip_url)
        if base_frame_url:
            navigation_url = fetch_and_parse_navigation_frame(base_frame_url)
            if navigation_url:
                ad_urls = fetch_and_print_ad_data(navigation_url, processed_urls_file)
                # Here you would process ENR 3.1 and 3.2 URLs
                process_ad_data(ad_urls)  # Process ENR 3.1 links
                

        
if __name__ == "__main__":
    starttime = time.time()
    main()
    endtime = time.time()
    print(f"Execution time: {endtime - starttime:.2f} seconds")


    


    