import pandas as pd
from model import Waypoint, Procedure, ProcedureDescription, TerminalHolding, AiracData, session
from sqlalchemy import select
import camelot
import os
import re
import fitz  # PyMuPDF
import pdfplumber

AIRPORT_ICAO = "VILK"
FOLDER_PATH = f"./{AIRPORT_ICAO}/"

def conversionDMStoDD(coord):
    direction = {"N": 1, "S": -1, "E": 1, "W": -1}
    new_dir = coord[-1]
    coord = coord[:-1]
    parts = re.split(r"[:.]", coord)
    if len(parts) == 3:
        HH, MM, SS = map(float, parts)
        decimal = 0
    elif len(parts) == 4:
        HH, MM, SS, decimal = map(float, parts)
    else:
        raise ValueError("Invalid coordinate format")
    decimal_degrees = (HH + MM / 60 + (SS + decimal / 100) / 3600) * direction[new_dir]
    return decimal_degrees

def get_active_process_id():
    active_record = session.query(AiracData).filter(AiracData.status == True).order_by(AiracData.created_At.desc()).first()
    if active_record:
        return active_record.id
    else:
        print("No active AIRAC record found.")
        return None

def is_valid_data(data):
    if not data:
        return False
    if re.match(r"(\s+|\s*-\s*)$", data):
        return False
    return True


def dms_format(value, direction):
    """Converts extracted value into D:M:S format for conversionDMStoDD()"""
    degrees = value[:2] if direction in ["N", "S"] else value[:3]  # 2 digits for lat, 3 for long
    minutes = value[2:4]
    seconds = value[4:]  # Remaining is seconds (including decimal)
    return f"{degrees}:{minutes}:{seconds}{direction}"  # Format correctly


def extract_insert_apch_27(file_name, rwy_dir):
    process_id = get_active_process_id()
    tables = camelot.read_pdf(FOLDER_PATH + file_name, pages="all")
    coding_table, terminal_holding_table, waypoint_table = None, None, None

    for table in tables:
        df = table.df  
        row_count = len(df)
        

        coding_start, holding_start, waypoint_start = None, None, None

       
        for i in range(row_count):
            
            row_text = " ".join(df.iloc[i].astype(str))  # Convert entire row to a string
            
            if re.search(r"Serial \nNo.", row_text, re.IGNORECASE):
                coding_start = i + 1  # Data starts from next row
            elif re.search(r"Inbound Track", row_text, re.IGNORECASE):
                holding_start = i + 1
            elif re.search(r"Waypoint", row_text, re.IGNORECASE):
                waypoint_start = i + 1

        
        if coding_start is not None:
            coding_table = df.iloc[coding_start:12]  # Rows 2-11 (Indexing is zero-based)

        if holding_start is not None:
            terminal_holding_table = df.iloc[holding_start:16]  # Rows 14-15

        if waypoint_start is not None:
            waypoint_table = df.iloc[waypoint_start:26]  # Rows 18-25

    # Process Terminal Holding Table
    if terminal_holding_table is not None:
        for _, row in terminal_holding_table.iterrows():
            row_data = re.split(r"\n+", row[0])  # Split row content into separate parts

            if len(row_data) < 6:  # Ensure row has enough values
                print("Skipping invalid row:", row_data)
                continue

            waypoint_name = row_data[1].strip()  
            inbound_track = row_data[5].strip()  
            max_speed = row_data[2].strip() 
            altitude_limits = row_data[6].strip() if len(row_data) > 6 else None

            # Fetch waypoint object
            waypoint_obj = session.execute(
                select(Waypoint).where(
                    Waypoint.airport_icao == AIRPORT_ICAO,
                    Waypoint.name == waypoint_name,
                )
            ).fetchone()
            if waypoint_obj:
                waypoint_obj = waypoint_obj[0]

            # Process course angle
            course_angle = inbound_track.replace(" )", ")")
            formatted_course_angle = re.sub(r"(\d+\.\d+°)(\d+\.\d+°)", r"(\1)\2", course_angle)

            # Create and store terminal holding object
            term_hold_obj = TerminalHolding(
                waypoint=waypoint_obj,
                path_descriptor=row_data[0].strip(), 
                course_angle=formatted_course_angle,
                turn_dir=row_data[4].strip() , 
                altitude_ll=altitude_limits,
                speed_limit=max_speed,
                dst_time=row_data[3].strip() ,  
            )
            session.add(term_hold_obj)
            
    # Process Waypoint Table
    if waypoint_table is not None:
        for _, row in waypoint_table.iterrows():
            row = [x for x in row if x.strip()]
            if len(row) < 2:
                continue
            
            result_row = session.execute(
                select(Waypoint).where(
                    Waypoint.airport_icao == AIRPORT_ICAO,
                    Waypoint.name == row[0].strip(),
                )
            ).fetchone()
            if result_row:
                continue
            coordinate =  f"{row[2]} {row[3]}"
            
            extracted_data = re.findall(r"([\d:.]+)([NS])\s*([\d:.]+)([EW])", coordinate)

            if not extracted_data:
                continue

            extracted_data = list(extracted_data[0])
            lat_value, lat_dir, lng_value, lng_dir = extracted_data
            lat_dms = dms_format(lat_value, lat_dir)
            lng_dms = dms_format(lng_value, lng_dir)
        
            # Convert to Decimal Degrees
            lat = conversionDMStoDD(lat_dms)
            lng = conversionDMStoDD(lng_dms)
            coordinates = f"{lat} {lng}"

            session.add(
                Waypoint(
                    airport_icao=AIRPORT_ICAO,
                    name=row[0].strip(),
                    coordinates_dd=coordinates,
                    geom=f"POINT({lng} {lat})",
                    process_id=process_id
                )
            )

    # Process Coding Table
    if coding_table is not None:
    
        procedure_name = re.search(r"(RNP.+)-CODING", file_name).groups()[0].replace("-", " ")
        procedure_obj = Procedure(
            airport_icao=AIRPORT_ICAO,
            rwy_dir=rwy_dir,
            type="APCH",
            name=procedure_name,
            process_id=process_id
        )
        session.add(procedure_obj)

        sequence_number = 1
        for _, row in coding_table.iterrows():
            # print(list(row))
            waypoint_obj = None
            if is_valid_data(row[2]):
                    waypoint_name = row[2].strip().replace("\n", "").replace(" ", "")
                    waypoint_obj = session.query(Waypoint).filter_by(airport_icao=AIRPORT_ICAO, name=waypoint_name).first()

            course_angle = row[4].replace("\n", "").replace("Mag", "").replace("True", "").replace("/", " ").strip()
            angles = course_angle.split()
            if len(angles) == 2:
                    course_angle = f"{angles[0]}({angles[1]})"

            proc_des_obj = ProcedureDescription(
                    procedure=procedure_obj,
                    sequence_number=sequence_number,
                    seq_num=row[0],
                    waypoint=waypoint_obj,
                    path_descriptor=row[1].strip(),
                    course_angle=course_angle,
                    mag_var=row[5].strip() if is_valid_data(row[5]) else None, 
                    turn_dir=row[7].strip() if is_valid_data(row[7]) else None,
                    altitude_ll=row[8].strip() if is_valid_data(row[8]) else None,
                    speed_limit=row[9].strip() if is_valid_data(row[9]) else None,
                    dst_time=row[6].strip() if is_valid_data(row[6]) else None,
                    vpa_tch=row[10].strip() if is_valid_data(row[10]) else None,
                    nav_spec=row[11].strip() if is_valid_data(row[11]) else None,
                    process_id=process_id
                )
            
            if is_valid_data(row[8]):
                    altitudes = row[8].strip().split()
                    proc_des_obj.altitude_ll = "  ".join(altitudes)

            session.add(proc_des_obj)
            if is_valid_data(data := row[3]):
                    proc_des_obj.fly_over = data == "Y"

            sequence_number += 1
            
            

def extract_insert_apch_09(file_name, rwy_dir):
    process_id = get_active_process_id()
    tables = camelot.read_pdf(FOLDER_PATH + file_name, pages="all")
    table_df = tables[0].df  
    table_df = table_df.drop(index=[0])  
    
    coding_data = []
    waypoint_data = []
    
    # Classify rows into coding or waypoint data
    for _, row in table_df.iterrows():
        row_list = list(row)
        row_list = [x.strip() for x in row_list if str(x).strip()]
        
        if not row_list:
            continue 
        
        
        if "RNP \nAPCH" in row_list:  
            coding_data.append(row_list)
            
        elif re.search(r"[NS]\s*\d+:\d+:\d+\.\d+\s*[EW]\s*\d+:\d+:\d+\.\d+", " ".join(row_list)):  
            waypoint_data.append(row_list)

   
    coding_df = pd.DataFrame(coding_data)
    # print('coding_df',coding_df)
    waypoint_df = pd.DataFrame(waypoint_data)
    # print('waypoint_df',list(waypoint_df))

    final_waypoint_data = []
    for _, row in waypoint_df.iterrows():
        row = list(row)
        row = [x.strip() for x in row if x.strip()]

        if len(row) < 2:
            continue 

        # Extract waypoints & coordinates
        waypoints = re.split(r"\s*\n\s*", row[0]) 
        coordinates = re.split(r"\s*\n\s*", row[1]) 
        waypoints = waypoints[1:]
        coordinates = coordinates[2:]


        for wp, coord in zip(waypoints, coordinates):
            final_waypoint_data.append([wp.strip(), coord.strip()])
            
        print('final_waypoint_data',final_waypoint_data)
        
    # Convert to DataFrame
    waypoint_df = pd.DataFrame(final_waypoint_data, columns=["Waypoint", "Coordinates"])

    # Process the waypoint table
    for _, row in waypoint_df.iterrows():
        waypoint_name =  row.iloc[0].strip() 
        coordinate_str = row.iloc[1].strip()

        result_row = session.execute(
                select(Waypoint).where(
                    Waypoint.airport_icao == AIRPORT_ICAO,
                    Waypoint.name == row[0].strip(),
                )
            ).fetchone()
        if result_row:
                continue
        extracted_data1 = [
            item
            for match in re.findall(r"([NS])\s*([\d:.]+)\s*([EW])\s*([\d:.]+)", coordinate_str)
            for item in match
        ]
        
        if len(extracted_data1) != 4:
            continue  
        
        lat_dir1, lat_value1, lng_dir1, lng_value1 = extracted_data1
        lat1 = conversionDMStoDD(lat_value1 + lat_dir1)
        lng1 = conversionDMStoDD(lng_value1 + lng_dir1)
        coordinates = f"{lat1} {lng1}"
        
        # Insert into database
        session.add(
            Waypoint(
                airport_icao=AIRPORT_ICAO,
                name=waypoint_name,
                coordinates_dd=coordinates,
                geom=f"POINT({lng1} {lat1})",
                process_id=process_id
            )
        )

    # Process the coding table
    procedure_name = (
        re.search(r"(RNP.+)-CODING", file_name).groups()[0].replace("-", " ")
    )
   
    procedure_obj = Procedure(
        airport_icao=AIRPORT_ICAO,
        rwy_dir=rwy_dir,
        type="APCH",
        name=procedure_name,
        process_id=process_id
    )
    
    session.add(procedure_obj)

    sequence_number = 1
    for _, row in coding_df.iterrows():
        row = list(row)
        
        waypoint_obj = None
        if bool(row[-1].strip()):
            if is_valid_data(row[2]):
                waypoint_name = row[2].strip().replace("\n", "").replace(" ", "")
                waypoint_obj = session.query(Waypoint).filter_by(
                    airport_icao=AIRPORT_ICAO, name=waypoint_name
                ).first()

            course_angle = row[4].replace("\n", "").replace(" ", "")
            
            proc_des_obj = ProcedureDescription(
                procedure=procedure_obj,
                sequence_number=sequence_number,
                seq_num=row[0],
                waypoint=waypoint_obj,
                path_descriptor=row[3].strip(),
                course_angle=course_angle,
                turn_dir=row[5].strip() if is_valid_data(row[5]) else None,
                altitude_ll=row[6].strip() if is_valid_data(row[6]) else None,
                speed_limit=row[7].strip() if is_valid_data(row[7]) else None,
                dst_time=row[8].strip() if is_valid_data(row[8]) else None,
                vpa_tch=row[9].strip() if is_valid_data(row[9]) else None,
                role_of_the_fix=row[10].strip() if is_valid_data(row[10]) else None,
                nav_spec=row[11].strip() if is_valid_data(row[11]) else None,
                process_id=process_id
            )
            
            if is_valid_data(row[11]):
                nav_specs = row[11].strip().split()  # Split on spaces or other delimiters
                proc_des_obj.nav_spec = "  ".join(nav_specs)   

            session.add(proc_des_obj)

            if is_valid_data(data := row[1]):
                if data == "Y":
                    proc_des_obj.fly_over = True
                elif data == "N":
                    proc_des_obj.fly_over = False

            sequence_number += 1

   
def extract_insert_apch(file_name):
    rwy_dir = re.search(r"RWY-(\d+[A-Z]?)", file_name).groups()[0]
    if rwy_dir == "09":
        extract_insert_apch_09(file_name, rwy_dir)
        return
    elif rwy_dir == "27":
        extract_insert_apch_27(file_name, rwy_dir)
        return
                                    
                                                                            
                                                                          
def main():
    file_names = os.listdir(FOLDER_PATH)
    apch_coding_file_names = []

    for file_name in file_names:
        if "CODING" in file_name and "RNP" in file_name:  
            apch_coding_file_names.append(file_name)
    
    for file_name in apch_coding_file_names:
       extract_insert_apch(file_name)
 
    session.commit()
    print("Data insertion complete.")


                                                       
if __name__ == "__main__":
    main()
    
         
   
 
 
 
 
                                     
                                        
                                        
