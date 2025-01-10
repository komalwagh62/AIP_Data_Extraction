import camelot
import re
import os

from sqlalchemy import select

from model import AiracData, session, Waypoint, Procedure, ProcedureDescription,TerminalHolding

##################
# EXTRACTOR CODE #
##################

import pandas as pd


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
        lat_dd = (lat_degrees + lat_minutes / 60 + lat_seconds / 3600) * direction[
            dir_part
        ]
        return lat_dd
    # Handle longitude (E/W)
    if dir_part in ["E", "W"]:
        # Longitude degrees are up to three digits, minutes are the next two digits, and seconds are the rest
        lon_degrees = int(num_part[:3])
        lon_minutes = int(num_part[3:5])
        lon_seconds = float(num_part[5:])
        lon_dd = (lon_degrees + lon_minutes / 60 + lon_seconds / 3600) * direction[
            dir_part
        ]
        return lon_dd


def is_valid_data(data):
    if not data:
        return False
    if pd.isna(data):
        return False
    if type(data) == str and re.match(r"(\s+|\s*-\s*)$", data):
        return False
    return True

# Function to get the active process_id from AiracData table
def get_active_process_id():
    # Query the AiracData table for the most recent active record
    active_record = session.query(AiracData).filter(AiracData.status == True).order_by(AiracData.created_At.desc()).first()
    if active_record:
        return active_record.id  # Assuming process_name is the desired process_id
    else:
        print("No active AIRAC record found.")
        return None


EXCEL_FILE = r"C:\Users\LENOVO\Desktop\ANS_Register_Extraction\AIP_Data_Extraction\AIP Supp 74-2023_VIDP RNAV SID & STAR.xlsx"
AIRPORT_ICAO = "VIDP"


def process_sid_procedures(df, procedure_type):
    process_id = get_active_process_id()
    procedure_obj = None
    

    for _, row in df.iterrows():
        waypoint_obj = None
        row = list(row)
        # print(row)
        if pd.isna(row).all():
            continue
        if pd.isna(row[-3]):
            original_name = row[0]
            print(original_name)
            icao_code = None
            rwy = None
            name = re.sub(r'\s*\([^)]*\)$', '', original_name).strip()
            match = re.search(r'\(([^)]+)\)$', original_name)
            if match:
                data = match.group(1).split(' - ')
                if len(data) == 2:
                    icao_code, rwy = data
                    print(icao_code)
                    icao_code = icao_code.strip()
                    rwy = rwy.strip()
                    
            if icao_code and rwy:    
             procedure_obj = Procedure(
                airport_icao=icao_code,
                type=procedure_type,
                name=name,
                rwy_dir=rwy,
                process_id=process_id
                
             )
             session.add(procedure_obj)
        # Initialize sequence number tracker
            sequence_number = 1
        if row[-3] == "RNAV 1" or row[-3] == "RNP 1":
            # print(f"row[2]: {row[2]}")  # Inspect row[2] value
            if not pd.isna(row[2]):
                # print(f"Row: {row}")  # Inspect the entire row
                airport_icao = icao_code  # Make sure this is defined
                print(airport_icao)
                waypoint_name = row[2].strip()
                # print(f"Querying Waypoint with ICAO: {airport_icao}, Name: {waypoint_name}")
                waypoint_obj = (
            session.query(Waypoint)
            .filter_by(airport_icao=airport_icao, name=waypoint_name)
            .first()
                )
            # print(f"Waypoint found: {waypoint_obj}", "hb")
            # Ensure `row[4]` is a string before calling replace
            if isinstance(row[4], str):
                course_angle = row[4].replace("\n", "").replace("  ", "").replace(" )", ")").replace(" Mag", "").replace(" True", "").replace("True", "").replace("/", "").replace("N/A","")
                course_angle = course_angle.replace('\n', ' ').strip()
            else:
            # Convert non-string types to a string, or handle the case appropriately
                course_angle = str(row[4])

            angles = course_angle.split()

            # Check if we have exactly two angle values
            if len(angles) == 2:
                course_angle = f"{angles[0]}({angles[1]})"
            
                # print(course_angle)

            proc_des_obj = ProcedureDescription(
                procedure=procedure_obj,
                sequence_number=sequence_number,
                seq_num=(row[0]) if is_valid_data(row[0]) else '-',
                waypoint=waypoint_obj,
                path_descriptor=row[1].strip() if is_valid_data(row[1]) else None,
                course_angle=course_angle if is_valid_data(course_angle) else None,
                turn_dir=row[6] if is_valid_data(row[6]) else None,
                altitude_ul=str(row[7]) if is_valid_data(row[7]) else None,
                altitude_ll=str(row[8]) if is_valid_data(row[8]) else None,
                speed_limit=row[9] if is_valid_data(row[9]) else None,
                dst_time=row[5] if is_valid_data(row[5]) else None,
                vpa_tch=row[10] if is_valid_data(row[10]) else None,
                nav_spec=row[11] if is_valid_data(row[11]) else None,
                process_id=process_id
            )
            # print(proc_des_obj)
            session.add(proc_des_obj)

            if is_valid_data(data := row[3]):
                if data == "Y":
                    proc_des_obj.fly_over = True
                elif data == "N":
                    proc_des_obj.fly_over = False
            sequence_number += 1
# session.commit()
#

def process_star_procedures(df, procedure_type):
    process_id = get_active_process_id()
    procedure_obj = None

    for idx, row in df.iterrows():
        
        waypoint_obj = None
        course_angle = None  # Initialize course_angle to ensure it's always defined
        row = list(row)
    
        if pd.isna(row).all():
            continue
        
        if pd.isna(row[-3]):
            original_name = row[0]
            # print(original_name)
            icao_code = None
            rwy = None
            name = re.sub(r'\s*\([^)]*\)$', '', original_name).strip()
            print(name)
            match = re.search(r'\(([^)]+)\)$', original_name)
            if match:
                last_segment = match.group(1)
                data = last_segment.split(' - ')
                if len(data) == 2:
                    icao_code = data[0].strip()
                    rwy_list = data[1].split(',')  # Split runway directions
            if icao_code and rwy_list:
                for rwy in rwy_list:
                    rwy = rwy.strip()  # Clean up each runway direction
                    procedure_obj = Procedure(
                        airport_icao=icao_code,
                        type=procedure_type,
                        name=name,  # Use the cleaned name
                        rwy_dir=rwy,
                        process_id=process_id
                    )
                    session.add(procedure_obj)

            # if icao_code and rwy:
            #     procedure_obj = Procedure(
            #         airport_icao=icao_code,
            #         type=procedure_type,
            #         name=name,
            #         rwy_dir=rwy,
            #         process_id=process_id
            #     )
            #     session.add(procedure_obj)

            # Initialize sequence number tracker
            sequence_number = 1
        # print(row)
        if row[-3] == "RNAV 1":
            
            # print(row[0])
            if not pd.isna(row[2]) and row[2].strip():
                waypoint_obj = (
                    session.query(Waypoint)
                    .filter_by(airport_icao=icao_code, name=row[2].strip())
                    .first()
                )
            # Ensure `row[4]` is a string before calling replace
            
            if isinstance(row[4], str):
                course_angle = row[4].replace("\n", "").replace("  ", "").replace(" )", ")").replace(" Mag", "").replace(" True", "").replace("True", "").replace("/", "").replace("N/A","")
                course_angle = course_angle.replace('\n', ' ').strip()

            else:
                # Convert non-string types to a string, or handle the case appropriately
                course_angle = None

            if course_angle:
                angles = course_angle.split()
                # Check if we have exactly two angle values
                if len(angles) == 2:
                    course_angle = f"{angles[0]}({angles[1]})"
                
                proc_des_obj = ProcedureDescription(
            procedure=procedure_obj,
            sequence_number=sequence_number,
            seq_num=(row[0]) if is_valid_data(row[0]) else '-',
            waypoint=waypoint_obj,
            path_descriptor=row[1].strip() if is_valid_data(row[1]) else None,
            course_angle=course_angle if is_valid_data(course_angle) else None,
            turn_dir=row[6] if is_valid_data(row[6]) else None,
            altitude_ul=str(row[7]) if is_valid_data(row[7]) else None,
            altitude_ll=str(row[8]) if is_valid_data(row[8]) else None,
            speed_limit=row[9] if is_valid_data(row[9]) else None,
            dst_time=row[5] if is_valid_data(row[5]) else None,
            vpa_tch=row[10] if is_valid_data(row[10]) else None,
            nav_spec=row[11] if is_valid_data(row[11]) else None,
            process_id=process_id
        )
        # print(proc_des_obj)
                session.add(proc_des_obj)

                if is_valid_data(data := row[3]):
                 if data == "Y":
                    proc_des_obj.fly_over = True
                 elif data == "N":
                    proc_des_obj.fly_over = False

                sequence_number += 1

def process_apch_procedures(df, procedure_type):
    process_id = get_active_process_id()
    procedure_obj = None
    course_angle = None
    
    for _, row in df.iterrows():
        waypoint_obj = None
        row = list(row)
     
        if pd.isna(row).all():
            continue
        if pd.isna(row[-1]):
            original_name = row[0]
            icao_code = None
            rwy = None
            name = re.sub(r'\s*\([^)]*\)$', '', original_name).strip()
            match = re.search(r'\(([^)]+)\)$', original_name)
            if match:
                last_segment = match.group(1)
                data = last_segment.split(' - ')
                if len(data) == 2:
                    icao_code = data[0].strip()
                    rwy_list = data[1].split(',')  # Split runway directions
            if icao_code and rwy_list:
                for rwy in rwy_list:
                    rwy = rwy.strip()  # Clean up each runway direction
                    procedure_obj = Procedure(
                        airport_icao=icao_code,
                        type=procedure_type,
                        name=name,  # Use the cleaned name
                        rwy_dir=rwy,
                        process_id=process_id
                    )
                    session.add(procedure_obj)
        
                    
            
        # Initialize sequence number tracker
            sequence_number = 1
        if row[-3] == "RNP APCH":
            print(row)
            if not pd.isna(row[2]):
                print(row[2])
                # print(row[2])
                waypoint_obj = (
                    session.query(Waypoint)
                    .filter_by(airport_icao=AIRPORT_ICAO, name=row[2].strip())
                    .first()
                )
            # Ensure `row[4]` is a string before calling replace
            if isinstance(row[4], str):
                course_angle = row[4].replace("\n", "").replace("  ", "").replace(" )", ")").replace(" Mag", "").replace(" True", "").replace("True", "").replace("/", "").replace("N/A","")
                course_angle = course_angle.replace('\n', ' ').strip()
            else:
                # Convert non-string types to a string, or handle the case appropriately
                course_angle = str(row[4])

            angles = course_angle.split()

            # Check if we have exactly two angle values
            if len(angles) == 2:
                course_angle = f"{angles[0]}({angles[1]})"
                # print(course_angle)
            proc_des_obj = ProcedureDescription(
                procedure=procedure_obj,
                sequence_number = sequence_number,
                seq_num=(row[0]),
                waypoint=waypoint_obj,
                path_descriptor=row[1].strip() if is_valid_data(row[1]) else None,
                course_angle=course_angle if is_valid_data(course_angle) else None,
                turn_dir=row[6] if is_valid_data(row[6]) else None,
                altitude_ul=str(row[7]) if is_valid_data(row[7]) else None,
                altitude_ll=str(row[8]) if is_valid_data(row[8]) else None,
                speed_limit=row[9] if is_valid_data(row[9]) else None,
                dst_time=row[5] if is_valid_data(row[5]) else None,
                vpa_tch=row[10] if is_valid_data(row[10]) else None,
                nav_spec=row[11] if is_valid_data(row[11]) else None,
                process_id=process_id
            )
            session.add(proc_des_obj)

            if is_valid_data(data := row[3]):
                if data == "Y":
                    proc_des_obj.fly_over = True
                elif data == "N":
                    proc_des_obj.fly_over = False
            sequence_number += 1
    # session.commit()



def main():
    df_wpt = pd.read_excel(EXCEL_FILE, sheet_name="WPT", engine="openpyxl")


    process_id = get_active_process_id()
    for _, row in df_wpt.iterrows():
        waypoint_name = row["Waypoint"]
        coordinates = row["Coordinates"]
        year = (row["Year"])  # Convert to string
        supp_no = str(row["Supp number"])  # Convert to string
        icao_code = row["ICAO Code"]
        # Use regex to extract latitude and longitude parts
        match = re.match(r"^\s*(\d+\.\d+[NS])\s+(\d+\.\d+[EW])", coordinates)
        if match:
         lat_lon = match.groups()  # Extract matched groups (latitude, longitude)
         lat = conversionDMStoDD(lat_lon[0])  # Convert latitude DMS to DD
         lon = conversionDMStoDD(lat_lon[1])  # Convert longitude DMS to DD
        
         # Format coordinates and geometry
         coordinates_dd = f"{lat} {lon}"
         geom_point = f"POINT({lon} {lat})"
        
         # Add the waypoint data to the database session
         session.add(
            Waypoint(
                airport_icao=icao_code,
                name=waypoint_name,
                coordinates_dd=coordinates_dd,
                geom=geom_point,
                # year=year ,
                # supp_number=supp_no,
                process_id=process_id
            )
        )
    # session.commit()
        
    df_hldg = pd.read_excel(EXCEL_FILE, sheet_name="HLDG")
    for _, row in df_hldg.iterrows():
        row = list(row)
        # print(row)
        waypoint_obj = None
        waypoint_obj = (
            session.query(Waypoint)
            .filter_by(airport_icao=AIRPORT_ICAO, name=(row[1]).strip())
            .first()
        )
        course_angle = row[3].replace("\n", "").replace("  ", "").replace(" )", ")").replace(" Mag", "").replace(" True", "").replace("/", "")
        angles = course_angle.split()
        # Check if we have exactly two angle values
        if len(angles) == 2:
            course_angle = f"{angles[0]}({angles[1]})"
        term_hold_obj = TerminalHolding(
            waypoint_id=waypoint_obj.id,
            path_descriptor=row[0].strip(),
            course_angle=course_angle,
            turn_dir=row[5] if is_valid_data(row[5]) else None,
            altitude_ul=str(row[6]) if is_valid_data(row[6]) else None,
            altitude_ll=str(row[7]) if is_valid_data(row[7]) else None,
            speed_limit=row[8] if is_valid_data(row[8]) else None,
            dst_time=row[4] if is_valid_data(row[4]) else None,
            vpa_tch=row[9] if is_valid_data(row[9]) else None,
            nav_spec=row[10] if is_valid_data(row[10]) else None,
            # process_id = process_id
        )
        session.add(term_hold_obj)
        if is_valid_data(data := row[2]):
            if data == "Y":
                term_hold_obj.fly_over = True
            elif data == "N":
                term_hold_obj.fly_over = False

    df_sid = pd.read_excel(EXCEL_FILE, sheet_name="SID")
    process_sid_procedures(df_sid, "SID")

    df_star = pd.read_excel(EXCEL_FILE, sheet_name="STAR")
    process_star_procedures(df_star, "STAR")
    
    df_apch = pd.read_excel(EXCEL_FILE, sheet_name="APCH")
    process_apch_procedures(df_apch, "APCH")

    session.commit()
    


if __name__ == "__main__":
    main()
