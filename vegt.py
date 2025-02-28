from model import Waypoint, Procedure, ProcedureDescription, TerminalHolding,AiracData, session
from sqlalchemy import select

##################
# EXTRACTOR CODE #
##################

import camelot
import fitz  # PyMuPDF
import re
import os
import pandas as pd
import pdfplumber


AIRPORT_ICAO = "VEGT"
FOLDER_PATH = f"./{AIRPORT_ICAO}/"

# Function to get the active process_id from AiracData table
def get_active_process_id():
    # Query the AiracData table for the most recent active record
    active_record = session.query(AiracData).filter(AiracData.status == True).order_by(AiracData.created_At.desc()).first()
    if active_record:
        return active_record.id  # Assuming process_name is the desired process_id
    else:
        print("No active AIRAC record found.")
        return None

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
 

def is_valid_data(data):
    if not data:
        return False
    if re.match(r"(\s+|\s*-\s*)$", data):
        return False
    return True


def extract_insert_sid_02(file_name, rwy_dir):
    process_id = get_active_process_id()
    data_frame = camelot.read_pdf(FOLDER_PATH + file_name, pages="1")[0].df
    for _, row in data_frame.iterrows():
        row = list(row)
        if re.match(r"^[A-Z]+\s+\d+[A-Z]*$", row[0]):
            procedure_name = row[0]
            name, designator = re.match(r"^([A-Z]+)\s+(\d+[A-Z]*)$", procedure_name).groups()

            procedure_obj = Procedure(
                        airport_icao=AIRPORT_ICAO,
                        rwy_dir='02',
                        type='SID',
                        name=name,
                        designator=designator,
                        process_id=process_id
                    )
            print(f"Inserting procedure for {rwy_dir}: {procedure_obj}")
            session.add(procedure_obj)
            seq_num = 1  # Initialize sequence number
            sequence_number = 1  # Sequence number tracker
                    
                    
        elif row[-1].strip():
            waypoint_name = row[0].strip()
            waypoint_obj = (
                        session.query(Waypoint)
                        .filter_by(airport_icao=AIRPORT_ICAO, name=waypoint_name)
                        .first()
            )
            course_angle = row[3].replace("\n", "").replace("  ", "").replace(" )", ")").replace(" Mag", "").replace(" True", "")
            angles = course_angle.split()
                            # Check if we have exactly two angle values
            if len(angles) == 2:
                course_angle = f"{angles[0]}({angles[1]})"
                    
            proc_des_obj = ProcedureDescription(
                    procedure=procedure_obj,  # Use the previous procedure object
                    sequence_number = sequence_number,
                    seq_num=seq_num,
                    waypoint=waypoint_obj,
                    path_descriptor=row[2].strip(),
                    course_angle=course_angle,
                    turn_dir=row[4].strip() if is_valid_data(row[4]) else None,
                    altitude_ul=row[5].strip() if is_valid_data(row[5]) else None,
                    altitude_ll=row[6].strip() if is_valid_data(row[6]) else None,
                    speed_limit=row[7].strip() if is_valid_data(row[7]) else None,
                    dst_time=row[8].strip() if is_valid_data(row[8]) else None,
                    nav_spec=row[9].strip() if is_valid_data(row[9]) else None,
                    process_id=process_id        
                    )
            session.add(proc_des_obj)
            if is_valid_data(data := row[1]):
                if data == "Y":
                    proc_des_obj.fly_over = True
                elif data == "N":
                    proc_des_obj.fly_over = False
            seq_num += 1  # Increment sequence number for next row
            sequence_number += 1
 
def extract_insert_sid_20(file_name, rwy_dir):
    process_id = get_active_process_id()
    
    with pdfplumber.open(FOLDER_PATH + file_name) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()

            for table in tables:
                coding_df = [[str(cell).strip() if cell is not None else "" for cell in row] for row in table if any(row)]
                # print(coding_df) 
                for row in coding_df:
                    row = list(row)

                    # Count non-empty values in the row
                    non_empty_values = [value.strip() for value in row if value.strip()]
                    
                    # If the row contains only one non-empty value, treat it as a procedure name
                    if len(non_empty_values) == 1:
                        procedure_name = non_empty_values[0]
                        name, designator = re.match(r"^([A-Z]+)\s+(\d+[A-Z]*)$", procedure_name).groups()

                        # Create a new procedure object
                        procedure_obj = Procedure(
                            airport_icao=AIRPORT_ICAO,
                            rwy_dir=rwy_dir,
                            type="SID",
                            name=name,
                            designator=designator,
                            process_id=process_id
                        )
                        print(f"Inserting procedure for {rwy_dir}: {procedure_obj}")
                        session.add(procedure_obj)
                        sequence_number = 1  # Reset sequence number for new procedure

                        skip_rows = 3  # Set counter to skip the next 3 rows
                        continue  # Move to the next row

                    # Skip rows if the counter is active
                    if skip_rows > 0:
                        skip_rows -= 1
                        continue  # Skip these rows

                    if procedure_obj is None:
                        continue  # Skip rows until a procedure is found

                    waypoint_name = row[2].strip()
                    waypoint_obj = session.query(Waypoint).filter_by(airport_icao=AIRPORT_ICAO, name=waypoint_name).first()

                    # Cleaning course angle values
                    course_angle = row[4].replace("\n", "").replace("  ", "").replace(" )", ")").replace(" Mag", "").replace(" True", "").replace(" / ", "")
                    angles = course_angle.split()
                    course_angle = f"{angles[0]}({angles[1]})" if len(angles) == 2 else course_angle

                    proc_des_obj = ProcedureDescription(
                        procedure=procedure_obj,
                        sequence_number=sequence_number,
                        seq_num=row[0],
                        waypoint=waypoint_obj,
                        path_descriptor=row[1],
                        course_angle=course_angle,
                        turn_dir=row[6] if is_valid_data(row[6]) else None,
                        altitude_ul=row[7] if is_valid_data(row[7]) else None,
                        altitude_ll=row[8] if is_valid_data(row[8]) else None,
                        speed_limit=row[9] if is_valid_data(row[9]) else None,
                        dst_time=row[5] if is_valid_data(row[5]) else None,
                        nav_spec=row[10] if is_valid_data(row[10]) else None,
                        process_id=process_id
                    )
                    session.add(proc_des_obj)

                    if is_valid_data(data := row[3]):
                        proc_des_obj.fly_over = True if data == "Y" else False

                    sequence_number += 1
        
                    
def extract_insert_sid(file_name):
    rwy_dir = re.search(r"RWY-(\d+[A-Z]?)", file_name).groups()[0]
    if rwy_dir == "02":
        extract_insert_sid_02(file_name, rwy_dir)
        return
    elif rwy_dir == "20":
        extract_insert_sid_20(file_name, rwy_dir)
        return

def extract_insert_star_02(file_name, rwy_dir):
    process_id = get_active_process_id()
    data_frame = camelot.read_pdf(FOLDER_PATH + file_name, pages="1")[0].df

    for _, row in data_frame.iterrows():
        row = list(row)
        if re.match(r"^[A-Z]+\s+\d+[A-Z]*$", row[0]):
            procedure_name = row[0]
            name, designator = re.match(r"^([A-Z]+)\s+(\d+[A-Z]*)$", procedure_name).groups()

            procedure_obj = Procedure(
                    airport_icao=AIRPORT_ICAO,
                    rwy_dir=rwy_dir,
                    type='STAR',
                    name=name,
                    designator=designator,
                    process_id=process_id
                )
            print(f"Inserting procedure for {rwy_dir}: {procedure_obj}")
            session.add(procedure_obj)
            prev_procedure_obj = procedure_obj
            seq_num = 1  # Initialize sequence number
            sequence_number = 1  # Sequence number tracker
                
        elif row[-1].strip():
            waypoint_name = row[0].strip()
            waypoint_obj = (
                session.query(Waypoint)
                .filter_by(airport_icao=AIRPORT_ICAO, name=waypoint_name)
                .first()
                )
            course_angle = row[3].replace("\n", "").replace("  ", "").replace(" )", ")").replace(" Mag", "").replace(" True", "")
            angles = course_angle.split()
                        # Check if we have exactly two angle values
            if len(angles) == 2:
                    course_angle = f"{angles[0]}({angles[1]})"
            proc_des_obj = ProcedureDescription(
                    procedure=prev_procedure_obj,  # Use the previous procedure object
                    sequence_number = sequence_number,
                    seq_num=seq_num,
                    waypoint=waypoint_obj,
                    path_descriptor=row[2].strip(),
                    course_angle=course_angle,
                    turn_dir=row[4].strip() if is_valid_data(row[4]) else None,
                    altitude_ul=row[5].strip() if is_valid_data(row[5]) else None,
                    altitude_ll=row[6].strip() if is_valid_data(row[6]) else None,
                    speed_limit=row[7].strip() if is_valid_data(row[7]) else None,
                    dst_time=row[8].strip() if is_valid_data(row[8]) else None,
                    nav_spec=row[9].strip() if is_valid_data(row[9]) else None,
                    process_id=process_id
                )
            session.add(proc_des_obj)
            if is_valid_data(data := row[1]):
                if data == "Y":
                    proc_des_obj.fly_over = True
                elif data == "N":
                    proc_des_obj.fly_over = False
            seq_num += 1  # Increment sequence number for next row
            sequence_number += 1



# Function to extract STAR procedures
def extract_insert_star_20(file_name, rwy_dir):
    process_id = get_active_process_id()
    if not process_id:
        print("No active AIRAC record found.")
        return

    file_path = FOLDER_PATH + file_name

    # Extract tables using Camelot
    tables = camelot.read_pdf(file_path, pages="all", flavor="stream")

    for table_index, star_table in enumerate(tables):
        coding_df = star_table.df
        coding_df = coding_df.drop(index=[0, 1])  # Remove headers

        if coding_df.empty:
            continue

        procedure_obj = None  # Placeholder for procedure
        sequence_number = 1
        skip_rows = 0  # Counter to track rows to skip

        for _, row in coding_df.iterrows():
            row = list(row)

            # Count non-empty values in the row
            non_empty_values = [value.strip() for value in row if value.strip()]
            
            # If the row contains only one non-empty value, treat it as a procedure name
            if len(non_empty_values) == 1:
                procedure_name = non_empty_values[0]
                name, designator = re.match(r"^([A-Z]+)\s+(\d+[A-Z]*)$", procedure_name).groups()

                # Create a new procedure object
                procedure_obj = Procedure(
                    airport_icao=AIRPORT_ICAO,
                    rwy_dir=rwy_dir,
                    type="STAR",
                    name=name,
                    designator=designator,
                    process_id=process_id
                )
                print(f"Inserting procedure for {rwy_dir}: {procedure_obj}")
                session.add(procedure_obj)
                sequence_number = 1  # Reset sequence number for new procedure

                skip_rows = 4  # Set counter to skip the next 4 rows
                continue  # Move to the next row

            # Skip rows if the counter is active
            if skip_rows > 0:
                skip_rows -= 1
                continue  # Skip these rows

            if procedure_obj is None:
                continue  # Skip rows until a procedure is found

            waypoint_name = row[2].strip()
            waypoint_obj = session.query(Waypoint).filter_by(airport_icao=AIRPORT_ICAO, name=waypoint_name).first()

            # Cleaning course angle values
            course_angle = row[4].replace("\n", "").replace("  ", "").replace(" )", ")").replace(" Mag", "").replace(" True", "").replace(" / ", "")
            angles = course_angle.split()
            course_angle = f"{angles[0]}({angles[1]})" if len(angles) == 2 else course_angle

            proc_des_obj = ProcedureDescription(
                procedure=procedure_obj,
                sequence_number=sequence_number,
                seq_num=row[0].strip(),
                waypoint=waypoint_obj,
                path_descriptor=row[1].strip(),
                course_angle=course_angle,
                turn_dir=row[6].strip() if is_valid_data(row[6]) else None,
                altitude_ul=row[7].strip() if is_valid_data(row[7]) else None,
                altitude_ll=row[8].strip() if is_valid_data(row[8]) else None,
                speed_limit=row[9].strip() if is_valid_data(row[9]) else None,
                dst_time=row[5].strip() if is_valid_data(row[5]) else None,
                nav_spec=row[10].strip() if is_valid_data(row[10]) else None,
                process_id=process_id
            )
            session.add(proc_des_obj)

            # Handle fly_over column
            if is_valid_data(data := row[3]):
                proc_des_obj.fly_over = True if data == "Y" else False

            sequence_number += 1


                        


 
                    
def extract_insert_star(file_name):
    rwy_dir = re.search(r"RWY-(\d+[A-Z]?)", file_name).groups()[0]
    if rwy_dir == "02":
        extract_insert_star_02(file_name, rwy_dir)
        return
    elif rwy_dir == "20":
        extract_insert_star_20(file_name, rwy_dir)
        return

                    
def extract_insert_apch(file_name, rwy_dir, tables):
    process_id = get_active_process_id()
    waypoint_tables = tables[1:]
    for waypoint_table in waypoint_tables:
        waypoint_df = waypoint_table.df
        waypoint_df = waypoint_df.drop(index=[0])
        for _, row in waypoint_df.iterrows():
            row = list(row)
            # print(row)
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
            if row[1] and re.search(r"([NS])\s*([\d:.]+)\s*([EW])\s*([\d:.]+)", row[1]):
                extracted_data1 = [
                    item
                    for match in re.findall(r"([NS])\s*([\d:.]+)\s*([EW])\s*([\d:.]+)", row[1])
                    for item in match
                ]
                # print(f"Extracted data: {extracted_data1}")
                lat_dir1, lat_value1, lng_dir1, lng_value1 = extracted_data1
                lat1 = conversionDMStoDD(lat_value1 + lat_dir1)
                lng1 = conversionDMStoDD(lng_value1 + lng_dir1)
                coordinates = f"{lat1} {lng1}"
                session.add(
                    Waypoint(
                        airport_icao=AIRPORT_ICAO,
                        name=row[0].strip(),
                        coordinates_dd = coordinates,
                        geom=f"POINT({lng1} {lat1})",
                        process_id=process_id
                    )
                )
    coding_df = tables[0].df
    # print(coding_df)
    coding_df = coding_df.drop(index=[0,1,2])
    

    procedure_name = (
        re.search(r"(RNP.+)-TABLE", file_name).groups()[0].replace("-", " ")
    )
    procedure_obj = Procedure(
        airport_icao=AIRPORT_ICAO,
        rwy_dir=rwy_dir,
        type="APCH",
        name=procedure_name,
        process_id = process_id
    )
    session.add(procedure_obj)

    # Initialize sequence number tracker
    sequence_number = 1
    for _, row in coding_df.iterrows():
        row = list(row)
        # print(row)
        waypoint_obj = None
        if bool(row[-1].strip()):
            # print(row)
            if is_valid_data(row[2]):
                waypoint_name = row[2].strip().replace("\n", "").replace(" ", "")
                waypoint_obj = (
                    session.query(Waypoint)
                    .filter_by(airport_icao=AIRPORT_ICAO, name=waypoint_name)
                    .first()
                )
            course_angle = row[4].replace("\n", "").replace(" ", "")
            proc_des_obj = ProcedureDescription(
                procedure=procedure_obj,
                sequence_number=sequence_number,
                seq_num=row[0],
                waypoint=waypoint_obj,
                path_descriptor=row[1].strip(),
                course_angle=course_angle,
                turn_dir=row[6].strip() if is_valid_data(row[6]) else None,
                altitude_ll=row[7].strip() if is_valid_data(row[7]) else None,
                speed_limit=row[8].strip() if is_valid_data(row[8]) else None,
                dst_time=row[5].strip() if is_valid_data(row[5]) else None,
                vpa_tch=row[9].strip() if is_valid_data(row[9]) else None,
                nav_spec=row[10].strip() if is_valid_data(row[10]) else None,
                process_id = process_id
            )

            session.add(proc_des_obj)
            if is_valid_data(data := row[3]):
                if data == "Y":
                    proc_des_obj.fly_over = True
                elif data == "N":
                    proc_des_obj.fly_over = False
            sequence_number += 1    
        else:
            data_parts = row[0].split(" \n")
            if len(data_parts) > 2:  # Check if it's not an empty string
                data_parts.insert(-1, data_parts[0])
                data_parts.pop(0)
                # data_parts.pop(0)
                data_parts.insert(4, data_parts[0])
                data_parts.pop(0)
                if is_valid_data(data_parts[2]):
                 waypoint_name = data_parts[2].strip().replace("\n", "").replace(" ", "")
                 waypoint_obj = (
                    session.query(Waypoint)
                    .filter_by(airport_icao=AIRPORT_ICAO, name=waypoint_name)
                    .first()
                )
                course_angle = data_parts[4].replace("\n", "").replace(" ", "")
                proc_des_obj = ProcedureDescription(
                procedure=procedure_obj,
                sequence_number=sequence_number,
                seq_num=data_parts[0],
                waypoint=waypoint_obj,
                path_descriptor=data_parts[1].strip(),
                course_angle=course_angle,
                turn_dir=data_parts[6].strip() if is_valid_data(data_parts[6]) else None,
                altitude_ll=data_parts[7].strip() if is_valid_data(data_parts[7]) else None,
                speed_limit=data_parts[8].strip() if is_valid_data(data_parts[8]) else None,
                dst_time=data_parts[5].strip() if is_valid_data(data_parts[5]) else None,
                vpa_tch=data_parts[9].strip() if is_valid_data(data_parts[9]) else None,
                nav_spec=data_parts[10].strip() if is_valid_data(data_parts[10]) else None,
                process_id = process_id
            )

                session.add(proc_des_obj)
                if is_valid_data(data := data_parts[3]):
                    if data == "Y":
                        proc_des_obj.fly_over = True
                    elif data == "N":
                        proc_des_obj.fly_over = False
                sequence_number += 1    
                 



def main():
    file_names = os.listdir(FOLDER_PATH)
    apch_coding_file_names = []
    waypoint_file_names = []
    sid_file_names = []
    star_file_names = []
    for file_name in file_names:
        if file_name.find("WAYPOINTS") > -1:
            waypoint_file_names.append(file_name)
        elif file_name.find("TABLE") > -1:
            if file_name.find("RNP") > -1:
                apch_coding_file_names.append(file_name)
        elif file_name.find("CODING") > -1:
            if file_name.find("SID") > -1:
                sid_file_names.append(file_name)
            elif file_name.find("STAR") > -1:
                star_file_names.append(file_name)
 
    # Inserting waypoint files first because procedures have dependency on it.
    for waypoint_file_name in waypoint_file_names:
        process_id = get_active_process_id()
        # process_id = get_active_process_id()
        # print("extracting Waypoint file:", waypoint_file_name)
        df = camelot.read_pdf(FOLDER_PATH + waypoint_file_name, pages="1")[0].df
        if re.search(r"WAYPOINT", df[0][0], re.I):
            df = df.drop(0)
        for _, row in df.iterrows():
            row = list(row)
            row = [x for x in row if x.strip()]
            result_row = session.execute(
                select(Waypoint).where(
                    Waypoint.airport_icao == AIRPORT_ICAO,
                    Waypoint.name == row[0].strip(),
                )
            ).fetchone()
            if result_row:
                # Not inserting duplicate waypoints
                continue
            if row[1] and re.search(r"([NS])\s*([\d:.]+)\s*([EW])\s*([\d:.]+)", row[1]):
                extracted_data1 = [
                    item
                    for match in re.findall(r"([NS])\s*([\d:.]+)\s*([EW])\s*([\d:.]+)", row[1])
                    for item in match
                ]
                # print(f"Extracted data: {extracted_data1}")
                lat_dir1, lat_value1, lng_dir1, lng_value1 = extracted_data1
                lat1 = conversionDMStoDD(lat_value1 + lat_dir1)
                lng1 = conversionDMStoDD(lng_value1 + lng_dir1)
                coordinates = f"{lat1} {lng1}"
                session.add(
                    Waypoint(
                        airport_icao=AIRPORT_ICAO,
                        name=row[0].strip(),
                        coordinates_dd = coordinates,
                        geom=f"POINT({lng1} {lat1})",
                        process_id=process_id
                    )
                )
        
    
    for file_name in sid_file_names:     
        extract_insert_sid(file_name)
        
    for file_name in star_file_names:     
        extract_insert_star(file_name)
    

    for file_name in apch_coding_file_names:
        tables = camelot.read_pdf(FOLDER_PATH + file_name, pages="all")
        rwy_dir = re.search(r"RWY-(\d+[A-Z]?)", file_name).groups()[0]
        extract_insert_apch(file_name, rwy_dir, tables)
        
    # Commit the changes to the database
    session.commit()
    print("Data added successfully to the database.")


if __name__ == "__main__":
    main()


