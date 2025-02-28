from model import Waypoint, Procedure, ProcedureDescription, TerminalHolding, AiracData, session
from sqlalchemy import select
import camelot
import os
import re
import fitz  # PyMuPDF

AIRPORT_ICAO = "VIDP"
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


def extract_sid_data(file_name, rwy_dir, tables):
    process_id = get_active_process_id()
    
    # Iterate over all detected tables
    for _, sid_table in enumerate(tables):
        coding_df = sid_table.df
        procedure_name = coding_df.iloc[0, 0].strip().replace("-", " ")
        coding_df = coding_df.drop(index=[0, 1])
        
        # Create the Procedure object only once
        procedure_obj = Procedure(
            airport_icao=AIRPORT_ICAO,
            rwy_dir=rwy_dir,
            type="SID",
            name=procedure_name,
            process_id=process_id
        )
        session.add(procedure_obj)
        sequence_number = 1

        for _, row in coding_df.iterrows():
            row = list(row)
            waypoint_name = row[2]
            waypoint_obj = None
            if is_valid_data(waypoint_name):
                waypoint_obj = (
                    session.query(Waypoint)
                    .filter_by(airport_icao=AIRPORT_ICAO, name=waypoint_name)
                    .first()
                )
            course_angle = row[4].replace("\n", "").replace("  ", "").replace(" )", ")").replace(" Mag", "").replace(" True", "").replace(" / ", "")
            angles = course_angle.split()
            if len(angles) == 2:
                course_angle = f"{angles[0]}({angles[1]})"

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
                vpa_tch=row[10].strip() if is_valid_data(row[10]) else None,
                nav_spec=row[11].strip() if is_valid_data(row[11]) else None,
                process_id=process_id
            )
            session.add(proc_des_obj)
            if is_valid_data(data := row[3]):
                if data == "Y":
                    proc_des_obj.fly_over = True
                elif data == "N":
                    proc_des_obj.fly_over = False    
            sequence_number += 1




def extract_star_data(file_name, rwy_dir, tables):
    process_id = get_active_process_id()

    # Iterate over all detected tables
    for _, star_table in enumerate(tables):
        coding_df = star_table.df
        # Extract and split procedure names
        procedure_name = coding_df.iloc[0, 0].strip()  # First row, first column
        # print(procedure_name,g)
        if "(cid:" in procedure_name:
            print(f"Warning: Procedure name '{procedure_name}' appears corrupted. Skipping.")
            continue

        # Match base name and suffixes
        match = re.match(
            r"^([\w\s\(\)]+)-?\s*(\d+[A-Z](?:/\d+[A-Z])*)\sSTAR$",  # Updated regex
            procedure_name
        )
        if not match:
            print(f"Warning: Procedure name '{procedure_name}' does not follow the expected format. Skipping.")
            continue

        base_name, suffixes = match.groups()
        split_procedure_names = [f"{base_name.strip()}-{suffix.strip()} STAR" for suffix in suffixes.split("/")]

        # Drop irrelevant rows
        coding_df = coding_df.drop(index=[0, 1])
        
        # Iterate through each split procedure name
        for proc_name in split_procedure_names:
            # Create a Procedure object for each procedure name
            procedure_obj = Procedure(
                airport_icao=AIRPORT_ICAO,
                rwy_dir=rwy_dir,
                type="STAR",
                name=proc_name,
                process_id=process_id
            )
            session.add(procedure_obj)

            sequence_number = 1

            for _, row in coding_df.iterrows():
                row = list(row)
                waypoint_name = row[2]
                waypoint_obj = None
                if is_valid_data(waypoint_name):
                    waypoint_obj = (
                        session.query(Waypoint)
                        .filter_by(airport_icao=AIRPORT_ICAO, name=waypoint_name)
                        .first()
                    )

                # Process course angle formatting
                course_angle = row[4].replace("\n", "").replace("  ", "").replace(" )", ")").replace(" Mag", "").replace(" True", "").replace(" / ", "")
                angles = course_angle.split()
                if len(angles) == 2:
                    course_angle = f"{angles[0]}({angles[1]})"
                    
                dst_time = row[5].strip() if is_valid_data(row[5]) else None
                turn_dir = row[6].strip() if is_valid_data(row[6]) else None
                
                if (dst_time == 'L' or dst_time == 'R') and not turn_dir:
                    dst_time, turn_dir = None, dst_time  # Swap the values

                # Create ProcedureDescription object
                proc_des_obj = ProcedureDescription(
                    procedure=procedure_obj,
                    sequence_number=sequence_number,
                    seq_num=row[0].strip(),
                    waypoint=waypoint_obj,
                    path_descriptor=row[1].strip(),
                    course_angle=course_angle,
                    turn_dir=turn_dir,
                    altitude_ul=row[7].strip() if is_valid_data(row[7]) else None,
                    altitude_ll=row[8].strip() if is_valid_data(row[8]) else None,
                    speed_limit=row[9].strip() if is_valid_data(row[9]) else None,
                    dst_time=dst_time,
                    vpa_tch=row[10].strip() if is_valid_data(row[10]) else None,
                    nav_spec=row[11].strip() if is_valid_data(row[11]) else None,
                    process_id=process_id
                )
                

                alt_speed_data = row[9].strip()
                if alt_speed_data:
                    alt_speed_values = alt_speed_data.split()
                    if len(alt_speed_values) == 2:
                        proc_des_obj.altitude_ll, proc_des_obj.speed_limit = alt_speed_values
                    elif len(alt_speed_values) == 4:
                        # proc_des_obj.altitude_ll, proc_des_obj.speed_limit, proc_des_obj.vpa_tch,proc_des_obj.nav_spec = alt_speed_values
                        proc_des_obj.altitude_ll = alt_speed_values[0]
                        proc_des_obj.speed_limit = alt_speed_values[1]
                        proc_des_obj.nav_spec = " ".join(alt_speed_values[2:])
                        
                alt_speed_data = row[8].strip()
                if alt_speed_data:
                    alt_speed_values = alt_speed_data.split()
                    if len(alt_speed_values) == 2:
                        proc_des_obj.altitude_ll, proc_des_obj.speed_limit = alt_speed_values
                    elif len(alt_speed_values) == 4:
                        proc_des_obj.altitude_ll = alt_speed_values[0]
                        proc_des_obj.speed_limit = alt_speed_values[1]
                        proc_des_obj.nav_spec = " ".join(alt_speed_values[2:])
                        
                vpa_tch_nav_spec = row[10].strip()
                if vpa_tch_nav_spec:
                    vpa_tch_nav_spec_parts = vpa_tch_nav_spec.split()
                    if len(vpa_tch_nav_spec_parts) == 3:
                        proc_des_obj.speed_limit = vpa_tch_nav_spec_parts[0]
                        proc_des_obj.nav_spec = " ".join(vpa_tch_nav_spec_parts[1:])
                        proc_des_obj.vpa_tch = None
                        
                session.add(proc_des_obj)
                if is_valid_data(data := row[3]):
                    if data == "Y":
                        proc_des_obj.fly_over = True
                    elif data == "N":
                        proc_des_obj.fly_over = False
                sequence_number += 1

def main():

    file_names = os.listdir(FOLDER_PATH)
    sid_file_names = []
    star_file_names = []

    for file_name in file_names:
        if file_name.find("TABLE") > -1:
            if file_name.find("SID") > -1:
                sid_file_names.append(file_name)
            elif file_name.find("STAR") > -1:
                star_file_names.append(file_name)
            
    for file_name in sid_file_names:
        tables = camelot.read_pdf(FOLDER_PATH + file_name, pages="all")
        rwy_dir = re.search(r"RWY-(\d+[A-Z]?)", file_name).groups()[0]
        extract_sid_data(file_name, rwy_dir, tables)

    for file_name in star_file_names:
        tables = camelot.read_pdf(FOLDER_PATH + file_name, pages="all")
        rwy_dir = re.search(r"RWY-(\d+[A-Z]?)", file_name).groups()[0]
        extract_star_data(file_name, rwy_dir, tables)

    session.commit()
    print("Data insertion complete.")  
   


if __name__ == "__main__":
    main()
