from model import Waypoint, Procedure, ProcedureDescription, TerminalHolding,AiracData, session
from sqlalchemy import select
##################
# EXTRACTOR CODE #
##################
import camelot
import os
import re
import fitz


AIRPORT_ICAO = "VASU"
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
    # Extract direction (N/S/E/W)
    new_dir = coord[-1]
    coord = coord[:-1]
    # Split degrees, minutes, and seconds parts
    parts = re.split(r"[:.]", coord)
    # Handle different number of parts
    if len(parts) == 3:
        HH, MM, SS = map(int, parts)
        decimal = 0
    elif len(parts) == 4:
        HH, MM, SS, decimal = map(int, parts)
    else:
        raise ValueError("Invalid coordinate format")
    # Calculate decimal degrees
    decimal_degrees = (HH + MM / 60 + (SS + decimal / 100) / 3600) * direction[new_dir]
    return decimal_degrees


def is_valid_data(data):
    if not data:
        return False
    if re.match(r"(\s+|\s*-\s*)$", data):
        return False
    return True


def extract_insert_apch(file_name, rwy_dir, tables):
    process_id = get_active_process_id()
    
    coding_df = tables[0].df
    
    apch_data_df = coding_df.loc[:, (coding_df != "").any(axis=0)]
    header_row = apch_data_df.iloc[0].tolist()
    header_row = header_row[0].split(" \n")
    turn_col_index = -1
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
    # Initialize sequence number tracker
    sequence_number = 1
    if "Turn" in header_row:
        course_col_index = header_row.index("Course ")
        turn_col_index = header_row.index("TM")
        if turn_col_index == course_col_index + 1:
            for _, row in coding_df.iloc[1:].iterrows():
                row = list(row)
                waypoint_obj = None

                if is_valid_data(row[2]):
                    waypoint_obj = (
                        session.query(Waypoint)
                        .filter_by(airport_icao=AIRPORT_ICAO, name=row[2].strip())
                        .first()
                    )
                course_angle=row[4].replace("\n", "").replace(" ", "").replace(" )", ")")
                proc_des_obj = ProcedureDescription(
                    procedure=procedure_obj,
                    seq_num=int(row[0]),
                    sequence_number=sequence_number,
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

    else:
            for _, row in coding_df.iloc[2:].iterrows():
                row = list(row)
                # print(row)
                waypoint_obj = None
                if bool(row[-1].strip()):
                #  print(row)
                 if is_valid_data(row[2]):
                    waypoint_obj = (
                        session.query(Waypoint)
                        .filter_by(airport_icao=AIRPORT_ICAO, name=row[2].strip())
                        .first()
                    )
                 course_angle=row[4].replace("\n", "").replace(" ", "").replace(" )", ")").replace("M/","")
                 proc_des_obj = ProcedureDescription(
                    procedure=procedure_obj,
                    seq_num=(row[0]),
                    sequence_number=sequence_number,
                    waypoint=waypoint_obj,
                    path_descriptor=row[1].strip(),
                    course_angle=course_angle,
                    turn_dir=row[6].strip() if is_valid_data(row[6]) else None,
                    altitude_ll=row[7].strip() if is_valid_data(row[7]) else None,
                    speed_limit=row[8].strip() if is_valid_data(row[8]) else None,
                    dst_time=row[5].strip() if is_valid_data(row[5]) else None,
                    vpa_tch=row[9].strip() if is_valid_data(row[9]) else None,
                    nav_spec=row[10].strip() if is_valid_data(row[10]) else None,
                    process_id=process_id
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
                    if data_parts[0].endswith("M/"):
                     data_parts.insert(5, (data_parts[0] +" "+ data_parts[-2]))
                     data_parts.pop(0)
                     data_parts.pop(-1)
                     print(data_parts,"der")
                     if data_parts[-1] == '':
                         data_parts.insert(-2, (data_parts[-1]))
                         data_parts.pop(-1)
                         data_parts.pop(0)
                    
                   
                


def main():
    file_names = os.listdir(FOLDER_PATH)
    apch_coding_file_names = []
    for file_name in file_names:
        if file_name.find("CODING") > -1:
            if file_name.find("RNP") > -1:
                apch_coding_file_names.append(file_name)
    waypoint_file_names = [
        file_name
        for file_name in file_names
        if "CODING" in file_name and "RNP" in file_name
    ]
    for waypoint_file_name in waypoint_file_names:
        process_id = get_active_process_id()
        with open(FOLDER_PATH + waypoint_file_name, "rb") as f:
            pdf = fitz.open(f)
            if len(pdf) >= 1:
                df = camelot.read_pdf(FOLDER_PATH + waypoint_file_name, pages="all", flavor="stream")[
                    1
                ].df
                
                # df = df.drop(0)
                for _, row in df[1:].iterrows():
                    row = [x.strip() for x in row]
                    # print(row)
                    if len(row) > 2:
                        waypoint_type = row[1]
                        waypoint_name = row[0]
                        waypoint_coordinates = row[2]
                        # Handle the insertion into the database here
                        match = re.search(
    r"([NS])\s*([\d:.]+)\s*([EW])\s*([\d:.]+)",
    waypoint_coordinates,
)
                        if match:
                         lat_dir1, lat_value1, lng_dir1, lng_value1 = match.groups()
                         lat1 = conversionDMStoDD(lat_value1 + lat_dir1)
                         lng1 = conversionDMStoDD(lng_value1 + lng_dir1)
                         coordinates = f"{lat1} {lng1}"
                         waypoint = Waypoint(
                            airport_icao=AIRPORT_ICAO,
                            type=waypoint_type,  # Set the "type" here
                            name=waypoint_name,
                            coordinates_dd = coordinates,
                            geom=f"POINT({lng1} {lat1})",
                            process_id=process_id
                        )
                         session.add(waypoint)
                    elif len(row) == 2:
                        waypoint_name = row[0]
                        waypoint_coordinates = row[1]
                        # Handle the insertion into the database here
                        lat_dir1, lat_value1, lng_dir1, lng_value1 = re.search(
                            r"([NS])\s*([\d:.]+)\s*([EW])\s*([\d:.]+)",
                            waypoint_coordinates,
                        ).groups()
                        lat1 = conversionDMStoDD(lat_value1 + lat_dir1)
                        # print(lat1)
                        lng1 = conversionDMStoDD(lng_value1 + lng_dir1)
                        coordinates = f"{lat1} {lng1}"
                        waypoint = Waypoint(
                            airport_icao=AIRPORT_ICAO,
                            name=waypoint_name,
                            coordinates_dd = coordinates,
                            geom=f"POINT({lng1} {lat1})",
                            process_id=process_id
                        )
                        session.add(waypoint)
   

    for file_name in apch_coding_file_names:
        tables = camelot.read_pdf(FOLDER_PATH + file_name, pages="all")
        rwy_dir = re.search(r"RWY-(\d+[A-Z]?)", file_name).groups()[0]
        extract_insert_apch(file_name, rwy_dir, tables)

    session.commit()

    print("Data insertion complete.")


if __name__ == "__main__":
    main()
