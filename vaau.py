import camelot
import re
import os

from sqlalchemy import select

from model import AiracData, session, Waypoint, Procedure, ProcedureDescription

##################
# EXTRACTOR CODE #
##################


AIRPORT_ICAO = "VAAU"
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
    waypoint_tables = tables[1:]
    for waypoint_table in waypoint_tables:
        waypoint_df = waypoint_table.df
        waypoint_df = waypoint_df.drop(index=[0, 1])
        for _, row in waypoint_df.iterrows():
            row = list(row)
            print(row)
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
            extracted_data1 = [
                item
                for match in re.findall(
                    r"([NS])\s*([\d:.]+)\s*([EW])\s*([\d:.]+)", row[1]
                )
                for item in match
            ]
            # print(extracted_data1)
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
                    process_id = process_id
                )
            )
    coding_df = tables[0].df
    coding_df = coding_df.drop(index=[0])
    apch_data_df = coding_df.loc[:, (coding_df != "").any(axis=0)]

    procedure_name = (
        re.search(r"(RNP.+)-CODING", file_name).groups()[0].replace("-", " ")
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
    for _, row in apch_data_df.iloc[1:].iterrows():
        row = list(row)
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
                altitude_ul=row[7].strip() if is_valid_data(row[7]) else None,
                altitude_ll=row[8].strip() if is_valid_data(row[8]) else None,
                speed_limit=row[9].strip() if is_valid_data(row[9]) else None,
                dst_time=row[5].strip() if is_valid_data(row[5]) else None,
                vpa_tch=row[10].strip() if is_valid_data(row[10]) else None,
                nav_spec=row[11].strip() if is_valid_data(row[11]) else None,
                process_id = process_id
            )
            # Split and handle altitude_ll and speed_limit
            alt_speed_data = row[8].strip()
            if alt_speed_data:
                alt_speed_values = alt_speed_data.split()
                if len(alt_speed_values) == 2:
                    (
                        proc_des_obj.altitude_ll,
                        proc_des_obj.speed_limit,
                    ) = alt_speed_values

            session.add(proc_des_obj)
            if is_valid_data(data := row[3]):
                if data == "Y":
                    proc_des_obj.fly_over = True
                elif data == "N":
                    proc_des_obj.fly_over = False
            sequence_number += 1

        else:
            if not is_valid_data(row[-1].strip()):
                if not row[0] or row[0] == "['']":
                    continue
                data_parts = row[0].split("\n")
                if data_parts[0].isdigit():
                    data_parts.insert(3, "")
                    data_parts.insert(4, "")
                    data_parts.insert(5, "")
                    data_parts.insert(6, "")
                    
                    data_parts.insert(7, data_parts[-1])
                    data_parts.pop(-1)
                    data_parts.insert(-1, "")
                    split_data = data_parts[8].split(' ')
                    if len(split_data) > 1:
                         data_parts[8] = split_data[0]
                        #  print(data_parts[8])
                         data_parts[9] = (split_data[1])
                         data_parts.insert(-1, "")
                    # print(data_parts)
                elif data_parts[0].endswith("° "):
                    data_to_insert = data_parts[-2] + " \n" + data_parts[-3]
                    data_parts.insert(-1, data_to_insert)
                    data_parts.pop(-4)
                    data_parts.pop(-3)
                    data_to_insert = data_parts[0] + " " + data_parts[-1]
                    data_parts.insert(5, data_to_insert)
                    data_parts.pop(0)
                    data_parts.pop(-1)
                    data_parts.insert(5, data_parts[3])
                    data_parts.pop(3)
                    data_parts.insert(3, "")
                    data_parts.insert(6, "")
                    data_parts.insert(7, "")
                    data_parts.insert(8, "")
                    data_parts.insert(9, "")
                    split_data = data_parts[-1].split(' \n')
                    if len(split_data) > 1:
                         data_parts[8] = split_data[1]
                        #  print(data_parts[8])
                         data_parts.append(split_data[0])
                         
                         data_parts.pop(-2)  
                         data_parts.insert(-2, "")

                # print(data_parts)
                waypoint_name = data_parts[2]
                if is_valid_data(waypoint_name):
                    waypoint_obj = (
                        session.query(Waypoint)
                        .filter_by(airport_icao=AIRPORT_ICAO, name=waypoint_name)
                        .first()
                    )
                course_angle = data_parts[4].strip().replace("\n", "").replace(" ", "")
                proc_des_obj = ProcedureDescription(
                    procedure=procedure_obj,
                    sequence_number=sequence_number,
                    seq_num=data_parts[0].strip(),
                    waypoint=waypoint_obj,
                    path_descriptor=data_parts[1].strip(),
                    course_angle=course_angle,
                    turn_dir=data_parts[6].strip()
                    if is_valid_data(data_parts[6])
                    else None,
                    altitude_ul=data_parts[7].strip()
                    if is_valid_data(data_parts[7])
                    else None,
                    altitude_ll=data_parts[8].strip()
                    if is_valid_data(data_parts[8])
                    else None,
                    speed_limit=data_parts[9].strip()
                    if is_valid_data(data_parts[9])
                    else None,
                    dst_time=data_parts[5].strip()
                    if is_valid_data(data_parts[5])
                    else None,
                    vpa_tch=data_parts[10].strip()
                    if is_valid_data(data_parts[10])
                    else None,
                    nav_spec=data_parts[11].strip()
                    if is_valid_data(data_parts[11])
                    else None,
                    process_id=process_id
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

    for file_name in file_names:
        if file_name.find("CODING") > -1:
            if file_name.find("RNP") > -1:
                apch_coding_file_names.append(file_name)

    for file_name in apch_coding_file_names:
        tables = camelot.read_pdf(FOLDER_PATH + file_name, pages="all")
        rwy_dir = re.search(r"RWY-(\d+[A-Z]?)", file_name).groups()[0]
        extract_insert_apch(file_name, rwy_dir, tables)

    session.commit()
    print("Data insertion complete.")


if __name__ == "__main__":
    main()