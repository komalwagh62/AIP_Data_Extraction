import camelot
import re
import os

from sqlalchemy import select

from model import AiracData, session, Waypoint, Procedure, ProcedureDescription
##################
# EXTRACTOR CODE #
##################




AIRPORT_ICAO = "VABV"
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


def extract_insert_apch(file_name, tables, rwy_dir):
    process_id = get_active_process_id()
    coding_df = tables[0].df
    coding_df = coding_df.drop(index=[0])
    apch_data_df = coding_df.loc[:, (coding_df != "").any(axis=0)]
    apch_data_df = apch_data_df[apch_data_df.iloc[:, 0] != ""]
    
    if not coding_df.empty and len(coding_df.columns) > 7:
        waypoint_list = coding_df.iloc[
            :, 3
        ].tolist()  # Extract WPT values from second-to-last column
        lat_long_list = coding_df.iloc[
            :, 8
        ].tolist()  # Extract Latitude/Longitude values from seventh column
        # Split the strings using '\n' and filter out empty parts
        waypoint_list = [
            part
            for waypoint in waypoint_list
            for part in waypoint.split("\n")
            if part.strip() != ""
        ]
        lat_long_list = [
            part
            for lat_long in lat_long_list
            for part in lat_long.split("\n")
            if part.strip() != ""
        ]
        # Remove the first index from waypoint_list
        waypoint_list.pop(0)
        # Remove the first two indices from lat_long_list
        lat_long_list = lat_long_list[2:]
        # Iterate through both lists and display the data
        for waypoint, lat_long in zip(waypoint_list, lat_long_list):
            # Check if the waypoint exists in the database
            waypoint1 = (
                session.query(Waypoint)
                .filter_by(airport_icao=AIRPORT_ICAO, name=waypoint.strip())
                .first()
            )
            lat_dir, lat_value, lng_dir, lng_value = re.findall(
                r"([NS])\s*([\d:.]+)\s*([EW])\s*([\d:.]+)", lat_long
            )[0]
            lat = conversionDMStoDD(lat_value + lat_dir)
            lng = conversionDMStoDD(lng_value + lng_dir)
            coordinates = f"{lat} {lng}"
                        
            # If the waypoint doesn't exist, add it to the database
            if not waypoint1:
                new_waypoint = Waypoint(
                    airport_icao=AIRPORT_ICAO,
                    name=waypoint.strip(),
                    coordinates_dd = coordinates,
                    geom=f"POINT({lng} {lat})",
                    process_id=process_id
                )
                session.add(new_waypoint)
                session.commit()
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
    for _, row in apch_data_df.iloc[1:].iterrows():
        
        row = list(row)
        print(row)
        waypoint_obj = None
        if bool(row[-1].strip()):
            if is_valid_data(row[3]):
                waypoint_name = (
                    row[3].strip().strip().replace("\n", "").replace(" ", "")
                )
                waypoint_obj = (
                    session.query(Waypoint)
                    .filter_by(airport_icao=AIRPORT_ICAO, name=waypoint_name)
                    .first()
                )
                # print(f"Waypoint name: {waypoint_name}, Waypoint object: {waypoint_obj}")
            course_angle = row[5].replace("\n", "").replace("  ", " ").replace(" )", ")").replace(" N/A", "")
            angles = course_angle.split()

            # Check if we have exactly two angle values
            if len(angles) == 2:
                course_angle = f"{angles[0]}({angles[1]})"
                print(course_angle)
                
            proc_des_obj = ProcedureDescription(
                procedure=procedure_obj,
                sequence_number=sequence_number,
                seq_num=int(row[0]),
                waypoint=waypoint_obj,
                path_descriptor=row[1].strip(),
                course_angle=course_angle,
                turn_dir=row[8].strip() if is_valid_data(row[8]) else None,
                altitude_ll=row[9].strip() if is_valid_data(row[9]) else None,
                speed_limit=row[10].strip() if is_valid_data(row[10]) else None,
                dst_time=row[6].strip() if is_valid_data(row[6]) else None,
                vpa_tch=row[11].strip() if is_valid_data(row[11]) else None,
                nav_spec=row[12].strip() if is_valid_data(row[12]) else None,
                process_id=process_id
            )
            session.add(proc_des_obj)
            if is_valid_data(data := row[4]):
                if data == "Y":
                    proc_des_obj.fly_over = True
                elif data == "N":
                    proc_des_obj.fly_over = False
            sequence_number += 1

        else:
            data_parts = row[0].split(" \n")
            # print(data_parts)
            if data_parts[0] == "RNP" or data_parts[0].endswith("°"):
                if data_parts[0] == "RNP":
                    data_parts[-1] = data_parts[0] +" \n"+ data_parts[-1]
                    data_parts.pop(0)
                    data_parts.insert(0, data_parts[-2])
                    data_parts.pop(-2)
                    # print(data_parts)
                elif data_parts[0].endswith("°"):
                    data_parts[-1] = data_parts[1] +" \n"+ data_parts[-1]
                    data_parts.pop(1)
                    data_parts.insert(0, data_parts[-3])
                    data_parts.pop(-3)
                    data_to_insert = data_parts[1] +" "+ data_parts[10]
                    data_parts.insert(5, data_to_insert)
                    data_parts.pop(1)
                    data_parts.pop(-2)
                    # print(data_parts)

                waypoint_name = data_parts[2]
                waypoint_obj = (
                    session.query(Waypoint)
                    .filter_by(airport_icao=AIRPORT_ICAO, name=waypoint_name)
                    .first()
                )
                course_angle = data_parts[4].replace("\n", "").replace("  ", " ").replace(" )", ")").replace(" N/A", "")
                angles = course_angle.split()

                # Check if we have exactly two angle values
                if len(angles) == 2:
                    course_angle = f"{angles[0]}({angles[1]})"
                    print(course_angle)
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
                    altitude_ll=data_parts[7].strip()
                    if is_valid_data(data_parts[7])
                    else None,
                    speed_limit=data_parts[8].strip()
                    if is_valid_data(data_parts[8])
                    else None,
                    dst_time=data_parts[5].strip()
                    if is_valid_data(data_parts[5])
                    else None,
                    vpa_tch=data_parts[9].strip()
                    if is_valid_data(data_parts[9])
                    else None,
                    nav_spec=data_parts[10].strip()
                    if is_valid_data(data_parts[10])
                    else None,
                    process_id=process_id
                )
                session.add(proc_des_obj)
                if is_valid_data(data := row[3]):
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
        extract_insert_apch(file_name, tables, rwy_dir)

    # Commit the changes to the database
    session.commit()
    # print("Waypoints added successfully to the database.")


if __name__ == "__main__":
    main()