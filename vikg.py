from model import Waypoint, Procedure, ProcedureDescription, TerminalHolding,AiracData, session
from sqlalchemy import select


##################
# EXTRACTOR CODE #
##################
import camelot
import os
import re


AIRPORT_ICAO = "VIKG"
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
    waypoints_list = []
    type_list = []
    lat_lon_list = []

    
    coding_df = tables[0].df
    coding_df = coding_df.drop(index=[0])
    apch_data_df = coding_df.loc[:, (coding_df != "").any(axis=0)]
    apch_data_df = apch_data_df[apch_data_df.iloc[:, 0] != ""]
    apch_data_df = apch_data_df.loc[
        :, apch_data_df.columns[apch_data_df.astype(bool).any()]
    ]
    empty_columns = apch_data_df.columns[apch_data_df.columns == ""]
    if not empty_columns.empty:
        for empty_column in empty_columns:
            idx = apch_data_df.columns.get_loc(empty_column)
            apch_data_df.insert(
                loc=idx, column=apch_data_df.columns[-1], value=apch_data_df.iloc[:, -1]
            )
            apch_data_df = apch_data_df.drop(columns=apch_data_df.columns[-1])

    apch_data_df.columns = range(1, len(apch_data_df.columns) + 1)

    if not coding_df.empty and len(coding_df.columns) > 10:
        for col in coding_df.columns:
            for entry in coding_df.iloc[:, col]:
                if "Waypoint Identifier" in entry or "Waypoint Identified" in entry:
                    waypoints = coding_df.iloc[:, col].tolist()
                    waypoints_list.append(waypoints)
                elif "Type" in entry:
                    type = coding_df.iloc[:, col].tolist()
                    type_list.append(type)
                elif "Latitude/Longitude \n(WGS84) \n(DD:MM:SS.SS)" in entry:
                    lat_longs = coding_df.iloc[:, col].tolist()
                    lat_lon_list.append(lat_longs)

    print_waypoint_data = False
    waypoint_list = []
    lat_long_list = []

    for waypoints in waypoints_list:
        for waypoint in waypoints:
            parts = waypoint.split("\n")
            if "Waypoint Identifier" in parts or "Waypoint Identified" in parts:
                print_waypoint_data = True
            if print_waypoint_data:
                waypoint_list.extend(parts)
        waypoint_list.pop(0)
        waypoint_list.pop(-1)
        # print(waypoint_list)

    print_type_data = False
    types_list = []
    for type in type_list:
        for typ in type:
            parts = typ.split("\n")
            if "Type" in parts:
                print_type_data = True
            if print_type_data:
                types_list.extend(parts)
        types_list.pop(0)
        types_list.pop(-1)
        # print(types_list)

    print_lat_data = False
    for lat_longs in lat_lon_list:
        for lat_long in lat_longs:
            parts = lat_long.split("\n")
            # print(parts)
            if "Latitude/Longitude " in parts:
                # print(parts)
                print_lat_data = True
            if print_lat_data:
                lat_long_list.extend(parts)

        lat_long_list = lat_long_list[3:]
        lat_long_list.pop(-1)
        # print(lat_long_list)

        for waypoint, typ, lat_longs in zip(waypoint_list, types_list, lat_long_list):
            # Check if the waypoint exists in the database
            waypoint1 = (
                session.query(Waypoint)
                .filter_by(airport_icao=AIRPORT_ICAO, name=waypoint.strip())
                .first()
            )
            lat_dir, lat_value, lng_dir, lng_value = re.findall(
                r"([NS])\s*([\d:.]+)\s*([EW])\s*([\d:.]+)", lat_longs
            )[0]
            lat = conversionDMStoDD(lat_value + lat_dir)
            lng = conversionDMStoDD(lng_value + lng_dir)
            coordinates = f"{lat} {lng}"
            # If the waypoint doesn't exist, add it to the database
            if not waypoint1:
                new_waypoint = Waypoint(
                    airport_icao=AIRPORT_ICAO,
                    name=waypoint.strip(),
                    type=typ.strip(),
                    coordinates_dd = coordinates,
                    geom=f"POINT({lng} {lat})",
                    process_id=process_id
                )
                session.add(new_waypoint)

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
        waypoint_obj = None
        if bool(row[len(row) - 1].strip()):
            # print(row)
            if is_valid_data(row[2]):
                waypoint_name = (
                    row[2].strip().strip().replace("\n", "").replace(" ", "")
                )
                waypoint_obj = (
                    session.query(Waypoint)
                    .filter_by(airport_icao=AIRPORT_ICAO, name=waypoint_name)
                    .first()
                )
            course_angle=row[4].replace("\n", "").replace(" ", "").replace(" )", ")")
            print(course_angle)
            proc_des_obj = ProcedureDescription(
                procedure=procedure_obj,
                sequence_number = sequence_number,
                seq_num=int(row[0]),
                waypoint=waypoint_obj,
                path_descriptor=row[1].strip(),
                course_angle=row[4]
                .replace("\n", "")
                .replace("  ", "")
                .replace(" )", ")"),
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
                data_parts = row[0].split(" \n")
                # print(data_parts)
            
                if data_parts[0] == "RNP"or data_parts[0].endswith("°"):
                    if data_parts[0] == "RNP":
                        data_parts[-1] = data_parts[0] + " " + data_parts[-1]
                        data_parts.pop(0)
                        data_parts.insert(0, data_parts[-2])
                        data_parts.pop(-2)
                        # print(data_parts)
                    elif data_parts[0].endswith("°"):
                        
                        
                        data_to_insert = data_parts[0] + " " + data_parts[-2]
                        data_parts.insert(5, data_to_insert)
                        data_parts.pop(0)
                        data_parts.pop(-2)
                        data_parts[-1] = data_parts[0] + " " + data_parts[-1]
                        data_parts.pop(0)
                        data_parts.insert(0, data_parts[-2])
                        data_parts.pop(-2)
                        # print(data_parts)
                        
                    waypoint_name = data_parts[2]
                    if is_valid_data(waypoint_name):
                        waypoint_obj = session.execute(
                            select(Waypoint).where(
                                Waypoint.airport_icao == AIRPORT_ICAO,
                                Waypoint.name == waypoint_name,
                            )
                        ).fetchone()[0]
                        course_angle=data_parts[4].strip().replace(" ", "")
                        print(course_angle)
                        proc_des_obj = ProcedureDescription(
                            procedure=procedure_obj,
                            sequence_number = sequence_number,
                            seq_num=data_parts[0].strip(),
                            waypoint=waypoint_obj,
                            path_descriptor=data_parts[1].strip(),
                            course_angle=data_parts[4].strip(),
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


    
            

def extract_insert_apch1(file_name, tables, rwy_dir):
# waypoint satart
    process_id = get_active_process_id()
    waypoint_tables = tables[1:]
    for waypoint_table in waypoint_tables:
        waypoint_df = waypoint_table.df
        # print("Waypoint Table:", waypoint_df)  # Add this line to print the waypoint_df
        if re.search(r"Waypoint", waypoint_df[0][0], re.I):
            waypoint_df = waypoint_df.drop(0)
        for _, row in waypoint_df.iterrows():
            row = list(row)
            row = [x for x in row if x.strip()]
            if len(row) < 2:  # length of the row list is less than 2.
                continue
            waypoint_name1 = row[0].strip()
            extracted_data1 = [
                item
                for match in re.findall(
                    r"([NS])\s*([\d:.]+)\s*([EW])\s*([\d:.]+)", row[1]
                )
                for item in match
            ]
            lat_dir1, lat_value1, lng_dir1, lng_value1 = extracted_data1
            lat1 = conversionDMStoDD(lat_value1 + lat_dir1)
            lng1 = conversionDMStoDD(lng_value1 + lng_dir1)
            coordinates = f"{lat1} {lng1}"
            # These lines query the database to check if a waypoint with the same name and airport ICAO code already exists.
            waypoint1 = (
                session.query(Waypoint)
                .filter(
                    Waypoint.airport_icao == AIRPORT_ICAO,
                    Waypoint.name == waypoint_name1,
                )
                .first()
            )
            # If not, it adds a new Waypoint object to the session with the extracted information and coordinates.
            if not waypoint1:
                session.add(
                    Waypoint(
                        airport_icao=AIRPORT_ICAO,
                        name=waypoint_name1,
                        coordinates_dd = coordinates,
                        geom=f"POINT({lng1} {lat1})",
                        process_id=process_id
                    )
                )


    #  appoer start
    coding_df = tables[0].df
    # print(coding_df)
    coding_df = coding_df.drop(0)
    # print(coding_df)
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
    for _, row in coding_df.iterrows():
        if not row[0].strip().isdigit():
          continue
        waypoint_obj = None
        if is_valid_data(row[2]):
            waypoint_obj = (
                session.query(Waypoint)
                .filter_by(airport_icao=AIRPORT_ICAO, name=row[2].strip())
                .first()
            )
        course_angle=row[4].replace("\n", "").replace(" ", "").replace(" )", ")")
        print(course_angle)
        # Create ProcedureDescription instance
        proc_des_obj = ProcedureDescription(
            procedure=procedure_obj,
            sequence_number = sequence_number,
            seq_num=int(row[0]),
            waypoint=waypoint_obj,
            path_descriptor=row[3].strip(),
            course_angle=row[4].replace("\n", "").replace("  ", "").replace(" )", ")"),
            turn_dir=row[5].strip() if is_valid_data(row[5]) else None,
            altitude_ul =row[6].strip() if is_valid_data(row[6]) else None,
            altitude_ll=row[7].strip() if is_valid_data(row[7]) else None,
            speed_limit=row[8].strip() if is_valid_data(row[8]) else None,
            dst_time=row[9].strip() if is_valid_data(row[9]) else None,
            vpa_tch=row[10].strip() if is_valid_data(row[10]) else None,
            # role_theFix =row[10].strip() if is_valid_data(row[10]) else None,
            nav_spec=row[11].strip() if is_valid_data(row[11]) else None,
            process_id=process_id
        )
        session.add(proc_des_obj)
        if is_valid_data(data := row[1]):
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
        if len(tables) == 1:
            extract_insert_apch(file_name, tables, rwy_dir)
        if len(tables) > 1:
            extract_insert_apch1(file_name, tables, rwy_dir)

    session.commit()
    print("Data insertion complete.")


if __name__ == "__main__":
    main()
