import camelot
import re
import os

from sqlalchemy import select

from model import session, Waypoint, Procedure, ProcedureDescription
##################
# EXTRACTOR CODE #
##################

import pandas as pd


AIRPORT_ICAO = "VOMM"
FOLDER_PATH = f"./{AIRPORT_ICAO}/"


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


def extract_insert_apch1(file_name, tables, rwy_dir):
    waypoints_list = []
    lat_lon_list = []

    coding_df = tables[0].df
    coding_df = coding_df.drop(index=[0])
    
    apch_data_df = coding_df.loc[:, (coding_df != "").any(axis=0)]

    if not coding_df.empty and len(coding_df.columns) > 10:
        for col in coding_df.columns:
            for entry in coding_df.iloc[:, col]:
                if "Waypoint Identifier" in entry:
                    waypoints = coding_df.iloc[:, col].tolist()
                    waypoints_list.append(waypoints)
                elif "Latitude/Longitude (WGS84)" in entry:
                    lat_longs = coding_df.iloc[:, col].tolist()
                    lat_lon_list.append(lat_longs)

    
    print_waypoint_data = False
    waypoint_list = []
    for waypoints in waypoints_list:
        for waypoint in waypoints:
            parts = waypoint.split("\n")
            if "Waypoint Identifier" in parts:
                print_waypoint_data = True
            if print_waypoint_data:
                waypoint_list.extend(parts)
        waypoint_list.pop(0)
        # print(waypoint_list)
        lat_long_list = [
        part
        for lat_longs in lat_lon_list
        for lat_long in lat_longs
        for part in lat_long.split("\n")
        if part.strip() != ""
    ]
        lat_long_list = lat_long_list[2:]
        
        for waypoint, lat_longs in zip(waypoint_list, lat_long_list):
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
                    coordinates_dd = coordinates,
                    geom=f"POINT({lng} {lat})",
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
    )
    session.add(procedure_obj)

    # header_row = apch_data_df.iloc[0].tolist()
    # turn_col_index = -1
    # if "Turn" in header_row:
    #     course_col_index = header_row.index("Course \n0M(0T)")
    #     turn_col_index = header_row.index("Turn")
    #     if turn_col_index == course_col_index + 1:
    if coding_df.iloc[0, 3] == "":
        for _, row in apch_data_df.iloc[1:].iterrows():
            row = list(row)
            # print(row)
            if bool(row[-1].strip()):
                # print(row)
                if is_valid_data(row[3]):
                    waypoint_obj = (
                        session.query(Waypoint)
                        .filter_by(airport_icao=AIRPORT_ICAO, name=row[3].strip())
                        .first()
                    )
                    course_angle = row[5].replace("\n", "").replace("  ", " ").replace(" )", ")").replace(" N/A", "")
                    angles = course_angle.split()

            # Check if we have exactly two angle values
                    if len(angles) == 2:
                        course_angle = f"{angles[0]}({angles[1]})"
                        
                    proc_des_obj = ProcedureDescription(
                        procedure=procedure_obj,
                        seq_num=row[0].strip(),
                        waypoint=waypoint_obj,
                        path_descriptor=row[1].strip(),
                        course_angle=course_angle,
                        turn_dir=row[8].strip() if is_valid_data(row[8]) else None,
                        altitude_ll=row[9].strip() if is_valid_data(row[9]) else None,
                        speed_limit=row[10].strip() if is_valid_data(row[10]) else None,
                        dst_time=row[6].strip() if is_valid_data(row[6]) else None,
                        vpa_tch=row[11].strip() if is_valid_data(row[11]) else None,
                        nav_spec=row[12].strip() if is_valid_data(row[12]) else None,
                    )
                    session.add(proc_des_obj)
                    if is_valid_data(data := row[4]):
                        if data == "Y":
                            proc_des_obj.fly_over = True
                        elif data == "N":
                            proc_des_obj.fly_over = False
            else:
                data_parts = row[0].split(" \n")
                if (
                    data_parts[0] == "IF"
                    or data_parts[0] == "RNP"
                    or data_parts[0].endswith("°")
                ):
                    if data_parts[0] == "IF":
                        data_parts.insert(0, data_parts[-1])
                        data_parts.pop(-1)
                        # print(data_parts)
                    elif data_parts[0] == "RNP":
                        data_parts[-1] = data_parts[0] + " " + data_parts[-1]
                        data_parts.pop(0)
                        data_parts.insert(0, data_parts[-2])
                        data_parts.pop(-2)
                        # print(data_parts)
                    elif data_parts[-2].endswith("°"):
                        data_parts[-1] = data_parts[1] + " " + data_parts[-1]
                        data_parts.pop(1)
                        data_parts.insert(0, data_parts[-3])
                        data_parts.pop(-3)
                        data_to_insert = data_parts[1] + " " + data_parts[-2]
                        data_parts.insert(5, data_to_insert)
                        data_parts.pop(1)
                        data_parts.pop(-2)
                        # print(data_parts)
                    else:
                        data_to_insert = data_parts[0] + " " + data_parts[-1]
                        data_parts.insert(4, data_to_insert)
                        data_parts.pop(0)
                        data_parts.pop(-1)
                        data_parts.insert(0, data_parts[-1])
                        data_parts.pop(-1)
                        # print(data_parts)

                    waypoint_name = data_parts[2]
                    if is_valid_data(waypoint_name):
                        waypoint_obj = session.execute(
                            select(Waypoint).where(
                                Waypoint.airport_icao == AIRPORT_ICAO,
                                Waypoint.name == waypoint_name,
                            )
                        ).fetchone()[0]
                        course_angle = data_parts[4].replace("\n", "").replace("  ", " ").replace(" )", ")").replace(" N/A", "")
                        angles = course_angle.split()

                        # Check if we have exactly two angle values
                        if len(angles) == 2:
                            course_angle = f"{angles[0]}({angles[1]})"
                        proc_des_obj = ProcedureDescription(
                            procedure=procedure_obj,
                            seq_num=data_parts[0].strip(),
                            waypoint=waypoint_obj,
                            path_descriptor=data_parts[1].strip(),
                            course_angle=data_parts[4].strip(),
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
                        )
                        session.add(proc_des_obj)
                        if is_valid_data(data := data_parts[3]):
                            if data == "Y":
                                proc_des_obj.fly_over = True
                            elif data == "N":
                                proc_des_obj.fly_over = False
    else:
            for _, row in apch_data_df.iloc[1:].iterrows():
                row = list(row)
                if bool(row[-1].strip()):
                    if is_valid_data(row[2]):
                        waypoint_obj = (
                            session.query(Waypoint)
                            .filter_by(airport_icao=AIRPORT_ICAO, name=row[2].strip())
                            .first()
                        )
                        proc_des_obj = ProcedureDescription(
                            procedure=procedure_obj,
                            seq_num=row[0].strip(),
                            waypoint=waypoint_obj,
                            path_descriptor=row[1].strip(),
                            course_angle=row[4]
                            .replace("\n", "")
                            .replace("  ", "")
                            .replace(" )", ")"),
                            turn_dir=row[5].strip() if is_valid_data(row[5]) else None,
                            altitude_ll=row[7].strip()
                            if is_valid_data(row[7])
                            else None,
                            speed_limit=row[8].strip()
                            if is_valid_data(row[8])
                            else None,
                            dst_time=row[9].strip() if is_valid_data(row[9]) else None,
                            vpa_tch=row[10].strip() if is_valid_data(row[10]) else None,
                            nav_spec=row[11].strip()
                            if is_valid_data(row[11])
                            else None,
                        )
                        session.add(proc_des_obj)
                        if is_valid_data(data := row[3]):
                            if data == "Y":
                                proc_des_obj.fly_over = True
                            elif data == "N":
                                proc_des_obj.fly_over = False
                else:
                    data_parts = row[0].split(" \n")
                    if data_parts[0] == "RNP" or data_parts[0].endswith("°"):
                        if data_parts[0] == "RNP":
                            data_parts[-1] = data_parts[0] + " \n" + data_parts[-1]
                            data_parts.pop(0)
                            data_parts.insert(0, data_parts[-2])
                            data_parts.pop(-2)
                            # print(data_parts)
                        elif data_parts[0].endswith("°"):
                            data_parts[-1] = data_parts[1] + " \n" + data_parts[-1]
                            data_parts.pop(1)
                            data_parts.insert(0, data_parts[-3])
                            data_parts.pop(-3)
                            data_to_insert = data_parts[1] + " " + data_parts[10]
                            data_parts.insert(5, data_to_insert)
                            data_parts.pop(1)
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
                            proc_des_obj = ProcedureDescription(
                                procedure=procedure_obj,
                                seq_num=data_parts[0].strip(),
                                waypoint=waypoint_obj,
                                path_descriptor=data_parts[1].strip(),
                                course_angle=data_parts[4].strip(),
                                turn_dir=data_parts[5].strip()
                                if is_valid_data(data_parts[5])
                                else None,
                                altitude_ll=data_parts[6].strip()
                                if is_valid_data(data_parts[6])
                                else None,
                                speed_limit=data_parts[7].strip()
                                if is_valid_data(data_parts[7])
                                else None,
                                dst_time=data_parts[8].strip()
                                if is_valid_data(data_parts[8])
                                else None,
                                vpa_tch=data_parts[9].strip()
                                if is_valid_data(data_parts[9])
                                else None,
                                nav_spec=data_parts[10].strip()
                                if is_valid_data(data_parts[10])
                                else None,
                            )
                            session.add(proc_des_obj)
                            if is_valid_data(data := data_parts[3]):
                                if data == "Y":
                                    proc_des_obj.fly_over = True
                                elif data == "N":
                                    proc_des_obj.fly_over = False


def main():
    file_names = os.listdir(FOLDER_PATH)
    waypoint_file_names = []
    apch_coding_file_names = []
    for file_name in file_names:
        if file_name.find("CODING") > -1:
            if file_name.find("RNP") > -1:
                waypoint_file_names.append(file_name)
                apch_coding_file_names.append(file_name)

    for file_name in apch_coding_file_names:
        tables = camelot.read_pdf(FOLDER_PATH + file_name, pages="all")
        rwy_dir = re.search(r"RWY-(\d+[A-Z]?)", file_name).groups()[0]
        extract_insert_apch1(file_name, tables, rwy_dir)
    # Commit the changes to the database
    session.commit()
    # print("Waypoints added successfully to the database.")


if __name__ == "__main__":
    main()
