from model import Waypoint, Procedure, ProcedureDescription, TerminalHolding, session
from sqlalchemy import select
##################
# EXTRACTOR CODE #
##################
import camelot
import os
import re
import pdftotext

AIRPORT_ICAO = "VASU"
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


def extract_insert_apch(file_name, rwy_dir, tables):
    waypoint_tables = tables[1:]
    for waypoint_table in waypoint_tables:
        waypoint_df = waypoint_table.df
        waypoint_df = waypoint_df.drop(index=[0])
        # print(waypoint_df)
        row = list(waypoint_df[1].unique())
        l = ["IAF", "IF", "MATF", "FAF", "LTP"]
        for _, row in waypoint_df.iterrows():
            if waypoint_df[0][1] in l:
                row = [x.strip() for x in row]
                waypoint_type = row[0]
                waypoint_name = row[1]
                waypoint_coordinates = row[2]
                result_row = session.execute(
                    select(Waypoint).where(
                        Waypoint.airport_icao == AIRPORT_ICAO,
                        Waypoint.name == waypoint_name,
                    )
                ).fetchone()
                if result_row:
                    # Not inserting duplicate waypoints
                    continue
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
                    type=waypoint_type,
                    coordinates_dd = coordinates,
                    geom=f"POINT({lng1} {lat1})",
                )
                session.add(waypoint)

            else:
                waypoint_type = row[1]
                waypoint_name = row[0]
                waypoint_coordinates = row[2]
                result_row = session.execute(
                    select(Waypoint).where(
                        Waypoint.airport_icao == AIRPORT_ICAO,
                        Waypoint.name == waypoint_name,
                    )
                ).fetchone()
                if result_row:
                    # Not inserting duplicate waypoints
                    continue
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
                    type=waypoint_type,  # Set the "type" here
                    name=waypoint_name,
                    coordinates_dd = coordinates,
                    geom=f"POINT({lng1} {lat1})",
                )
                session.add(waypoint)
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
    )
    session.add(procedure_obj)

    if "Turn" in header_row:
        course_col_index = header_row.index("Course ")
        turn_col_index = header_row.index("TM")
        if turn_col_index == course_col_index + 1:
            for _, row in coding_df.iloc[1:].iterrows():
                row = list(row)
                # print(row_values)
                waypoint_obj = None

                if is_valid_data(row[2]):
                    waypoint_obj = (
                        session.query(Waypoint)
                        .filter_by(airport_icao=AIRPORT_ICAO, name=row[2].strip())
                        .first()
                    )
                proc_des_obj = ProcedureDescription(
                    procedure=procedure_obj,
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
                )
                session.add(proc_des_obj)
                if is_valid_data(data := row[3]):
                    if data == "Y":
                        proc_des_obj.fly_over = True
                    elif data == "N":
                        proc_des_obj.fly_over = False

        else:
            for _, row in coding_df.iloc[1:].iterrows():
                row = list(row)
                # print(row)
                waypoint_obj = None

                if is_valid_data(row[2]):
                    waypoint_obj = (
                        session.query(Waypoint)
                        .filter_by(airport_icao=AIRPORT_ICAO, name=row[2].strip())
                        .first()
                    )
                proc_des_obj = ProcedureDescription(
                    procedure=procedure_obj,
                    seq_num=int(row[0]),
                    waypoint=waypoint_obj,
                    path_descriptor=row[1].strip(),
                    course_angle=row[4]
                    .replace("\n", "")
                    .replace("  ", "")
                    .replace(" )", ")"),
                    turn_dir=row[5].strip() if is_valid_data(row[5]) else None,
                    altitude_ul=row[7].strip() if is_valid_data(row[7]) else None,
                    altitude_ll=row[8].strip() if is_valid_data(row[8]) else None,
                    speed_limit=row[9].strip() if is_valid_data(row[9]) else None,
                    dst_time=row[6].strip() if is_valid_data(row[6]) else None,
                    vpa_tch=row[10].strip() if is_valid_data(row[10]) else None,
                    nav_spec=row[11].strip() if is_valid_data(row[11]) else None,
                )
                session.add(proc_des_obj)
                if is_valid_data(data := row[3]):
                    if data == "Y":
                        proc_des_obj.fly_over = True
                    elif data == "N":
                        proc_des_obj.fly_over = False


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
