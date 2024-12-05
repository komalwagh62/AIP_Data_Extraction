from model import Waypoint, Procedure, ProcedureDescription, TerminalHolding, session
from sqlalchemy import select
from pyproj import Proj, transform

##################
# EXTRACTOR CODE #
##################
import camelot
import os
import re

utm_proj = Proj(proj='utm', zone=43, ellps='WGS84', south=False)  # south=False for northern hemisphere
wgs84_proj = Proj(proj='latlong', datum='WGS84')  # Latitude/Longitude

AIRPORT_ICAO = "VABP"
FOLDER_PATH = f"./{AIRPORT_ICAO}/"


def conversionDMStoDD(coord):
    direction = {"N": 1, "S": -1, "E": 1, "W": -1}

    # Remove non-numeric characters
    coord_numeric = "".join(char for char in coord if char.isdigit() or char == ".")

    # Check if the coordinate is in decimal format
    try:
        decimal_degrees = float(coord_numeric)
        return decimal_degrees
    except ValueError:
        pass  # Continue with degrees, minutes, and seconds conversion

    # Check if the coordinate string has at least one character
    if len(coord) > 0:
        # Extract direction (N/S/E/W)
        new_dir = coord[-1]

        # Split degrees, minutes, and seconds parts
        parts = re.split(r"[:.]", coord_numeric)

        # Handle different numbers of parts
        if len(parts) == 3:
            HH, MM, SS = map(int, parts)
            decimal = 0
        elif len(parts) == 4:
            HH, MM, SS, decimal = map(int, parts)
        else:
            raise ValueError("Invalid coordinate format")

        # Calculate decimal degrees
        decimal_degrees = (HH + MM / 60 + (SS + decimal / 100) / 3600) * direction[
            new_dir
        ]
        return decimal_degrees
    else:
        # Handle the case where the coordinate string is empty
        return None


def is_valid_data(data):
    if not data:
        return False
    if re.match(r"(\s+|\s*-\s*)$", data):
        return False
    return True


def extract_insert_apch(file_name, rwy_dir, tables):
    coding_df = tables[0].df
    apch_data_df = coding_df.loc[:, (coding_df != "").any(axis=0)]
    # print(apch_data_df)
    
    # extract waypoint data
    index_waypoint_start = None
    for i, row in apch_data_df.iterrows():
        row_values = list(row)
        if "Waypoint Coordinates" in row_values[0]:
            index_waypoint_start = i
            break
    apch_data_df_waypoint = apch_data_df.loc[index_waypoint_start + 2 :]
    apch_data_df_waypoint = apch_data_df_waypoint.loc[
        :, (apch_data_df_waypoint != "").any(axis=0)
    ]
    for _, row in apch_data_df_waypoint.iterrows():
        row = list(row)
        # print(row)
        latitude = conversionDMStoDD(row[2])
        longitude = conversionDMStoDD(row[3])
        longitude, latitude = transform(utm_proj, wgs84_proj, latitude, longitude)
        coordinates = f"{latitude} {longitude}"
                        
        waypoint_obj = Waypoint(
            airport_icao=AIRPORT_ICAO,
            name=row[0],
            type=row[1],
            coordinates_dd = coordinates,
            geom=f"POINT({longitude} {latitude})",
        )
        session.add(waypoint_obj)

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

    # extract procedure_desc data
    for _, row in apch_data_df.iloc[2:].iterrows():
        row = list(row)
        if row[0].isdigit():
            waypoint_name = row[4].strip()
            waypoint_obj = (
                session.query(Waypoint)
                .filter(
                    Waypoint.airport_icao == AIRPORT_ICAO,
                    Waypoint.name == waypoint_name,
                )
                .first()
            )
            course_angle = None
            if is_valid_data(row[8]) and is_valid_data(row[9]):
             course_angle = f"{row[8]}({row[9]})"
            #  print(course_angle)
            proc_des_obj = ProcedureDescription(
                procedure=procedure_obj,
                seq_num=row[0].strip(),
                path_descriptor=row[1].strip(),
                waypoint=waypoint_obj,
                course_angle=course_angle,
                mag_var=row[11].strip() if is_valid_data(row[11]) else None,
                turn_dir=row[15].strip() if is_valid_data(row[15]) else None,
                altitude_ll=row[16].strip() if is_valid_data(row[16]) else None,
                speed_limit=row[18].strip() if is_valid_data(row[18]) else None,
                dst_time=row[13].strip() if is_valid_data(row[13]) else None,
                vpa_tch=row[19].strip() if is_valid_data(row[19]) else None,
                nav_spec=row[21].strip() if is_valid_data(row[21]) else None,
            )
            session.add(proc_des_obj)
            if is_valid_data(data := row[5]):
             if data == "Y":
                proc_des_obj.fly_over = True
             elif data == "N":
                proc_des_obj.fly_over = False

    # extract terminal holding data
    index_holding_start = None
    for i, row in apch_data_df.iterrows():
        row_values = list(row)
        if "Holding" in row_values[0]:
            index_holding_start = i
            break
    if index_holding_start is not None:
        apch_data_df_holding = apch_data_df.loc[index_holding_start + 2 :]
        apch_data_df = apch_data_df_holding.loc[
            :, (apch_data_df_holding != "").any(axis=0)
        ]
        for _, row in apch_data_df.iterrows():
            row = list(row)
            if not any(row):
                break
            # print(row)
            vals = row[7].split("/")
            row[6] = vals[0]
            row[7] = vals[1]
            waypoint_name = row[2].strip()
            waypoint_obj = (
                session.query(Waypoint)
                .filter(
                    Waypoint.airport_icao == AIRPORT_ICAO,
                    Waypoint.name == waypoint_name,
                )
                .first()
            )
            # print(waypoint_obj)
            course_angle=row[4].replace("\n", "").replace(" ", "").replace(" )", ")")
            term_hold_obj = TerminalHolding(
                waypoint=waypoint_obj,
                path_descriptor=row[0].strip(),
                course_angle=row[4]
                .replace("\n", "")
                .replace(" ", "")
                .replace(" )", ")"),
                turn_dir=row[9].strip() if is_valid_data(row[9]) else None,
                altitude_ul=row[7].strip() if is_valid_data(row[7]) else None,
                altitude_ll=row[6].strip() if is_valid_data(row[6]) else None,
                speed_limit=row[5].strip() if is_valid_data(row[5]) else None,
                dst_time=row[8].strip() if is_valid_data(row[8]) else None,
            )
            session.add(term_hold_obj)

       
    


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

    # session.commit()
    print("Data insertion complete.")


if __name__ == "__main__":
    main()
