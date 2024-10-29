from model import Waypoint, Procedure, ProcedureDescription, TerminalHolding, session
from sqlalchemy import select
##################
# EXTRACTOR CODE #
##################
import camelot
import os
import re


AIRPORT_ICAO = "VAAH"
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
        print(row)
        if row[1] == "":
            vals = row[0].split()
            
            row[0] = vals[0]
            row[1] = vals[1]
            vals = row[2].split()
            row[2] = vals[0]
            
            row[3] = vals[1]
        print(row[2])
        print(row[3])


        latitude = conversionDMStoDD(row[2])
        longitude = conversionDMStoDD(row[3])
        waypoint_obj = Waypoint(
            airport_icao=AIRPORT_ICAO,
            name=row[0],
            type=row[1],
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

    # extract terminal holding data
    index_holding_start = None
    for i, row in apch_data_df.iterrows():
        row_values = list(row)
        if "Holding" in row_values[0]:
            index_holding_start = i
            break
    if index_holding_start is not None:
        apch_data_df_holding = apch_data_df.loc[index_holding_start + 2 :]
        for _, row in apch_data_df_holding.iterrows():
            row = list(row)
            if not any(row):
                break
            data_parts = row[0].split("\n")
            waypoint_obj = None
            waypoint_name = data_parts[1]
            if is_valid_data(waypoint_name):
                waypoint_obj = (
                    session.query(Waypoint)
                    .filter(
                        Waypoint.airport_icao == AIRPORT_ICAO,
                        Waypoint.name == waypoint_name,
                    )
                    .first()
                )
                # print(waypoint_obj)
            term_hold_obj = TerminalHolding(
                    waypoint=waypoint_obj,
                    path_descriptor=data_parts[0].strip(),
                    course_angle=data_parts[2].strip(),
                    turn_dir=data_parts[6].strip()
                    if is_valid_data(data_parts[6])
                    else None,
                    altitude_ll=data_parts[4].strip()
                    if is_valid_data(data_parts[4])
                    else None,
                    speed_limit=data_parts[3].strip()
                    if is_valid_data(data_parts[3])
                    else None,
                    dst_time=data_parts[5].strip()
                    if is_valid_data(data_parts[5])
                    else None,
                )
            session.add(term_hold_obj)
    # extract procedure_desc data
    for _, row in apch_data_df.iloc[2:].iterrows():
        row = list(row)
        if not any(row):
            break
        # print(row)
        if row[1] == "":
            vals = row[2].split()
            row[1] = vals[0]
            row[2] = vals[1]
            # print(row)
        waypoint_obj = None
        waypoint_name = row[2].strip()
        waypoint_obj = (
            session.query(Waypoint)
            .filter(
                Waypoint.airport_icao == AIRPORT_ICAO,
                Waypoint.name == waypoint_name,
            )
            .first()
        )
        proc_des_obj = ProcedureDescription(
            procedure=procedure_obj,
            seq_num=row[0].strip(),
            path_descriptor=row[1].strip(),
            waypoint=waypoint_obj,
            course_angle=row[4].replace("\n", "").replace(" ", "").replace(" )", ")"),
            mag_var=row[5].strip() if is_valid_data(row[5]) else None,
            turn_dir=row[7].strip() if is_valid_data(row[7]) else None,
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


if __name__ == "__main__":
    main()
