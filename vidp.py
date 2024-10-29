import camelot
import re
import os

from sqlalchemy import select

from model import session, Waypoint, Procedure, ProcedureDescription,TerminalHolding

##################
# EXTRACTOR CODE #
##################
import os
import re
import pdftotext
import pandas as pd


def conversionDMStoDD(coord):
    direction = {"N": 1, "S": -1, "E": 1, "W": -1}
    # Extract the direction (N/S or E/W) and numeric part
    dir_part = coord[-1]
    num_part = coord[:-1]
    # Handle latitude (N/S)
    if dir_part in ["N", "S"]:
        # Latitude degrees are up to two digits, minutes are the next two digits, and seconds are the rest
        lat_degrees = int(num_part[:2])
        lat_minutes = int(num_part[2:4])
        lat_seconds = float(num_part[4:])
        lat_dd = (lat_degrees + lat_minutes / 60 + lat_seconds / 3600) * direction[
            dir_part
        ]
        return lat_dd
    # Handle longitude (E/W)
    if dir_part in ["E", "W"]:
        # Longitude degrees are up to three digits, minutes are the next two digits, and seconds are the rest
        lon_degrees = int(num_part[:3])
        lon_minutes = int(num_part[3:5])
        lon_seconds = float(num_part[5:])
        lon_dd = (lon_degrees + lon_minutes / 60 + lon_seconds / 3600) * direction[
            dir_part
        ]
        return lon_dd


def is_valid_data(data):
    if not data:
        return False
    if pd.isna(data):
        return False
    if type(data) == str and re.match(r"(\s+|\s*-\s*)$", data):
        return False
    return True



EXCEL_FILE = r"C:\Users\LENOVO\Desktop\ANS_Register_Extraction\ans_regist\AIP Supp 74-2023_VIDP RNAV SID & STAR.xlsx"
AIRPORT_ICAO = "VIDP"


def process_procedures(df, procedure_type):
    procedure_obj = None

    for _, row in df.iterrows():
        waypoint_obj = None
        row = list(row)
        if pd.isna(row).all():
            continue
        if pd.isna(row[-1]):
            procedure_obj = Procedure(
                airport_icao=AIRPORT_ICAO,
                type=procedure_type,
                name=row[0],
                rwy_dir='Default_Value'
            )
            session.add(procedure_obj)

        if row[-1] == "RNAV 1":
            if not pd.isna(row[2]):
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
                .replace('"', "")
                .replace("\n", "")
                .replace("  ", "")
                .replace(" )", ")") if is_valid_data(row[4]) else None,
                turn_dir=row[6] if is_valid_data(row[6]) else None,
                altitude_ul=str(row[7]) if is_valid_data(row[7]) else None,
                altitude_ll=str(row[8]) if is_valid_data(row[8]) else None,
                speed_limit=row[9] if is_valid_data(row[9]) else None,
                dst_time=row[5] if is_valid_data(row[5]) else None,
                vpa_tch=row[10] if is_valid_data(row[10]) else None,
                nav_spec=row[11] if is_valid_data(row[11]) else None,
            )
            session.add(proc_des_obj)

            if is_valid_data(data := row[3]):
                if data == "Y":
                    proc_des_obj.fly_over = True
                elif data == "N":
                    proc_des_obj.fly_over = False


def main():
    df_wpt = pd.read_excel(EXCEL_FILE, sheet_name="WPT")
    for _, row in df_wpt.iterrows():
        waypoint_name = row["Waypoint"]
        coordinates = row["Coordinates"]
        match = re.match(r"(\d+\.\d+[NS])\s+(\d+\.\d+[EW])", coordinates)
        lat_lon = match.groups()
        lat = conversionDMStoDD(lat_lon[0])
        lon = conversionDMStoDD(lat_lon[1])
        
        session.add(
            Waypoint(
                airport_icao=AIRPORT_ICAO,
                name=waypoint_name,
                geom=f"POINT({lon} {lat})",
            )
        )

    df_hldg = pd.read_excel(EXCEL_FILE, sheet_name="HLDG")
    for _, row in df_hldg.iterrows():
        row = list(row)
        # print(row)
        waypoint_obj = None
        waypoint_obj = (
            session.query(Waypoint)
            .filter_by(airport_icao=AIRPORT_ICAO, name=(row[1]).strip())
            .first()
        )
        term_hold_obj = TerminalHolding(
            waypoint_id=waypoint_obj.id,
            path_descriptor=row[0].strip(),
            course_angle=row[3]
            .replace("\n", "")
            .replace("  ", "")
            .replace(" )", ")"),
            turn_dir=row[5] if is_valid_data(row[5]) else None,
            altitude_ul=str(row[6]) if is_valid_data(row[6]) else None,
            altitude_ll=str(row[7]) if is_valid_data(row[7]) else None,
            speed_limit=row[8] if is_valid_data(row[8]) else None,
            dst_time=row[4] if is_valid_data(row[4]) else None,
            vpa_tch=row[9] if is_valid_data(row[9]) else None,
            nav_spec=row[10] if is_valid_data(row[10]) else None,
        )
        session.add(term_hold_obj)
        if is_valid_data(data := row[2]):
            if data == "Y":
                term_hold_obj.fly_over = True
            elif data == "N":
                term_hold_obj.fly_over = False

    df_sid = pd.read_excel(EXCEL_FILE, sheet_name="SID")
    process_procedures(df_sid, "SID")

    df_star = pd.read_excel(EXCEL_FILE, sheet_name="STAR")
    process_procedures(df_star, "STAR")

    session.commit()


if __name__ == "__main__":
    main()
