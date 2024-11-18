from model import Waypoint, Procedure, ProcedureDescription, TerminalHolding, session
from sqlalchemy import select


##################
# EXTRACTOR CODE #
##################
import camelot
import pdftotext
import re
import os

AIRPORT_ICAO = "VOBL"

FOLDER_PATH = f"./{AIRPORT_ICAO}/"


def conversionDMStoDD(coord):  # to convert DMS into Decimal Degrees
    direction = {"N": 1, "S": -1, "E": 1, "W": -1}
    new_dir = coord[len(coord) - 1 :]
    coord = coord[: len(coord) - 1]
    decimals = coord.split(".")
    decimal = "00"
    if len(decimals) > 1:
        coord, decimal = decimals[0], decimals[1]
    SS = coord[len(coord) - 2 :]
    coord = coord[: len(coord) - 2]
    MM = coord[len(coord) - 2 :]
    coord = coord[: len(coord) - 2]
    HH = coord
    return (
        float(HH) + float(MM) / 60 + float(str(SS) + "." + str(decimal)) / 3600
    ) * direction[new_dir]


def is_valid_data(data):
    if not data:
        return False
    if re.match(r"(\s+|\s*-\s*)$", data):
        return False
    return True


def insert_terminal_holdings(df):
    df = df.drop(0)
    for _, row in df.iterrows():
        waypoint_obj = session.execute(
            select(Waypoint).where(
                Waypoint.airport_icao == AIRPORT_ICAO,
                Waypoint.name == row[0].strip(),
            )
        ).fetchone()[0]
        term_hold_obj = TerminalHolding(
            waypoint_id=waypoint_obj.id,
            path_descriptor=row[1].strip(),
            course_angle=row[3].replace("\n", "").replace("  ", "").replace(" )", ")"),
            turn_dir=row[4].strip() if is_valid_data(row[4]) else None,
            altitude_ul=row[5].strip() if is_valid_data(row[5]) else None,
            altitude_ll=row[6].strip() if is_valid_data(row[6]) else None,
            speed_limit=row[7].strip() if is_valid_data(row[7]) else None,
            dst_time=row[8].strip() if is_valid_data(row[8]) else None,
            vpa_tch=row[9].strip() if is_valid_data(row[9]) else None,
            nav_spec=row[10].strip() if is_valid_data(row[10]) else None,
        )
        session.add(term_hold_obj)
        if is_valid_data(data := row[2]):
            if data == "Y":
                term_hold_obj.fly_over = True
            elif data == "N":
                term_hold_obj.fly_over = False


def extract_insert_sid_star(file_name, type_):
    rwy_dir = re.search(r"RWY-(\d+[A-Z]?)", file_name).groups()[0]
    file_path = FOLDER_PATH + file_name
    print(f"Extracting from: {file_path}")
    procedure_names = None
    with open(file_path, "rb") as f:
        # To get procedure names
        texts = pdftotext.PDF(f)
        procedure_names = re.findall(r"([A-Z]+\s+[A-Z0-9]+)\s*Waypoint", texts[0])
    tables = camelot.read_pdf(
        file_path, pages="all"
    )  # Converting PDF to table DataFrames
    if len(procedure_names) != len(tables):
        # In case there's issue in capturing procedure names
        print("Error: Length of procedures and tables aren't matching")
        exit()

    for i in range(len(procedure_names)):
        procedure_name = procedure_names[i]
        if procedure_name == "TERMINAL HOLDINGS":
            insert_terminal_holdings(tables[i].df)
            continue
        procedure_obj = Procedure(
            airport_icao=AIRPORT_ICAO,
            rwy_dir=rwy_dir,
            type=type_,
        )
        session.add(procedure_obj)
        procedure_obj.name, procedure_obj.designator = procedure_name.split()

        df = tables[i].df
        nav_spec_col_index = df.shape[1] - 1
        filter_ = df[nav_spec_col_index].apply(
            lambda x: bool(x.strip())
        )  # Filtering out invalid rows caught due to extraction
        df = df[filter_]
        seq_num = 1
        for _, row in df.iterrows():
            waypoint_obj = None
            if is_valid_data(row[0]):
                # Checking if fix is a waypoint or RWY
                waypoint_obj = session.execute(
                    select(Waypoint).where(
                        Waypoint.airport_icao == AIRPORT_ICAO,
                        Waypoint.name == row[0].strip(),
                    )
                ).fetchone()[0]
            proc_des_obj = ProcedureDescription(
                procedure=procedure_obj,
                seq_num=seq_num,
                waypoint=waypoint_obj,
                path_descriptor=row[1].strip(),
                course_angle=row[3]
                .replace("\n", "")
                .replace("  ", "")
                .replace(" )", ")"),
                turn_dir=row[4].strip() if is_valid_data(row[4]) else None,
                altitude_ul=row[5].strip() if is_valid_data(row[5]) else None,
                altitude_ll=row[6].strip() if is_valid_data(row[6]) else None,
                speed_limit=row[7].strip() if is_valid_data(row[7]) else None,
                dst_time=row[8].strip() if is_valid_data(row[8]) else None,
                vpa_tch=row[9].strip() if is_valid_data(row[9]) else None,
                nav_spec=row[10].strip() if is_valid_data(row[10]) else None,
            )
            session.add(proc_des_obj)
            if is_valid_data(data := row[2]):
                if data == "Y":
                    proc_des_obj.fly_over = True
                elif data == "N":
                    proc_des_obj.fly_over = False
            seq_num += 1


def extract_insert_apch(file_name):
    if re.search(r"RNP-[A-Z]-RWY", file_name):
        if file_name.find("RWY-27R") > -1:
            pass
        else:
            return
    tables = camelot.read_pdf(FOLDER_PATH + file_name, pages="1")
    waypoint_tables = tables[1:]
    for waypoint_table in waypoint_tables:
        waypoint_df = waypoint_table.df
        if re.search(r"Waypoint", waypoint_df[0][0], re.I):
            waypoint_df = waypoint_df.drop(0)
        for _, row in waypoint_df.iterrows():
            row = list(row)
            row = [x for x in row if x.strip()]
            result_row = session.execute(
                select(Waypoint).where(
                    Waypoint.airport_icao == AIRPORT_ICAO,
                    Waypoint.name == row[0].strip(),
                )
            ).fetchone()
            if result_row:
                # Not inserting duplicate waypoints
                continue
            data = row[1]
            data = data.replace(":", "")
            lat, lng = re.findall(r"([A-Z])\s*([\d.]+)", data)
            lat = lat[1] + lat[0]
            lng = lng[1] + lng[0]
            lat, lng = conversionDMStoDD(lat), conversionDMStoDD(lng)
            coordinates = f"{lat} {lng}"
            session.add(
                Waypoint(
                    airport_icao=AIRPORT_ICAO,
                    name=row[0].strip(),
                    coordinates_dd = coordinates,
                    geom=f"POINT({lng} {lat})",
                )
            )
    coding_df = tables[0].df
    coding_df = coding_df.drop(0)

    rwy_dir = re.search(r"RWY-(\d+[A-Z]?)", file_name).groups()[0]
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

    for _, row in coding_df.iterrows():
        waypoint_obj = None
        if is_valid_data(row[2]):
            # Checking if fix is a waypoint or RWY
            waypoint_obj = session.execute(
                select(Waypoint).where(
                    Waypoint.airport_icao == AIRPORT_ICAO,
                    Waypoint.name == row[2].strip(),
                )
            ).fetchone()[0]
        proc_des_obj = ProcedureDescription(
            procedure=procedure_obj,
            seq_num=row[0],
            waypoint=waypoint_obj,
            path_descriptor=row[3].strip(),
            course_angle=row[4].replace("\n", "").replace("  ", "").replace(" )", ")"),
            turn_dir=row[5].strip() if is_valid_data(row[4]) else None,
            altitude_ll=row[6].strip() if is_valid_data(row[6]) else None,
            speed_limit=row[7].strip() if is_valid_data(row[7]) else None,
            dst_time=row[8].strip() if is_valid_data(row[8]) else None,
            vpa_tch=row[9].strip() if is_valid_data(row[9]) else None,
            nav_spec=row[10].strip() if is_valid_data(row[10]) else None,
        )
        session.add(proc_des_obj)
        if is_valid_data(data := row[1]):
            if data == "Y":
                proc_des_obj.fly_over = True
            elif data == "N":
                proc_des_obj.fly_over = False


def main():
    file_names = os.listdir(FOLDER_PATH)
    # Capturing coding files of all procedures, and waypoint files
    sid_coding_file_names = []
    star_coding_file_names = []
    apch_coding_file_names = []
    waypoint_file_names = []
    for file_name in file_names:
        if file_name.find("WAYPOINTS") > -1:
            waypoint_file_names.append(file_name)
        elif file_name.find("CODING") > -1:
            if file_name.find("SID") > -1:
                sid_coding_file_names.append(file_name)
            elif file_name.find("STAR") > -1:
                star_coding_file_names.append(file_name)
            else:
                apch_coding_file_names.append(file_name)

    # Inserting waypoint files first because procedures have dependency on it.
    for waypoint_file_name in waypoint_file_names:
        # print("extracting Waypoint file:", waypoint_file_name)
        df = camelot.read_pdf(FOLDER_PATH + waypoint_file_name, pages="1")[0].df
        if re.search(r"WAYPOINT", df[0][0], re.I):
            df = df.drop(0)
        for _, row in df.iterrows():
            row = list(row)
            row = [x for x in row if x.strip()]
            result_row = session.execute(
                select(Waypoint).where(
                    Waypoint.airport_icao == AIRPORT_ICAO,
                    Waypoint.name == row[0].strip(),
                )
            ).fetchone()
            if result_row:
                # Not inserting duplicate waypoints
                continue
            data = row[1]
            data = data.replace(":", "")
            lat, lng = re.findall(r"([A-Z])\s*([\d.]+)", data)
            lat = lat[1] + lat[0]
            lng = lng[1] + lng[0]
            lat, lng = conversionDMStoDD(lat), conversionDMStoDD(lng)
            coordinates = f"{lat} {lng}"
            session.add(
                Waypoint(
                    airport_icao=AIRPORT_ICAO,
                    name=row[0].strip(),
                    coordinates_dd = coordinates,
                    geom=f"POINT({lng} {lat})",
                )
            )
    for file_name in sid_coding_file_names:
        extract_insert_sid_star(file_name, "SID")
    for file_name in star_coding_file_names:
        extract_insert_sid_star(file_name, "STAR")
    for file_name in apch_coding_file_names:
        extract_insert_apch(file_name)
    session.commit()


if __name__ == "__main__":
    main()