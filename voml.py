import camelot
import re
import os

from sqlalchemy import select

from model import session, Waypoint, Procedure, ProcedureDescription


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


def extract_insert_apch(file_name):
    rwy_dir = re.search(r"RWY-(\d+[A-Z]?)", file_name).groups()[0]
    tables = camelot.read_pdf(FOLDER_PATH + file_name, pages="1")
    df = tables[0].df
    df = df.drop(index=[0])
    waypoint_types, waypoint_names, waypoint_coords = [], [], []
    waypoint_types = [x for x in df.iloc[:, 3].tolist() if x]
    waypoint_types.pop(0)  # removing heading
    waypoint_names = [x for x in df.iloc[:, 7].tolist() if x]
    waypoint_names.pop(0)  # removing heading
    waypoint_coords = [x for x in df.iloc[:, 10].tolist() if x]
    waypoint_coords.pop(0)  # removing heading

    for type_, name, lat_long in zip(waypoint_types, waypoint_names, waypoint_coords):
        result_row = session.execute(
            select(Waypoint).where(
                Waypoint.airport_icao == AIRPORT_ICAO,
                Waypoint.name == name,  # TODO: Add column for type_
            )
        ).fetchone()
        if result_row:
            # Not inserting duplicate waypoints
            continue
        lat_long = re.sub(r"[^NEWS\d. ]", "", lat_long).split()
        lat, long = lat_long[1] + lat_long[0], lat_long[3] + lat_long[2]
        lat, long = conversionDMStoDD(lat), conversionDMStoDD(long)
        coordinates = f"{lat} {long}"
        session.add(
            Waypoint(
                airport_icao=AIRPORT_ICAO,
                name=name,
                coordinates_dd = coordinates,
                geom=f"POINT({long} {lat})",
            )
        )

    # procedure extraction
    df = df[df[1] != ""]  # targeting only procedure description rows
    df = df.loc[:, (df != "").any(axis=0)]  # deleting empty columns
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
    for i, row in df.iterrows():
        row = list(row)
        if not re.match(r"\d+$", row[0]):
            # the case where all datas of a row gets clubbed into single cell
            if row[1]:
                # skipping header row
                continue
            x = row[0]
            if i == 1:
                l = re.split(r"0M\(0T\)\s*\n\s*", x)
                row = re.split(r"\s*\n\s*", l[1])
                row[-1] = row.pop(1) + " " + row[-1]
                row[0] = row[0] + "\n" + row.pop(-2)
                row.insert(0, row.pop(-2))
                val = row.pop(1)
                row.insert(6, val)
            else:
                row = re.split(r"\s*\n\s*", x)
                course_angle = row.pop(0) + "\n" + row.pop(-2)
                row[-1] = row.pop(0) + " " + row[-1]
                row.insert(0, row.pop(-2))
                row.insert(4, course_angle)
        waypoint_obj = None
        if is_valid_data(data := row[2]):
            waypoint_obj = session.execute(
                select(Waypoint).where(
                    Waypoint.airport_icao == AIRPORT_ICAO,
                    Waypoint.name == data,
                )
            ).fetchone()[0]
        proc_des_obj = ProcedureDescription(
            procedure=procedure_obj,
            seq_num=row[0],
            waypoint=waypoint_obj,
            path_descriptor=row[3].strip(),
            course_angle=row[4].replace("\n", "").replace("  ", "").replace(" )", ")"),
            turn_dir=row[5].strip() if is_valid_data(row[5]) else None,
            altitude_ll=row[6].strip() if is_valid_data(row[6]) else None,
            speed_limit=row[7].strip() if is_valid_data(row[7]) else None,
            dst_time=row[8].strip() if is_valid_data(row[8]) else None,
            vpa_tch=row[9].strip() if is_valid_data(row[9]) else None,
            role_of_the_fix=row[10].strip() if is_valid_data(row[10]) else None,
            nav_spec=row[11].strip() if is_valid_data(row[11]) else None,
        )
        session.add(proc_des_obj)
        if is_valid_data(data := row[1]):
            if data == "Y":
                proc_des_obj.fly_over = True
            elif data == "N":
                proc_des_obj.fly_over = False


AIRPORT_ICAO = "VOML"

FOLDER_PATH = f"./{AIRPORT_ICAO}/"


def main():
    file_names = os.listdir(FOLDER_PATH)
    # Capturing APCH files names
    apch_coding_file_names = []
    for file_name in file_names:
        if file_name.find("CODING") > -1:
            apch_coding_file_names.append(file_name)

    for file_name in apch_coding_file_names:
        extract_insert_apch(file_name)
    session.commit()


if __name__ == "__main__":
    main()
