import camelot
import re
import os

from sqlalchemy import select

from model import AiracData, session, Waypoint, Procedure, ProcedureDescription

AIRPORT_ICAO = "VAPR"

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
    process_id = get_active_process_id()
    rwy_dir = re.search(r"RWY-(\d+[A-Z]?)", file_name).groups()[0]
    tables = camelot.read_pdf(FOLDER_PATH + file_name, pages="1")
    df = tables[0].df
    df = df.drop(index=[0, 1])
    waypoint_names, waypoint_coords = [], []
    waypoint_names = [x for x in df.loc[:, 3].tolist() if x]
    waypoint_names.pop(0)  # removing heading
    waypoint_coords = [x for x in df.loc[:, 8].tolist() if x]
    waypoint_coords.pop(0)  # removing heading

    for name, lat_long in zip(waypoint_names, waypoint_coords):
        result_row = session.execute(
            select(Waypoint).where(
                Waypoint.airport_icao == AIRPORT_ICAO,
                Waypoint.name == name,
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
                process_id=process_id
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
        process_id=process_id
    )
    session.add(procedure_obj)
    # Initialize sequence number tracker
    sequence_number = 1
    for _, row in df.iterrows():
        row = list(row)
        if not re.match(r"\d+$", row[0]):
            x = row[0]
            row = re.split(r" *\n *", x)
            if x[0].isalpha():
                row[-1] = row.pop(0) + " " + row[-1]
                row.insert(0, row.pop(-2))
            elif x[0].isnumeric():
                row[0] = row[0] + "\n" + row.pop(-2)  # combining course values
                row[1] = row[1] + " " + row.pop(-1)  # combining nav spec values
                row.insert(0, row.pop(-1))  # arranging seq num
                row.append(row.pop(2))
                course_angle = row.pop(1)
                row.insert(4, course_angle)
        waypoint_obj = None
        if is_valid_data(data := row[2]):
            waypoint_obj = session.execute(
                select(Waypoint).where(
                    Waypoint.airport_icao == AIRPORT_ICAO,
                    Waypoint.name == data,
                )
            ).fetchone()[0]
        course_angle = row[4].replace("\n", "").replace("  ", "").replace(" )", ")").replace(" Mag", "").replace(" True", "")

        angles = course_angle.split()
                        # Check if we have exactly two angle values
        if len(angles) == 2:
            if not angles[1].startswith("(") and not angles[1].endswith(")"):
        # Add brackets around the second angle
                course_angle = f"{angles[0]}({angles[1]})"
            else:
        # If the second angle already has brackets, keep it as is
                course_angle = f"{angles[0]}{angles[1]}"
        
    # Handle cases where there are not exactly two angles
        elif len(angles) == 1:  # Handle cases where the string might not split properly
    # Look for patterns like "178.24°177.99°" and add brackets around the second angle
            course_angle = re.sub(r"(\d+\.\d+°)(\d+\.\d+°)", r"\1(\2)", course_angle)

        print(course_angle,"jn")
        proc_des_obj = ProcedureDescription(
            procedure=procedure_obj,
            sequence_number=sequence_number,
            seq_num=row[0],
            waypoint=waypoint_obj,
            path_descriptor=row[1].strip(),
            course_angle=course_angle,
            turn_dir=row[6].strip() if is_valid_data(row[6]) else None,
            altitude_ll=row[7].strip() if is_valid_data(row[7]) else None,
            speed_limit=row[8].strip() if is_valid_data(row[8]) else None,
            dst_time=row[5].strip() if is_valid_data(row[5]) else None,
            vpa_tch=row[9].strip() if is_valid_data(row[9]) else None,
            nav_spec=row[10].strip() if is_valid_data(row[10]) else None,
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
