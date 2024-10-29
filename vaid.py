import camelot
import re
import os

from sqlalchemy import select

from model import session, Waypoint, Procedure, ProcedureDescription
##################
# EXTRACTOR CODE #
##################
import camelot
import os
import re
import pdftotext

AIRPORT_ICAO = "VAID"
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
        for _, row in waypoint_df.iterrows():
            row = list(row)
            # print(row)
            row = [x for x in row if x.strip()]
            if len(row) < 2:
                continue
            result_row = session.execute(
                select(Waypoint).where(
                    Waypoint.airport_icao == AIRPORT_ICAO,
                    Waypoint.name == row[0].strip(),
                )
            ).fetchone()
            if result_row:
                continue
            extracted_data1 = [
                item
                for match in re.findall(
                    r"([NS])\s*([\d:.]+)\s*([EW])\s*([\d:.]+)", row[1]
                )
                for item in match
            ]
            # print(extracted_data1)
            lat_dir1, lat_value1, lng_dir1, lng_value1 = extracted_data1
            lat1 = conversionDMStoDD(lat_value1 + lat_dir1)
            lng1 = conversionDMStoDD(lng_value1 + lng_dir1)
            session.add(
                Waypoint(
                    airport_icao=AIRPORT_ICAO,
                    name=row[0].strip(),
                    geom=f"POINT({lng1} {lat1})",
                )
            )
    coding_df = tables[0].df
    # print(coding_df)
    coding_df = coding_df.drop(index=[0, 1])
    apch_data_df = coding_df.loc[:, (coding_df != "").any(axis=0)]

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
    
    for _, row in apch_data_df.iterrows():
        row = list(row)
        waypoint_obj = None
        if bool(row[-1].strip()):
            # print(row)
            alt_speed_data = row[1].strip()
            if alt_speed_data:
                alt_speed_values = alt_speed_data.split(" \n")
                if len(alt_speed_values) == 2:
                    path_descriptor, waypoint_name = alt_speed_values

                vpa_tch = None
                nav_spec = None

                if len(row) >= 10:
                    vpa_tch_nav_spec_parts = row[9].strip().split()
                    nav_spec = row[10].strip() if len(row) > 10 else None

                    last_part = vpa_tch_nav_spec_parts[-1]
                    if last_part == "APCH" and any(
                        char.isdigit() for char in vpa_tch_nav_spec_parts[0]
                    ):
                        vpa_tch = vpa_tch_nav_spec_parts[0]
                        nav_spec = " ".join(vpa_tch_nav_spec_parts[1:])
                    else:
                        nav_spec = row[9]
            if is_valid_data(waypoint_name):
                waypoint_obj = (
                    session.query(Waypoint)
                    .filter_by(
                        airport_icao=AIRPORT_ICAO,
                        name=waypoint_name,
                    )
                    .first()
                )
                proc_des_obj = ProcedureDescription(
                    procedure=procedure_obj,
                    seq_num=row[0],
                    waypoint=waypoint_obj,
                    path_descriptor=path_descriptor,
                    fly_over=row[3] == "Y" if is_valid_data(row[3]) else False,
                    course_angle=row[4]
                    .replace("\n", "")
                    .replace("  ", "")
                    .replace(" )", ""),
                    turn_dir=row[6].strip() if is_valid_data(row[6]) else None,
                    altitude_ll=row[7].strip() if is_valid_data(row[7]) else None,
                    speed_limit=row[8].strip() if is_valid_data(row[8]) else None,
                    dst_time=row[5].replace("\n", ""),
                    vpa_tch=vpa_tch,
                    nav_spec=nav_spec,
                )
                session.add(proc_des_obj)
                if is_valid_data(data := row[3]):
                    if data == "Y":
                        proc_des_obj.fly_over = True
                    elif data == "N":
                        proc_des_obj.fly_over = False
                        
        else:
                data_parts = row[0].split(" \n")
                # print(data_parts)
                if data_parts[0].isdigit() or data_parts[0].endswith("°"):
                    if data_parts[0].isdigit():
                        if data_parts[-1] != 'RNP APCH':
                         data_parts.insert(-3, data_parts[-1])
                         data_parts.pop(-1)
                        #  data_parts.pop(4)
                            
                        # print(data_parts)
                        
                    elif data_parts[0].endswith("°"):
                        data_to_insert = data_parts[0] + " " + data_parts[-1]
                        data_parts.insert(4, data_to_insert)
                        data_parts.pop(0)
                        data_parts.pop(-1)
                        data_parts.pop(0)
                        data_parts.insert(2, data_parts[3])
                        data_parts.pop(4)
                        data_parts.insert(3, data_parts[4])
                        data_parts.pop(5)
                        # data_parts.pop(6)
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
