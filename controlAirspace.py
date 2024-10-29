from model import ControlAirspace, session
import pandas as pd
from shapely.geometry import Point
from shapely.wkt import dumps as wkt_dumps
from shapely import wkt

# Define the URL of the webpage containing the data
EXCEL_FILE = r"C:\Users\LENOVO\Desktop\ANS_Register_Extraction\AIP_Data_Extraction\ENR final database 2201.xlsx"

def processControlAirspace(df_control_airspace, type):
    for _, row in df_control_airspace.iterrows():
        ICAOCode = str(row["ICAOCode"])
        AirspaceType = str(row["AirspaceType"])
        AirspaceCenter = str(row["AirspaceCenter"])
        AirspaceClassification = str(row["AirspaceClassification"])
        ControlledAirspaceName = str(row["ControlledAirspaceName"])
        UC_AirspacePolygon = str(row["UC_AirspacePolygon"])
        LowerLimit = str(row["LowerLimit"])
        UpperLimit = str(row["UpperLimit"])
        
        polygon_wkt = None
        # buffer_polygon= None
        
        if UC_AirspacePolygon.startswith("ST_Buffer"):
            try:
                buffer_info = UC_AirspacePolygon.split("ST_MakePoint(")[1].split(")")[0]
                coordinates = buffer_info.split(',')

                if len(coordinates) == 2:
                    longitude, latitude = map(float, coordinates)

                buffer_distance_str = UC_AirspacePolygon.split("geography,")[1].split("*")[0].strip()
                buffer_distance = float(buffer_distance_str) / 1852  # Conversion factor from meters to nautical miles

                # Create a Point object
                point = Point(longitude, latitude)
                buffer_polygon = point.buffer(buffer_distance, quadsegs=8)
                
                # Convert the buffer polygon to Well-Known Text (WKT) format
                polygon_wkt = wkt_dumps(buffer_polygon)
            except Exception as e:
                print(f"Error processing ST_Buffer: {e}")
        
        control_airspace = ControlAirspace(
            ICAOCode=ICAOCode,
            AirspaceType=AirspaceType,
            AirspaceCenter=AirspaceCenter,
            AirspaceClassification = AirspaceClassification,
            ControlledAirspaceName=ControlledAirspaceName,
            LowerLimit = LowerLimit,
            UpperLimit=UpperLimit,
            UC_AirspacePolygon=UC_AirspacePolygon,
            geom = polygon_wkt
        )
        
        session.add(control_airspace)
        
def processControlledAirspace(df_control_airspace, type):
    for _, row in df_control_airspace.iterrows():
        ICAOCode = str(row["ICAOCode"])
        AirspaceCenter = str(row["AirspaceCenter"])
        AirspaceClassification = str(row["AirspaceClassification"])
        AirspaceType = str(row["AirspaceType"])
        LowerLimit = str(row["LowerLimit"])
        UpperLimit = str(row["UpperLimit"])
        ControlledAirspaceName = str(row["ControlledAirspaceName"])
        UC_AirspacePolygon = str(row["UC_AirspacePolygon"])
        
        
        
        
        polygon_wkt = None
        
        if UC_AirspacePolygon.startswith("Polygon"):
            try:
                coordinates_str = UC_AirspacePolygon.split("((")[1].split("))")[0]
                coordinates = [list(map(float, coord.split())) for coord in coordinates_str.split(',')]
                if all(isinstance(coord, list) for coord in coordinates):
                    coordinates.append(coordinates[0])  # Close the polygon
                    polygon_wkt = f'POLYGON(({",".join([f"{coord[0]} {coord[1]}" for coord in coordinates])}))'
                    print(polygon_wkt)
                else:
                    print(f"Invalid coordinates format for RecodeID")
            except (IndexError, ValueError) as e:
                print(f"")
        
        
        control_airspace = ControlAirspace(
            ICAOCode=ICAOCode,
            AirspaceType=AirspaceType,
            AirspaceCenter=AirspaceCenter,
            AirspaceClassification = AirspaceClassification,
            ControlledAirspaceName=ControlledAirspaceName,
            LowerLimit = LowerLimit,
            UpperLimit=UpperLimit,
            UC_AirspacePolygon=UC_AirspacePolygon,
            geom = polygon_wkt
        )
        session.add(control_airspace)

def main():
    df_control_airspace = pd.read_excel(EXCEL_FILE, sheet_name="Control Buffer")
    processControlAirspace(df_control_airspace, "Control Airspace")
    df_restrictive_airspace = pd.read_excel(EXCEL_FILE, sheet_name="Controlled Airspace copy")
    processControlledAirspace(df_restrictive_airspace, "Control Airspace")
    session.commit()

if __name__ == "__main__":
    main()
