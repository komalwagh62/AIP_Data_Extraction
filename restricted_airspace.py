from model import RestrictedAirspace, session
import pandas as pd
from shapely.geometry import Point
from shapely.wkt import dumps as wkt_dumps
from shapely import wkt

# Define the URL of the webpage containing the data
EXCEL_FILE = r"C:\Users\LENOVO\Desktop\ANS_Register_Extraction\AIP_Data_Extraction\ENR final database 2201.xlsx"

def processRestrictedAirspace(df_control_airspace, type):
    for _, row in df_control_airspace.iterrows():
        RecodeID = row['RecodeID']
        ICAOCode = str(row["ICAOCode"])
        RestrictiveAirspaceDesignation = str(row["RestrictiveAirspaceDesignation"])
        MultipleCode = str(row["MultipleCode"])
        RestrictiveType = str(row["RestrictiveType"])
        SequenceNumber = str(row["SequenceNumber"])
        Level = str(row["Level"])
        TimeCode = str(row["TimeCode"])
        NOTAM = str(row["NOTAM"])
        BoundaryVia = str(row["BoundaryVia"])
        Latitude = str(row["Latitude"])
        Longitude = str(row["Longitude"])
        LatitudeDeci = str(row["LatitudeDeci"])
        LongitudeDeci = str(row["LongitudeDeci"])
        ARCOriginLatitude = str(row["ARCOriginLatitude"])
        ARCOriginLongitude = str(row["ARCOriginLongitude"])
        ARCOriginLatitudeDeci = str(row["ARCOriginLatitudeDeci"])
        ARCOriginLongitudeDeci = str(row["ARCOriginLongitudeDeci"])
        ARCDistance = str(row["ARCDistance"])
        ARCBearing = str(row["ARCBearing"])
        LowerLimit = str(row["LowerLimit"])
        LowerLimitConvert = str(row["LowerLimitConvert"])
        UnitIndicator = str(row["UnitIndicator"])
        UpperLimit = str(row["UpperLimit"])
        UpperLimitConvert = str(row["UpperLimitConvert"])
        UnitIndicator1 = str(row["UnitIndicator1"])
        RestrictiveAirspaceName = str(row["RestrictiveAirspaceName"])
        UR_restrictivePoligon = str(row["UR_restrictivePoligon"])
        
        polygon_wkt = None
        buffer_polygon= None
        
                
        if UR_restrictivePoligon.startswith("ST_Buffer"):
            try:
                buffer_info = UR_restrictivePoligon.split("ST_MakePoint(")[1].split(")")[0]
                # print(buffer_info)
                
                # print(point_str)
                # if "(" in buffer_info and ")" in buffer_info:
                coordinates = buffer_info.split(',')
                # print(coordinates)
    
                if len(coordinates) == 2:
                        longitude, latitude = map(float, coordinates)
                        # print("Longitude:", longitude)
                        # print("Latitude:", latitude)
                buffer_distance_str = UR_restrictivePoligon.split("geography,")[1].split("*")[0].strip()
                print(buffer_distance_str)  # Print buffer_distance_str before conversion

                buffer_distance = float(buffer_distance_str) / 185.2  # Conversion factor
                print(buffer_distance)

        # Create a Point object
                point = Point(longitude, latitude)
                geometry = wkt.loads(f"POINT({longitude} {latitude})")
                # print(geometry)
                ewkb_geometry = geometry.wkb_hex
                buffer_polygon = point.buffer(buffer_distance, quadsegs=8)
                print(buffer_polygon)
            
                # print(point) 

              
        # Convert the buffer polygon to Well-Known Text (WKT) format
                polygon_wkt = wkt_dumps(buffer_polygon)

                # print(f"Buffered WKT for RecodeID {RecodeID}: {polygon_wkt}")  # Debugging print
            except Exception as e:
                print(f"Error processing ST_Buffer for RecodeID {RecodeID}: {e}")
        UR_restrictivePoligon_text = str(row["UR_restrictivePoligon_text"])
        processedHigh = str(row["processedHigh"])
        processedLow = str(row["processedLow"])
        processed = str(row["processed"])
        
        restricted_airspace = RestrictedAirspace(
            RecodeID=RecodeID,
            ICAOCode=ICAOCode,
            RestrictiveAirspaceDesignation=RestrictiveAirspaceDesignation,
            MultipleCode=MultipleCode,
            RestrictiveType=RestrictiveType,
            SequenceNumber=SequenceNumber,
            Level=Level,
            TimeCode=TimeCode,
            NOTAM=NOTAM,
            BoundaryVia=BoundaryVia,
            Latitude=Latitude,
            Longitude=Longitude,
            LatitudeDeci=LatitudeDeci,
            LongitudeDeci=LongitudeDeci,
            ARCOriginLatitude=ARCOriginLatitude,
            ARCOriginLongitude=ARCOriginLongitude,
            ARCOriginLatitudeDeci=ARCOriginLatitudeDeci,
            ARCOriginLongitudeDeci=ARCOriginLongitudeDeci,
            ARCDistance=ARCDistance,
            ARCBearing=ARCBearing,
            LowerLimit=LowerLimit,
            LowerLimitConvert=LowerLimitConvert,
            UnitIndicator=UnitIndicator,
            UpperLimit=UpperLimit,
            UpperLimitConvert=UpperLimitConvert,
            UnitIndicator1=UnitIndicator1,
            RestrictiveAirspaceName=RestrictiveAirspaceName,
            UR_restrictivePoligon=UR_restrictivePoligon,
            UR_restrictivePoligon_text=UR_restrictivePoligon_text,
            processedHigh=processedHigh,
            processedLow=processedLow,
            processed=processed,
            eometry=polygon_wkt
        )

        if polygon_wkt:
            restricted_airspace.UR_restrictive = polygon_wkt
        
        session.add(restricted_airspace)

def main():
    df_restrictive_airspace = pd.read_excel(EXCEL_FILE, sheet_name="Restricted Airspace")
    processRestrictedAirspace(df_restrictive_airspace, "Restrictive Airspace")
    session.commit()

if __name__ == "__main__":
    main()