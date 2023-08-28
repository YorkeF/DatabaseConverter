import sys, getopt, os, pymysql.cursors
from config import mysql_host, mysql_user, mysql_password, mysql_database_name, current_directory

"""
To be used after 'getInsertStatements.py'

'getInsertStatements.py' will create databases from .db3 files.
Use this script on those created databases to convert them into PACP format.

Usage:
python3 StillwaterNewCCTVConversion.py -i <name_of_database_to_convert>
"""



# Replace these values with your MySQL server details
#mysql_host = "localhost"
#mysql_user = "root"
#mysql_password = "admin"
#mysql_database_name = "infratie_scripts"
#current_directory = os.getcwd()



def main(argv):
    inputfile = argv

    connection = pymysql.connect(host=mysql_host,
                                 user=mysql_user,
                                 password=mysql_password,
                                 database=inputfile,
                                 cursorclass=pymysql.cursors.DictCursor)
    MediaInspectionPK = 1
    MediaConditionPKCount = 1
    ConditionID = 1
    with connection:
        with connection.cursor() as cursor:

            # print("Converted_"+inputfile)
            create_tables("Converted_" + inputfile)

            sql = "SELECT * FROM SECTION;"
            cursor.execute(sql)
            sectionResults = cursor.fetchall()
            for index, result in enumerate(sectionResults):

                # print(result["OBJ_FromNode_REF"])
                sql = "SELECT * FROM NODE WHERE OBJ_PK = '{OBJ_FROMNODE_REF}'".format(
                    OBJ_FROMNODE_REF=result["OBJ_FromNode_REF"])
                cursor.execute(sql)
                FromNodeResults = cursor.fetchone()

                sql = "SELECT * FROM NODE WHERE OBJ_PK = '{OBJ_TONODE_REF}'".format(
                    OBJ_TONODE_REF=result["OBJ_ToNode_REF"])
                cursor.execute(sql)
                ToNodeResults = cursor.fetchone()

                # print(FromNodeResults)
                # print(ToNodeResults)

                sql = "SELECT * FROM SECINSP WHERE INS_Section_FK = '{OBJ_PK}';".format(OBJ_PK=result["OBJ_PK"])
                cursor.execute(sql)
                SECINSPResults = cursor.fetchone()
                # print(SECINSPResults.get("INS_StartDate").date())
                sql = "INSERT INTO {db}.INSPECTIONS VALUES ({InspectionID}, \"{Surveyed_By}\", \"{Owner}\", \"{Certificate_Number}\", \"{Customer}\", \"{Drainage_Area}\", \"{PO_Number}\", \"{Pipe_Segment_Reference}\", \"{Date}\", \"{Time}\", \"{Street}\", \"{City}\", \"{Location_Details}\", \"{Upstream_MH}\", {Up_Rim_to_Invert}, {Up_Grade_to_Invert}, {Up_Rim_to_Grade}, \"{Downstream_MH}\", {Down_Rim_to_Invert}, {Down_Grade_to_Invert}, {Down_Rim_to_Grade}, \"{Sewer_Use}\", \"{Direction}\", \"{Flow_Control}\", {Height}, {Width}, \"{Shape}\", \"{Material}\", \"{Lining_Method}\", {Pipe_Joint_Length}, {Total_Length}, {Length_Surveyed}, {Year_Laid}, {Year_Renewed}, \"{Media_Label}\", \"{Purpose}\", \"{Sewer_Category}\", \"{Pre_Cleaning}\", {Date_Cleaned}, \"{Weather}\", \"{Location_Code}\", \"{Additional_Info}\", {Reverse_Setup}, {Sheet_Number}, {IsImperial}, {PressureValue}, \"{WorkOrder}\", \"{Project}\", \"{Northing}\", \"{Easting}\", \"{Elevation}\", \"{Coordinate_System}\", \"{GPS_Accuracy}\", \"{Data_Standard}\", \"{Organization}\")".format(
                    db="Converted_" + inputfile,
                    InspectionID=index + 1,
                    Surveyed_By="null",

                    Owner="null",
                    Certificate_Number="null",
                    Customer="null",
                    Drainage_Area=result.get("OBJ_DrainageArea", "null"),  # if result is not None else "null",
                    PO_Number="null",
                    Pipe_Segment_Reference=result.get("OBJ_Key", "null"),
                    Date=SECINSPResults.get("INS_StartDate").date(),
                    Time=SECINSPResults.get("INS_StartDate").time(),
                    Street=result.get("OBJ_Street", "null"),
                    City=result.get("OBJ_City", "null"),
                    Location_Details="null",
                    Upstream_MH=FromNodeResults.get("OBJ_Key", "null") if FromNodeResults is not None else "null",
                    Up_Rim_to_Invert=FromNodeResults.get("OBJ_RimToInvert",
                                                         "null") if FromNodeResults is not None else "null",
                    Up_Grade_to_Invert=FromNodeResults.get("OBJ_GradeToInvert",
                                                           "null") if FromNodeResults is not None else "null",
                    Up_Rim_to_Grade=FromNodeResults.get("OBJ_RimToInvert",
                                                        "null") if FromNodeResults is not None else "null",
                    Downstream_MH=ToNodeResults.get("OBJ_Key", "null") if ToNodeResults is not None else "null",
                    Down_Rim_to_Invert=ToNodeResults.get("OBJ_RimToInvert",
                                                         "null") if ToNodeResults is not None else "null",
                    Down_Grade_to_Invert=ToNodeResults.get("OBJ_GradeToInvert",
                                                           "null") if ToNodeResults is not None else "null",
                    Down_Rim_to_Grade=ToNodeResults.get("OBJ_RimToInvert",
                                                        "null") if ToNodeResults is not None else "null",
                    Sewer_Use=result.get("OBJ_Usage", "null"),
                    Direction=SECINSPResults.get("INS_InspectionDir", "null"),
                    Flow_Control="null",
                    Height=result.get("OBJ_Size1", "null"),
                    Width=result.get("OBJ_Size2", "null"),
                    Shape=result.get("OBJ_Shape", "null"),
                    Material=result.get("OBJ_Material", "null"),
                    Lining_Method=result.get("OBJ_Lining", "null"),
                    Pipe_Joint_Length=result.get("OBJ_PipeJointLength", "null"),
                    Total_Length=result.get("OBJ_Length", "null"),
                    Length_Surveyed=SECINSPResults.get("INS_InspectedLength", "null"),
                    Year_Laid=result.get("OBJ_ConstructionDate", "null"),
                    Year_Renewed=result.get("OBJ_RenewDate", "null"),
                    Media_Label="null",
                    Purpose=SECINSPResults.get("INS_Purpose", "null"),
                    Sewer_Category="null",
                    Pre_Cleaning=SECINSPResults.get("INS_CleanedMethod", "null"),
                    Date_Cleaned=SECINSPResults.get("INS_CleanedDate", "null"),
                    Weather=SECINSPResults.get("INS_Weather", "null"),
                    Location_Code="null",
                    Additional_Info="null",
                    Reverse_Setup="null",
                    Sheet_Number="null",
                    IsImperial="null",
                    PressureValue="null",
                    WorkOrder="null",
                    Project="null",
                    Northing="null",
                    Easting="null",
                    Elevation="null",
                    Coordinate_System="null",
                    GPS_Accuracy="null",
                    Data_Standard=SECINSPResults.get("INS_RateText", "null"),
                    Organization=result.get("OBJ_City", "null")
                )
                sql = sql.replace(", 'null'", ", null")
                sql = sql.replace(", 'None'", ", null")
                sql = sql.replace(", None", ", null")
                sql = sql.replace("None", "null")
                # sql = sql.replace(" 'None',", " null,")
                # sql = sql.replace(" None,", " null,")
                # sql = sql.replace(" 'null',", " null")

                cursor.execute(sql)
                connection.commit()
                print(SECINSPResults.get("INS_StartDate").date())

                sql = "SELECT * FROM SECOBS WHERE OBS_Inspection_FK = '{SECINSP_PK}' ORDER BY OBS_DISTANCE".format(
                    SECINSP_PK=SECINSPResults["INS_PK"])
                cursor.execute(sql)
                SECOBSResults = cursor.fetchall()

                sql = "SELECT * FROM SECOBSMM WHERE OMM_FILETYPE = 'MP4' OR OMM_FILETYPE = 'mp4'"
                cursor.execute(sql)
                InspectionMediaResults = cursor.fetchall()
                for InspectionMediaResult in InspectionMediaResults:
                    sql = "SELECT OBJ_PK FROM SECTION WHERE OBJ_PK = (SELECT INS_Section_FK FROM SECINSP WHERE INS_PK = (SELECT OBS_Inspection_FK FROM SECOBS WHERE OBS_PK = (SELECT OMM_Observation_FK FROM SECOBSMM WHERE OMM_PK = '{PK}')));".format(
                        PK=InspectionMediaResult["OMM_PK"])
                    cursor.execute(sql)
                    tempOBJ_PK = cursor.fetchone()
                    if (result["OBJ_PK"] == tempOBJ_PK["OBJ_PK"]):
                        sql = "INSERT INTO {db}.MEDIA_INSPECTIONS VALUES ({MediaInspectionID}, {InspectionID}, '{Video_Name}', 'media_inspections/Stillwater/')".format(
                            db="Converted_" + inputfile, MediaInspectionID=MediaInspectionPK, InspectionID=index + 1,
                            Video_Name=InspectionMediaResult["OMM_FileName"])
                        sql = sql.replace("None", "null")
                        cursor.execute(sql)
                        connection.commit()
                        MediaInspectionPK += 1
                        # print(sql)

                # sql = "SELECT OBJ_PK FROM SECTION WHERE OBJ_PK = (SELECT INS_Section_FK FROM SECINSP WHERE INS_PK = (SELECT OBS_Inspection_FK FROM SECOBS WHERE OBS_PK = (SELECT OMM_Observation_FK FROM SECOBSMM WHERE OMM_PK = {PK})));".format()
                # cursor.execute(sql)
                # InspectionMediaResults = cursor.fetchone()

                # if (result["OBJ_PK"] == InspectionMediaResults["OBJ_PK"]):

                #     for InspectionMediaResult in InspectionMediaResults:
                #         if (result["OBJ_PK"] == InspectionMediaResult["OBJ_PK"]):
                #             sql = "INSERT INTO MEDIA_INSPECTIONS VALUES ({MediaInspectionID}, {InspectionID}, {Video_Name}, 'media_inspections/Stillwater/')".format(MediaInspectionID = index + 1, InspectionID = index + 1, Video_Name = InspectionMediaResult["OMM_FileName"])
                #             print(sql)

                for SECOBSResult in SECOBSResults:

                    sql = "SELECT * FROM SECOBSMM WHERE OMM_Observation_FK = '{PrimaryKey}' AND (OMM_FileType != 'MP4' OR OMM_FileType != 'mp4')".format(
                        PrimaryKey=SECOBSResult["OBS_PK"])
                    cursor.execute(sql)
                    SECOBSMMResults = cursor.fetchall()
                    OBS_AtJoint = SECOBSResult["OBS_AtJoint"]
                    if OBS_AtJoint is not None and len(OBS_AtJoint) > 0:
                        NewJoint = ord(OBS_AtJoint)
                    else:
                        # Handle the case where OBS_AtJoint is None or an empty string
                        NewJoint = None  # or assign a default value as needed
                    # print(SECOBSMMResults)

                    for SECOBSMMResult in SECOBSMMResults:
                        sql = "INSERT INTO {db}.MEDIA_CONDITIONS VALUES ({MediaConditionID}, {ConditionID}, '{Image_Reference}', 'media_conditions/Stillwater/')".format(
                            db="Converted_" + inputfile, MediaConditionID=MediaConditionPKCount,
                            ConditionID=ConditionID, Image_Reference=SECOBSMMResult["OMM_FileName"])
                        sql = sql.replace("None", "null")
                        cursor.execute(sql)
                        connection.commit()
                        # print(sql)
                        MediaConditionPKCount += 1
                    sql = "INSERT INTO {db}.CONDITIONS VALUES ({ConditionID}, {InspectionID}, {Distance}, {Counter}, \"{PACP_Code}\", \"{Continuous}\", {Value_1st_Dimension}, {Value_2nd_Dimension}, \"{Value_Percent}\", {Joint}, {Clock_At_From}, {Clock_To}, \"{Remarks}\", \"{VCR_Time}\", {GradeO}, {ScoreO}, {GradeS}, {ScoreS}, {GradeH}, {ScoreH})".format(
                        db="Converted_" + inputfile,
                        ConditionID=ConditionID,
                        InspectionID=index + 1,
                        Distance=SECOBSResult["OBS_Distance"],
                        Counter="null",
                        PACP_Code=SECOBSResult["OBS_OpCode"],
                        Continuous=SECOBSResult["OBS_CD_Code1"],
                        Value_1st_Dimension=SECOBSResult["OBS_Q1_Value"],
                        Value_2nd_Dimension=SECOBSResult["OBS_Q2_Value"],
                        Value_Percent=SECOBSResult["OBS_Q3_Value"],
                        Joint=NewJoint,
                        # Joint = ord(SECOBSResult["OBS_AtJoint"]),
                        Clock_At_From=SECOBSResult["OBS_ClockPos1"],
                        Clock_To=SECOBSResult["OBS_ClockPos2"],
                        Remarks=SECOBSResult["OBS_Memo"],
                        VCR_Time=SECOBSResult["OBS_TimeCtr"],
                        GradeO=SECOBSResult["OBS_GradeO"],
                        ScoreO=SECOBSResult["OBS_ScoreO"],
                        GradeS=SECOBSResult["OBS_GradeS"],
                        ScoreS=SECOBSResult["OBS_ScoreS"],
                        GradeH=SECOBSResult["OBS_GradeH"],
                        ScoreH=SECOBSResult["OBS_ScoreH"]

                    )
                    sql = sql.replace(", 'null'", ", null")
                    sql = sql.replace(", 'None'", ", null")
                    sql = sql.replace(", None", ", null")
                    # sql = sql.replace(None, "null")
                    sql = sql.replace("None", "null")
                    sql = sql.replace('null', "null")
                    # print(SECOBSMMResult["OBS_GradeO"])
                    cursor.execute(sql)
                    connection.commit()
                    ConditionID += 1

                # sql.format(InspectionID=1)
                # sql.format(Surveyed_By=1)
                # sql.format(InspectionID=, Surveyed_By= , Certificate_Number= , Owner= , Customer= , Drainage_Area= , PO_Number= , Pipe_Segment_Reference= , Date= , Time= , Street= , City= , Location_Details= , Upstream_MH= , Up_Rim_to_Invert= , Up_Grade_to_Invert= , Up_Rim_to_Grade= , Downstream_MH= , Down_Rim_to_Invert= , Down_Grade_to_Invert= , Down_Rim_to_Grade= , Sewer_Use= , Direction= , Flow_Control= , Height= , Width= , Shape= , Material= , Lining_Method= , Pipe_Joint_Length= , Total_Length= , Length_Surveyed= , Year_Laid= , Year_Renewed= , Media_Label= , Purpose= , Sewer_Category= , Pre_Cleaning= , Date_Cleaned= , Weather= , Location_Code= , Additional_Info= , Reverse_Setup= , Sheet_Number= , IsImperial= , PressureValue= , WorkOrder= , Project= , Northing= , Easting= , Elevation= , Coordinate_System= , GPS_Accuracy)
                # print(result)


def create_tables(database):
    create_database(database)
    connection = pymysql.connect(host=mysql_host,
                                 user=mysql_user,
                                 password=mysql_password,
                                 database=database,
                                 cursorclass=pymysql.cursors.DictCursor)
    with connection:
        with connection.cursor() as cursor:
            sql = """
            CREATE TABLE `Inspections` (
                `InspectionId` int NOT NULL,
                `Surveyed_By` varchar(20) DEFAULT NULL,

                `Owner` varchar(30) DEFAULT NULL,
                `Certificate_Number` varchar(100) DEFAULT NULL,
                `Customer` varchar(30) DEFAULT NULL,
                `Drainage_Area` varchar(15) DEFAULT NULL,
                `PO_Number` varchar(15) DEFAULT NULL,
                `Pipe_Segment_Reference` varchar(25) DEFAULT NULL,
                `Date` varchar(100) DEFAULT NULL,
                `Time` varchar(100) DEFAULT NULL,
                `Street` varchar(64) DEFAULT NULL,
                `City` varchar(64) DEFAULT NULL,
                `Location_Details` varchar(255) DEFAULT NULL,
                `Upstream_MH` varchar(25) DEFAULT NULL,
                `Up_Rim_to_Invert` decimal(7,3) DEFAULT NULL,
                `Up_Grade_to_Invert` decimal(7,3) DEFAULT NULL,
                `Up_Rim_to_Grade` decimal(7,3) DEFAULT NULL,
                `Downstream_MH` varchar(25) DEFAULT NULL,
                `Down_Rim_to_Invert` decimal(7,3) DEFAULT NULL,
                `Down_Grade_to_Invert` decimal(7,3) DEFAULT NULL,
                `Down_Rim_to_Grade` decimal(7,3) DEFAULT NULL,
                `Sewer_Use` varchar(15) DEFAULT NULL,
                `Direction` varchar(10) DEFAULT NULL,
                `Flow_Control` varchar(25) DEFAULT NULL,
                `Height` smallint DEFAULT NULL,
                `Width` smallint DEFAULT NULL,
                `Shape` varchar(15) DEFAULT NULL,
                `Material` varchar(64) DEFAULT NULL,
                `Lining_Method` varchar(30) DEFAULT NULL,
                `Pipe_Joint_Length` decimal(7,3) DEFAULT NULL,
                `Total_Length` decimal(7,1) DEFAULT NULL,
                `Length_Surveyed` decimal(7,1) DEFAULT NULL,
                `Year_Laid` smallint DEFAULT NULL,
                `Year_Renewed` smallint DEFAULT NULL,
                `Media_Label` varchar(64) DEFAULT NULL,
                `Purpose` varchar(64) DEFAULT NULL,
                `Sewer_Category` varchar(10) DEFAULT NULL,
                `Pre_Cleaning` varchar(15) DEFAULT NULL,
                `Date_Cleaned` varchar(100) DEFAULT NULL,
                `Weather` varchar(12) DEFAULT NULL,
                `Location_Code` varchar(30) DEFAULT NULL,
                `Additional_Info` longtext,
                `Reverse_Setup` smallint DEFAULT NULL,
                `Sheet_Number` smallint DEFAULT NULL,
                `IsImperial` smallint DEFAULT NULL,
                `PressureValue` decimal(7,3) DEFAULT NULL,
                `WorkOrder` varchar(20) DEFAULT NULL,
                `Project` varchar(64) DEFAULT NULL,
                `Northing` varchar(50) DEFAULT NULL,
                `Easting` varchar(50) DEFAULT NULL,
                `Elevation` varchar(50) DEFAULT NULL,
                `Coordinate_System` varchar(50) DEFAULT NULL,
                `GPS_Accuracy` varchar(50) DEFAULT NULL,
                `Data_Standard` varchar(50) DEFAULT NULL,
                `Organization` varchar(45) DEFAULT NULL,
                PRIMARY KEY (`InspectionId`)
            )
            """
            cursor.execute(sql)
            sql = """
            CREATE TABLE `Conditions` (
                `ConditionId` int NOT NULL,
                `InspectionId` int DEFAULT NULL,
                `Distance` decimal(7,1) DEFAULT NULL,
                `Counter` int DEFAULT NULL,
                `PACP_Code` longtext DEFAULT NULL,
                `Continuous` longtext DEFAULT NULL,
                `Value_1st_Dimension` int DEFAULT NULL,
                `Value_2nd_Dimension` int DEFAULT NULL,
                `Value_Percent` longtext DEFAULT NULL,
                `Joint` int DEFAULT NULL,
                `Clock_At_From` int DEFAULT NULL,
                `Clock_To` int DEFAULT NULL,
                `Remarks` longtext DEFAULT NULL,
                `VCR_Time` longtext DEFAULT NULL,
                `GradeO` int DEFAULT NULL,
                `ScoreO` int DEFAULT NULL,
                `GradeS` int DEFAULT NULL,
                `ScoreS` int DEFAULT NULL,
                `GradeH` int DEFAULT NULL,
                `ScoreH` int DEFAULT NULL,
                PRIMARY KEY (`ConditionId`)
            )
            """
            cursor.execute(sql)
            sql = """
            CREATE TABLE `Media_Inspections` (
                `MediaID` int NOT NULL,
                `InspectionId` int DEFAULT NULL,
                `Video_Name` longtext DEFAULT NULL,
                `Video_Location` longtext DEFAULT NULL,
                PRIMARY KEY (`MediaID`)
                )
            """
            cursor.execute(sql)
            sql = """
            CREATE TABLE `Media_Conditions` (
                `MediaID` int NOT NULL,
                `ConditionId` int DEFAULT NULL,
                `Image_Name` longtext DEFAULT NULL,
                `Image_Location` longtext DEFAULT NULL,
                PRIMARY KEY (`MediaID`)
                )
            """
            cursor.execute(sql)


def create_database(database):
    connection = pymysql.connect(host=mysql_host,
                                 user=mysql_user,
                                 password=mysql_password,
                                 cursorclass=pymysql.cursors.DictCursor)
    with connection:
        with connection.cursor() as cursor:
            sql = "CREATE DATABASE {db}".format(db=database)
            cursor.execute(sql)


if __name__ == "__main__":
    main(sys.argv[1:])
