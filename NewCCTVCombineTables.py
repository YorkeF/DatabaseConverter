import sys, getopt, os, pymysql.cursors
from datetime import datetime
from config import mysql_host, mysql_user, mysql_password, mysql_database_name, current_directory


# Replace these values with your MySQL server details
#mysql_host = "localhost"
#mysql_user = "root"
#mysql_password = "admin"
#mysql_database_name = "infratie_scripts"
#current_directory = os.getcwd()


"""

Takes an SQL script, that is already in PACP format, and combines them to one database.

Usage:
python3 NewCCTVCombineTables.py -i <database_to_compile_in> -d <converted_database name>

Create a database to hold all the compiled databases
Uses this format:
CREATE TABLE `Inspections_inserted` (
  `InspectionId` int NOT NULL,
  `Surveyed_By` varchar(20) DEFAULT NULL,
  `Certificate_Number` varchar(50) DEFAULT NULL,
  `Owner` varchar(30) DEFAULT NULL,
  `Customer` varchar(30) DEFAULT NULL,
  `Drainage_Area` varchar(15) DEFAULT NULL,
  `PO_Number` varchar(15) DEFAULT NULL,
  `Pipe_Segment_Reference` varchar(25) DEFAULT NULL,
  `Date` date DEFAULT NULL,
  `Time` time(6) DEFAULT NULL,
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
  `Total_Length` decimal(7,3) DEFAULT NULL,
  `Length_Surveyed` decimal(7,3) DEFAULT NULL,
  `Year_Laid` smallint DEFAULT NULL,
  `Year_Renewed` smallint DEFAULT NULL,
  `Media_Label` varchar(64) DEFAULT NULL,
  `Purpose` varchar(64) DEFAULT NULL,
  `Sewer_Category` varchar(10) DEFAULT NULL,
  `Pre_Cleaning` varchar(15) DEFAULT NULL,
  `Date_Cleaned` datetime(6) DEFAULT NULL,
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
  PRIMARY KEY (`InspectionId`)
)

CREATE TABLE `Conditions_inserted` (
  `ConditionId` int NOT NULL,
  `InspectionId` int DEFAULT NULL,
  `Distance` int DEFAULT NULL,
  `Counter` int DEFAULT NULL,
  `PACP_Code` longtext,
  `Continuous` longtext,
  `Value_1st_Dimension` int DEFAULT NULL,
  `Value_2nd_Dimension` int DEFAULT NULL,
  `Value_Percent` longtext,
  `Joint` int DEFAULT NULL,
  `Clock_At_From` int DEFAULT NULL,
  `Clock_To` int DEFAULT NULL,
  `Remarks` longtext,
  `VCR_Time` longtext,
  PRIMARY KEY (`ConditionId`)
)

CREATE TABLE `Media_Inspections_inserted` (
  `MediaID` int NOT NULL,
  `InspectionId` int DEFAULT NULL,
  `Video_Name` longtext,
  `Video_Location` longtext,
  PRIMARY KEY (`MediaID`)
)

CREATE TABLE `Media_Conditions_inserted` (
  `MediaID` int NOT NULL,
  `ConditionId` int DEFAULT NULL,
  `Image_Name` longtext,
  `Image_Location` longtext,
  PRIMARY KEY (`MediaID`)
)


"""


def transfer_to_database(inputfile, database):
    connection = pymysql.connect(host=mysql_host,
                                 user=mysql_user,
                                 password=mysql_password,
                                 database=inputfile,
                                 cursorclass=pymysql.cursors.DictCursor)

    with connection:
        with connection.cursor() as cursor:
            # Read a single record
            sql = "SELECT `InspectionID` FROM `inspections` ORDER BY `InspectionID` DESC LIMIT 1"
            cursor.execute(sql)
            result = cursor.fetchone()
            try:
                inspectionPK = int(result["InspectionID"])
            except:
                inspectionPK = 0

            sql = "SELECT `ConditionID` FROM `conditions` ORDER BY `ConditionID` DESC LIMIT 1"
            cursor.execute(sql)
            result = cursor.fetchone()
            try:
                conditionPK = int(result["ConditionID"])
            except:
                conditionPK = 0

            sql = "SELECT `MediaID` FROM `media_inspections` ORDER BY `MediaID` DESC LIMIT 1"
            cursor.execute(sql)
            result = cursor.fetchone()
            try:
                mediaInpsectionsID = int(result["MediaID"])
            except:
                mediaInpsectionsID = 0

            sql = "SELECT `MediaID` FROM `media_conditions` ORDER BY `MediaID` DESC LIMIT 1"
            cursor.execute(sql)
            result = cursor.fetchone()
            try:
                mediaConditionsID = int(result["MediaID"])
            except:
                mediaConditionsID = 0

    connection = pymysql.connect(host=mysql_host,
                                 user=mysql_user,
                                 password=mysql_password,
                                 database=database,
                                 cursorclass=pymysql.cursors.DictCursor)
    with connection:
        with connection.cursor() as cursor:

            sql = "SELECT * FROM inspections"
            cursor.execute(sql)
            results = cursor.fetchall()
            for result in results:
                insertInspections(result, database, inspectionPK, inputfile)

            sql = "SELECT * FROM Conditions"
            cursor.execute(sql)
            results = cursor.fetchall()

            for result in results:
                insertConditions(result, database, conditionPK, inspectionPK, inputfile)

            sql = "SELECT * FROM Media_Inspections"
            cursor.execute(sql)
            results = cursor.fetchall()

            for result in results:
                insertMediaInspections(result, database, mediaInpsectionsID, inspectionPK, inputfile)

            sql = "SELECT * FROM Media_Conditions"
            cursor.execute(sql)
            results = cursor.fetchall()

            for result in results:
                insertMediaConditions(result, database, mediaConditionsID, conditionPK, inputfile)


def insertInspections(result, database, inspectionPK, inputfile):
    connection = pymysql.connect(host=mysql_host,
                                 user=mysql_user,
                                 password=mysql_password,
                                 database=inputfile,
                                 cursorclass=pymysql.cursors.DictCursor)
    with connection:
        with connection.cursor() as cursor:
            sql = "INSERT INTO inspections VALUES ( {InspectionId}, \"{Surveyed_By}\", \"{Owner}\", \"{Certificate_Number}\", \"{Customer}\", \"{Drainage_Area}\", \"{PO_Number}\", \"{Pipe_Segment_Reference}\", \"{Date}\", \"{Time}\", \"{Street}\", \"{City}\", \"{Location_Details}\", \"{Upstream_MH}\", {Up_Rim_to_Invert}, {Up_Grade_to_Invert}, {Up_Rim_to_Grade}, \"{Downstream_MH}\", {Down_Rim_to_Invert}, {Down_Grade_to_Invert}, {Down_Rim_to_Grade}, \"{Sewer_Use}\", \"{Direction}\", \"{Flow_Control}\", {Height}, {Width}, \"{Shape}\", \"{Material}\", \"{Lining_Method}\", {Pipe_Joint_Length}, {Total_Length}, {Length_Surveyed}, {Year_Laid}, {Year_Renewed}, \"{Media_Label}\", \"{Purpose}\", \"{Sewer_Category}\", \"{Pre_Cleaning}\", \"{Date_Cleaned}\", \"{Weather}\", \"{Location_Code}\", \"{Additional_Info}\", {Reverse_Setup}, {Sheet_Number}, {IsImperial}, {PressureValue}, \"{WorkOrder}\", \"{Project}\", \"{Northing}\", \"{Easting}\", \"{Elevation}\", \"{Coordinate_System}\", \"{GPS_Accuracy}\", \"{Data_Standard}\", \"{Organization}\", \"{filename}\")".format(
                InspectionId=result["InspectionId"] + inspectionPK,
                Surveyed_By=result["Surveyed_By"],

                Owner=result["Owner"],
                Certificate_Number=result["Certificate_Number"],
                Customer=result["Customer"],
                Drainage_Area=result["Drainage_Area"],
                PO_Number=result["PO_Number"],
                Pipe_Segment_Reference=result["Pipe_Segment_Reference"],
                Date=result["Date"],
                Time=result["Time"],
                Street=result["Street"],
                City=result["City"],
                Location_Details=result["Location_Details"],
                Upstream_MH=result["Upstream_MH"],
                Up_Rim_to_Invert=result["Up_Rim_to_Invert"],
                Up_Grade_to_Invert=result["Up_Grade_to_Invert"],
                Up_Rim_to_Grade=result["Up_Rim_to_Grade"],
                Downstream_MH=result["Downstream_MH"],
                Down_Rim_to_Invert=result["Down_Rim_to_Invert"],
                Down_Grade_to_Invert=result["Down_Grade_to_Invert"],
                Down_Rim_to_Grade=result["Down_Rim_to_Grade"],
                Sewer_Use=result["Sewer_Use"],
                Direction=result["Direction"],
                Flow_Control=result["Flow_Control"],
                Height=result["Height"],
                Width=result["Width"],
                Shape=result["Shape"],
                Material=result["Material"],
                Lining_Method=result["Lining_Method"],
                Pipe_Joint_Length=result["Pipe_Joint_Length"],
                Total_Length=result["Total_Length"],
                Length_Surveyed=result["Length_Surveyed"],
                Year_Laid=result["Year_Laid"],
                Year_Renewed=result["Year_Renewed"],
                Media_Label=result["Media_Label"],
                Purpose=result["Purpose"],
                Sewer_Category=result["Sewer_Category"],
                Pre_Cleaning=result["Pre_Cleaning"],
                Date_Cleaned=result["Date_Cleaned"],
                Weather=result["Weather"],
                Location_Code=result["Location_Code"],
                Additional_Info=result["Additional_Info"],
                Reverse_Setup=result["Reverse_Setup"],
                Sheet_Number=result["Sheet_Number"],
                IsImperial=result["IsImperial"],
                PressureValue=result["PressureValue"],
                WorkOrder=result["WorkOrder"],
                Project=result["Project"],
                Northing=result["Northing"],
                Easting=result["Easting"],
                Elevation=result["Elevation"],
                Coordinate_System=result["Coordinate_System"],
                GPS_Accuracy=result["GPS_Accuracy"],
                Data_Standard=result["Data_Standard"],
                Organization=result["Organization"],
                filename=database

                # Organization = "null"
            )
            sql = sql.replace(", \"null\"", ", null")
            sql = sql.replace(", \"None\"", ", null")
            sql = sql.replace(", None", ", null")
            sql = sql.replace(", \"None\"", ", null")

            # sql = sql.replace(', "null"', ", null")
            cursor.execute(sql)
            connection.commit()


def insertConditions(result, database, conditionPK, inspectionPK, inputfile):
    connection = pymysql.connect(host=mysql_host,
                                 user=mysql_user,
                                 password=mysql_password,
                                 database=inputfile,
                                 cursorclass=pymysql.cursors.DictCursor)

    with connection:
        with connection.cursor() as cursor:
            sql = "INSERT INTO conditions VALUES ({ConditionId}, {InspectionId}, {Distance}, {Counter}, \"{PACP_Code}\", \"{Continuous}\", {Value_1st_Dimension}, {Value_2nd_Dimension}, \"{Value_Percent}\", {Joint}, {Clock_At_From}, {Clock_To}, \"{Remarks}\", \"{VCR_Time}\", \"{GradeO}\", \"{ScoreO}\", \"{GradeS}\", \"{ScoreS}\", \"{GradeH}\", \"{ScoreH}\")".format(
                ConditionId=result["ConditionId"] + conditionPK,
                InspectionId=result["InspectionId"] + inspectionPK,
                Distance=result["Distance"],
                Counter=result["Counter"],
                PACP_Code=result["PACP_Code"],
                Continuous=result["Continuous"],
                Value_1st_Dimension=result["Value_1st_Dimension"],
                Value_2nd_Dimension=result["Value_2nd_Dimension"],
                Value_Percent=result["Value_Percent"],
                Joint=result["Joint"],
                Clock_At_From=result["Clock_At_From"],
                Clock_To=result["Clock_To"],
                Remarks=result["Remarks"],
                VCR_Time=result["VCR_Time"],
                GradeO=result["GradeO"],
                ScoreO=result["ScoreO"],
                GradeS=result["GradeS"],
                ScoreS=result["ScoreS"],
                GradeH=result["GradeH"],
                ScoreH=result["ScoreH"]
            )
            sql = sql.replace(", \"null\"", ", null")
            sql = sql.replace(", \"None\"", ", null")
            sql = sql.replace(", None", ", null")
            sql = sql.replace(", \"None\"", ", null")
            # print(sql)

            cursor.execute(sql)
            connection.commit()


def insertMediaInspections(result, database, mediaInpsectionsID, inspectionPK, inputfile):
    connection = pymysql.connect(host=mysql_host,
                                 user=mysql_user,
                                 password=mysql_password,
                                 database=inputfile,
                                 cursorclass=pymysql.cursors.DictCursor)

    with connection:
        with connection.cursor() as cursor:
            sql = "INSERT INTO Media_Inspections VALUES ({MediaID}, {InspectionId}, \"{Video_Name}\", \"{Video_Location}\")".format(
                MediaID=result["MediaID"] + mediaInpsectionsID, InspectionId=result["InspectionId"] + inspectionPK,
                Video_Name=result["Video_Name"],
                Video_Location="http://10.221.233.1:8080/media_inspections/Stillwater/")
            # print(sql)
            cursor.execute(sql)
            connection.commit()


def insertMediaConditions(result, database, mediaConditionsID, conditionPK, inputfile):
    connection = pymysql.connect(host=mysql_host,
                                 user=mysql_user,
                                 password=mysql_password,
                                 database=inputfile,
                                 cursorclass=pymysql.cursors.DictCursor)

    with connection:
        with connection.cursor() as cursor:
            sql = "INSERT INTO Media_Conditions VALUES ({MediaID}, {ConditionId}, \"{Image_Name}\", \"{Image_Location}\")".format(
                MediaID=result["MediaID"] + mediaConditionsID, ConditionId=result["ConditionId"] + conditionPK,
                Image_Name=result["Image_Name"], Image_Location=result["Image_Location"])
            # print(sql)
            cursor.execute(sql)
            connection.commit()
