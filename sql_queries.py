import configparser

# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

study_table_drop = "DROP TABLE IF EXISTS dimStudy"
location_name_table_drop = "DROP TABLE IF EXISTS dimLocation"
date_table_drop = "DROP TABLE IF EXISTS dimDate"
name_table_drop = "DROP TABLE IF EXISTS dimName"
bat_tracking_table_drop = "DROP TABLE IF EXISTS factBatTracking"
staging_table_drop = "DROP TABLE IF EXISTS staging_tracking"

# CREATE TABLES

study_table_create = """CREATE TABLE IF NOT EXISTS dimStudy (
                            StudyID            BIGINT,
                            StudyName          VARCHAR,
                            StudyObjective     VARCHAR(MAX),
                            StudyType          VARCHAR,
                            Citation           VARCHAR(MAX)
                        );"""

location_table_create = """CREATE TABLE IF NOT EXISTS dimLocation (
                                 LocationID         BIGINT,
                                 LocationLatitude   DECIMAL,
                                 LocationLongitude  DECIMAL,
                                 StateOrProvince    VARCHAR,
                                 CountryCode        VARCHAR(2)
                           );"""

date_table_create = """CREATE TABLE IF NOT EXISTS dimDate (
                            DateObserved   DATE,
                            Day            INT,   
                            Month          INT,
                            Year           INT  
                       );"""

name_table_create = """CREATE TABLE IF NOT EXISTS dimName (
                            NameID          BIGINT,
                            SpeciesName     VARCHAR,
                            VernacularName  VARCHAR
                       );"""

bat_tracking_table_create = """CREATE TABLE IF NOT EXISTS factBatTracking (
                                StudyID                     BIGINT,
                                NameID                      BIGINT,
                                SpeciesName                 VARCHAR,
                                VernacularName              VARCHAR,
                                LocationID                  BIGINT,
                                DateObserved                DATE
                               );"""

staging_table_create = """CREATE TABLE IF NOT EXISTS staging_tracking (
                                taxonKey           BIGINT,
                                species            VARCHAR,
                                gbifID             BIGINT,
                                stateProvince      VARCHAR,
                                countryCode        VARCHAR(2),
                                decimalLatitude    DECIMAL,
                                decimalLongitude   DECIMAL,
                                eventDate          DATE,
                                day                INT,
                                month              INT,
                                year               INT,
                                id                 INT,
                                name               VARCHAR,
                                study_objective    VARCHAR(MAX),
                                study_type         VARCHAR(MAX),
                                citation           VARCHAR(MAX),
                                vernacularName     VARCHAR
                          );"""
                               
# STAGING TABLES

staging_copy = ("""COPY staging_tracking
                    FROM 's3://udacityfinalprojectanimaltracking/staging_complete.csv'
                    CREDENTIALS 'aws_iam_role={}'
                    REGION 'us-west-2'
                    CSV
                    BLANKSASNULL EMPTYASNULL
                    IGNOREHEADER 1;
                 """).format(config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

study_table_insert = """INSERT INTO dimStudy (
                            StudyID,
                            StudyName,
                            StudyObjective,
                            StudyType,
                            Citation)
                        SELECT st.id,
                               st.name,
                               st.study_objective,
                               st.study_type,
                               st.citation
                        FROM staging_tracking st
                        WHERE st.id > 0;"""

location_table_insert = """INSERT INTO dimLocation (
                                 LocationID,
                                 LocationLatitude,
                                 LocationLongitude,
                                 StateOrProvince,
                                 CountryCode)
                           SELECT st.gbifID,
                                  st.decimalLatitude,
                                  st.decimalLongitude,
                                  st.stateProvince,
                                  st.countryCode
                           FROM staging_tracking st;"""

date_table_insert = """INSERT INTO dimDate (
                            DateObserved,
                            Day,   
                            Month,
                            Year)
                       SELECT st.eventDate,
                              st.day,
                              st.month,
                              st.year                       
                       FROM staging_tracking st
                       WHERE st.year > 0 AND
                             st.month > 0 AND
                             st.day > 0;"""

name_table_insert = """INSERT INTO dimName (
                           NameID,
                           SpeciesName,
                           VernacularName)
                       SELECT st.taxonKey,
                              st.species,
                              st.vernacularName
                       FROM staging_tracking st
                       WHERE st.taxonKey > 0;"""

bat_tracking_table_insert = """INSERT INTO factBatTracking (
                                   StudyID,
                                   NameID,
                                   SpeciesName,
                                   VernacularName,
                                   LocationID,
                                   DateObserved)                       
                               SELECT st.id,
                                    st.taxonKey,
                                    st.species,
                                    st.vernacularName,
                                    st.gbifID,
                                    st.eventDate
                               FROM staging_tracking st;"""

# QUERY LISTS

create_table_queries = [study_table_create, location_table_create, date_table_create, name_table_create, bat_tracking_table_create, staging_table_create]

drop_table_queries = [study_table_drop, location_name_table_drop, date_table_drop, name_table_drop, bat_tracking_table_drop, staging_table_drop]

copy_table_queries = [staging_copy]

insert_table_queries = [study_table_insert, location_table_insert, date_table_insert, name_table_insert, bat_tracking_table_insert]