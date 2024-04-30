USE ROLE TRAINING_ROLE;

CREATE DATABASE IF NOT EXISTS VIPER_db;
CREATE SCHEMA IF NOT EXISTS VIPER_db.cdc_lab;
USE SCHEMA VIPER_db.cdc_lab;

CREATE WAREHOUSE IF NOT EXISTS VIPER_wh;
USE WAREHOUSE VIPER_wh;


-- 3.2.0   Create the tables
--         Use your worksheet and SQL to create the tables.

-- 3.2.1   First create a table what will you will use to contain the data.
--         This table does need Update_Timestamp column. This reflects when a
--         row was inserted/updated or deleted.
drop table if exists CUR_GLF_Golfers;

CREATE OR REPLACE TABLE CUR_GLF_Golfers
(Golfer_ID   INTEGER
,First_Name   VARCHAR(100)
,Last_Name    VARCHAR(100)
,Middle_Initial  VARCHAR(1)
,Date_Of_Birth DATE
,Email_Address Varchar(100)
,DML_TYPE_CODE VARCHAR(1)
,AUD_TYPE VARCHAR(2)
,LOAD_KEY_HASH VARCHAR 
,JOB_EXECUTION_ID Integer
,CREATE_TIMESTAMP  timestamp_ntz
,LOAD_TIMESTAMP    timestamp_ntz
);


-- 3.2.2   Next create the history table.
drop table if exists ARC_GLF_Golfers;

CREATE OR REPLACE TABLE ARC_GLF_Golfers
(Golfer_ID   INTEGER
,First_Name   VARCHAR(100)
,Last_Name    VARCHAR(100)
,Middle_Initial  VARCHAR(1)
,Date_Of_Birth DATE
,Email_Address Varchar(100)
,DML_TYPE_CODE VARCHAR(1)
,AUD_TYPE VARCHAR(2)
,LOAD_KEY_HASH VARCHAR 
,JOB_EXECUTION_ID Integer
,CREATE_TIMESTAMP  timestamp_ntz
,LOAD_TIMESTAMP    timestamp_ntz
);

drop table  if exists  LND_GLF_Golfers;

CREATE OR REPLACE TABLE LND_GLF_Golfers
(Golfer_ID   INTEGER
,First_Name   VARCHAR(100)
,Last_Name    VARCHAR(100)
,Middle_Initial  VARCHAR(1)
,Date_Of_Birth DATE
,Email_Address Varchar(100)
,LOAD_KEY_HASH VARCHAR
,JOB_EXECUTION_ID Integer
,CREATE_TIMESTAMP  timestamp_ntz
,LOAD_TIMESTAMP    timestamp_ntz
);

-- 3.3.0   Create some data   (This is initial load into "CUR")
SET create_timestamp = current_timestamp()::timestamp_ntz;
SET update_timestamp = current_timestamp()::timestamp_ntz;
SET JOB_EXEC_ID = 100;
--BEGIN;
INSERT INTO CUR_GLF_Golfers VALUES
 (1,'Arnold','Palmer',NULL,to_date('1929-09-10','YYYY-MM-DD'), 'arnold.palmer@snowflakeuni.com','I','PT','1',$JOB_EXEC_ID,$create_timestamp,$update_timestamp)
,(2,'Ben','Hogan',NULL,to_date('1984-05-21','YYYY-MM-DD'), 'ben.hogan@snowflakeuni.com','I','PT','2',$JOB_EXEC_ID,$create_timestamp,$update_timestamp)
,(3,'Greg','Norman',NULL,to_date('1955-02-10','YYYY-MM-DD'), 'greg.norman@snowflakeuni.com','I','PT','3',$JOB_EXEC_ID,$create_timestamp,$update_timestamp)
,(4,'Dustin','Johnson',NULL,to_date('1981-09-13','YYYY-MM-DD'), 'dustin.johnson@snowflakeuni.com','I','PT','4',$JOB_EXEC_ID,$create_timestamp,$update_timestamp)
,(5,'Bernhard','Langer',NULL,to_date('1957-08-27','YYYY-MM-DD'), 'bernhard.langer@snowflakeuni.com','I','PT','5',$JOB_EXEC_ID,$create_timestamp,$update_timestamp);
COMMIT;

SELECT * FROM CUR_GLF_Golfers;
-- wait a few seconds to get new timestamps

--the load into CLN is the load from a landing table with same format as source (amap).  the timestamps are loaded 

SET create_timestamp = current_timestamp()::timestamp_ntz;
SET update_timestamp = current_timestamp()::timestamp_ntz;
SET JOB_EXEC_ID = 101;

INSERT INTO LND_GLF_Golfers VALUES
 (1,'Arnold','Palmer',NULL,to_date('1929-09-10','YYYY-MM-DD'), 'arnold.palmer@snowflakeuni.com','1',$JOB_EXEC_ID,$create_timestamp,$update_timestamp)
,(3,'Greg','Norman',NULL,to_date('1955-02-10','YYYY-MM-DD'), 'the_shark@snowflakeuni.com','3',$JOB_EXEC_ID,$create_timestamp,$update_timestamp)
,(4,'Dustin','Johnson',NULL,to_date('1981-09-13','YYYY-MM-DD'), 'dustin.johnson@snowflakeuni.com','4',$JOB_EXEC_ID,$create_timestamp,$update_timestamp)
,(5,'Bernhard','Langer',NULL,to_date('1957-08-27','YYYY-MM-DD'), 'bernhard.langer@snowflakeuni.com','5',$JOB_EXEC_ID,$create_timestamp,$update_timestamp)
,(6,'Tiger','Woods',NULL,to_date('1967-08-27','YYYY-MM-DD'), 'tiger@snowflakeuni.com','6',$JOB_EXEC_ID,$create_timestamp,$update_timestamp);
COMMIT;


SELECT * FROM LND_GLF_Golfers;

-- 3.3.1   Now compare new data from CLN table Golfers.

---Inserts, then Updates, then Deletes

SET create_timestamp = current_timestamp()::timestamp_ntz;
SET update_timestamp = current_timestamp()::timestamp_ntz;

INSERT INTO ARC_GLF_Golfers
	(DML_TYPE_CODE,AUD_TYPE, JOB_EXECUTION_ID, CREATE_TIMESTAMP, LOAD_TIMESTAMP, LOAD_KEY_HASH, 
    Golfer_ID,First_Name,Last_Name ,Middle_Initial ,Date_Of_Birth ,Email_Address)   
							SELECT 'U' AS DML_TYPE_CODE,'UP' AS AUD_TYPE,  l.JOB_EXECUTION_ID 
								AS JOB_EXECUTION_ID, CURRENT_TIMESTAMP, $update_timestamp, l.LOAD_KEY_HASH,
                                l.Golfer_ID,l.First_Name,l.Last_Name ,l.Middle_Initial ,l.Date_Of_Birth ,l.Email_Address
							FROM CUR_GLF_Golfers AS c   
								JOIN LND_GLF_Golfers  AS l ON c.LOAD_KEY_HASH = l.LOAD_KEY_HASH
							WHERE c.First_Name <> l.First_Name or 
                                  c.Last_Name <> l.Last_Name or 
                                  c.Middle_Initial <> l.Middle_Initial or 
                                  c.Date_Of_Birth <> l.Date_Of_Birth or 
                                  c.Email_Address  <> l.Email_Address;

INSERT INTO ARC_GLF_Golfers
	(DML_TYPE_CODE,AUD_TYPE, JOB_EXECUTION_ID, CREATE_TIMESTAMP, LOAD_TIMESTAMP, LOAD_KEY_HASH, 
    Golfer_ID,First_Name,Last_Name ,Middle_Initial ,Date_Of_Birth ,Email_Address)   
							SELECT 'I' AS DML_TYPE_CODE,'PT' AS AUD_TYPE,  101
								AS JOB_EXECUTION_ID, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, l.LOAD_KEY_HASH,
                                l.Golfer_ID,l.First_Name,l.Last_Name ,l.Middle_Initial ,l.Date_Of_Birth ,l.Email_Address
							FROM CUR_GLF_Golfers AS c   
								RIGHT JOIN LND_GLF_Golfers  AS l ON c.LOAD_KEY_HASH = l.LOAD_KEY_HASH
							WHERE c.LOAD_KEY_HASH IS NULL ; 

INSERT INTO ARC_GLF_Golfers
	(DML_TYPE_CODE,AUD_TYPE, JOB_EXECUTION_ID, CREATE_TIMESTAMP, LOAD_TIMESTAMP, LOAD_KEY_HASH, 
    Golfer_ID,First_Name,Last_Name ,Middle_Initial ,Date_Of_Birth ,Email_Address)   
							SELECT 'D' AS DML_TYPE_CODE,'DL' AS AUD_TYPE,  101 
								AS JOB_EXECUTION_ID, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, c.LOAD_KEY_HASH,
                                c.Golfer_ID,c.First_Name,c.Last_Name ,c.Middle_Initial ,c.Date_Of_Birth ,c.Email_Address
							FROM CUR_GLF_Golfers AS c   
								LEFT JOIN LND_GLF_Golfers  AS l ON c.LOAD_KEY_HASH = l.LOAD_KEY_HASH
							WHERE l.LOAD_KEY_HASH IS NULL ; 



--         Show streams

-- 3.3.3   Query the stream, it should show no rows.

SELECT * FROM CUR_GLF_Golfers;
SELECT * FROM LND_GLF_Golfers;
SELECT * FROM ARC_GLF_Golfers;



--delete 
DELETE from CUR_GLF_Golfers
where golfer_id IN (select golfer_id from ARC_GLF_Golfers where DML_TYPE_CODE IN ('D', 'U') and JOB_EXECUTION_ID = 101 ) ;

SELECT * FROM CUR_GLF_Golfers;

INSERT INTO CUR_GLF_Golfers
select * from ARC_GLF_Golfers where DML_TYPE_CODE IN ('I','U') and JOB_EXECUTION_ID = 101 ;

SELECT * FROM CUR_GLF_Golfers order by GOLFER_ID ASC;



SELECT * FROM ARC_GLF_Golfers ORDER BY Golfer_Id;

SELECT * FROM VIPER_db.public.Golfers;

--         History after second insert

-- 3.5.18  Clean up.

--USE SCHEMA VIPER_db.public;
--DROP SCHEMA VIPER_db.cdc_lab CASCADE;