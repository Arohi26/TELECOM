USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

CREATE OR REPLACE DATABASE TELECOM_DB;
USE DATABASE TELECOM_DB;
CREATE OR REPLACE SCHEMA RAW;
CREATE OR REPLACE SCHEMA STAGING;
CREATE OR REPLACE SCHEMA MART;

CREATE OR REPLACE FILE FORMAT CSV_FORMAT
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1;

CREATE OR REPLACE STAGE CDR_STAGE
FILE_FORMAT = CSV_FORMAT;


---RAW TABLE
CREATE OR REPLACE TABLE RAW.CDR_RAW (
    CallID INT,
    Caller STRING,
    Receiver STRING,
    Duration INT,
    CallType STRING,
    TowerID STRING,
    Timestampp STRING
);

--LOADING DATA
--LOADED BY INGESTION

COPY INTO RAW.CDR_RAW 
FROM @CDR_STAGE/CDR_RAW_Telecom.csv
FILE_FORMAT = CSV_FORMAT;

SELECT*FROM RAW.CDR_RAW;





--CUSTOMER_MASTER TABLE
CREATE OR REPLACE TABLE MART.CUSTOMER_MASTER (
    PhoneNumber STRING,
    CustomerName STRING,
    PlanType STRING
);
--LOAD
COPY INTO MART.CUSTOMER_MASTER 
FROM @CDR_STAGE/CUSTOMER_MASTER_Telecom.csv
FILE_FORMAT = CSV_FORMAT;

SELECT*FROM MART.CUSTOMER_MASTER;

--TOWER TABLE
CREATE OR REPLACE TABLE MART.TOWER_MASTER (
    TowerID STRING,
    Region STRING,
    City STRING
);

--LOAD
COPY INTO MART.TOWER_MASTER 
FROM @CDR_STAGE/TOWER_MASTER_Telecom.csv
FILE_FORMAT = CSV_FORMAT;
SELECT*FROM MART.TOWER_MASTER;

--STAGE TABLE
CREATE OR REPLACE TABLE STAGING.STG_CDR (
    CallID INT,
    Caller STRING,
    Receiver STRING,
    Duration INT,
    CallType STRING,
    TowerID STRING,
    Timestampp TIMESTAMP,
    Revenue FLOAT,
    IsInternational INT,
    Region STRING,
    City STRING
);

--TRANSFORMATION OF STG_CDR
INSERT INTO STAGING.STG_CDR
SELECT DISTINCT
    R.CallID,
    R.Caller,
    R.Receiver,
    COALESCE(R.Duration, 0),
    R.CallType,
    R.TowerID,
    TO_TIMESTAMP(R.Timestampp),
    COALESCE(R.Duration, 0) * 0.02 AS Revenue,
    CASE 
        WHEN R.CallType = 'INT' THEN 1 
        ELSE 0 
    END AS IsInternational,
    T.Region,
    T.City
FROM RAW.CDR_RAW R
LEFT JOIN MART.TOWER_MASTER T
ON R.TowerID = T.TowerID
ORDER BY R.CallID;

SELECT*FROM STAGING.STG_CDR;



--FACT TABLE
CREATE OR REPLACE TABLE MART.FACT_CDR (
    CDR_KEY INT AUTOINCREMENT,
    CallID INT,
    Caller STRING,
    Receiver STRING,
    Duration INT,
    Revenue FLOAT,
    IsInternational INT,
    CallDate DATE,
    Region STRING,
    City STRING
);

--FACT TABLE
MERGE INTO MART.FACT_CDR T
USING (
    SELECT
        CallID,
        Caller,
        Receiver,
        Duration,
        Revenue,
        IsInternational,
        CAST(Timestampp AS DATE) AS CallDate,
        Region,
        City
    FROM STAGING.STG_CDR
) S
ON T.CallID = S.CallID

WHEN MATCHED THEN 
    UPDATE SET
        T.Duration = S.Duration,
        T.Revenue = S.Revenue,
        T.IsInternational = S.IsInternational,
        T.CallDate = S.CallDate,
        T.Region = S.Region,
        T.City = S.City

WHEN NOT MATCHED THEN 
    INSERT (
        CallID,
        Caller,
        Receiver,
        Duration,
        Revenue,
        IsInternational,
        CallDate,
        Region,
        City
    )
    VALUES (
        S.CallID,
        S.Caller,
        S.Receiver,
        S.Duration,
        S.Revenue,
        S.IsInternational,
        S.CallDate,
        S.Region,
        S.City
    );

SELECT * FROM MART.FACT_CDR;


--to check  if user is valid
CREATE OR REPLACE TABLE STAGING.INVALID_PHONE AS
SELECT *
FROM STAGING.STG_CDR
WHERE 
    LENGTH(Caller) <> 10


OR LENGTH(Receiver) <> 10
    OR Duration < 0
    AND CallID IS  NULL;
SELECT*FROM STAGING.INVALID_PHONE;    


--dashboard in file #Dashboardhcl.png


SELECT 
    Caller,
    SUM(Duration) AS TotalDuration
FROM MART.FACT_CDR
GROUP BY Caller
HAVING SUM(Duration) > 1000;

CREATE OR REPLACE NOTIFICATION INTEGRATION EMAIL_INT
TYPE = EMAIL
ENABLED = TRUE
ALLOWED_RECIPIENTS(
--EMAILS
);

CALL SYSTEM$SEND_EMAIL(
    'EMAIL_INT',
    'EMAILS',
    'High Usage Alert',
    'Some users have exceeded call usage limit.'
);

CALL SYSTEM$SEND_EMAIL(
    'EMAIL_INT',
    'EMAILS',
    'High Usage Alert',
    (
        SELECT LISTAGG(
            'User: ' || Caller || ' Duration: ' || SUM(Duration),
            '\n'
        )
        FROM MART.FACT_CDR
        GROUP BY Caller
        HAVING SUM(Duration) > 1000
    )
);