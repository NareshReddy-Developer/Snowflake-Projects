--Snowflake class



VM 8 RAM GB, 16 CPU, 500 GB


STORAGE (TB) + COMPUTE 
1 HOUR 

1 CREDIT 1 2 

1 TB PER MONTH : 40 23 , 

50 TB 

CREDIT 

Snowflake edition:
	Standard
	Enterprise: time travel, multi-cluster vH, Materialized views
	Business Critical

cloud provider:
	aws
	azure
	gcp
	
snowflake CLI:	
snowsql 



SF Account: https://db41642.ap-south-1.aws.snowflakecomputing.com/

SF Account: db41642.ap-south-1.aws
User: *****
Pwd: *****

snowsql  -a db41642.ap-south-1.aws -u sfdec2021


USE WAREHOUSE COMPUTE_WH;


CREATE DATABASE IF NOT EXISTS SALES_DB;
CREATE SCHEMA IF NOT EXISTS LANDING;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS DW;

USE WAREHOUSE COMPUTE_WH;
USE ROLE SYSADMIN;
USE DATABASE SALES_DB;
USE SCHEMA LANDING;



CREATE OR REPLACE TABLE DW.DIM_CUSTOMER
(
	SK_CUST NUMBER,
	BK_CUST NUMBER,
	CNAME VARCHAR(20),
	CDOB DATE,
	GENDER VARCHAR(10),
	PHONE VARCHAR(20),
	EMAIL VARCHAR(20),
	CITY VARCHAR(20),
	STATE VARCHAR(20),
	COUNTRY VARCHAR(20),
	CONTACT	VARCHAR(20),
	CREATED_DATE DATETIME,
	CREATED_BY VARCHAR(20),
	UPDATED_DATE DATETIME,
	UPDATED_BY VARCHAR(20)	
);

CREATE OR REPLACE TABLE LANDING.CONTACT_DETAIL
(
	CUSTID NUMBER,
	CITY VARCHAR(20),
	STATE VARCHAR(20),
	COUNTRY VARCHAR(20),
	CONTACT	VARCHAR(20)
);


S3 FILE -> LANDING -> STAGING -> DW (DIM/FACT TABLES, VIEWS(REPOTING)) 

ACCOUNTADMIN ROLE:
SNOWFLAKE DB:
	ACCOUNT_USAGE
	INFORMATION_SCHEMA -> DB METADATA
	

"SNOWFLAKE"."INFORMATION_SCHEMA"."COLUMNS"

DROP TABLE "SALES_DB"."LANDING"."DIM_CUSTOMER";



(customer_detail.csv, contact.csv)S3 FILE 
-> customer_detail, contact(LANDING) 
-> STAGING(customer) 
-> DIM_CUSTOMER(DW)


CREATE OR REPLACE TABLE LANDING.CUSTOMER_DETAIL
(
CUSTID NUMBER,
CNAME VARCHAR(20),
DOB VARCHAR(20),
DOJ VARCHAR(20),
GENDER VARCHAR(20), --MALE, FEMALE
PHONE VARCHAR(20), --"91 9004444" --> "+919004444"
EMAIL VARCHAR(20)
);


CREATE OR REPLACE STAGE LANDING.IN_STAGE;

@LANDING.IN_STAGE;

SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE();

CREATE FILE FORMAT "SALES_DB"."LANDING".CSV_FF_COMPRESED 
TYPE = 'PARQUET' 
COMPRESSION = 'SNAPPY' 
BINARY_AS_TEXT = TRUE;

CREATE OR REPLACE FILE FORMAT LANDING.CSV_FF_COMPRESED 
TYPE = 'CSV' 
COMPRESSION = 'GZIP' 
FIELD_DELIMITER = ',' 
RECORD_DELIMITER = '\n' 
SKIP_HEADER = 1 
FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
TRIM_SPACE = TRUE 
ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE 
ESCAPE = 'NONE' 
ESCAPE_UNENCLOSED_FIELD = '\134' 
DATE_FORMAT = 'AUTO' 
TIMESTAMP_FORMAT = 'AUTO' 
NULL_IF = ('\\N', '','NULL','NA');

YYY-MM-DD HH24:MI:SS
DD-MM-YYYY
HH24:MI:SS

SHOW SATGES;


SHOW FILE FORMATS;
DESC FILE FORMAT CSV_FF_COMPRESED;

COPY INTO LANDING.CUSTOMER_DETAIL
FROM '@LANDING.IN_STAGE/customer_detail.csv.gz' 
	FILE_FORMAT = (FORMAT_NAME = CSV_FF_COMPRESED) 
	--FILES = ('customer_detail.csv.gz')
	--PATTERN = 'customer_detail_.*[.]csv.gz'
	VALIDATION_MODE = RETURN_ERRORS
	--ON_ERROR = CONTINUE
	;


COPY INTO LANDING.CUSTOMER_DETAIL
FROM '@LANDING.IN_STAGE/CUSTOMER/' 
	FILE_FORMAT = (FORMAT_NAME = CSV_FF_COMPRESED) 
	--FILES = ('customer_detail.csv.gz')
	--PATTERN = 'customer_detail_.*[.]csv.gz'
	--VALIDATION_MODE = RETURN_ERRORS
	ON_ERROR = CONTINUE
	;
	
customer_detail_.*[.]csv.gz

customer_detail_20211216.csv.gz
customer_detail_20211217.csv.gz

USE DATABASE SALES_DB;
USE SCHEMA LANDING;

SHOW STAGES;
LIST @IN_STAGE;

PUT file://C:/Snowflake_cla/sourcedata/customer_detail.csv @IN_STAGE/CUSTOMER/;

GET @IN_STAGE/CUSTOMER/ file://C:/Snowflake_cla/sourcedata/customer_detail.csv;

in_stage/customer_detail.csv.gz


SELECT 
	t.$1,
	t.$2,
	t.$3,
	--t.$4,
	,TO_DATE( t.$4, 'DD-MM-YYYY HH24:MI:SS') AS DOJ
	--t.$5,
	CASE
		WHEN t.$5 = 'M' THEN 'MALE'
		WHEN t.$5 = 'F' THEN 'FEMALE'
		ELSE 'NA'
	END AS GENDER,
	--t.$6,
	CONCAT('+', REPLACE( t.$6 ,' ', '')) AS PHONE
	t.$7
	
FROM '@LANDING.IN_STAGE/customer_detail.csv.gz' t ;

YYY-MM-DD

TO_DATE( <string_expr> [, <format> ] )
TO_DATE( <string_expr> [, <format> ] )

TO_DATE( <string_expr> )


CASE
    WHEN t.$5 = 'M' THEN 'MALE'
	WHEN t.$5 = 'F' THEN 'FEMALE'
    ELSE 'NA'
END

CONCAT , ||

'+' || REPLACE( t.$6 ,' ', '')
CONCAT('+', REPLACE( t.$6 ,' ', ''))

CONCAT(t.$6, t.$7)

DD-MM-YYYY
25-12-1990

DATE_FROM_PARTS( SUBSTR(t.$3, 7, 4), SUBSTR(t.$3, 4, 2), SUBSTR(t.$3, 1, 2))
DATE_FROM_PARTS( 1990, 12, 25 )
SUBSTR(t.$3, 7, 4)

SELECT 
	t.$1,
	t.$2,
	DATE_FROM_PARTS( SUBSTR(t.$3, 7, 4), SUBSTR(t.$3, 4, 2), SUBSTR(t.$3, 1, 2)),
	DATE_FROM_PARTS( SUBSTR(t.$4, 7, 4), SUBSTR(t.$4, 4, 2), SUBSTR(t.$4, 1, 2)) AS DOJ,
	CASE
		WHEN t.$5 = 'M' THEN 'MALE'
		WHEN t.$5 = 'F' THEN 'FEMALE'
		ELSE 'NA'
	END AS GENDER,
	CONCAT('+', REPLACE( t.$6 ,' ', '')) AS PHONE,
	t.$7
	
FROM '@LANDING.IN_STAGE/customer_detail.csv.gz' t 
WHERE t.$1 <> 'custid';


SELECT 
	t.$4,
	TO_DATE(CAST(t.$4 AS VARCHAR(20), 'DD-MM-YYYY HH24:MI:SS') AS DOJ
FROM '@LANDING.IN_STAGE/customer_detail.csv.gz' t ;
WHERE t.$1 != 'custid';

SELECT TO_TIMESTAMP('20-11-2000 10:30:20', 'DD-MM-YYYY HH24:MI:SS')
TO_DATE(TO_TIMESTAMP(SUBSTR(t.$4, 2, LEN(t.$4)-2), 'DD-MM-YYYY HH24:MI:SS'))

TO_DATE(SUBSTR(t.$4, 2, LEN(t.$4)-2), 'DD-MM-YYYY HH24:MI:SS')

TO_DATE(t.$4, 'DD-MM-YYYY HH24:MI:SS')
"20-11-2000 10:30:20"
2, 20

SUBSTR(t.$4, 2, LEN(t.$4)-2)

SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE(), CURRENT_USER(),
CURRENT_WAREHOUSE();



to_timestamp_tz

TO_DATE(t.$4, 'DD-MM-YYYY HH24:MI:SS')

I will join for Call & ISS Session - on 20th December, 2021 at 9:30 AM.

MM/DD/YYYY HH24:MI:SS
DD-MM-YYYY HH24:MI:SS
 Nitilekha

COPY INTO LANDING.CUSTOMER_DETAIL
FROM '@LANDING.IN_STAGE/customer_detail.csv.gz' 
	FILE_FORMAT = (FORMAT_NAME = CSV_FF_COMPRESED FIELD_DELIMITER = '|') 
	--FILES = ('customer_detail.csv.gz')
	--PATTERN = 'customer_detail_.*[.]csv.gz'
	--VALIDATION_MODE = RETURN_ERRORS
	ON_ERROR = CONTINUE;
	
	
COPY INTO LANDING.CUSTOMER_DETAIL
FROM 
(
	SELECT 
	t.$1,
	t.$2,
	DATE_FROM_PARTS( SUBSTR(t.$3, 8, 4), SUBSTR(t.$3, 5, 2), SUBSTR(t.$3, 2, 2)),
	DATE_FROM_PARTS( SUBSTR(t.$4, 8, 4), SUBSTR(t.$4, 5, 2), SUBSTR(t.$4, 2, 2)) AS DOJ,
	CASE
		WHEN t.$5 = 'M' THEN 'MALE'
		WHEN t.$5 = 'F' THEN 'FEMALE'
		ELSE 'NA'
	END AS GENDER,
	CONCAT('+', REPLACE( t.$6 ,' ', '')) AS PHONE,
	t.$7
	
	FROM '@LANDING.IN_STAGE/customer_detail.csv.gz' t
)
	FILE_FORMAT = (FORMAT_NAME = CSV_FF_COMPRESED) 
	--FILES = ('customer_detail.csv.gz')
	--PATTERN = 'customer_detail_.*[.]csv.gz'
	VALIDATION_MODE = RETURN_ERRORS	


	--TO_DATE(SUBSTR(t.$3, 2, LEN(t.$3)-2), 'DD-MM-YYYY'),
	--TO_DATE(SUBSTR(t.$4, 2, LEN(t.$4)-2), 'DD-MM-YYYY HH24:MI:SS') AS DOJ,
	
SELECT 
	t.$1,
	t.$2,
	TO_DATE(SUBSTR(t.$3, 2, LEN(t.$3)-2), 'DD-MM-YYYY'),
	TO_DATE(SUBSTR(t.$4, 2, LEN(t.$4)-2), 'DD-MM-YYYY HH24:MI:SS') AS DOJ,
	CASE
		WHEN t.$5 = 'M' THEN 'MALE'
		WHEN t.$5 = 'F' THEN 'FEMALE'
		ELSE 'NA'
	END AS GENDER,
	CONCAT('+', REPLACE( t.$6 ,' ', '')) AS PHONE,
	t.$7
	
FROM '@LANDING.IN_STAGE/customer_detail.csv.gz' t



CREATE WAREHOUSE SAMPLE_WH 
WITH WAREHOUSE_SIZE = 'XSMALL' 
WAREHOUSE_TYPE = 'STANDARD' 
AUTO_SUSPEND = 60 
AUTO_RESUME = TRUE 
MIN_CLUSTER_COUNT = 1 
MAX_CLUSTER_COUNT = 1 
SCALING_POLICY = 'STANDARD';


ALTER WAREHOUSE "SAMPLE_WH" SET WAREHOUSE_SIZE = 'SMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 1 SCALING_POLICY = 'STANDARD' COMMENT = '';


CREATE WAREHOUSE SAMPLE_WH_MULTI WITH WAREHOUSE_SIZE = 'MEDIUM' WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = 300 AUTO_RESUME = TRUE MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 5 SCALING_POLICY = 'STANDARD';



ACCOUNTADMIN 
	SYSADMIN : WH, DB
	SECURITYADMIN: USER , ROLES, GRANT PERMISSION
		USERADMIN: USER , ROLES
		
	PUBLIC
	
	

COPY INTO LANDING.CUSTOMER_DETAIL
FROM 
(	SELECT 
	t.$1,	t.$2,
	t.$3 AS DOB,
	t.$4 AS DOJ,
	CASE
		WHEN t.$5 = 'M' THEN 'MALE'
		WHEN t.$5 = 'F' THEN 'FEMALE'
		ELSE 'NA'
	END AS GENDER,
	CONCAT('+', REPLACE( t.$6 ,' ', '')) AS PHONE,
	t.$7	
	FROM '@LANDING.IN_STAGE/customer_detail.csv.gz' t
)
	FILE_FORMAT = (FORMAT_NAME = CSV_FF_COMPRESED) 
	ON_ERROR = CONTINUE;
   
 REMOVE @LANDING.IN_STAGE/CUSTOMER/customer_detail.csv.gz;

 
CREATE OR REPLACE TABLE LANDING.CONTACT_DETAIL
(
	CUSTID NUMBER,
	CITY VARCHAR(20),
	STATE VARCHAR(20),
	COUNTRY VARCHAR(20),
	CONTACT	VARCHAR(20)
);



COPY INTO LANDING.CONTACT_DETAIL
FROM '@LANDING.IN_STAGE/CUSTOMER/contact.csv.gz' 
FILE_FORMAT = (FORMAT_NAME = CSV_FF_COMPRESED) 
ON_ERROR = CONTINUE
--VALIDATION_MODE = RETURN_ERRORS	
;

LIST @IN_STAGE/CUSTOMER/;

PUT file://C:/Snowflake_cla/sourcedata/contact.csv @IN_STAGE/CUSTOMER/;

PUT file://C:/Snowflake_cla/sourcedata/customer_detail1.csv @IN_STAGE/CUSTOMER/;
customer_detail1

PUT file://C:/Snowflake_cla/sourcedata/customer_detail_4.csv @IN_STAGE/CUSTOMER/ OVERWRITE = TRUE;


CREATE OR REPLACE VIEW STAGING.VW_CUSTOMER_CONTACT 
AS 
SELECT
	CUSTOMER.CUSTID AS BK_CUST,
	CUSTOMER.CNAME,
	TO_DATE(CUSTOMER.DOB, 'DD-MM-YYYY') AS DOB,
	TO_DATE(CUSTOMER.DOJ, 'DD-MM-YYYY HH24:MI:SS') AS DOJ,	
	CUSTOMER.GENDER ,
	CUSTOMER.PHONE ,
	CUSTOMER.EMAIL ,
	CONTACT.CITY,
	CONTACT.STATE ,
	CONTACT.COUNTRY ,
	CONTACT.CONTACT	,
	CURRENT_DATE() AS CREATED_DATE ,
	CURRENT_USER() AS CREATED_BY ,
	CURRENT_DATE() AS UPDATED_DATE ,
	CURRENT_USER() AS UPDATED_BY 
	
FROM LANDING.CUSTOMER_DETAIL CUSTOMER 
INNER JOIN LANDING.CONTACT_DETAIL CONTACT 
	ON CUSTOMER.CUSTID = CONTACT.CUSTID ;

SELECT
	CUSTOMER.CUSTID AS BK_CUST,
	CUSTOMER.CNAME,
	TO_DATE(CUSTOMER.DOB, 'DD-MM-YYYY') AS DOB,
	TO_DATE(CUSTOMER.DOJ, 'DD-MM-YYYY HH24:MI:SS') AS DOJ,	
	CUSTOMER.GENDER ,
	CUSTOMER.PHONE ,
	CUSTOMER.EMAIL ,
	CONTACT.CITY,
	CONTACT.STATE ,
	CONTACT.COUNTRY ,
	CONTACT.CONTACT	,
	CURRENT_DATE() AS CREATED_DATE ,
	CURRENT_USER() AS CREATED_BY ,
	CURRENT_DATE() AS UPDATED_DATE ,
	CURRENT_USER() AS UPDATED_BY 
	
FROM LANDING.CONTACT_DETAIL CONTACT 
LEFT JOIN LANDING.CUSTOMER_DETAIL CUSTOMER
	ON CUSTOMER.CUSTID = CONTACT.CUSTID 
WHERE ;
	
	
	
CREATE SEQUENCE DW.SEQ_CUSTOMER START 1 INCREMENT 1;

MERGE INTO DW.DIM_CUSTOMER T 
USING STAGING.VW_CUSTOMER_CONTACT S 
ON T.BK_CUST = S.BK_CUST
WHEN NOT MATCHED THEN 
INSERT 
(	SK_CUST ,
	BK_CUST ,
	CNAME ,
	DOB ,
	GENDER ,
	PHONE ,
	EMAIL ,
	CITY ,
	STATE ,
	COUNTRY ,
	CONTACT	,
	CREATED_DATE ,
	CREATED_BY ,
	UPDATED_DATE ,
	UPDATED_BY 
)
VALUES 
(
	DW.SEQ_CUSTOMER.NEXTVAL,
	S.BK_CUST ,
	S.CNAME ,
	S.DOB ,
	S.GENDER ,
	S.PHONE ,
	S.EMAIL ,
	S.CITY ,
	S.STATE ,
	S.COUNTRY ,
	S.CONTACT	,
	S.CREATED_DATE ,
	S.CREATED_BY ,
	S.UPDATED_DATE ,
	S.UPDATED_BY
)

WHEN MACTHED AND 
	(
		S.CNAME != T.CNAME OR 
		S.DOB != T.DOB OR 
		S.GENDER != T.GENDER OR 
		S.PHONE != T.PHONE OR 
		S.EMAIL != T.PHONE OR 
		S.CITY != T.CITY OR 
		S.STATE != T.STATE OR 
		S.COUNTRY != T.COUNTRY OR 
		S.CONTACT != T.CONTACT  
	) THEN 
UPDATE SET 
		T.CNAME = S.CNAME , 
		T.DOB = S.DOB , 
		T.GENDER = S.GENDER , 
		T.PHONE = S.PHONE , 
		T.EMAIL = S.EMAIL , 
		T.CITY = S.CITY , 
		T.STATE = S.STATE , 
		T.COUNTRY = S.COUNTRY , 
		T.CONTACT = S.CONTACT  
		;
		
		
