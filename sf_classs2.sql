customer_detail.csv, contact.csv) S3 FILE 
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


 
CREATE OR REPLACE TABLE LANDING.CONTACT_DETAIL
(
	CUSTID NUMBER,
	CITY VARCHAR(20),
	STATE VARCHAR(20),
	COUNTRY VARCHAR(20),
	CONTACT	VARCHAR(20)
);




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

CREATE OR REPLACE STAGE LANDING.IN_STAGE;



PUT file://C:/Snowflake_cla/sourcedata/customer_detail.csv @IN_STAGE/CUSTOMER/;


PUT file://C:/Snowflake_cla/sourcedata/contact.csv @IN_STAGE/CUSTOMER/;


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
	
	FROM '@LANDING.IN_STAGE/CUSTOMER/customer_detail.csv.gz' t
)
	FILE_FORMAT = (FORMAT_NAME = CSV_FF_COMPRESED) 
	--FILES = ('customer_detail.csv.gz')
	--PATTERN = 'customer_detail_.*[.]csv.gz'
	VALIDATION_MODE = RETURN_ERRORS
	ON_ERROR = CONTINUE
	
COPY INTO LANDING.CONTACT_DETAIL
FROM '@LANDING.IN_STAGE/CUSTOMER/contact.csv.gz' 
FILE_FORMAT = (FORMAT_NAME = CSV_FF_COMPRESED) 
ON_ERROR = CONTINUE
--VALIDATION_MODE = RETURN_ERRORS	
;



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
	
	

MERGE INTO DW.DIM_CUSTOMER T 
USING STAGING.VW_CUSTOMER_CONTACT S 
	ON T.BK_CUST = S.BK_CUST
	
WHEN NOT MATCHED THEN --new rows
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

WHEN MACTHED AND --update rows
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
		
		
SELECT * FROM DW.DIM_CUSTOMER;


create function simple_table_function ()
  returns table (x integer, y integer)
  as
  $$
    select 1, 2
    union all
    select 3, 4
  $$
  ;
  
  
  create or replace function get_countries_for_user ( id number )
  returns table (country_code char, country_name varchar)
  as 'select distinct c.country_code, c.country_name
      from user_addresses a, countries c
      where a.user_id = id
      and c.country_code = a.country_code';

select * from table(simple_table_function());
	  
reate or replace function js_factorial(d double)
  returns double
  language javascript
  strict
  as '
  if (D <= 0) {
    return 1;
  } else {
    var result = 1;
    for (var i = 2; i <= D; i++) {
      result = result * i;
    }
    return result;
  }
  ';
  
  UDF
	SCALAR VALUE -- PRE DEFINED FUNCTION LIKE SUBSTR, UPPER 
	TABLE VALUE --
	
	
	create function pi_udf()
  returns float
  as '3.141592654::FLOAT'
  ;
  
  
  CREATE [ OR REPLACE ] [ SECURE ] FUNCTION <name> ( [ <arg_name> <arg_data_type> ] [ , ... ] )
  RETURNS { <result_data_type> | TABLE ( <col_name> <col_data_type> [ , ... ] ) }
  [ [ NOT ] NULL ]
  [ { CALLED ON NULL INPUT | { RETURNS NULL ON NULL INPUT | STRICT } } ]
  [ VOLATILE | IMMUTABLE ]
  [ COMMENT = '<string_literal>' ]
  AS '<function_definition>'
  
  DATE -> INTEGER
  
  DATEDIFF( <date_or_time_part>, <date_or_time_expr1>, <date_or_time_expr2> )
  datediff(year, '2010-04-09 14:39:20'::timestamp, 
                      '2013-05-08 23:39:20'::timestamp) 
  
  year, month, day) or time part (e.g. hour, minute, second
  
  CURRENT DATE - DOB DAYS
  CREATE OR REPLACE FUNCTION DW.UDF_AGE(DT DATE) 
  RETURNS NUMBER NOT NULL 
  AS 
  '
	DATEDIFF(DAY, DT, CURRENT_DATE())
  
  ';
  
  CREATE OR REPLACE FUNCTION DW.UDF_AGE_YEARS(DT DATE) 
  RETURNS NUMBER NOT NULL 
  AS 
  '
	DATEDIFF(YEAR, DT, CURRENT_DATE())
  
  ';
  
  SELECT DW.UDF_EMAIL_NAME(EMAIL)
  
  CREATE OR REPLACE FUNCTION DW.UDF_EMAIL_NAME(NAME VARCHAR(50)) 
  RETURNS VARCHAR NOT NULL
  AS 
  '
	SPLIT_PART(NAME, ''@'', 2)
  
  ';
  
  ''
  
  SELECT DW.UDF_AGE('2000-01-01');
  SELECT DW.UDF_AGE_YEARS('2000-01-01');
  
  ABC@XYZ.COM
  
  ABCDDDD@XYZ.COM
  
  SPLIT_PART(<string>, <delimiter>, <partNumber>)
  
  split_part('11.22.33', '.',  0)
  
  SPLIT_PART('ABCDDDD@XYZ.COM', '@', 0)
  SPLIT_PART('ABCDDDD@XYZ.COM', '@', 1)
  
  
  SPLIT_PART(
  SPLIT_PART('ABCDDDD@XYZ.COM', '@', 2),
  '.', 1)
  
  VW_
  V_
CREATE VIEW DW.VW_DIM_CUSTOMER 
AS 
SELECT 
	BK_CUST ,
	CNAME ,
	DW.UDF_AGE(CDOB) AS AGE_DAYS ,
	DW.UDF_AGE_YEARS(CDOB) AS AGE_YEARS,
	GENDER ,
	PHONE ,
	DW.UDF_EMAIL_NAME(EMAIL) AS EMAIL ,
	UPPER(CITY) AS CITY,
	UPPER(STATE) AS STATE,
	UPPER(COUNTRY) AS COUNTRY,
	UPPER(CONTACT) AS CONTACT	

FROM DW.DIM_CUSTOMER;

SELECT * FROM DW.VW_DIM_CUSTOMER ;


SELECT * FROM DW.UDF_F1() ;


CREATE OR REPLACE FUNCTION DW.UDF_CUSTOMER(NAME VARCHAR)
RETURNS TABLE 
(
	BK_CUST NUMBER,
	CNAME VARCHAR,
	AGE_DAYS NUMBER,
	AGE_YEARS NUMBER,
	GENDER VARCHAR,
	PHONE VARCHAR,
	EMAIL VARCHAR,
	CITY VARCHAR,
	STATE VARCHAR,
	COUNTRY VARCHAR,
	CONTACT VARCHAR
) 
AS 
'
	SELECT 
		BK_CUST ,
		CNAME ,
		DW.UDF_AGE(CDOB),
		DW.UDF_AGE_YEARS(CDOB),
		GENDER ,
		PHONE ,
		DW.UDF_EMAIL_NAME(EMAIL),
		UPPER(CITY),
		UPPER(STATE),
		UPPER(COUNTRY),
		UPPER(CONTACT)	

	FROM DW.DIM_CUSTOMER
	WHERE GENDER = NAME

';


 
SELECT * FROM TABLE(DW.UDF_CUSTOMER('FEMALE'));

UDF - SQL + JAVASCRIPT 

---SP 
JAVASCRIPT

DML + LOGIC (VARIABLE, VARIABLE ASSNED, IF , ) 
DDL 

VARIABLE

IF 
	INSERT 
ELSE 
	UPDATE 
 
WHILE 


TRY 
CATCH
	
DW.UDF_CUSTOMER -- COUNT 

CREATE or REPLACE PROCEDURE DW)
  RETURNS VARCHAR
  LANGUAGE javascript
  AS
  $$
  var rs = snowflake.execute( { sqlText: 
      `INSERT INTO table1 ("column 1") 
           SELECT 'value 1' AS "column 1" ;`
       } );
  return 'Done.';
  $$;
 
SELECT COUNT(*) FROM DW.DIM_CUSTOMER;



CREATE OR REPLACE PROCEDURE DW.USP_GET_COUNT()
RETURNS VARCHAR NOT NULL
LANGUAGE javascript
AS 
$$

	var stmt = snowflake.createStatement
	(
		{sqlText: "SELECT COUNT(*) FROM DW.DIM_CUSTOMER;"}
	);
	
	var rs = stmt.execute();
	
	rs.next();
	
	var cnt = rs.getColumnValue(1);
	
	return cnt;	
$$;

CALL DW.USP_GET_COUNT();


CALL DW.USP_INSERT_EMP();


CREATE OR REPLACE TABLE LANDING.EMP 
( EID NUMBER, ENAME VARCHAR(10), SAL NUMBER, DOB DATE,
DOJ DATETIME
);

SELECT * FROM LANDING.EMP;

INSERT INTO LANDING.EMP(EID, ENAME, SAL, DOB, DOJ)
VALUES(101, 'A', 10000, '1999-10-15', '2015-10-15 10:30:00');


CREATE OR REPLACE PROCEDURE DW.USP_INSERT_EMP
(EID FLOAT, ENAME VARCHAR, SAL FLOAT)
RETURNS VARCHAR NOT NULL
LANGUAGE javascript
AS 
$$
	var stmt = snowflake.createStatement
	(
		{sqlText: "INSERT INTO LANDING.EMP(EID, ENAME, SAL)	VALUES(?, ?, ?);",
		binds: [EID, ENAME, SAL]}
	);
	
	var rs = stmt.execute();
	
	return 'sucess';
$$;

CALL DW.USP_INSERT_EMP(201, 'B', 20000);





CREATE OR REPLACE PROCEDURE DW.USP_INSERT_EMP
(EID FLOAT, ENAME VARCHAR, SAL FLOAT, DOB DATE)
RETURNS VARCHAR NOT NULL
LANGUAGE javascript
AS 
$$
	var stmt = snowflake.createStatement
	(
		{sqlText: "INSERT INTO LANDING.EMP(EID, ENAME, SAL, DOB) VALUES(:1, :2, :3, :4 );",
		binds: [EID, ENAME, SAL, DOB.toISOString()]}
	);
	
	var rs = stmt.execute();
	
	return 'sucess';
$$;

CALL DW.USP_INSERT_EMP(201, 'B', 20000, '2000-10-25');

CALL DW.USP_INSERT_EMP(201, 'B', 20000, '2000-10-25'::TIMESTAMP_NTZ::DATE);





CREATE OR REPLACE PROCEDURE DW.USP_INSERT_EMP
(EID FLOAT, ENAME VARCHAR, SAL FLOAT, DOB DATE, DOJ DATETIME)
RETURNS VARCHAR NOT NULL
LANGUAGE javascript
AS 
$$
	var stmt = snowflake.createStatement
	(
		{sqlText: "INSERT INTO LANDING.EMP(EID, ENAME, SAL, DOB, DOJ) VALUES(:1, :2, :3, :4, :5 );",
		binds: [EID, ENAME, SAL, DOB.toISOString(), DOJ.toISOString()]}
	);
	
	var rs = stmt.execute();
	
	return 'sucess';
$$;

CALL DW.USP_INSERT_EMP(201, 'B', 20000, '2000-10-25');

CALL DW.USP_INSERT_EMP(201, 'B', 20000, '2000-10-25', '2019-12-22 11:50:33');


CALL DW.USP_INSERT_EMP(201, 'B', 20000, '2000-10-25'::TIMESTAMP_NTZ::DATE);





:1::TIMESTAMP_LTZ::DATE

MYDATE.toISOString()


DATETIME/TIMESTAMP
TIMESTAMP_NTZ
TIMESTAMP_LTZ
TIMESTAMP_TZ 


SELECT 
	CAST(EID AS NUMBER) AS ID,
	EID::NUMBER AS ID
FROM 

NUMBER - > NUMBER(38,0)
NUMBER(10, 0) -> 
NUMBER(5, 2) -> 10.50

VARCHAR -> VARCHAR(16777216) 16MB

CHAR - > VARCHAR(1) 
STRING, TEXT

SQL SERVER -> TBALE --> SF


while (rs.next())
{

}

snowflake
snowflake.createStatement (steme1)
snowflake.execute

ResultSet 


var stmt = snowflake.createStatement(
   {sqlText: "INSERT INTO table1 (col1) VALUES (1);"}
   );
   
   