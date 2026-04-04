USE DATABASE util_db;
USE SCHEMA PUBLIC;
USE ROLE accountadmin;

CREATE OR REPLACE API INTEGRATION dora_api_integration
api_provider = aws_api_gateway
api_aws_role_arn = 'arn:aws:iam::321463406630:role/snowflakeLearnerAssumedRole'
enabled = true
api_allowed_prefixes = ('https://awy6hshxy4.execute-api.us-west-2.amazonaws.com/dev/edu_dora');

CREATE OR REPLACE EXTERNAL FUNCTION util_db.public.grader(
    step            VARCHAR,
    passed          BOOLEAN,
    actual          INTEGER,
    expected        INTEGER,
    description     VARCHAR
)
RETURNS VARIANT
api_integration = dora_api_integration
context_headers = (current_timestamp, current_account, current_statement, current_account_name) AS 'https://awy6hshxy4.execute-api.us-west-2.amazonaws.com/dev/edu_dora/grader';

USE ROLE accountadmin;
USE DATABASE util_db;
USE SCHEMA PUBLIC;
SELECT GRADER (
    step,
    (
        actual = expected
    ),
    actual,
    expected,
    description
) 
AS graded_results FROM (
    SELECT 'DORA_IS_WORKING'
    AS step,
    (
        SELECT 123
    )
    AS actual,
    123 AS expected,
    'Dora is working!' AS description
);

SELECT grader
(
    step,
    (
        actual = expected
    ),
    actual,
    expected,
    description
)
AS graded_results
FROM 
(
    SELECT 'CMCW01' 
    AS step,
    (
        SELECT COUNT(*)
        FROM snowflake.account_usage.databases
        WHERE database_name = 'INTL_DB' 
        AND deleted IS NULL
    )
    AS actual,
    1 AS expected,
    'Created INTL_DB' AS description
);

SELECT grader
(
    step,
    (
        actual = expected
    ),
    actual,
    expected,
    description
)
AS graded_results
FROM 
(
    SELECT 'CMCW02' 
    AS step,
    (
        SELECT COUNT(*)
        FROM intl_db.information_schema.tables
        WHERE table_schema = 'PUBLIC' 
        AND table_name = 'INT_STDS_ORG_3166'
    )
    AS actual,
    1 AS expected,
    'ISO table created' AS description
);

SELECT grader
(
    step,
    (
        actual = expected
    ),
    actual,
    expected,
    description
)
AS graded_results
FROM 
(
    SELECT 'CMCW03' 
    AS step,
    (
        SELECT row_count
        FROM intl_db.information_schema.tables
        WHERE table_name = 'INT_STDS_ORG_3166'
    )
    AS actual,
    249 AS expected,
    'ISO Table Loaded' AS description
);

SELECT grader
(
    step,
    (
        actual = expected
    ),
    actual,
    expected,
    description
)
AS graded_results
FROM 
(
    SELECT 'CMCW04' 
    AS step,
    (
        SELECT COUNT(*)
        FROM intl_db.public.nations_sample_plus_iso
    )
    AS actual,
    249 AS expected,
    'Nations Sample Plus Iso' AS description
);

SELECT grader
(
    step,
    (
        actual = expected
    ),
    actual,
    expected,
    description
)
AS graded_results
FROM 
(
    SELECT 'CMCW05' 
    AS step,
    (
        SELECT row_count
        FROM intl_db.information_schema.tables
        WHERE table_schema = 'PUBLIC'
        AND table_name = 'COUNTRY_CODE_TO_CURRENCY_CODE'
    )
    AS actual,
    265 AS expected,
    'CCTCC Table Loaded' AS description
);

SELECT grader
(
    step,
    (
        actual = expected
    ),
    actual,
    expected,
    description
)
AS graded_results
FROM 
(
    SELECT 'CMCW06' 
    AS step,
    (
        SELECT row_count
        FROM intl_db.information_schema.tables
        WHERE table_schema = 'PUBLIC'
        AND table_name = 'CURRENCIES'
    )
    AS actual,
    151 AS expected,
    'Currencies table loaded' AS description
);

SELECT grader
(
    step,
    (
        actual = expected
    ),
    actual,
    expected,
    description
)
AS graded_results
FROM 
(
    SELECT 'CMCW07' 
    AS step,
    (
        SELECT COUNT(*)
        FROM intl_db.public.simple_currency
    )
    AS actual,
    265 AS expected,
    'Simple Currency Looks Good' AS description
);

SHOW SHARES;
SELECT grader
(
    step,
    (
        actual = expected
    ),
    actual,
    expected,
    description
)
AS graded_results
FROM 
(
    SELECT 'CMCW08' 
    AS step,
    (
        SELECT IFF(COUNT(*)>0, 1, 0)
        FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        WHERE "kind" = 'OUTBOUND'
        AND "database_name" = 'INTL_DB'
    )
    AS actual,
    1 AS expected,
    'Outbound Share Created From INTL_DB' AS description
);

SHOW RESOURCE MONITORS;
SELECT grader
(
    step,
    (
        actual = expected
    ),
    actual,
    expected,
    description
)
AS graded_results
FROM 
(
    SELECT 'CMCW09' 
    AS step,
    (
        SELECT IFF(COUNT(*)>0, 1, 0)
        FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        WHERE "name" = 'DAILY_3_CREDIT_LIMIT'
        AND "credit_quota" = 3
        AND "frequency" = 'DAILY'
    )
    AS actual,
    1 AS expected,
    'Resource Monitors Exist' AS description
);


-- Once you are sure the accounts are configured correctly to pass, you need to check to see if the METADATA is ready to be queried. To do this, go to the ORGANIZATON_USAGE schema of THE SNOWFLAKE database (the one with the App symbol that we talked about in Lesson 2). Navigate to the ACCOUNTS view of that schema, then to the Data Preview Tab.

-- The Data Preview Tab shows the results of the same view that DORA CMCW12 and CMCW13 are querying. So, if this data preview doesn't show your accounts, neither will the DORA checks.

-- We checked this view 23 hours after we set up our ACME account and that was not enough time. We then went to sleep, woke up and checked again and all of the accounts shown in the screenshot above had appeared. This makes us think that people who reported 72 hour wait times were not performing the steps correctly. Please let us know if you did everything exactly right and still had to wait more than 48 hours.

-- Since you may need to wait a few days before tests 12 and 13 pass, feel free to pass CMCW14 and start BADGE 3: Data Application Builders Workshop!

-- Then come back here in two or three days to run these tests!


-- set the worksheet drop lists to match the location of your GRADER function
--DO NOT MAKE ANY CHANGES BELOW THIS LINE

--RUN THIS DORA CHECK IN YOUR ORIGINAL TRIAL ACCOUNT (WDE)
select grader(step, (actual = expected), actual, expected, description) as graded_results from ( 
SELECT 'CMCW12' as step 
,( select count(*) 
   from SNOWFLAKE.ORGANIZATION_USAGE.ACCOUNTS 
   where account_name = 'ACME' 
   and region like 'GCP_%' 
   and deleted_on is null
  ) as actual 
, 1 as expected 
,'ACME Account Added on GCP Platform' as description 
); 

-- set the worksheet drop lists to match the location of your GRADER function
--DO NOT MAKE ANY CHANGES BELOW THIS LINE

--RUN THIS DORA CHECK IN YOUR ORIGINAL TRIAL ACCOUNT (WDE)

select grader(step, (actual = expected), actual, expected, description) as graded_results from (
SELECT 
  'CMCW13' as step
 ,( select count(*) 
   from SNOWFLAKE.ORGANIZATION_USAGE.ACCOUNTS 
   where account_name = 'AUTO_DATA_UNLIMITED' 
   and region like 'AZURE_%'
   and deleted_on is null) as actual
 , 1 as expected
 ,'ADU Account Added on AZURE' as description
); 

