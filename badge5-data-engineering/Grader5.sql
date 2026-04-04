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
context_headers = (current_timestamp, current_account, current_statement, current_account_name) AS 'https;//awy6hshxy4.execute-api.us-west-2.amazonaws.com/dev/edu_dora/grader';

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
    SELECT 'DNGW01' 
    AS step,
    (
        -- logic here
    )
    AS actual,
    0 AS expected,
    '' AS description
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
    SELECT 'DNGW01' 
    AS step,
    (
        -- logic here
        select count(*)  
        from ags_game_audience.raw.logs
        where is_timestamp_ntz(to_variant(datetime_iso8601))= TRUE 
    )
    AS actual,
    250 AS expected,
    'Project DB and Log File Set Up Correctly' AS description
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
    SELECT 'DNGW02' 
    AS step,
    (
        -- logic here
        select sum(tally) from(
        select (count(*) * -1) as tally
        from ags_game_audience.raw.logs 
        union all
        select count(*) as tally
        from ags_game_audience.raw.game_logs)  
    )
    AS actual,
    250 AS expected,
    'View is filtered' AS description
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
    SELECT 'DNGW03' 
    AS step,
    (
        select count(*) 
        from ags_game_audience.enhanced.logs_enhanced
        where dow_name = 'Sat'
        and tod_name = 'Early evening'   
        and gamer_name like '%prajina'
    )
    AS actual,
    2 AS expected,
    'Playing the game on a Saturday evening' AS description
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
    SELECT 'DNGW04' 
    AS step,
    (
        select count(*)/iff (count(*) = 0, 1, count(*))
        from table(ags_game_audience.information_schema.task_history
        (task_name=>'LOAD_LOGS_ENHANCED'))
    )
    AS actual,
    1 AS expected,
    'Task exists and has been run at least once' AS description
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
    SELECT 'DNGW05' 
    AS step,
    (
        select max(tally) from (
        select
            CASE WHEN SCHEDULED_FROM = 'SCHEDULE' and STATE= 'SUCCEEDED' 
            THEN 1 ELSE 0 END as tally 
        from table(ags_game_audience.information_schema.task_history (task_name=>'GET_NEW_FILES')))
    )
    AS actual,
    1 AS expected,
    'Task succeeds from schedule' AS description
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
    SELECT 'DNGW06' 
    AS step,
    (
        select CASE WHEN pipe_status:executionState::text = 'RUNNING' THEN 1 ELSE 0 END 
        from(
        select parse_json(SYSTEM$PIPE_STATUS( 'ags_game_audience.raw.PIPE_GET_NEW_FILES' )) as pipe_status)
    )
    AS actual,
    1 AS expected,
    'Pipe exists and is RUNNING' AS description
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
    SELECT 'DNGW07' 
    AS step,
    (
        select count(*)/count(*) from snowflake.account_usage.query_history
        where query_text like '%case when game_session_length < 10%'
    )
    AS actual,
    1 AS expected,
    'Curated Data Lesson completed' AS description
);

-- DO NOT EDIT THIS CODE
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
 SELECT
 'DNGW01' as step
  ,(
      select count(*)  
      from ags_game_audience.raw.logs
      where is_timestamp_ntz(to_variant(datetime_iso8601))= TRUE 
   ) as actual
, 250 as expected
, 'Project DB and Log File Set Up Correctly' as description
); 

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
SELECT
   'DNGW02' as step
   ,( select sum(tally) from(
        select (count(*) * -1) as tally
        from ags_game_audience.raw.logs 
        union all
        select count(*) as tally
        from ags_game_audience.raw.game_logs)     
     ) as actual
   ,250 as expected
   ,'View is filtered' as description
); 

