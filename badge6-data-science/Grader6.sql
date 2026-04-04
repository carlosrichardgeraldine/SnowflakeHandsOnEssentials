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
    SELECT 'DSCW01' 
    AS step,
    (
        select  iff(count(*)>=5, 5, 0)
        from (
        select model_name
        from SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AISQL_USAGE_HISTORY
        where function_name ilike '%AI_COMPLETE%'
        group by model_name
        )
    )
    AS actual,
    5 AS expected,
    'Used Different models when exploring Cortex Playground' AS description
);

list @camillas_db.cortex_analyst.cortex_analyst_model_stage;

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
    SELECT 'DSCW02' 
    AS step,
    (
        select IFF(count(*)>0,1,0) 
        from table(result_scan(last_query_id()))
        where "name" = 'cortex_analyst_model_stage/CAMILLAS_JUNE_TOURNAMENT.yaml'
    )
    AS actual,
    1 AS expected,
    'Semantic Model Complete' AS description
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
    SELECT 'DSCW03' 
    AS step,
    (
        select round(count(*)/iff(count(*)=0,1,count(*)),0) as tally
        from snowflake.account_usage.query_history
        where query_text like '%CREATE SNOWFLAKE.ML.FORECAST camillas_practice_goal_forecasting%'
        and execution_status = 'SUCCESS'
    )
    AS actual,
    1 AS expected,
    'Created Forecast Model' AS description
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
    SELECT 'DSCW04' 
    AS step,
    (
        select round(count(*)/iff(count(*)=0,1,count(*)),0) as tally
        from snowflake.account_usage.query_history
        where query_text like '%CREATE SNOWFLAKE.ML.FORECAST camillas_practice_goal_4cast%'
        and execution_status = 'SUCCESS'
    )
    AS actual,
    1 AS expected,
    'Improved Forecast Model' AS description
);

call camillas_db.classification.player_position_classification!SHOW_FEATURE_IMPORTANCE();

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
    SELECT 'DSCW05' 
    AS step,
    (
        select count(*) from table(result_scan(last_query_id()))
        where FEATURE in ('PASSES','MINUTES_PLAYED','DRIBBLES','ASSISTS', 'SAVES', 'CLAIMS')
    )
    AS actual,
    6 AS expected,
    'Classification Model Complete' AS description
);