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
    SELECT 'DABW001'
    AS step,
    (
        select count(*)
        from SMOOTHIES.PUBLIC.FRUIT_OPTIONS
    )
    AS actual,
    25 AS expected,
    'Fruit Options table looks good' AS description
);

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
    SELECT 'DABW002'
    AS step,
    (
        select IFF(count(*)>=5,5,0)
        from (select ingredients from smoothies.public.orders
        group by ingredients)
    )
    AS actual,
    5 AS expected,
    'At least 5 different orders entered' AS description
);

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
    SELECT 'DABW003'
    AS step,
    (
        select ascii(fruit_name) from smoothies.public.fruit_options
        where fruit_name ilike 'z%'
    )
    AS actual,
    90 AS expected,
    'A mystery check for the inquisitive' AS description
);

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
    SELECT 'DABW004'
    AS step,
    (
        select count(*) from smoothies.information_schema.columns
        where table_schema = 'PUBLIC' 
        and table_name = 'ORDERS'
        and column_name = 'ORDER_FILLED'
        and column_default = 'FALSE'
        and data_type = 'BOOLEAN'
    )
    AS actual,
    1 AS expected,
    'Order Filled is Boolean' AS description
);

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
    SELECT 'DABW005'
    AS step,
    (
        select IFF(count(*)>=2, 2, 0) as num_sis_apps
        from (
            select count(*) as tally
            from snowflake.account_usage.query_history
            where query_text like 'execute streamlit%'
            group by query_text)
    )
    AS actual,
    2 AS expected,
    'There seem to be 2 SiS Apps' AS description
);

set this = -10.5;
set that = 2;
set the_other = 1000;

CREATE FUNCTION sum_mystery_bag_vars(var1 number, var2 number, var3 number)
    RETURNS NUMBER AS 'SELECT var1+var2+var3';

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
    SELECT 'DABW006'
    AS step,
    (
        select util_db.public.sum_mystery_bag_vars($this,$that,$the_other)
    )
    AS actual,
    991.5 AS expected,
    'Mystery Bag Function Output' AS description
);

CREATE FUNCTION neutralize_whining(var1 varchar(255))
    RETURNS VARCHAR(255) AS 'SELECT INITCAP(var1)';

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
    SELECT 'DABW008'
    AS step,
    (
        select hash(neutralize_whining('bUt mOm i wAsHeD tHe dIsHes yEsTeRdAy'))
    )
    AS actual,
    -4759027801154767056 AS expected,
    'WHINGE UDF Works' AS description
);

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
    SELECT 'DABW008'
    AS step,
    (
        select sum(hash_ing) from
        (
            select hash(ingredients) as hash_ing
            from smoothies.public.orders
            where order_ts is not null
            and name_on_order is not null
            and (name_on_order = 'Kevin' and order_filled = FALSE and hash_ing = 7976616299844859825)
            or (name_on_order ='Divya' and order_filled = TRUE and hash_ing = -6112358379204300652)
            or (name_on_order ='Xi' and order_filled = TRUE and hash_ing = 1016924841131818535)
        )
    )
    AS actual,
    2881182761772377708 AS expected,
    'Followed challenge lab directions' AS description
);