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
    SELECT 'DLKW01' 
    AS step,
    (
        select count(*)  
        from ZENAS_ATHLEISURE_DB.INFORMATION_SCHEMA.STAGES 
        where stage_schema = 'PRODUCTS'
        and 
        (stage_type = 'Internal Named' 
        and stage_name = ('PRODUCT_METADATA'))
        or stage_name = ('SWEATSUITS')
    )
    AS actual,
    2 AS expected,
    'Zena stages look good' AS description
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
    SELECT 'DLKW02' 
    AS step,
    (
        select sum(tally) from
        (
            select count(*) as tally
            from ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATBAND_PRODUCT_LINE
            where length(product_code) > 7 
            union
            select count(*) as tally
            from ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUIT_SIZES
            where LEFT(sizes_available,2) = char(13)||char(10)
        )     
    )
    AS actual,
    0 AS expected,
    'Leave data where it lands.' AS description
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
    SELECT 'DLKW03' 
    AS step,
    (
        select count(*) from ZENAS_ATHLEISURE_DB.PRODUCTS.CATALOG
    )
    AS actual,
    180 AS expected,
    'Cross-joined view exists' AS description
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
    SELECT 'DLKW04' 
    AS step,
    (
        select count(*) 
        from zenas_athleisure_db.products.catalog_for_website 
        where upsell_product_desc not like '%e, Bl%'
    )
    AS actual,
    6 AS expected,
    'Relentlessly resourceful' AS description
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
    SELECT 'DLKW05' 
    AS step,
    (
    select sum(tally)
    from 
        (
            select count(*) as tally
            from mels_smoothie_challenge_db.information_schema.stages 
            union all
            select count(*) as tally
            from mels_smoothie_challenge_db.information_schema.file_formats
        )
    )
    AS actual,
    4 AS expected,
    'Camila\'s Trail Data is Ready to Query' AS description
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
    SELECT 'DLKW06' 
    AS step,
    (
        select count(*) as tally
        from mels_smoothie_challenge_db.information_schema.views 
        where table_name in ('CHERRY_CREEK_TRAIL','DENVER_AREA_TRAILS')
    )
    AS actual,
    2 AS expected,
    'Mel\'s views on the geospatial data from Camila' AS description
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
    SELECT 'DLKW07' 
    AS step,
    (
        select round(max(max_northsouth))
        from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_AND_BOUNDARIES
    )
    AS actual,
    40 AS expected,
    'Trails Northern Extent' AS description
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
    SELECT 'DLKW08' 
    AS step,
    (
        select truncate(distance_to_melanies)
        from mels_smoothie_challenge_db.locations.denver_bike_shops
        where name like '%Mojo%'
    )
    AS actual,
    14084 AS expected,
    'Bike Shop View Distance Calc works' AS description
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
    SELECT 'DLKW09' 
    AS step,
    (
        select row_count
        from mels_smoothie_challenge_db.information_schema.tables
        where table_schema = 'TRAILS'
        and table_name = 'SMV_CHERRY_CREEK_TRAIL'
    )
    AS actual,
    3526 AS expected,
    'Secure Materialized View Created' AS description
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
    SELECT 'DLKW10' 
    AS step,
    (
        select row_count
        from MY_ICEBERG_DB.INFORMATION_SCHEMA.TABLES
        where table_catalog = 'MY_ICEBERG_DB'
        and table_name like 'CCT_%'
        and table_type = 'BASE TABLE'
    )
    AS actual,
    100 AS expected,
    'Iceberg table created and populated!' AS description
);