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
    SELECT 'DWW01' 
    AS step,
    (
        SELECT COUNT(*)
        FROM garden_plants.information_schema.schemata
        WHERE schema_name
        IN
        (
            'FLOWERS',
            'VEGGIES',
            'FRUITS'
        )
    )
    AS actual,
    3 AS expected,
    'Created 3 Garden Plant schemas' AS description
);

SELECT grader (
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
    SELECT 'DWW02'
    AS step,
    (
        SELECT COUNT(*)
        FROM garden_plants.information_schema.schemata
        WHERE schema_name = 'PUBLIC'
    )
    AS actual,
    0 AS expected,
    'Deleted PUBLIC schema.' AS description
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
    SELECT 'DWW03'
    AS step,
    (
        SELECT COUNT(*)
        FROM garden_plants.information_schema.tables
        WHERE table_name = 'ROOT_DEPTH'
    )
    AS actual,
    1 AS expected,
    'ROOT_DEPTH Table Exists' AS description    
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
    SELECT 'DWW04'
    AS step,
    (
        SELECT COUNT(*)
        AS schemas_found
        FROM util_db.information_schema.schemata
    )
    AS actual,
    2 AS expected,
    'UTIL_DB Schemas' as description
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
    SELECT 'DWW05'
    AS step,
    (
        SELECT row_count
        FROM garden_plants.information_schema.tables
        WHERE table_name = 'ROOT_DEPTH'
    )
    AS actual,
    3 AS expected,
    'ROOT_DEPTH row count' AS description
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
    SELECT 'DWW06' 
    AS step,
    (
        SELECT COUNT(*)
        FROM garden_plants.information_schema.tables
        WHERE table_name = 'VEGETABLE_DETAILS'
    )
    AS actual,
    1 AS expected,
    'VEGETABLE_DETAILS Table' AS description
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
    SELECT 'DWW07' 
    AS step,
    (
        SELECT row_count
        FROM garden_plants.information_schema.tables
        WHERE table_name = 'VEGETABLE_DETAILS'
    )
    AS actual,
    41 AS expected,
    'VEG_DETAILS row count' AS description
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
    SELECT 'DWW08' 
    AS step,
    (
        SELECT IFF
            (
            COUNT(*)=0,
            0,
            COUNT(*)/COUNT(*)
            )
        FROM TABLE
        (
            information_schema.query_history()
        )
        WHERE query_text
        LIKE 'execute NOTEBOOK%Uncle Yer%'
    )
    AS actual,
    1 AS expected,
    'Notebook success!' AS description
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
    SELECT 'DWW09' 
    AS step,
    (
        SELECT IFF
            (
            COUNT(*)=0,
            0,
            COUNT(*)/COUNT(*)
            )
        FROM snowflake.account_usage.query_history
        WHERE query_text
        LIKE 'execute streamlit "GARDEN_PLANTS"."FRUITS".%'
    )
    AS actual,
    1 AS expected,
    'SiS App Works' AS description
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
    SELECT 'DWW10' 
    AS step,
    (
        SELECT COUNT(*)
        FROM util_db.information_schema.stages
        WHERE stage_name='MY_INTERNAL_STAGE'
        AND stage_type IS NULL
    )
    AS actual,
    1 AS expected,
    'Internal stage created' AS description
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
    SELECT 'DWW11' 
    AS step,
    (
        SELECT row_count
        FROM garden_plants.information_schema.tables
        WHERE table_name = 'VEGETABLE_DETAILS_SOIL_TYPE'
    )
    AS actual,
    42 AS expected,
    'Veg Det Soil Type Count' AS description
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
    SELECT 'DWW12' 
    AS step,
    (
        SELECT row_count
        FROM garden_plants.information_schema.tables
        WHERE table_name = 'VEGETABLE_DETAILS_PLANT_HEIGHT'
    )
    AS actual,
    41 AS expected,
    'Veg Detail Plant Height Count' AS description
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
    SELECT 'DWW13' 
    AS step,
    (
        SELECT row_count
        FROM garden_plants.information_schema.tables
        WHERE table_name = 'LU_SOIL_TYPE'
    )
    AS actual,
    8 AS expected,
    'Soil Type Look Up Table' AS description
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
    SELECT 'DWW14' 
    AS step,
    (
        SELECT COUNT(*)
        FROM garden_plants.information_schema.file_formats
        WHERE file_format_name = 'L9_CHALLENGE_FF'
        AND field_delimiter = '\t'
    )
    AS actual,
    1 AS expected,
    'Challenge File Format Created' AS description
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
    SELECT 'DWW15' 
    AS step,
    (
        SELECT COUNT(*)
        FROM library_card_catalog.public.book_to_author ba
        JOIN library_card_catalog.public.author a
        ON ba.author_uid = a.author_uid
        JOIN library_card_catalog.public.book b
        ON ba.book_uid = b.book_uid
    )
    AS actual,
    6 AS expected,
    '3NF DB was Created.' AS description
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
    SELECT 'DWW16' 
    AS step,
    (
        SELECT row_count
        FROM library_card_catalog.information_schema.tables
        WHERE table_name = 'AUTHOR_INGEST_JSON'
    )
    AS actual,
    6 AS expected,
    'Check number of rows' AS description
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
    SELECT 'DWW17' 
    AS step,
    (
        SELECT row_count
        FROM library_card_catalog.information_schema.tables
        WHERE table_name = 'NESTED_INGEST_JSON'
    )
    AS actual,
    5 AS expected,
    'Check number of rows' AS description
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
    SELECT 'DWW18' 
    AS step,
    (
        SELECT row_count
        FROM social_media_floodgates.information_schema.tables
        WHERE table_name = 'TWEET_INGEST'
    )
    AS actual,
    9 AS expected,
    'Check number of rows' AS description
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
    SELECT 'DWW19' 
    AS step,
    (
        SELECT COUNT(*)
        FROM social_media_floodgates.information_schema.views
        WHERE table_name = 'HASHTAGS_NORMALIZED'
    )
    AS actual,
    1 AS expected,
    'Check number of rows' AS description
);