SELECT 'Hello World!';
SELECT 'Hello World!' AS "Greeting";

SHOW DATABASES;
SHOW SCHEMAS;
SHOW SCHEMAS IN ACCOUNT;

CREATE TABLE root_depth (
    root_depth_id       NUMBER(1),
    root_depth_code     TEXT(1),
    root_depth_name     TEXT(7),
    unit_of_measure     TEXT(2),
    range_min           NUMBER(2),
    range_max           NUMBER(2)
);

INSERT INTO root_depth
VALUES (
    1,
    'S',
    'Shallow',
    'cm',
    45,
    60
);

INSERT INTO root_depth
VALUES (
    2,
    'M',
    'Medium',
    'cm',
    30,
    45
),
(
    3,
    'D',
    'Deep',
    'cm',
    60,
    90
);

SELECT * FROM root_depth;

USE SCHEMA information_schema;

SELECT *
FROM schemata;
SELECT * 
FROM schemata
WHERE schema_name IN (
    'FLOWERS',
    'FRUITS',
    'VEGGIES'
);

SELECT COUNT(*) AS schemas_found,
'3' AS schemas_expected
FROM schemata
WHERE schema_name
IN (
    'FLOWERS',
    'FRUITS',
    'VEGGIES'
);

USE SCHEMA veggies;

CREATE TABLE vegetable_details
(
    plant_name          VARCHAR(25),
    root_depth_code     VARCHAR(1)
);

SELECT *
FROM vegetable_details;

SELECT *
FROM vegetable_details
WHERE plant_name = 'Spinach'
AND root_depth_code = 'D';

DELETE FROM vegetable_details
WHERE plant_name = 'Spinach'
AND root_depth_code = 'D';

SELECT *
FROM vegetable_details;

USE SCHEMA flowers;

CREATE TABLE flower_details
(
    plant_name          VARCHAR(25),
    root_depth_code     VARCHAR(1)
);

SELECT *
FROM vegetable_details;

USE SCHEMA fruits;

CREATE TABLE fruit_details
(
    plant_name          VARCHAR(25),
    root_depth_code     VARCHAR(1)
);

USE SCHEMA veggies;

CREATE TABLE vegetable_details_soil_type
(
    plant_name      VARCHAR(25),
    soil_type       NUMBER(1,0)
);

CREATE FILE FORMAT pipecolsep_oneheadrow
    type = 'CSV'
    field_delimiter = '|'
    skip_header = 1;

COPY INTO vegetable_details_soil_type
FROM @util_db.public.my_internal_stage
    files = ('VEG_NAME_TO_SOIL_TYPE_PIPE.txt')
    file_format = (format_name=pipecolsep_oneheadrow);

SELECT COUNT(*)
FROM vegetable_details_soil_type;

SELECT *
FROM vegetable_details_soil_type;

CREATE FILE FORMAT commasep_dblquot_oneheadrow
    type = 'CSV'
    field_delimiter = ','
    skip_header = 1
    field_optionally_enclosed_by = '"';

SELECT $1
FROM @util_db.public.my_internal_stage/LU_SOIL_TYPE.tsv;

SELECT $1, $2, $3
FROM @util_db.public.my_internal_stage/LU_SOIL_TYPE.tsv
(
    file_format => commasep_dblquot_oneheadrow
);

CREATE FILE FORMAT l9_challenge_ff --yes daddy, I want the DWW Badge so bad... ughhh
    type = 'CSV'
    field_delimiter = '\t'
    skip_header = 1;

SELECT $1, $2, $3
FROM @util_db.public.my_internal_stage/LU_SOIL_TYPE.tsv
(
    file_format => l9_challenge_ff --please daddy let me earn the badge....
);

CREATE TABLE lu_soil_type
(
    soil_type_id            NUMBER,
    soil_type               VARCHAR(15),
    soil_description        VARCHAR(75)
);

COPY INTO lu_soil_type
FROM @util_db.public.my_internal_stage
    files = ('LU_SOIL_TYPE.tsv')
    file_format = (format_name=l9_challenge_ff); --gimme the badge I want it bad yeah....

SELECT *
FROM lu_soil_type;

USE SCHEMA veggies;

SELECT $1, $2, $3, $4
FROM @util_db.public.my_internal_stage/veg_plant_height.csv
(
    file_format => commasep_dblquot_oneheadrow
);

CREATE TABLE vegetable_details_plant_height
(
    plant_name              VARCHAR(25),
    uom                     VARCHAR(1),
    low_end_of_range        NUMBER(2,0),
    high_end_of_range       NUMBER(2,0)
);

COPY INTO vegetable_details_plant_height
FROM @util_db.public.my_internal_stage
    files = ('veg_plant_height.csv')
    file_format = (format_name=commasep_dblquot_oneheadrow);

SELECT *
FROM vegetable_details_plant_height;

USE ROLE sysadmin;

CREATE DATABASE library_card_catalog
    comment = 'DWW Lesson 10';

USE DATABASE library_card_catalog;

CREATE TABLE book
(
    book_uid            NUMBER AUTOINCREMENT,
    title               VARCHAR(50),
    year_published      NUMBER(4,0)
);

INSERT INTO book
(
    title,
    year_published
)
VALUES
(
    'Food',
    2001
),
(
    'Food',
    2006
),
(
    'Food',
    2008
),
(
    'Food',
    2016
),
(
    'Food',
    2015
);

SELECT *
FROM book;

CREATE TABLE author
(
    author_uid      NUMBER,
    first_name      VARCHAR(50),
    middle_name     VARCHAR(50),
    last_name       VARCHAR(50)
);

INSERT INTO author
VALUES
(
    1, 'Fiona', '', 'Macdonald'
),
(
    2, 'Gian', 'Paulo', 'Faleschini'
);

SELECT *
FROM author;

CREATE SEQUENCE seq_author_uid
    start = 1,
    increment = 1,
    ORDER,
    comment = 'Use this to fill in the author_UID';

SELECT seq_author_uid.nextval; 

DROP SEQUENCE seq_author_uid;

USE DATABASE library_card_catalog;
USE SCHEMA public;
CREATE SEQUENCE seq_author_uid
    start = 3,
    increment = 1,
    ORDER,
    comment = 'Use this to fill in the author_UID every time you add a row';

INSERT INTO author
(
    author_uid,
    first_name,
    middle_name,
    last_name
)
VALUES
(
    seq_author_uid.nextval, 'Laura', 'K', 'Egendorf'
),
(
    seq_author_uid.nextval, 'Jan', '', 'Grover'
),
(
    seq_author_uid.nextval, 'Jennifer', '', 'Clapp'
),
(
    seq_author_uid.nextval, 'Kathleen', '', 'Petelinsek'
);

SELECT *
FROM author;

CREATE TABLE book_to_author
(
    book_uid        NUMBER,
    author_uid      NUMBER
);

INSERT INTO book_to_author
(
    book_uid,
    author_uid
)
VALUES
(
    1,
    1
),
(
    1,
    2
),
(
    2,
    3
),
(
    3,
    4
),
(
    4,
    5
),
(
    5,
    6
);

SELECT *
FROM book_to_author ba
JOIN author a
ON ba.author_uid = a.author_uid
JOIN book b
ON b.book_uid = ba.book_uid;

CREATE TABLE author_ingest_json
(
    raw_author      VARIANT
);

CREATE FILE FORMAT json_file_format
    type = 'json'
    compression = 'auto'
    enable_octal = FALSE
    allow_duplicate = FALSE
    strip_outer_array = TRUE
    strip_null_values = FALSE
    ignore_utf8_errors = FALSE;

SELECT $1
FROM @util_db.public.my_internal_stage/author_with_header.json
(
    file_format => json_file_format
);

COPY INTO author_ingest_json
FROM @util_db.public.my_internal_stage
    files = ('author_with_header.json')
    file_format = (format_name=json_file_format);

SELECT *
FROM author_ingest_json;

SELECT raw_author:AUTHOR_UID
FROM author_ingest_json;

SELECT
    raw_author:AUTHOR_UID,
    raw_author:FIRST_NAME::STRING AS first_name,
    raw_author:MIDDLE_NAME::STRING AS middle_name,
    raw_author:LAST_NAME::STRING AS last_name,
FROM author_ingest_json;

CREATE TABLE nested_ingest_json
(
    raw_nested_book     VARIANT
);

SELECT $1
FROM @util_db.public.my_internal_stage/json_book_author_nested.txt
(
    file_format => json_file_format
);

COPY INTO nested_ingest_json
FROM @util_db.public.my_internal_stage
    files = ('json_book_author_nested.txt')
    file_format = (format_name=json_file_format);

SELECT raw_nested_book
FROM nested_ingest_json;

SELECT raw_nested_book:year_published
FROM nested_ingest_json;

SELECT raw_nested_book:authors
FROM nested_ingest_json;

SELECT value:first_name
FROM
    nested_ingest_json,
    LATERAL FLATTEN
    (
        INPUT => raw_nested_book:authors
    );

SELECT value:first_name
FROM
    nested_ingest_json,
    TABLE 
    (
        FLATTEN
        (
            raw_nested_book:authors
        )
    );

SELECT 
    value:first_name::VARCHAR,
    value:last_name::VARCHAR
FROM
    nested_ingest_json,
    LATERAL FLATTEN
    (
        INPUT => raw_nested_book:authors
    );


SELECT 
    value:first_name::VARCHAR AS first_nm,
    value:last_name::VARCHAR AS last_nm
FROM
    nested_ingest_json,
    LATERAL FLATTEN
    (
        INPUT => raw_nested_book:authors
    );

CREATE DATABASE social_media_floodgates;
USE DATABASE social_media_floodgates;
USE SCHEMA public;
CREATE TABLE tweet_ingest
(
    raw_status      VARIANT
);

CREATE FILE FORMAT json_file_format
    type = 'json'
    compression = 'auto'
    enable_octal = FALSE
    allow_duplicate = FALSE
    strip_outer_array = TRUE
    strip_null_values = FALSE
    ignore_utf8_errors = FALSE;

SELECT $1
FROM @util_db.public.my_internal_stage/nutrition_tweets.json
(
    file_format => json_file_format
);

COPY INTO tweet_ingest
FROM @util_db.public.my_internal_stage
    files = ('nutrition_tweets.json')
    file_format = (format_name=json_file_format);

SELECT raw_status
FROM tweet_ingest;

SELECT raw_status:entities
FROM tweet_ingest;

SELECT raw_status:entities:hashtags
FROM tweet_ingest;

SELECT raw_status:entities:hashtags[0].text
FROM tweet_ingest
WHERE raw_status:entities:hashtags[0].text IS NOT NULL;

SELECT raw_status:created_at::date
FROM tweet_ingest
ORDER BY raw_status:created_at::date;

SELECT value
FROM 
    tweet_ingest,
    LATERAL FLATTEN
        (
            INPUT => raw_status:entities:urls
        );

SELECT value
FROM
    tweet_ingest,
    TABLE
    (
        FLATTEN
        (
            raw_status:entities:urls
        )
    );

SELECT value:text::VARCHAR AS hashtag_used
FROM
    tweet_ingest,
    LATERAL FLATTEN
    (
        INPUT => raw_status:entities:hashtags
    );

CREATE VIEW social_media_floodgates.public.hashtags_normalized AS
SELECT
    t.raw_status:user:name::string AS user_name,
    t.raw_status:id::string AS tweet_id,
    h.value:text::string AS hashtag_used
FROM social_media_floodgates.public.tweet_ingest t,
     LATERAL FLATTEN
     (
        INPUT => t.raw_status:entities:hashtags
    ) h;

SELECT *
FROM hashtags_normalized;