DROP DATABASE snowflake_sample_data;
ALTER DATABASE sfsalesshared_sfc_samples_prod3_sample_data
RENAME TO snowflake_sample_data;

USE DATABASE snowflake_sample_data;
USE SCHEMA tpch_sf1;

SELECT DISTINCT c_mktsegment
FROM customer;

SELECT c_mktsegment, COUNT(*) AS count
FROM customer
GROUP BY c_mktsegment
ORDER BY count;

SELECT n_nationkey, n_name, n_regionkey
FROM nation;

SELECT r_regionkey, r_name
FROM region;

SELECT
    r_name AS region,
    n_name AS nation
FROM snowflake_sample_data.tpch_sf1.nation
JOIN snowflake_sample_data.tpch_sf1.region
ON n_regionkey = r_regionkey
ORDER BY region, nation;

SELECT
    r_name as region,
    COUNT(n_name) as num_countries
FROM snowflake_sample_data.tpch_sf1.nation
JOIN snowflake_sample_data.tpch_sf1.region
ON n_regionkey = r_regionkey
GROUP BY region;

CREATE DATABASE intl_db;
USE SCHEMA public;

CREATE WAREHOUSE intl_wh
WITH
    warehouse_size = 'XSMALL',
    warehouse_type = 'STANDARD',
    auto_suspend = 600,
    auto_resume = TRUE;

USE WAREHOUSE intl_wh;

CREATE TABLE int_stds_org_3166
(
    iso_country_name            VARCHAR(100),
    country_name_official       VARCHAR(200),
    sovreignty                  VARCHAR(40),
    alpha_code_2digit           VARCHAR(2),
    alpha_code_3digit           VARCHAR(3),
    numeric_country_code        INTEGER,
    iso_subdivision             VARCHAR(15),
    internet_domain_code        VARCHAR(10)
);

CREATE FILE FORMAT pipe_dblquote_header_cr
    type = 'CSV'
    compression = 'AUTO'
    field_delimiter = '|'
    record_delimiter = '\r'
    skip_header = 1
    field_optionally_enclosed_by = '\042'
    trim_space = FALSE;

SHOW STAGES IN ACCOUNT;
CREATE STAGE util_db.public.aws_s3_bucket url = 's3://uni-cmcw';
LIST @util_db.public.aws_s3_bucket;

COPY INTO int_stds_org_3166
FROM @util_db.public.aws_s3_bucket
    files = ('ISO_Countries_UTF8_pipe.csv')
    file_format = (format_name = pipe_dblquote_header_cr);

SELECT 
    COUNT(*) AS found,
    '249' AS expected
FROM int_stds_org_3166;

SELECT
    iso_country_name,
    country_name_official,
    alpha_code_2digit,
    r_name AS region
FROM int_stds_org_3166 i
LEFT JOIN snowflake_sample_data.tpch_sf1.nation n
    ON UPPER(i.iso_country_name) = n.n_name
LEFT JOIN snowflake_sample_data.tpch_sf1.region r
    ON n_regionkey = r_regionkey;

CREATE VIEW intl_db.public.nations_sample_plus_iso
(
    iso_country_name,
    country_name_official,
    alpha_code_2digit,
    region
)
AS
    SELECT
        iso_country_name,
        country_name_official,
        alpha_code_2digit,
        r_name AS region
    FROM int_stds_org_3166 i
    LEFT JOIN snowflake_sample_data.tpch_sf1.nation n
    ON UPPER(i.iso_country_name) = n.n_name
    LEFT JOIN snowflake_sample_data.tpch_sf1.region r
    ON n_regionkey = r_regionkey;

SELECT *
FROM nations_sample_plus_iso;

CREATE TABLE currencies
(
    currency_id INTEGER,
    currency_char_code VARCHAR(3),
    currency_symbol VARCHAR(4),
    currency_digital_code VARCHAR(3),
    currency_digital_name VARCHAR(30)
)
    comment = 'Information about currencies including character codes, symbols, digital codes, etc.';

CREATE TABLE country_code_to_currency_code
(
    country_char_code VARCHAR(3),
    country_numeric_code INTEGER,
    country_name VARCHAR(100),
    currency_name VARCHAR(100),
    currency_char_code VARCHAR(3),
    currency_numeric_code INTEGER
)
    comment = 'Mapping tale currencies to countries';

CREATE FILE FORMAT csv_comma_lf_header
    type = 'CSV'
    field_delimiter = ','
    record_delimiter = '\n'
    skip_header = 1;

COPY INTO currencies
FROM @util_db.public.aws_s3_bucket
    files = ('currencies.csv')
    file_format = (format_name = csv_comma_lf_header);

COPY INTO country_code_to_currency_code
FROM @util_db.public.aws_s3_bucket
    files = ('country_code_to_currency_code.csv')
    file_format = (format_name = csv_comma_lf_header);

CREATE VIEW simple_currency
(
    cty_code,
    cur_code
)
AS
    SELECT
        country_char_code,
        currency_char_code
    FROM country_code_to_currency_code;

SELECT *
FROM simple_currency;

USE ROLE ORGADMIN;
SELECT SYSTEM$ENABLE_GLOBAL_DATA_SHARING_FOR_ACCOUNT('UNB40488');
SELECT SYSTEM$IS_GLOBAL_DATA_SHARING_ENABLED_FOR_ACCOUNT('UNB40488

