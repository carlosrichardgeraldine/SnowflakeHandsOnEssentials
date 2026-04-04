CREATE DATABASE ags_game_audience;
DROP SCHEMA public;
CREATE SCHEMA raw;

CREATE OR REPLACE TABLE game_logs
(
    raw_log     VARIANT
);

CREATE STAGE uni_kishore URL = 's3://uni-kishore';
LIST @uni_kishore;

CREATE FILE FORMAT ff_json_logs
    type = 'JSON'
    strip_outer_array = true;

SELECT $1
FROM @uni_kishore/kickoff
(file_format => ff_json_logs);

SELECT COUNT($1)
FROM @uni_kishore/kickoff
(file_format => ff_json_logs);

COPY INTO game_logs
FROM @uni_kishore/kickoff
file_format = (format_name = ff_json_logs);

SELECT
    $1:agent::VARCHAR                   AS agent,
    $1:datetime_iso8601::DATETIME       AS datetime_iso8601,
    $1:user_event::VARCHAR              AS user_event,
    $1:user_login::VARCHAR              AS user_login,
    raw_log
FROM game_logs;

CREATE VIEW raw.logs AS
SELECT
    $1:agent::VARCHAR                   AS agent,
    $1:datetime_iso8601::DATETIME       AS datetime_iso8601,
    $1:user_event::VARCHAR              AS user_event,
    $1:user_login::VARCHAR              AS user_login,
    raw_log
FROM game_logs
WHERE ip_address;

SELECT CURRENT_TIMESTAMP();
ALTER SESSION SET timezone = 'UTC';
SELECT CURRENT_TIMESTAMP();

LIST @uni_kishore;

COPY INTO game_logs
FROM @uni_kishore/updated_feed
file_format = (format_name = ff_json_logs);

SELECT COUNT(*)
FROM logs;

SELECT *
FROM logs
WHERE agent IS NOT NULL;

CREATE OR REPLACE VIEW RAW.logs AS
SELECT
    $1:ip_address::VARCHAR              AS ip_address,
    $1:datetime_iso8601::DATETIME       AS datetime_iso8601,
    $1:user_event::VARCHAR              AS user_event,
    $1:user_login::VARCHAR              AS user_login,
    raw_log
FROM game_logs
WHERE ip_address IS NOT NULL;

SELECT COUNT(*) FROM raw.logs;

SELECT *
FROM logs
WHERE user_login ILIKE '%prajina%';

select parse_ip('100.41.16.160','inet');

CREATE SCHEMA enhanced;

select start_ip, end_ip, start_ip_int, end_ip_int, city, region, country, timezone
from IPINFO_GEOLOC.demo.location
where parse_ip('100.41.16.160', 'inet'):ipv4 --Kishore's Headset's IP Address
BETWEEN start_ip_int AND end_ip_int;

select logs.*
       , loc.city
       , loc.region
       , loc.country
       , loc.timezone
from AGS_GAME_AUDIENCE.RAW.LOGS logs
join IPINFO_GEOLOC.demo.location loc
where parse_ip(logs.ip_address, 'inet'):ipv4 
BETWEEN start_ip_int AND end_ip_int;

SELECT logs.ip_address
, logs.user_login
, logs.user_event
, logs.datetime_iso8601
, city
, region
, country
, timezone 
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int;

SELECT
    logs.ip_address,
    logs.user_login,
    logs.user_event,
    logs.datetime_iso8601,
    city,
    region,
    country,
    timezone,
    CONVERT_TIMEZONE('UTC', timezone, logs.datetime_iso8601) AS game_event_ltz
FROM ags_game_audience.raw.logs logs
JOIN ipinfo_geoloc.demo.location loc
    ON ipinfo_geoloc.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND ipinfo_geoloc.public.to_int(logs.ip_address)
BETWEEN start_ip_int AND end_ip_int;

SELECT
    logs.ip_address,
    logs.user_login,
    logs.user_event,
    logs.datetime_iso8601,
    city,
    region,
    country,
    timezone,
    CONVERT_TIMEZONE('UTC', timezone, logs.datetime_iso8601) AS game_event_ltz,
    DAYNAME(game_event_ltz) AS dow_name
FROM ags_game_audience.raw.logs logs
JOIN ipinfo_geoloc.demo.location loc
    ON ipinfo_geoloc.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND ipinfo_geoloc.public.to_int(logs.ip_address)
BETWEEN start_ip_int AND end_ip_int;

create table ags_game_audience.raw.time_of_day_lu
(  hour number
   ,tod_name varchar(25)
);

insert into ags_game_audience.raw.time_of_day_lu
values
(6,'Early morning'),
(7,'Early morning'),
(8,'Early morning'),
(9,'Mid-morning'),
(10,'Mid-morning'),
(11,'Late morning'),
(12,'Late morning'),
(13,'Early afternoon'),
(14,'Early afternoon'),
(15,'Mid-afternoon'),
(16,'Mid-afternoon'),
(17,'Late afternoon'),
(18,'Late afternoon'),
(19,'Early evening'),
(20,'Early evening'),
(21,'Late evening'),
(22,'Late evening'),
(23,'Late evening'),
(0,'Late at night'),
(1,'Late at night'),
(2,'Late at night'),
(3,'Toward morning'),
(4,'Toward morning'),
(5,'Toward morning');

select tod_name, listagg(hour,',') 
from ags_game_audience.raw.time_of_day_lu
group by tod_name;

SELECT
    logs.ip_address,
    logs.user_login,
    logs.user_event,
    logs.datetime_iso8601,
    city,
    region,
    country,
    timezone,
    CONVERT_TIMEZONE('UTC', timezone, logs.datetime_iso8601) AS game_event_ltz,
    DAYNAME(game_event_ltz) AS dow_name,
    tod_name
FROM ags_game_audience.raw.logs logs
JOIN ipinfo_geoloc.demo.location loc
    ON ipinfo_geoloc.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND ipinfo_geoloc.public.to_int(logs.ip_address)
BETWEEN start_ip_int AND end_ip_int
JOIN ags_game_audience.raw.time_of_day_lu tod
    ON HOUR(game_event_ltz) = tod.hour;

SELECT
    logs.ip_address,
    logs.user_login AS gamer_name,
    logs.user_event AS game_event_name,
    logs.datetime_iso8601 AS game_event_utc,
    city,
    region,
    country,
    timezone,
    CONVERT_TIMEZONE('UTC', timezone, logs.datetime_iso8601) AS game_event_ltz,
    DAYNAME(game_event_ltz) AS dow_name,
    tod_name
FROM ags_game_audience.raw.logs logs
JOIN ipinfo_geoloc.demo.location loc
    ON ipinfo_geoloc.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND ipinfo_geoloc.public.to_int(logs.ip_address)
BETWEEN start_ip_int AND end_ip_int
JOIN ags_game_audience.raw.time_of_day_lu tod
    ON HOUR(game_event_ltz) = tod.hour;

CREATE OR REPLACE TABLE logs_enhanced AS
(
    SELECT
        logs.ip_address,
        logs.user_login AS gamer_name,
        logs.user_event AS game_event_name,
        logs.datetime_iso8601 AS game_event_utc,
        city,
        region,
        country,
        timezone,
        CONVERT_TIMEZONE('UTC', timezone, logs.datetime_iso8601) AS game_event_ltz,
        DAYNAME(game_event_ltz) AS dow_name,
        tod_name
    FROM ags_game_audience.raw.logs logs
    JOIN ipinfo_geoloc.demo.location loc
        ON ipinfo_geoloc.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
    AND ipinfo_geoloc.public.to_int(logs.ip_address)
    BETWEEN start_ip_int AND end_ip_int
    JOIN ags_game_audience.raw.time_of_day_lu tod
        ON HOUR(game_event_ltz) = tod.hour
);

TRUNCATE TABLE logs_enhanced;

INSERT INTO xlogs_enhanced
(
    SELECT
        logs.ip_address,
        logs.user_login AS gamer_name,
        logs.user_event AS game_event_name,
        logs.datetime_iso8601 AS game_event_utc,
        city,
        region,
        country,
        timezone,
        CONVERT_TIMEZONE('UTC', timezone, logs.datetime_iso8601) AS game_event_ltz,
        DAYNAME(game_event_ltz) AS dow_name,
        tod_name
    FROM ags_game_audience.raw.logs logs
    JOIN ipinfo_geoloc.demo.location loc
        ON ipinfo_geoloc.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
    AND ipinfo_geoloc.public.to_int(logs.ip_address)
    BETWEEN start_ip_int AND end_ip_int
    JOIN ags_game_audience.raw.time_of_day_lu tod
        ON HOUR(game_event_ltz) = tod.hour
);

CREATE OR REPLACE TABLE logs_enhanced_bu
CLONE logs_enhanced;

-- MERGE INTO logs_enhanced e
-- USING ags_game_audience.raw.logs r
--     ON r.user_login = e.gamer_name
-- WHEN MATCHED THEN
--     UPDATE SET ip_address = 'Hey I updated matching rows!';

MERGE INTO logs_enhanced e
USING ags_game_audience.raw.logs r
    ON r.user_login = e.gamer_name
    AND r.datetime_iso8601 = e.game_event_utc
    AND r.user_event = e.game_event_name
WHEN MATCHED THEN
    UPDATE SET ip_address = 'Hey I updated matching rows!';

SELECT *
FROM logs_enhanced;

ALTER TABLE logs_enhanced RENAME TO xlogs_enhanced;
ALTER TABLE logs_enhanced_bu RENAME TO logs_enhanced;

MERGE INTO logs_enhanced e
USING
(
    SELECT
        logs.ip_address,
        logs.user_login AS gamer_name,
        logs.user_event AS game_event_name,
        logs.datetime_iso8601 AS game_event_utc,
        city,
        region,
        country,
        timezone,
        CONVERT_TIMEZONE('UTC', timezone, logs.datetime_iso8601) AS game_event_ltz,
        DAYNAME(game_event_ltz) AS dow_name,
        tod_name
    FROM ags_game_audience.raw.logs logs
    JOIN ipinfo_geoloc.demo.location loc
        ON ipinfo_geoloc.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
    AND ipinfo_geoloc.public.to_int(logs.ip_address)
    BETWEEN start_ip_int AND end_ip_int
    JOIN ags_game_audience.raw.time_of_day_lu tod
        ON HOUR(game_event_ltz) = tod.hour
) r
    ON r.gamer_name = e.gamer_name
    AND r.game_event_utc = e.game_event_utc
    AND r.game_event_name = e.game_event_name
WHEN NOT MATCHED THEN
    INSERT
    (
        ip_address,
        gamer_name,
        game_event_name,
        game_event_utc,
        city,
        region,
        country,
        timezone,
        game_event_ltz,
        dow_name,
        tod_name
    )
    VALUES
    (
        ip_address,
        gamer_name,
        game_event_name,
        game_event_utc,
        city,
        region,
        country,
        timezone,
        game_event_ltz,
        dow_name,
        tod_name
    ); 

CREATE TASK ags_game_audience.raw.load_logs_enhanced
    warehouse = 'COMPUTE_wh'
    schedule = '5 minute'
AS
    SELECT 'hello';

EXECUTE TASK ags_game_audience.raw.load_logs_enhanced;

SHOW TASKS IN ACCOUNT;

DESCRIBE TASK ags_game_audience.raw.load_logs_enhanced;

CREATE OR REPLACE TASK ags_game_audience.raw.load_logs_enhanced
    warehouse = 'COMPUTE_wh'
    schedule = '5 minute'
AS
    SELECT
        logs.ip_address,
        logs.user_login AS gamer_name,
        logs.user_event AS game_event_name,
        logs.datetime_iso8601 AS game_event_utc,
        city,
        region,
        country,
        timezone,
        CONVERT_TIMEZONE('UTC', timezone, logs.datetime_iso8601) AS game_event_ltz,
        DAYNAME(game_event_ltz) AS dow_name,
        tod_name
    FROM ags_game_audience.raw.logs logs
    JOIN ipinfo_geoloc.demo.location loc
        ON ipinfo_geoloc.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
    AND ipinfo_geoloc.public.to_int(logs.ip_address)
    BETWEEN start_ip_int AND end_ip_int
    JOIN ags_game_audience.raw.time_of_day_lu tod
        ON HOUR(game_event_ltz) = tod.hour;

SELECT COUNT(*)
FROM ags_game_audience.enhanced.logs_enhanced;

EXECUTE TASK ags_game_audience.raw.load_logs_enhanced;

SELECT COUNT(*)
FROM ags_game_audience.enhanced.logs_enhanced;

CREATE OR REPLACE TASK ags_game_audience.raw.load_logs_enhanced
warehouse = 'COMPUTE_wh'
schedule = '5 minute'
AS
    INSERT INTO ags_game_audience.enhanced.logs_enhanced
    SELECT
        logs.ip_address,
        logs.user_login AS gamer_name,
        logs.user_event AS game_event_name,
        logs.datetime_iso8601 AS game_event_utc,
        city,
        region,
        country,
        timezone,
        CONVERT_TIMEZONE('UTC', timezone, logs.datetime_iso8601) AS game_event_ltz,
        DAYNAME(game_event_ltz) AS dow_name,
        tod_name
    FROM ags_game_audience.raw.logs logs
    JOIN ipinfo_geoloc.demo.location loc
        ON ipinfo_geoloc.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
    AND ipinfo_geoloc.public.to_int(logs.ip_address)
    BETWEEN start_ip_int AND end_ip_int
    JOIN ags_game_audience.raw.time_of_day_lu tod
        ON HOUR(game_event_ltz) = tod.hour;

DESCRIBE TASK raw.load_logs_enhanced;

EXECUTE TASK ags_game_audience.raw.load_logs_enhanced;

SELECT COUNT(*) from logs_enhanced;

TRUNCATE TABLE logs_enhanced;

CREATE OR REPLACE TASK ags_game_audience.raw.load_logs_enhanced
warehouse = 'COMPUTE_wh'
schedule = '5 minute'
AS
    MERGE INTO ags_game_audience.enhanced.logs_enhanced e
    USING
    (
        SELECT
            logs.ip_address,
            logs.user_login AS gamer_name,
            logs.user_event AS game_event_name,
            logs.datetime_iso8601 AS game_event_utc,
            city,
            region,
            country,
            timezone,
            CONVERT_TIMEZONE('UTC', timezone, logs.datetime_iso8601) AS game_event_ltz,
            DAYNAME(game_event_ltz) AS dow_name,
            tod_name
        FROM ags_game_audience.raw.logs logs
        JOIN ipinfo_geoloc.demo.location loc
            ON ipinfo_geoloc.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
        AND ipinfo_geoloc.public.to_int(logs.ip_address)
        BETWEEN start_ip_int AND end_ip_int
        JOIN ags_game_audience.raw.time_of_day_lu tod
            ON HOUR(game_event_ltz) = tod.hour
    ) r
        ON r.gamer_name = e.gamer_name
        AND r.game_event_utc = e.game_event_utc
        AND r.game_event_name = e.game_event_name
    WHEN NOT MATCHED THEN
        INSERT
        (
            ip_address,
            gamer_name,
            game_event_name,
            game_event_utc,
            city,
            region,
            country,
            timezone,
            game_event_ltz,
            dow_name,
            tod_name
        )
        VALUES
        (
            ip_address,
            gamer_name,
            game_event_name,
            game_event_utc,
            city,
            region,
            country,
            timezone,
            game_event_ltz,
            dow_name,
            tod_name
        );

EXECUTE TASK ags_game_audience.raw.load_logs_enhanced;

SELECT COUNT(*) from logs_enhanced;

EXECUTE TASK ags_game_audience.raw.load_logs_enhanced;

SELECT COUNT(*) from logs_enhanced;

USE SCHEMA raw;

CREATE STAGE uni_kishore_pipeline
    URL = 's3://uni-kishore-pipeline'
    DIRECTORY = 
    (
        ENABLE = true
        AUTO_REFRESH = false
    );

LIST @uni_kishore_pipeline;

CREATE TABLE pl_game_logs
(
    raw_log     VARIANT
);

COPY INTO pl_game_logs
FROM @uni_kishore_pipeline
    file_format = (format_name = ff_json_logs);

CREATE OR REPLACE TASK get_new_files
    warehouse = 'COMPUTE_wh'
    schedule = '5 minute'
AS
    COPY INTO pl_game_logs
    FROM @uni_kishore_pipeline
        file_format = (format_name = ff_json_logs);

EXECUTE TASK get_new_files;

CREATE OR REPLACE VIEW pl_logs AS
SELECT
    $1:ip_address::VARCHAR              AS ip_address,
    $1:datetime_iso8601::DATETIME       AS datetime_iso8601,
    $1:user_event::VARCHAR              AS user_event,
    $1:user_login::VARCHAR              AS user_login,
    raw_log
FROM pl_game_logs
WHERE ip_address IS NOT NULL;

SELECT * 
FROM pl_logs

MERGE INTO ags_game_audience.enhanced.logs_enhanced e
USING
(
    SELECT
        pl_logs.ip_address,
        pl_logs.user_login AS gamer_name,
        pl_logs.user_event AS game_event_name,
        pl_logs.datetime_iso8601 AS game_event_utc,
        city,
        region,
        country,
        timezone,
        CONVERT_TIMEZONE('UTC', timezone, pl_logs.datetime_iso8601) AS game_event_ltz,
        DAYNAME(game_event_ltz) AS dow_name,
        tod_name
    FROM ags_game_audience.raw.pl_logs pl_logs
    JOIN ipinfo_geoloc.demo.location loc
        ON ipinfo_geoloc.public.TO_JOIN_KEY(pl_logs.ip_address) = loc.join_key
    AND ipinfo_geoloc.public.to_int(pl_logs.ip_address)
    BETWEEN start_ip_int AND end_ip_int
    JOIN ags_game_audience.raw.time_of_day_lu tod
        ON HOUR(game_event_ltz) = tod.hour
) r
    ON r.gamer_name = e.gamer_name
    AND r.game_event_utc = e.game_event_utc
    AND r.game_event_name = e.game_event_name
WHEN NOT MATCHED THEN
    INSERT
    (
        ip_address,
        gamer_name,
        game_event_name,
        game_event_utc,
        city,
        region,
        country,
        timezone,
        game_event_ltz,
        dow_name,
        tod_name
    )
    VALUES
    (
        ip_address,
        gamer_name,
        game_event_name,
        game_event_utc,
        city,
        region,
        country,
        timezone,
        game_event_ltz,
        dow_name,
        tod_name
    );
    
CREATE OR REPLACE TASK ags_game_audience.raw.load_logs_enhanced
warehouse = 'COMPUTE_wh'
schedule = '5 minute'
AS
    MERGE INTO ags_game_audience.enhanced.logs_enhanced e
    USING
    (
        SELECT
            pl_logs.ip_address,
            pl_logs.user_login AS gamer_name,
            pl_logs.user_event AS game_event_name,
            pl_logs.datetime_iso8601 AS game_event_utc,
            city,
            region,
            country,
            timezone,
            CONVERT_TIMEZONE('UTC', timezone, pl_logs.datetime_iso8601) AS game_event_ltz,
            DAYNAME(game_event_ltz) AS dow_name,
            tod_name
        FROM ags_game_audience.raw.pl_logs pl_logs
        JOIN ipinfo_geoloc.demo.location loc
            ON ipinfo_geoloc.public.TO_JOIN_KEY(pl_logs.ip_address) = loc.join_key
        AND ipinfo_geoloc.public.to_int(pl_logs.ip_address)
        BETWEEN start_ip_int AND end_ip_int
        JOIN ags_game_audience.raw.time_of_day_lu tod
            ON HOUR(game_event_ltz) = tod.hour
    ) r
        ON r.gamer_name = e.gamer_name
        AND r.game_event_utc = e.game_event_utc
        AND r.game_event_name = e.game_event_name
    WHEN NOT MATCHED THEN
        INSERT
        (
            ip_address,
            gamer_name,
            game_event_name,
            game_event_utc,
            city,
            region,
            country,
            timezone,
            game_event_ltz,
            dow_name,
            tod_name
        )
        VALUES
        (
            ip_address,
            gamer_name,
            game_event_name,
            game_event_utc,
            city,
            region,
            country,
            timezone,
            game_event_ltz,
            dow_name,
            tod_name
        );

CREATE OR REPLACE RESOURCE MONITOR limiter
WITH
    CREDIT_QUOTA = 1
    FREQUENCY = daily
    START_TIMESTAMP = immediately
TRIGGERS ON 75 PERCENT DO NOTIFY
    ON 100 PERCENT DO SUSPEND
    ON 110 PERCENT DO SUSPEND_IMMEDIATE;

ALTER TASK get_new_files RESUME;
ALTER TASK load_logs_enhanced RESUME;

LIST @uni_kishore_pipeline; --340
SELECT COUNT(*) FROM pl_logs;
SELECT COUNT(*) FROM enhanced.logs_enhanced;

CREATE OR REPLACE TASK ags_game_audience.raw.load_logs_enhanced
    -- warehouse = 'COMPUTE_wh'
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    -- schedule = '5 minute'
    AFTER ags_game_audience.raw.get_new_files
AS
    MERGE INTO ags_game_audience.enhanced.logs_enhanced e
    USING
    (
        SELECT
            pl_logs.ip_address,
            pl_logs.user_login AS gamer_name,
            pl_logs.user_event AS game_event_name,
            pl_logs.datetime_iso8601 AS game_event_utc,
            city,
            region,
            country,
            timezone,
            CONVERT_TIMEZONE('UTC', timezone, pl_logs.datetime_iso8601) AS game_event_ltz,
            DAYNAME(game_event_ltz) AS dow_name,
            tod_name
        FROM ags_game_audience.raw.pl_logs pl_logs
        JOIN ipinfo_geoloc.demo.location loc
            ON ipinfo_geoloc.public.TO_JOIN_KEY(pl_logs.ip_address) = loc.join_key
        AND ipinfo_geoloc.public.to_int(pl_logs.ip_address)
        BETWEEN start_ip_int AND end_ip_int
        JOIN ags_game_audience.raw.time_of_day_lu tod
            ON HOUR(game_event_ltz) = tod.hour
    ) r
        ON r.gamer_name = e.gamer_name
        AND r.game_event_utc = e.game_event_utc
        AND r.game_event_name = e.game_event_name
    WHEN NOT MATCHED THEN
        INSERT
        (
            ip_address,
            gamer_name,
            game_event_name,
            game_event_utc,
            city,
            region,
            country,
            timezone,
            game_event_ltz,
            dow_name,
            tod_name
        )
        VALUES
        (
            ip_address,
            gamer_name,
            game_event_name,
            game_event_utc,
            city,
            region,
            country,
            timezone,
            game_event_ltz,
            dow_name,
            tod_name
        );

CREATE OR REPLACE TASK get_new_files
    -- warehouse = 'COMPUTE_wh'
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    -- schedule = '10 minute'
    schedule = '10 minute'
AS
    COPY INTO pl_game_logs
    FROM @uni_kishore_pipeline
        file_format = (format_name = ff_json_logs);

 SELECT 
    METADATA$FILENAME as log_file_name --new metadata column
  , METADATA$FILE_ROW_NUMBER as log_file_row_id --new metadata column
  , current_timestamp(0) as load_ltz --new local time of load
  , get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
  , get($1,'user_event')::text as USER_EVENT
  , get($1,'user_login')::text as USER_LOGIN
  , get($1,'ip_address')::text as IP_ADDRESS    
  FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
  (file_format => 'ff_json_logs');

  CREATE TABLE ed_pipeline_logs AS
   SELECT 
    METADATA$FILENAME as log_file_name --new metadata column
  , METADATA$FILE_ROW_NUMBER as log_file_row_id --new metadata column
  , current_timestamp(0) as load_ltz --new local time of load
  , get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
  , get($1,'user_event')::text as USER_EVENT
  , get($1,'user_login')::text as USER_LOGIN
  , get($1,'ip_address')::text as IP_ADDRESS    
  FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
  (file_format => 'ff_json_logs');

  create or replace TABLE AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS (
	LOG_FILE_NAME VARCHAR(100),
	LOG_FILE_ROW_ID NUMBER(18,0),
	LOAD_LTZ TIMESTAMP_LTZ(0),
	DATETIME_ISO8601 TIMESTAMP_NTZ(9),
	USER_EVENT VARCHAR(25),
	USER_LOGIN VARCHAR(100),
	IP_ADDRESS VARCHAR(100)
);

--truncate the table rows that were input during the CTAS, if you used a CTAS and didn't recreate it with shorter VARCHAR fields
truncate table ED_PIPELINE_LOGS;

--reload the table using your COPY INTO
COPY INTO ED_PIPELINE_LOGS
FROM (
    SELECT 
    METADATA$FILENAME as log_file_name 
  , METADATA$FILE_ROW_NUMBER as log_file_row_id 
  , current_timestamp(0) as load_ltz 
  , get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
  , get($1,'user_event')::text as USER_EVENT
  , get($1,'user_login')::text as USER_LOGIN
  , get($1,'ip_address')::text as IP_ADDRESS    
  FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
)
file_format = (format_name = ff_json_logs);

CREATE OR REPLACE PIPE PIPE_GET_NEW_FILES
auto_ingest=true
aws_sns_topic='arn:aws:sns:us-west-2:321463406630:dngw_topic'
AS 
COPY INTO ED_PIPELINE_LOGS
FROM (
    SELECT 
    METADATA$FILENAME as log_file_name 
  , METADATA$FILE_ROW_NUMBER as log_file_row_id 
  , current_timestamp(0) as load_ltz 
  , get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
  , get($1,'user_event')::text as USER_EVENT
  , get($1,'user_login')::text as USER_LOGIN
  , get($1,'ip_address')::text as IP_ADDRESS    
  FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
)
file_format = (format_name = ff_json_logs);

TRUNCATE enhanced.logs_enhanced;

SELECT * FROM enhanced.logs_enhanced;

ALTER PIPE ags_game_audience.raw.PIPE_GET_NEW_FILES REFRESH;

CREATE OR REPLACE TASK ags_game_audience.raw.load_logs_enhanced
    -- warehouse = 'COMPUTE_wh'
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    schedule = '5 minute'
AS
    MERGE INTO ags_game_audience.enhanced.logs_enhanced e
    USING
    (
        SELECT
            ed_pipeline_logs.ip_address,
            ed_pipeline_logs.user_login AS gamer_name,
            ed_pipeline_logs.user_event AS game_event_name,
            ed_pipeline_logs.datetime_iso8601 AS game_event_utc,
            city,
            region,
            country,
            timezone,
            CONVERT_TIMEZONE('UTC', timezone, ed_pipeline_logs.datetime_iso8601) AS game_event_ltz,
            DAYNAME(game_event_ltz) AS dow_name,
            tod_name
        FROM ags_game_audience.raw.ed_pipeline_logs ed_pipeline_logs
        JOIN ipinfo_geoloc.demo.location loc
            ON ipinfo_geoloc.public.TO_JOIN_KEY(ed_pipeline_logs.ip_address) = loc.join_key
        AND ipinfo_geoloc.public.to_int(ed_pipeline_logs.ip_address)
        BETWEEN start_ip_int AND end_ip_int
        JOIN ags_game_audience.raw.time_of_day_lu tod
            ON HOUR(game_event_ltz) = tod.hour
    ) r
        ON r.gamer_name = e.gamer_name
        AND r.game_event_utc = e.game_event_utc
        AND r.game_event_name = e.game_event_name
    WHEN NOT MATCHED THEN
        INSERT
        (
            ip_address,
            gamer_name,
            game_event_name,
            game_event_utc,
            city,
            region,
            country,
            timezone,
            game_event_ltz,
            dow_name,
            tod_name
        )
        VALUES
        (
            ip_address,
            gamer_name,
            game_event_name,
            game_event_utc,
            city,
            region,
            country,
            timezone,
            game_event_ltz,
            dow_name,
            tod_name
        );

select parse_json(SYSTEM$PIPE_STATUS( 'ags_game_audience.raw.PIPE_GET_NEW_FILES' ));

--create a stream that will keep track of changes to the table
create or replace stream ags_game_audience.raw.ed_cdc_stream 
on table AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS;

--look at the stream you created
show streams;

--check to see if any changes are pending (expect FALSE the first time you run it)
--after the Snowpipe loads a new file, expect to see TRUE
select system$stream_has_data('ed_cdc_stream');

--query the stream
select * 
from ags_game_audience.raw.ed_cdc_stream; 

--check to see if any changes are pending
select system$stream_has_data('ed_cdc_stream');

--if your stream remains empty for more than 10 minutes, make sure your PIPE is running
select SYSTEM$PIPE_STATUS('PIPE_GET_NEW_FILES');

--if you need to pause or unpause your pipe
--alter pipe PIPE_GET_NEW_FILES set pipe_execution_paused = true;
--alter pipe PIPE_GET_NEW_FILES set pipe_execution_paused = false;

--make a note of how many rows are in the stream
select * 
from ags_game_audience.raw.ed_cdc_stream; 

 
--process the stream by using the rows in a merge 
MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
USING (
        SELECT cdc.ip_address 
        , cdc.user_login as GAMER_NAME
        , cdc.user_event as GAME_EVENT_NAME
        , cdc.datetime_iso8601 as GAME_EVENT_UTC
        , city
        , region
        , country
        , timezone
        , CONVERT_TIMEZONE( 'UTC',timezone,cdc.datetime_iso8601) as game_event_ltz
        , DAYNAME(game_event_ltz) as DOW_NAME
        , TOD_NAME
        from ags_game_audience.raw.ed_cdc_stream cdc
        JOIN ipinfo_geoloc.demo.location loc 
        ON ipinfo_geoloc.public.TO_JOIN_KEY(cdc.ip_address) = loc.join_key
        AND ipinfo_geoloc.public.TO_INT(cdc.ip_address) 
        BETWEEN start_ip_int AND end_ip_int
        JOIN AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod
        ON HOUR(game_event_ltz) = tod.hour
      ) r
ON r.GAMER_NAME = e.GAMER_NAME
AND r.GAME_EVENT_UTC = e.GAME_EVENT_UTC
AND r.GAME_EVENT_NAME = e.GAME_EVENT_NAME 
WHEN NOT MATCHED THEN 
INSERT (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, TIMEZONE, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME)
        VALUES
        (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, TIMEZONE, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME);
 
--Did all the rows from the stream disappear? 
select * 
from ags_game_audience.raw.ed_cdc_stream; 

--Create a new task that uses the MERGE you just tested
create or replace task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='XSMALL'
	SCHEDULE = '5 minutes'
	as 
MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
USING (
        SELECT cdc.ip_address 
        , cdc.user_login as GAMER_NAME
        , cdc.user_event as GAME_EVENT_NAME
        , cdc.datetime_iso8601 as GAME_EVENT_UTC
        , city
        , region
        , country
        , timezone
        , CONVERT_TIMEZONE( 'UTC',timezone,cdc.datetime_iso8601) as game_event_ltz
        , DAYNAME(game_event_ltz) as DOW_NAME
        , TOD_NAME
        from ags_game_audience.raw.ed_cdc_stream cdc
        JOIN ipinfo_geoloc.demo.location loc 
        ON ipinfo_geoloc.public.TO_JOIN_KEY(cdc.ip_address) = loc.join_key
        AND ipinfo_geoloc.public.TO_INT(cdc.ip_address) 
        BETWEEN start_ip_int AND end_ip_int
        JOIN AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod
        ON HOUR(game_event_ltz) = tod.hour
      ) r
ON r.GAMER_NAME = e.GAMER_NAME
AND r.GAME_EVENT_UTC = e.GAME_EVENT_UTC
AND r.GAME_EVENT_NAME = e.GAME_EVENT_NAME 
WHEN NOT MATCHED THEN 
INSERT (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, TIMEZONE, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME)
        VALUES
        (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, TIMEZONE, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME);
        
--Resume the task so it is running
alter task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED resume;

create or replace task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='XSMALL'
	-- SCHEDULE = '5 minutes'
    TARGET_COMPLETION_INTERVAL='5 MINUTES'
WHEN
    system$stream_has_data('ed_cdc_stream')
	as 
MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
USING (
        SELECT cdc.ip_address 
        , cdc.user_login as GAMER_NAME
        , cdc.user_event as GAME_EVENT_NAME
        , cdc.datetime_iso8601 as GAME_EVENT_UTC
        , city
        , region
        , country
        , timezone
        , CONVERT_TIMEZONE( 'UTC',timezone,cdc.datetime_iso8601) as game_event_ltz
        , DAYNAME(game_event_ltz) as DOW_NAME
        , TOD_NAME
        from ags_game_audience.raw.ed_cdc_stream cdc
        JOIN ipinfo_geoloc.demo.location loc 
        ON ipinfo_geoloc.public.TO_JOIN_KEY(cdc.ip_address) = loc.join_key
        AND ipinfo_geoloc.public.TO_INT(cdc.ip_address) 
        BETWEEN start_ip_int AND end_ip_int
        JOIN AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod
        ON HOUR(game_event_ltz) = tod.hour
      ) r
ON r.GAMER_NAME = e.GAMER_NAME
AND r.GAME_EVENT_UTC = e.GAME_EVENT_UTC
AND r.GAME_EVENT_NAME = e.GAME_EVENT_NAME 
WHEN NOT MATCHED THEN 
INSERT (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, TIMEZONE, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME)
        VALUES
        (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, TIMEZONE, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME);

alter task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED resume;

alter pipe mypipe set pipe_execution_paused = true;

CREATE SCHEMA curated;

--You can run this code in a WORKSHEET

--the ListAgg function can put both login and logout into a single column in a single row
-- if we don't have a logout, just one timestamp will appear
select GAMER_NAME
      , listagg(GAME_EVENT_LTZ,' / ') as login_and_logout
from AGS_GAME_AUDIENCE.ENHANCED.xLOGS_ENHANCED 
group by gamer_name;

--You can run this code in a WORKSHEET

select GAMER_NAME
       ,game_event_ltz as login 
       ,lead(game_event_ltz) 
                OVER (
                    partition by GAMER_NAME 
                    order by GAME_EVENT_LTZ
                ) as logout
       ,coalesce(datediff('mi', login, logout),0) as game_session_length
from AGS_GAME_AUDIENCE.ENHANCED.xLOGS_ENHANCED
order by game_session_length desc;

-- Put this code into a DASHBOARD TILE QUERY

--We added a case statement to bucket the session lengths
select case when game_session_length < 10 then '< 10 mins'
            when game_session_length < 20 then '10 to 19 mins'
            when game_session_length < 30 then '20 to 29 mins'
            when game_session_length < 40 then '30 to 39 mins'
            else '> 40 mins' 
            end as session_length
            ,tod_name
from (
select GAMER_NAME
       , tod_name
       ,game_event_ltz as login 
       ,lead(game_event_ltz) 
                OVER (
                    partition by GAMER_NAME 
                    order by GAME_EVENT_LTZ
                ) as logout
       ,coalesce(datediff('mi', login, logout),0) as game_session_length
from AGS_GAME_AUDIENCE.ENHANCED.xLOGS_ENHANCED)
where logout is not null;