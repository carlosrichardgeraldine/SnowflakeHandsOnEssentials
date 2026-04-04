CREATE DATABASE camillas_db;
DROP SCHEMA public;
CREATE SCHEMA cortex_analyst;
CREATE STAGE cortex_analyst_model_stage
    encryption = (type = 'snowflake_sse');
DROP STAGE cortex_analyst_model_stage;
CREATE WAREHOUSE llm_wh WITH
  WAREHOUSE_TYPE = STANDARD
  WAREHOUSE_SIZE = XSMALL;

create or replace table camillas_db.cortex_analyst.camillas_teams
( 
    team_id number,
    team_name varchar(50),
    kit_color varchar(20),
    coach varchar(100),
    emoji_symbol varchar(5)
);

insert into camillas_db.cortex_analyst.camillas_teams
values
(1,'Blue Sky Strikers','cerulean','Stormy McLeod', '💙☁️⚡️'),
(2,'Pitch Blazing Bombers','emerald','Kelly Groen','🌱🔥💣' ),
(3,'Solar Flashing Flares','marigold','Ravi Bahsin', '☀️🔥'),
(4,'Terracotta Tirade','terracotta','Clay Skála', '🪴💪');

create or replace table camillas_db.cortex_analyst.match_locations
(
 location_id number, 
 location_name varchar(50)
);

insert into camillas_db.cortex_analyst.match_locations
values 
(1, 'Main Street Park - Pitch 1'),
(2, 'Main Street Park - Pitch 2'),
(3, 'Central Park - North Pitch'),
(4, 'Central Park - South Pitch');

create or replace table camillas_db.cortex_analyst.match_schedule
( 
    home_team_id number,
    away_team_id number,
    location_id number,
    match_datetime timestamp_ntz  
);

insert into camillas_db.cortex_analyst.match_schedule
values
(1,2,1,'2025-06-07 08:00:00'),
(3,4,2,'2025-06-07 08:00:00'),
(2,3,3,'2025-06-07 12:00:00'),
(1,4,4,'2025-06-07 12:00:00'),
(1,3,1,'2025-06-07 16:00:00'),
(2,4,2,'2025-06-07 16:00:00');

LIST @cortex_analyst_model_stage;

CREATE SCHEMA forecasting;
CREATE WAREHOUSE ml_wh WITH
  WAREHOUSE_TYPE = STANDARD
  WAREHOUSE_SIZE = XSMALL;

CREATE or replace FILE FORMAT csv
    type = csv
    skip_header = 1;

LIST @cortex_analyst.cortex_analyst_model_stage;

create or replace table camillas_db.forecasting.practice_stats (
	practice_date timestamp_ntz,
	goals_scored number,
	goals_attempted number
);

SELECT $1, $2, $3
FROM @cortex_analyst.cortex_analyst_model_stage/stats_collected_at_practice.csv
(file_format => csv);

COPY INTO practice_stats
FROM @cortex_analyst.cortex_analyst_model_stage/stats_collected_at_practice.csv
file_format = (format_name = csv);

SELECT *
FROM practice_stats
ORDER BY practice_date;

SELECT *
FROM practice_stats
ORDER BY practice_date DESC;

-- make a view that uses data from march to july for training the model
create or replace view camillas_db.forecasting.train_model_practice_data(
	  practice_date,
	  goals_attempted,
	  goals_scored
) as
  select 
    practice_date, 
    goals_attempted,
    goals_scored
  from camillas_db.forecasting.practice_stats
  where practice_date < '2025-07-01';

-- make a view that uses data from july forward for validating the model
create or replace view camillas_db.forecasting.validate_model_practice_data(
	   practice_date,
	   goals_attempted,
	   goals_scored
) as
  select 
    practice_date, 
    goals_attempted,
    goals_scored
  from camillas_db.forecasting.practice_stats
  where practice_date >= '2025-07-01';

  -- This is your Cortex Project.
-----------------------------------------------------------
-- SETUP
-----------------------------------------------------------
use role ACCOUNTADMIN;
use warehouse ML_WH;
use database CAMILLAS_DB;
use schema FORECASTING;

-- Inspect the first 10 rows of your training data. This is the data we'll use to create your model.
-- select * from TRAIN_MODEL_PRACTICE_DATA limit 10;

-- Prepare your training data. Timestamp_ntz is a required format.
-- CREATE VIEW TRAIN_MODEL_PRACTICE_DATA_v1 AS SELECT
--     * EXCLUDE PRACTICE_DATE,
--     to_timestamp_ntz(PRACTICE_DATE) as PRACTICE_DATE_v1
-- FROM TRAIN_MODEL_PRACTICE_DATA;

-- Prepare your prediction data. Timestamp_ntz is a required format.
-- CREATE VIEW VALIDATE_MODEL_PRACTICE_DATA_v1 AS SELECT
--     * EXCLUDE PRACTICE_DATE,
--     to_timestamp_ntz(PRACTICE_DATE) as PRACTICE_DATE_v1
-- FROM VALIDATE_MODEL_PRACTICE_DATA;

-----------------------------------------------------------
-- CREATE PREDICTIONS
-----------------------------------------------------------
-- Create your model.
CREATE SNOWFLAKE.ML.FORECAST camillas_practice_goal_forecasting(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'TRAIN_MODEL_PRACTICE_DATA'),
    TIMESTAMP_COLNAME => 'PRACTICE_DATE',
    TARGET_COLNAME => 'GOALS_SCORED'
);

-- Generate predictions and store the results to a table.
BEGIN
    -- This is the step that creates your predictions.
    CALL camillas_practice_goal_forecasting!FORECAST(
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'VALIDATE_MODEL_PRACTICE_DATA'),
        TIMESTAMP_COLNAME => 'PRACTICE_DATE',
        -- Here we set your prediction interval.
        CONFIG_OBJECT => {'prediction_interval': 0.95}
    );
    -- These steps store your predictions to a table.
    LET x := SQLID;
    CREATE TABLE first_goals_forecast AS SELECT * FROM TABLE(RESULT_SCAN(:x));
END;

-- View your predictions.
SELECT * FROM first_goals_forecast;

-- Union your predictions with your historical data, then view the results in a chart.
SELECT PRACTICE_DATE, GOALS_SCORED AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
    FROM TRAIN_MODEL_PRACTICE_DATA
UNION ALL
SELECT ts as PRACTICE_DATE, NULL AS actual, forecast, lower_bound, upper_bound
    FROM first_goals_forecast;

-----------------------------------------------------------
-- INSPECT RESULTS
-----------------------------------------------------------

-- Inspect the accuracy metrics of your model. 
CALL camillas_practice_goal_forecasting!SHOW_EVALUATION_METRICS();

-- Inspect the relative importance of your features, including auto-generated features. 
CALL camillas_practice_goal_forecasting!EXPLAIN_FEATURE_IMPORTANCE();


create or replace view camillas_db.forecasting.train_2_model_practice_data(
	practice_date,
	day_of_week,
	goals_attempted,
	goals_scored
) as
select 
practice_date,
dayname(practice_date) as day_of_week,
goals_attempted,
goals_scored
from camillas_db.forecasting.practice_stats
where practice_date < '2025-07-01';

create or replace view camillas_db.forecasting.validate_2_model_practice_data(
	practice_date,
	day_of_week,
	goals_attempted,
	goals_scored
) as
select 
practice_date,
dayname(practice_date) as day_of_week,
goals_attempted,
goals_scored
from camillas_db.forecasting.practice_stats
where practice_date >= '2025-07-01';

-- This is your Cortex Project.
-----------------------------------------------------------
-- SETUP
-----------------------------------------------------------
use role ACCOUNTADMIN;
use warehouse ML_WH;
use database CAMILLAS_DB;
use schema FORECASTING;

-- Inspect the first 10 rows of your training data. This is the data we'll use to create your model.
-- select * from TRAIN_2_MODEL_PRACTICE_DATA limit 10;

-- Prepare your training data. Timestamp_ntz is a required format.
-- CREATE VIEW TRAIN_2_MODEL_PRACTICE_DATA AS SELECT
--     * EXCLUDE PRACTICE_DATE,
--     to_timestamp_ntz(PRACTICE_DATE) as PRACTICE_DATE
-- FROM TRAIN_2_MODEL_PRACTICE_DATA;

-- Prepare your prediction data. Timestamp_ntz is a required format.
-- CREATE VIEW VALIDATE_2_MODEL_PRACTICE_DATA AS SELECT
--     * EXCLUDE PRACTICE_DATE,
--     to_timestamp_ntz(PRACTICE_DATE) as PRACTICE_DATE
-- FROM VALIDATE_2_MODEL_PRACTICE_DATA;

-----------------------------------------------------------
-- CREATE PREDICTIONS
-----------------------------------------------------------
-- Create your model.
CREATE SNOWFLAKE.ML.FORECAST camillas_practice_goal_4cast_w_dayofweek(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'TRAIN_2_MODEL_PRACTICE_DATA'),
    SERIES_COLNAME => 'DAY_OF_WEEK',
    TIMESTAMP_COLNAME => 'PRACTICE_DATE',
    TARGET_COLNAME => 'GOALS_SCORED',
    CONFIG_OBJECT => { 'ON_ERROR': 'SKIP' }
);

-- Generate predictions and store the results to a table.
BEGIN
    -- This is the step that creates your predictions.
    CALL camillas_practice_goal_4cast_w_dayofweek!FORECAST(
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'VALIDATE_2_MODEL_PRACTICE_DATA'),
        SERIES_COLNAME => 'DAY_OF_WEEK',
        TIMESTAMP_COLNAME => 'PRACTICE_DATE',
        -- Here we set your prediction interval.
        CONFIG_OBJECT => {'prediction_interval': 0.95}
    );
    -- These steps store your predictions to a table.
    LET x := SQLID;
    CREATE TABLE second_goals_forecast AS SELECT * FROM TABLE(RESULT_SCAN(:x));
END;

-- View your predictions.
SELECT * FROM second_goals_forecast;

-- Union your predictions with your historical data, then view the results in a chart.
SELECT DAY_OF_WEEK, PRACTICE_DATE, GOALS_SCORED AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
    FROM TRAIN_2_MODEL_PRACTICE_DATA
UNION ALL
SELECT replace(series, '"', '') as DAY_OF_WEEK, ts as PRACTICE_DATE, NULL AS actual, forecast, lower_bound, upper_bound
    FROM second_goals_forecast;

-----------------------------------------------------------
-- INSPECT RESULTS
-----------------------------------------------------------

-- Inspect the accuracy metrics of your model. 
CALL camillas_practice_goal_4cast_w_dayofweek!SHOW_EVALUATION_METRICS();

-- Inspect the relative importance of your features, including auto-generated features. 
CALL camillas_practice_goal_4cast_w_dayofweek!EXPLAIN_FEATURE_IMPORTANCE();

select practice_date, goals_scored as actual, null as forecast_1, NULL as forecast_2
    from train_2_model_practice_data
UNION ALL
select ts as practice_date, NULL as actual, forecast as forecast_1, NULL as forecast_2
    from first_goals_forecast
UNION ALL    
select ts as practice_date, NULL as actual, null as forecast_1, forecast as forecast_2
    from second_goals_forecast

CREATE SCHEMA classification;

create or replace table camillas_db.classification.train_player_position (
	player_id number(38,0),
	position_code varchar(1),
	game number(38,0),
	minutes_played number(38,0),
	goals number(38,0),
	assists number(38,0),
	shots number(38,0),
	passes number(38,0),
	sprint_distance number(38,0),
	saves number(38,0),
	dribbles number(38,0),
	blocks number(38,0),
	claims number(38,0)
);

LIST @cortex_analyst.cortex_analyst_model_stage;

SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
FROM @cortex_analyst.cortex_analyst_model_stage/train_player_positions.csv
(file_format => forecasting.csv);

create or replace table camillas_db.classification.train_player_position (
	player_id number(38,0),
	position_code varchar(1),
	game number(38,0),
	minutes_played number(38,0),
	goals number(38,0),
	assists number(38,0),
	shots number(38,0),
	passes number(38,0),
	sprint_distance number(38,0),
	saves number(38,0),
	dribbles number(38,0),
	blocks number(38,0),
	claims number(38,0)
);

COPY INTO train_player_position
FROM @cortex_analyst.cortex_analyst_model_stage/train_player_positions.csv
file_format = (format_name = forecasting.csv);

create or replace table camillas_db.classification.unclassified_player_positions (
	player_id number(38,0),
	game_id number(38,0),
	mins_played number(38,0),
	goals_made number(38,0),
	assists number(38,0),
	shots number(38,0),
	passes number(38,0),
	sprint_distance number(38,0),
	saves number(38,0),
	dribbles number(38,0),
	blocks number(38,0),
	claims number(38,0)
);

LIST @cortex_analyst.cortex_analyst_model_stage;

SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
FROM @cortex_analyst.cortex_analyst_model_stage/unclassified_player_data.csv
(file_format => forecasting.csv);

COPY INTO unclassified_player_positions
FROM @cortex_analyst.cortex_analyst_model_stage/unclassified_player_data.csv
file_format = (format_name = forecasting.csv);

-- This is your Cortex Project.
-----------------------------------------------------------
-- SETUP
-----------------------------------------------------------
use role ACCOUNTADMIN;
use warehouse ML_WH;
use database CAMILLAS_DB;
use schema CLASSIFICATION;

-- Inspect the first 10 rows of your training data. This is the data we'll
-- use to create your model.
-- select * from TRAIN_PLAYER_POSITION limit 10;

-- Inspect the first 10 rows of your prediction data. This is the data the model
-- will use to generate predictions.
-- select * from UNCLASSIFIED_PLAYER_POSITIONS limit 10;

-----------------------------------------------------------
-- CREATE PREDICTIONS
-----------------------------------------------------------
-- Create your model.
CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION player_position_classification(
    INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'TRAIN_PLAYER_POSITION'),
    TARGET_COLNAME => 'POSITION_CODE',
    CONFIG_OBJECT => { 'ON_ERROR': 'SKIP' }
);

-- Inspect your logs to ensure training completed successfully. 
CALL player_position_classification!SHOW_TRAINING_LOGS();

-- Generate predictions as new columns in to your prediction table.
CREATE TABLE my_player_pos_code_classifs AS SELECT
    *, 
    player_position_classification!PREDICT(
        OBJECT_CONSTRUCT(*),
        -- This option alows the prediction process to complete even if individual rows must be skipped.
        {'ON_ERROR': 'SKIP'}
    ) as predictions
from UNCLASSIFIED_PLAYER_POSITIONS;

-- View your predictions.
-- SELECT * FROM my_player_pos_code_classifs;

-- Parse the prediction results into separate columns. 
-- Note: This is a just an example. Be sure to update this to reflect 
-- the classes in your dataset.
-- SELECT * EXCLUDE predictions,
--         predictions:class AS class,
--         round(predictions['probability'][class], 3) as probability
-- FROM my_player_pos_code_classifs;

-----------------------------------------------------------
-- INSPECT RESULTS
-----------------------------------------------------------

-- Inspect your model's evaluation metrics.
CALL player_position_classification!SHOW_EVALUATION_METRICS();
CALL player_position_classification!SHOW_GLOBAL_EVALUATION_METRICS();
CALL player_position_classification!SHOW_CONFUSION_MATRIX();

-- Inspect the relative importance of your features, including auto-generated features.  
CALL player_position_classification!SHOW_FEATURE_IMPORTANCE();

SELECT *
FROM my_player_pos_code_classifs;