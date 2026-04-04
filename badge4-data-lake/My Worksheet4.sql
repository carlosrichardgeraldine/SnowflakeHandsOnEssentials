CREATE DATABASE zenas_athleisure_db;
DROP SCHEMA public;
CREATE SCHEMA products;
CREATE STAGE sweatsuits
    encryption = (type = 'snowflake_sse');
CREATE STAGE product_metadata
    encryption = (type = 'snowflake_full');

list @product_metadata;

select $1
from @product_metadata/sweatsuit_sizes.txt; 

create file format zmd_file_format_1
RECORD_DELIMITER = '^';

select $1
from @product_metadata/product_coordination_suggestions.txt
(file_format => zmd_file_format_1);

create file format zmd_file_format_2
FIELD_DELIMITER = '^';  

select $1
from @product_metadata/product_coordination_suggestions.txt
(file_format => zmd_file_format_2);

create or replace file format zmd_file_format_3
FIELD_DELIMITER = '='
RECORD_DELIMITER = '^'; 

select $1, $2
from @product_metadata/product_coordination_suggestions.txt
(file_format => zmd_file_format_3);

create or replace file format zmd_file_format_1
RECORD_DELIMITER = ';'
TRIM_SPACE = TRUE;

select $1 as sizes_available
from @product_metadata/sweatsuit_sizes.txt
(file_format => zmd_file_format_1);

create or replace file format zmd_file_format_2
FIELD_DELIMITER = '|'
RECORD_DELIMITER = ';'
TRIM_SPACE = TRUE;

select $1, $2, $3
from @product_metadata/swt_product_line.txt
(file_format => zmd_file_format_2);

SELECT REPLACE($1, CHAR(13)||CHAR(10))
FROM @product_metadata/swt_product_line.txt
(file_format => zmd_file_format_2);

SELECT REPLACE($1, CHAR(13)||CHAR(10)) AS sizes_available
FROM @product_metadata/sweatsuit_sizes.txt
(file_format => zmd_file_format_1);

SELECT REPLACE($1, CHAR(13)||CHAR(10))
FROM @product_metadata/product_coordination_suggestions.txt
(file_format => zmd_file_format_3);

create view zenas_athleisure_db.products.sweatsuit_sizes as 
SELECT REPLACE($1, CHAR(13)||CHAR(10)) AS sizes_available
FROM @product_metadata/sweatsuit_sizes.txt
(file_format => zmd_file_format_1);

CREATE VIEW zenas_athleisure_db.products.SWEATBAND_PRODUCT_LINE AS
SELECT REPLACE($1, CHAR(13)||CHAR(10)) AS product_code, $2 AS headband_description, $3 AS wristband_description
FROM @product_metadata/swt_product_line.txt
(file_format => zmd_file_format_2);

CREATE VIEW zenas_athleisure_db.products.SWEATBAND_COORDINATION AS
SELECT REPLACE($1, CHAR(13)||CHAR(10)) AS product_code, $2 as has_matching_sweatsuit
FROM @product_metadata/product_coordination_suggestions.txt
(file_format => zmd_file_format_3);

select metadata$filename, metadata$file_row_number
from @sweatsuits/purple_sweatsuit.png;

select metadata$filename, MAX(metadata$file_row_number)
from @sweatsuits
group by metadata$filename
order by metadata$filename;

select * 
from directory(@sweatsuits);
select * 
from sweatsuits;

select INITCAP(REPLACE(REPLACE(relative_path, '_', ' '), '.png')) as product_name
from directory(@sweatsuits);

create or replace table zenas_athleisure_db.products.sweatsuits (
	color_or_style varchar(25),
	file_name varchar(50),
	price number(5,2)
);

insert into  zenas_athleisure_db.products.sweatsuits 
          (color_or_style, file_name, price)
values
 ('Burgundy', 'burgundy_sweatsuit.png',65)
,('Charcoal Grey', 'charcoal_grey_sweatsuit.png',65)
,('Forest Green', 'forest_green_sweatsuit.png',64)
,('Navy Blue', 'navy_blue_sweatsuit.png',65)
,('Orange', 'orange_sweatsuit.png',65)
,('Pink', 'pink_sweatsuit.png',63)
,('Purple', 'purple_sweatsuit.png',64)
,('Red', 'red_sweatsuit.png',68)
,('Royal Blue',	'royal_blue_sweatsuit.png',65)
,('Yellow', 'yellow_sweatsuit.png',67);

select INITCAP(REPLACE(REPLACE(relative_path, '_', ' '), '.png')) as product_name, *
from directory(@sweatsuits) d
join sweatsuits s
on d.relative_path = s.file_name;

CREATE VIEW product_list AS
select INITCAP(REPLACE(REPLACE(relative_path, '_', ' '), '.png')) as product_name, s.file_name, s.color_or_style, s.price, d.file_url
from directory(@sweatsuits) d
join sweatsuits s
on d.relative_path = s.file_name;

CREATE VIEW catalog AS
SELECT * FROM product_list p
cross join sweatsuit_sizes;

create table zenas_athleisure_db.products.upsell_mapping
(
sweatsuit_color_or_style varchar(25)
,upsell_product_code varchar(10)
);

insert into zenas_athleisure_db.products.upsell_mapping
(
sweatsuit_color_or_style
,upsell_product_code 
)
VALUES
('Charcoal Grey','SWT_GRY')
,('Forest Green','SWT_FGN')
,('Orange','SWT_ORG')
,('Pink', 'SWT_PNK')
,('Red','SWT_RED')
,('Yellow', 'SWT_YLW');

create view catalog_for_website as 
select color_or_style
,price
,file_name
, get_presigned_url(@sweatsuits, file_name, 3600) as file_url
,size_list
,coalesce('Consider: ' ||  headband_description || ' & ' || wristband_description, 'Consider: White, Black or Grey Sweat Accessories')  as upsell_product_desc
from
(   select color_or_style, price, file_name
    ,listagg(sizes_available, ' | ') within group (order by sizes_available) as size_list
    from catalog
    group by color_or_style, price, file_name
) c
left join upsell_mapping u
on u.sweatsuit_color_or_style = c.color_or_style
left join sweatband_coordination sc
on sc.product_code = u.upsell_product_code
left join sweatband_product_line spl
on spl.product_code = sc.product_code;

CREATE DATABASE mels_smoothie_challenge_db;
DROP SCHEMA public;
CREATE SCHEMA trails;
CREATE STAGE trails_geojson
    encryption = (type = 'snowflake_sse');
CREATE STAGE trails_parquet
    encryption = (type = 'snowflake_sse');

CREATE FILE FORMAT ff_json
    type = 'json'
    compression = 'auto'
    enable_octal = FALSE
    allow_duplicate = FALSE
    strip_outer_array = TRUE
    strip_null_values = FALSE
    ignore_utf8_errors = FALSE;

CREATE FILE FORMAT ff_parquet
    type = 'parquet';

SELECT * FROM @trails_geojson
(file_format => ff_json);

SELECT * FROM @trails_parquet
(file_format => ff_parquet);

SELECT COUNT(*) FROM @trails_parquet
(file_format => ff_parquet);

SELECT
    $1:sequence_1::NUMBER           AS point_id,
    $1:trail_name::VARCHAR          AS trail_name,
    $1:elevation::FLOAT             AS elevation,
    $1:latitude::number(11,8)       AS lat,
    $1:longitude::number(11,8)      AS lng,
    $1:sequence_2::NUMBER           AS sequence_2
FROM @trails_parquet
(file_format => ff_parquet)
ORDER BY point_id;

CREATE VIEW cherry_creek_trail AS
SELECT
    $1:sequence_1::NUMBER           AS point_id,
    $1:trail_name::VARCHAR          AS trail_name,
    $1:elevation::FLOAT             AS elevation,
    $1:latitude::number(11,8)       AS lat,
    $1:longitude::number(11,8)      AS lng,
    $1:sequence_2::NUMBER           AS sequence_2
FROM @trails_parquet
(file_format => ff_parquet)
ORDER BY point_id;

select top 100 
 lng||' '||lat as coord_pair
,'POINT('||coord_pair||')' as trail_point
from cherry_creek_trail;

create or replace view cherry_creek_trail as
select 
 $1:sequence_1 as point_id,
 $1:trail_name::varchar as trail_name,
 $1:latitude::number(11,8) as lng,
 $1:longitude::number(11,8) as lat,
 lng||' '||lat as coord_pair
from @trails_parquet
(file_format => ff_parquet)
order by point_id;

select 
'LINESTRING('||
listagg(coord_pair, ',') 
within group (order by point_id)
||')' as my_linestring
from cherry_creek_trail
where point_id <= 10
group by trail_name;

select
$1:features[0]:properties:Name::string as feature_name
,$1:features[0]:geometry:coordinates::string as feature_coordinates
,$1:features[0]:geometry::string as geometry
,$1:features[0]:properties::string as feature_properties
,$1:crs:properties:name::string as specs
,$1 as whole_object
from @trails_geojson (file_format => ff_json);

CREATE VIEW denver_area_trails AS
select
$1:features[0]:properties:Name::string as feature_name
,$1:features[0]:geometry:coordinates::string as feature_coordinates
,$1:features[0]:geometry::string as geometry
,$1:features[0]:properties::string as feature_properties
,$1:crs:properties:name::string as specs
,$1 as whole_object
from @trails_geojson (file_format => ff_json);

select 
'LINESTRING('||
listagg(coord_pair, ',') 
within group (order by point_id)
||')' as my_linestring
,st_length(to_geography(my_linestring)) as length_of_trail
from cherry_creek_trail
group by trail_name;

SELECT 
    feature_name,
    ST_LENGTH(TO_GEOGRAPHY(whole_object)) AS wo_length,
    ST_LENGTH(TO_GEOGRAPHY(geometry)) AS geom_length
FROM denver_area_trails;

CREATE OR REPLACE VIEW denver_area_trails AS
select
$1:features[0]:properties:Name::string as feature_name
,$1:features[0]:geometry:coordinates::string as feature_coordinates
,$1:features[0]:geometry::string as geometry
,ST_LENGTH(TO_GEOGRAPHY(geometry)) AS trail_length
,$1:features[0]:properties::string as feature_properties
,$1:crs:properties:name::string as specs
,$1 as whole_object
from @trails_geojson (file_format => ff_json);

SELECT * FROM denver_area_trails;

create or replace view DENVER_AREA_TRAILS_2 as
select 
trail_name as feature_name
,'{"coordinates":['||listagg('['||lng||','||lat||']',',') within group (order by point_id)||'],"type":"LineString"}' as geometry
,st_length(to_geography(geometry))  as trail_length
from cherry_creek_trail
group by trail_name;

select feature_name, geometry, trail_length
from DENVER_AREA_TRAILS
union all
select feature_name, geometry, trail_length
from DENVER_AREA_TRAILS_2;

select feature_name
, to_geography(geometry) as my_linestring
, st_xmin(my_linestring) as min_eastwest
, st_xmax(my_linestring) as max_eastwest
, st_ymin(my_linestring) as min_northsouth
, st_ymax(my_linestring) as max_northsouth
, trail_length
from DENVER_AREA_TRAILS
union all
select feature_name
, to_geography(geometry) as my_linestring
, st_xmin(my_linestring) as min_eastwest
, st_xmax(my_linestring) as max_eastwest
, st_ymin(my_linestring) as min_northsouth
, st_ymax(my_linestring) as max_northsouth
, trail_length
from DENVER_AREA_TRAILS_2;

CREATE VIEW trails_and_boundaries AS
select feature_name
, to_geography(geometry) as my_linestring
, st_xmin(my_linestring) as min_eastwest
, st_xmax(my_linestring) as max_eastwest
, st_ymin(my_linestring) as min_northsouth
, st_ymax(my_linestring) as max_northsouth
, trail_length
from DENVER_AREA_TRAILS
union all
select feature_name
, to_geography(geometry) as my_linestring
, st_xmin(my_linestring) as min_eastwest
, st_xmax(my_linestring) as max_eastwest
, st_ymin(my_linestring) as min_northsouth
, st_ymax(my_linestring) as max_northsouth
, trail_length
from DENVER_AREA_TRAILS_2;

select 'POLYGON(('|| 
    min(min_eastwest)||' '||max(max_northsouth)||','|| 
    max(max_eastwest)||' '||max(max_northsouth)||','|| 
    max(max_eastwest)||' '||min(min_northsouth)||','|| 
    min(min_eastwest)||' '||min(min_northsouth)||'))' AS my_polygon
from trails_and_boundaries;

-- Melanie's Location into a 2 Variables (mc for melanies cafe)
set mc_lng='-104.97300245114094';
set mc_lat='39.76471253574085';

--Confluence Park into a Variable (loc for location)
set loc_lng='-105.00840763333615'; 
set loc_lat='39.754141917497826';

--Test your variables to see if they work with the Makepoint function
select st_makepoint($mc_lng,$mc_lat) as melanies_cafe_point;
select st_makepoint($loc_lng,$loc_lat) as confluent_park_point;

--use the variables to calculate the distance from 
--Melanie's Cafe to Confluent Park
select st_distance(
        st_makepoint($mc_lng,$mc_lat)
        ,st_makepoint($loc_lng,$loc_lat)
        ) as mc_to_cp;

CREATE SCHEMA locations;
USE SCHEMA locations;

CREATE OR REPLACE FUNCTION distance_to_mc(loc_lng number(38,32),loc_lat number(38,32))
  RETURNS FLOAT
  AS
  $$
   st_distance(
        st_makepoint(-104.97300245114094,39.76471253574085)
        ,st_makepoint(loc_lng,loc_lat)
        )
  $$
  ;

--Tivoli Center into the variables 
set tc_lng='-105.00532059763648'; 
set tc_lat='39.74548137398218';

select distance_to_mc($tc_lng,$tc_lat);

select * 
from OPENSTREETMAP_DENVER.DENVER.V_OSM_DEN_AMENITY_SUSTENANCE
where 
    ((amenity in ('fast_food','cafe','restaurant','juice_bar'))
    and 
    (name ilike '%jamba%' or name ilike '%juice%'
     or name ilike '%superfruit%'))
 or 
    (cuisine like '%smoothie%' or cuisine like '%juice%');

CREATE VIEW competition AS
select * 
from OPENSTREETMAP_DENVER.DENVER.V_OSM_DEN_AMENITY_SUSTENANCE
where 
    ((amenity in ('fast_food','cafe','restaurant','juice_bar'))
    and 
    (name ilike '%jamba%' or name ilike '%juice%'
     or name ilike '%superfruit%'))
 or 
    (cuisine like '%smoothie%' or cuisine like '%juice%');

SELECT
 name
 ,cuisine
 , ST_DISTANCE(
    st_makepoint('-104.97300245114094','39.76471253574085')
    , coordinates
  ) AS distance_to_melanies
 ,*
FROM  competition
ORDER by distance_to_melanies;

CREATE OR REPLACE FUNCTION distance_to_mc(lng_and_lat GEOGRAPHY)
  RETURNS FLOAT
  AS
  $$
   st_distance(
        st_makepoint('-104.97300245114094','39.76471253574085')
        ,lng_and_lat
        )
  $$
  ;

  SELECT
 name
 ,cuisine
 ,distance_to_mc(coordinates) AS distance_to_melanies
 ,*
FROM  competition
ORDER by distance_to_melanies;

-- Tattered Cover Bookstore McGregor Square
set tcb_lng='-104.9956203'; 
set tcb_lat='39.754874';

--this will run the first version of the UDF
select distance_to_mc($tcb_lng,$tcb_lat);

--this will run the second version of the UDF, bc it converts the coords 
--to a geography object before passing them into the function
select distance_to_mc(st_makepoint($tcb_lng,$tcb_lat));

--this will run the second version bc the Sonra Coordinates column
-- contains geography objects already
select name
, distance_to_mc(coordinates) as distance_to_melanies 
, ST_ASWKT(coordinates)
from OPENSTREETMAP_DENVER.DENVER.V_OSM_DEN_SHOP
where shop='books' 
and name like '%Tattered Cover%'
and addr_street like '%Wazee%';

CREATE OR REPLACE VIEW DENVER_BIKE_SHOPS AS
SELECT
    name,
    distance_to_mc(coordinates) AS distance_to_melanies,
    *  EXCLUDE (name)
FROM OPENSTREETMAP_DENVER.DENVER.V_OSM_DEN_SHOP_OUTDOORS_AND_SPORT_VEHICLES
WHERE shop = 'bicycle';

SELECT * FROM denver_bike_shops
ORDER BY distance_to_melanies;

USE SCHEMA trails;

create or replace external table T_CHERRY_CREEK_TRAIL(
my_filename varchar(100) as (metadata$filename::varchar(100))
)
location= @trails_parquet
auto_refresh = true
file_format = (type = parquet);

CREATE STAGE external_aws_dlkw url = 's3://uni-dlkw';
LIST @external_aws_dlkw;

create or replace external table T_CHERRY_CREEK_TRAIL(
my_filename varchar(100) as (metadata$filename::varchar(100))
)
location= @external_aws_dlkw
auto_refresh = true
file_format = (type = parquet);

SELECT * FROM t_cherry_creek_trail;

create secure materialized view MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.SMV_CHERRY_CREEK_TRAIL(
	POINT_ID,
	TRAIL_NAME,
	LNG,
	LAT,
	COORD_PAIR,
    DISTANCE_TO_MELANIES
) as
select 
 value:sequence_1 as point_id,
 value:trail_name::varchar as trail_name,
 value:latitude::number(11,8) as lng,
 value:longitude::number(11,8) as lat,
 lng||' '||lat as coord_pair,
 locations.distance_to_mc(st_makepoint(lng, lat)) as distance_to_melanies
from t_cherry_creek_trail;

CREATE OR REPLACE EXTERNAL VOLUME iceberg_external_volume
   STORAGE_LOCATIONS =
      (
         (
            NAME = 'iceberg-s3-us-west-2'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://uni-dlkw-iceberg'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::321463406630:role/dlkw_iceberg_role'
            STORAGE_AWS_EXTERNAL_ID = 'dlkw_iceberg_id'
         )
      );

DESC EXTERNAL VOLUME iceberg_external_volume;

create database my_iceberg_db
  catalog = 'SNOWFLAKE'
  external_volume = 'iceberg_external_volume';

set table_name = 'CCT_'||current_account();

create iceberg table identifier($table_name) (
    point_id number(10,0),
    trail_name string,
    coord_pair string,
    distance_to_melanies decimal(20,10),
    user_name string
)
  base_location = $table_name
  as
  select top 100
      point_id,
      trail_name,
      coord_pair,
      distance_to_melanies,
      current_user()
  from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.SMV_CHERRY_CREEK_TRAIL;

update identifier($table_name)
set user_name = 'I am amazing!!'
where point_id = 1;