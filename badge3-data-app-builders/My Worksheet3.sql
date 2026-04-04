CREATE DATABASE smoothies;
CREATE OR REPLACE SCHEMA public;

CREATE TABLE fruit_options
(
    fruit_name      VARCHAR(25),
    fruit_id        VARCHAR(3)
);

CREATE FILE FORMAT two_headerrow_pct_delim
    type = 'CSV',
    skip_header = 2,
    field_delimiter = '%',
    trim_space = TRUE;

CREATE STAGE my_uploaded_files;

SELECT $1, $2
FROM @smoothies.public.my_uploaded_files/fruits_available_for_smoothies.txt
(file_format => two_headerrow_pct_delim);

COPY INTO smoothies.public.fruit_options
FROM @smoothies.public.my_uploaded_files
    files = ('fruits_available_for_smoothies.txt')
    file_format = (format_name = 'smoothies.public.two_headerrow_pct_delim');
    -- on_error = abort_statement
    -- validation_mode = return_errors
    -- purge = true;

CREATE TABLE orders
(
    ingredients VARCHAR(200)
);

ALTER TABLE orders
ADD COLUMN 
    name_on_order       VARCHAR(200);

ALTER TABLE smoothies.public.orders
ADD COLUMN order_filled BOOLEAN DEFAULT FALSE;

CREATE SEQUENCE order_seq
    start = 1
    increment = 2
    ORDER
    comment = 'Provide a unique id for each smoothie order';
    
TRUNCATE TABLE smoothies.public.orders;

ALTER TABLE smoothies.public.orders
ADD COLUMN order_uid integer
default smoothies.public.order_seq.nextval
constraint order_uid unique enforced; 

create or replace table smoothies.public.orders (
       order_uid integer default smoothies.public.order_seq.nextval,
       order_filled boolean default false,
       name_on_order varchar(100),
       ingredients varchar(200),
       constraint order_uid unique (order_uid),
       order_ts timestamp_ltz default current_timestamp()
);

ALTER TABLE smoothies.public.fruit_options
ADD COLUMN SEARCH_ON STRING;

SELECT * FROM smoothies.public.fruit_options;

-- Apples → Apple
UPDATE smoothies.public.fruit_options
SET SEARCH_ON = 'apple'
WHERE FRUIT_NAME = 'Apples';

-- Blueberries → Blueberry
UPDATE smoothies.public.fruit_options
SET SEARCH_ON = 'blueberry'
WHERE FRUIT_NAME = 'Blueberries';

-- Cantaloupe → NOT IN API
UPDATE smoothies.public.fruit_options
SET SEARCH_ON = NULL
WHERE FRUIT_NAME = 'Cantaloupe';

-- Dragon Fruit → Dragonfruit
UPDATE smoothies.public.fruit_options
SET SEARCH_ON = 'dragonfruit'
WHERE FRUIT_NAME = 'Dragon Fruit';

-- Elderberries → Elderberry
UPDATE smoothies.public.fruit_options
SET SEARCH_ON = 'elderberry'
WHERE FRUIT_NAME = 'Elderberries';

-- Figs → Figs (API uses plural)
UPDATE smoothies.public.fruit_options
SET SEARCH_ON = 'figs'
WHERE FRUIT_NAME = 'Figs';

-- Guava → Guava
UPDATE smoothies.public.fruit_options
SET SEARCH_ON = 'guava'
WHERE FRUIT_NAME = 'Guava';

-- Honeydew → Honeydew
UPDATE smoothies.public.fruit_options
SET SEARCH_ON = 'honeydew'
WHERE FRUIT_NAME = 'Honeydew';

-- Jackfruit → Jackfruit
UPDATE smoothies.public.fruit_options
SET SEARCH_ON = 'jackfruit'
WHERE FRUIT_NAME = 'Jackfruit';

-- Kiwi → Kiwi
UPDATE smoothies.public.fruit_options
SET SEARCH_ON = 'kiwi'
WHERE FRUIT_NAME = 'Kiwi';

-- Lime → Lime
UPDATE smoothies.public.fruit_options
SET SEARCH_ON = 'lime'
WHERE FRUIT_NAME = 'Lime';

-- Mango → Mango
UPDATE smoothies.public.fruit_options
SET SEARCH_ON = 'mango'
WHERE FRUIT_NAME = 'Mango';

-- Nectarine → Nectarine
UPDATE smoothies.public.fruit_options
SET SEARCH_ON = 'nectarine'
WHERE FRUIT_NAME = 'Nectarine';

-- Orange → Orange
UPDATE smoothies.public.fruit_options
SET SEARCH_ON = 'orange'
WHERE FRUIT_NAME = 'Orange';

-- Papaya → Papaya
UPDATE smoothies.public.fruit_options
SET SEARCH_ON = 'papaya'
WHERE FRUIT_NAME = 'Papaya';

-- Quince → Quince
UPDATE smoothies.public.fruit_options
SET SEARCH_ON = 'quince'
WHERE FRUIT_NAME = 'Quince';

-- Raspberries → Raspberry
UPDATE smoothies.public.fruit_options
SET SEARCH_ON = 'raspberry'
WHERE FRUIT_NAME = 'Raspberries';

-- Strawberries → Strawberry
UPDATE smoothies.public.fruit_options
SET SEARCH_ON = 'strawberry'
WHERE FRUIT_NAME = 'Strawberries';

-- Tangerine → Tangerine
UPDATE smoothies.public.fruit_options
SET SEARCH_ON = 'tangerine'
WHERE FRUIT_NAME = 'Tangerine';

-- Ugli Fruit → Ugli Fruit (Jamaican Tangelo)
UPDATE smoothies.public.fruit_options
SET SEARCH_ON = 'ugli fruit (jamaican tangelo)'
WHERE FRUIT_NAME = 'Ugli Fruit';

-- Vanilla Fruit → Vanilla Fruit
UPDATE smoothies.public.fruit_options
SET SEARCH_ON = 'vanilla fruit'
WHERE FRUIT_NAME = 'Vanilla Fruit';

-- Watermelon → Watermelon
UPDATE smoothies.public.fruit_options
SET SEARCH_ON = 'watermelon'
WHERE FRUIT_NAME = 'Watermelon';

-- Ximenia → Ximenia (Hog Plum)
UPDATE smoothies.public.fruit_options
SET SEARCH_ON = 'ximenia (hog plum)'
WHERE FRUIT_NAME = 'Ximenia';

-- Yerba Mate → NOT IN API
UPDATE smoothies.public.fruit_options
SET SEARCH_ON = NULL
WHERE FRUIT_NAME = 'Yerba Mate';

-- Ziziphus Jujube → Ziziphus Jujube
UPDATE smoothies.public.fruit_options
SET SEARCH_ON = 'ziziphus jujube'
WHERE FRUIT_NAME = 'Ziziphus Jujube';

SELECT FRUIT_NAME, SEARCH_ON
FROM smoothies.public.fruit_options
ORDER BY FRUIT_NAME;