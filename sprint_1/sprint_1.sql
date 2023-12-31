-- Database: sprint_1

--DROP DATABASE IF EXISTS sprint_1;

/*CREATE DATABASE sprint_1
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'Russian_Russia.1252'
    LC_CTYPE = 'Russian_Russia.1252'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;
*/	
DROP SCHEMA IF EXISTS raw_data CASCADE;
DROP SCHEMA IF EXISTS car_shop CASCADE;

CREATE SCHEMA IF NOT EXISTS raw_data;

CREATE TABLE raw_data.sales (
	id INTEGER PRIMARY KEY,
	auto VARCHAR(100) NOT NULL,
	gasoline_consumption NUMERIC(3,1),
	price NUMERIC(9, 2) NOT NULL,
	date DATE NOT NULL,
	person_name VARCHAR(50) NOT NULL,
	phone VARCHAR(50),
	discount SMALLINT DEFAULT 0,
	brand_origin VARCHAR(50)
);

COPY raw_data.sales(id,auto,gasoline_consumption,price,date,person_name,phone,discount,brand_origin) 
FROM 'C:\tmp\cars.csv' WITH CSV HEADER NULL 'null';

CREATE SCHEMA IF NOT EXISTS car_shop;

-- PARSING CARS.
	-- CAR's BRANDS ORIGIN.
	CREATE TABLE car_shop.brands_origins (
		id SMALLSERIAL PRIMARY KEY, -- countries count is below 210.
		name VARCHAR(50) -- The longest country name is 'The United Kingdom of Great Britain and Northern Ireland.' - 49 symbols.
	);
	
	INSERT INTO car_shop.brands_origins(name)
	SELECT
		brand_origin
	FROM raw_data.sales
	GROUP BY brand_origin;
	
	-- CAR's BRANDS.
	CREATE TABLE car_shop.brands (
		id SMALLSERIAL PRIMARY KEY, -- 4 byte is not required due to total count of brands won't exceed 2^16.
		name VARCHAR(20) UNIQUE NOT NULL, -- Not expecting name bigger than 20 symbols.
		origin_id INT2 DEFAULT NULL -- countries count is below 210.
	);

	INSERT INTO car_shop.brands(name, origin_id)
	SELECT
		SPLIT_PART(auto, ' ', 1) AS brand_name,
		car_shop.brands_origins.id AS origin_id
	FROM raw_data.sales
	LEFT JOIN car_shop.brands_origins ON brand_origin = car_shop.brands_origins.name
	GROUP BY brand_name, origin_id;

	-- CAR's MARKS.
	CREATE TABLE car_shop.marks (
		id SERIAL PRIMARY KEY, -- potentially can exceed 2^16.
		name VARCHAR(25) UNIQUE NOT NULL, -- longest one (Shàngqì Dàzhòng Pàsàtè Língdù) - chatgpt.
		consumption NUMERIC(3,1) DEFAULT 0 CHECK (consumption >= 0) -- it's easier to keep in preformated type, but also float8 can be used. 
	);

	INSERT INTO car_shop.marks(name, consumption)
	SELECT
		SUBSTR(SPLIT_PART(auto, ', ', 1), STRPOS(auto, ' ') + 1)  AS mark_name,
		CASE 
			WHEN gasoline_consumption IS NULL THEN 0
			ELSE gasoline_consumption::NUMERIC(3,1)
		END AS consumption
	FROM raw_data.sales
	GROUP BY mark_name, consumption;
	
	-- CAR's COLORS.
	CREATE TABLE car_shop.colors (
		id SMALLSERIAL PRIMARY KEY, -- don't expect colors count more than 2^16.
		name VARCHAR(20) UNIQUE NOT NULL -- longest color name is 'Glaucous', but name can be mixed from several words.
	);
	
	INSERT INTO car_shop.colors(name)
	SELECT
		SPLIT_PART(auto, ' ',-1) AS color_name
	FROM raw_data.sales
	GROUP BY color_name;
	
	-- CAR's CONFIGS
	CREATE TABLE car_shop.configs (
		id SERIAL PRIMARY KEY,
		brand_id INT2 NOT NULL,
		mark_id INTEGER NOT NULL,
		color_id INT2 NOT NULL
	);
	
	INSERT INTO car_shop.configs (brand_id, mark_id, color_id)
	SELECT
		car_shop.brands.id,
		car_shop.marks.id,
		car_shop.colors.id
	FROM raw_data.sales
	LEFT JOIN car_shop.brands ON SPLIT_PART(auto, ' ', 1) = car_shop.brands.name
	LEFT JOIN car_shop.marks ON SUBSTR(SPLIT_PART(auto, ', ', 1), STRPOS(auto, ' ') + 1) = car_shop.marks.name
	LEFT JOIN car_shop.colors ON SPLIT_PART(auto, ' ',-1) = car_shop.colors.name
	GROUP BY (car_shop.brands.id, car_shop.marks.id, car_shop.colors.id);

-- PARSING CLIENTS.
	-- CLIENT's NAMES AND PHONE.
	CREATE TABLE car_shop.clients (
		id SERIAL PRIMARY KEY, -- unique clients count can be huge.
		first_name VARCHAR(25) NOT NULL, -- typically should be enough.
		last_name VARCHAR(25) NOT NULL, -- typically should be enough.
		phone VARCHAR(25) DEFAULT NULL, -- typically should be enough.
		CONSTRAINT unique_client UNIQUE (first_name, last_name, phone) -- unique client identifier. name + surname can be repeated.
	);

	INSERT INTO car_shop.clients(first_name, last_name, phone)
	SELECT DISTINCT
		SPLIT_PART(person_name, ' ', 1) AS first_name,
		SPLIT_PART(person_name, ' ', 2) AS last_name,
		phone
	FROM raw_data.sales;

-- SALES TABLE GENERATION.
CREATE TABLE car_shop.sales (
	id SERIAL PRIMARY KEY,
	config_id INTEGER REFERENCES car_shop.configs(id),
	price NUMERIC NOT NULL,
	date DATE NOT NULL,
	client_id INTEGER REFERENCES car_shop.clients(id),
	discount INT2 DEFAULT 0
);

INSERT INTO car_shop.sales(config_id, price, date, client_id, discount)
SELECT
	car_shop.configs.id,
	price,
	date,
	car_shop.clients.id,
	discount
FROM raw_data.sales
LEFT JOIN car_shop.brands ON SPLIT_PART(auto, ' ', 1) = car_shop.brands.name
LEFT JOIN car_shop.marks ON SUBSTR(SPLIT_PART(auto, ', ', 1), STRPOS(auto, ' ') + 1) = car_shop.marks.name
LEFT JOIN car_shop.colors ON SPLIT_PART(auto, ' ', -1) = car_shop.colors.name
LEFT JOIN car_shop.configs ON (car_shop.brands.id = car_shop.configs.brand_id AND 
								   car_shop.marks.id = car_shop.configs.mark_id 
								   AND car_shop.colors.id = car_shop.configs.color_id)
LEFT JOIN car_shop.clients ON (SPLIT_PART(person_name, ' ', 1) = car_shop.clients.first_name AND
						  SPLIT_PART(person_name, ' ', 2) = car_shop.clients.last_name AND
						  raw_data.sales.phone = car_shop.clients.phone);

SELECT * FROM car_shop.sales;

--STAGE 2. TASK 1.
SELECT 100 * (COUNT(*) - COUNT(gasoline_consumption)::double precision) / COUNT(*) AS nulls_percentage_gasoline_consumption 
FROM raw_data.sales;

--STAGE 2. TASK 2.
SELECT
	car_shop.brands.name AS brand_name,
	EXTRACT(YEAR FROM date)::INT2 AS year,
	ROUND(AVG(price), 2) AS price_avg
FROM car_shop.sales AS sales
LEFT JOIN car_shop.configs ON car_shop.configs.id = sales.config_id
LEFT JOIN car_shop.brands ON car_shop.brands.id = car_shop.configs.brand_id
GROUP BY brand_name, year
ORDER BY brand_name, year;

--STAGE 2. TASK 3.
SELECT
	EXTRACT(MONTH FROM date)::INT2 AS month,
	EXTRACT(YEAR FROM date)::INT2 AS year,
	ROUND(AVG(price), 2)::NUMERIC(9,2) AS price_avg
FROM car_shop.sales AS s
WHERE 
	EXTRACT(YEAR FROM s.date) = 2022
GROUP BY month, year
ORDER BY month;
	
--STAGE 2. TASK 4.
SELECT
	CONCAT_WS(' ', car_shop.clients.first_name, car_shop.clients.last_name) AS person,
	STRING_AGG(CONCAT_WS(' ', car_shop.brands.name, car_shop.marks.name), ', ') AS cars
FROM car_shop.sales
LEFT JOIN car_shop.configs ON car_shop.configs.id = sales.config_id
LEFT JOIN car_shop.brands ON car_shop.brands.id = car_shop.configs.brand_id
LEFT JOIN car_shop.marks ON car_shop.marks.id = car_shop.configs.mark_id
LEFT JOIN car_shop.clients ON car_shop.clients.id = sales.client_id
GROUP BY person
ORDER BY person;

--STAGE 2. TASK 5.
SELECT
	car_shop.brands_origins.name AS brand_origin,
	ROUND(MAX(100 * price / (100 - discount)), 2) AS price_max,
	ROUND(MIN(100 * price / (100 - discount)), 2) AS price_min
FROM car_shop.sales AS sales
LEFT JOIN car_shop.configs ON car_shop.configs.id = sales.config_id
LEFT JOIN car_shop.brands ON car_shop.brands.id = car_shop.configs.brand_id
INNER JOIN car_shop.brands_origins ON car_shop.brands_origins.id = car_shop.brands.origin_id
WHERE car_shop.brands_origins.name IS NOT NULL
GROUP BY brand_origin;

--STAGE 2. TASK 6.
SELECT
	COUNT(*) AS persons_from_usa_count
FROM car_shop.clients AS c
WHERE c.phone LIKE '+1%';
	