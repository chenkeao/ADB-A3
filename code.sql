-- Create the main Snowflake database for the Kyle Sushi data warehouse
CREATE OR REPLACE DATABASE kylesushi_exp2_db;

-- Switch context to the new database
USE DATABASE kylesushi_exp2_db;

-- Create the Medallion architecture schemas: bronze for raw data,
-- silver for cleansed and integrated data, and gold for future analytics
CREATE OR REPLACE SCHEMA bronze;
CREATE OR REPLACE SCHEMA silver;
CREATE OR REPLACE SCHEMA gold;

-- Create a dedicated compute warehouse for ETL and query processing.
-- Auto-suspend and auto-resume ensure cost efficiency by pausing when idle.
CREATE OR REPLACE WAREHOUSE kylesushi_exp2_wh
  WITH WAREHOUSE_SIZE = 'SMALL'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- Set the session context to the compute warehouse
USE WAREHOUSE kylesushi_exp2_wh;


The database and warehouse creation scripts initialize the working environment and enforce compute governance. The three schemas implement the physical layer of the Medallion architecture.

Data ingestion begins in the bronze schema. This layer stores exact replicas of source system files, loaded from external storage without transformation. Each CREATE TABLE command in this stage defines a structure consistent with the raw CSV files, maintaining full traceability.

sql
-- Switch to the bronze schema
USE SCHEMA bronze;

-- Create a stage for uploading and loading raw CSV files
-- The stage serves as a temporary storage location for ingestion
CREATE OR REPLACE STAGE csv_stage
  FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

-- Define the raw transaction data table
-- Each table mirrors the source file with minimal constraints or data typing
CREATE OR REPLACE TABLE transactions_raw (
    transaction_id STRING,
    store_id STRING,
    staff_id STRING,
    transaction_date TIMESTAMP,
    payment_method STRING,
    total_amount FLOAT,
    discount FLOAT
);

-- Load the raw transaction data from the staged CSV into the bronze table
COPY INTO transactions_raw
  FROM @csv_stage/transactions.csv
  FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

-- Similarly define transaction line-level data
CREATE OR REPLACE TABLE transaction_lines_raw (
    transaction_id STRING,
    product_id STRING,
    quantity INT,
    unit_price FLOAT,
    total_price FLOAT,
    customisation STRING
);

COPY INTO transaction_lines_raw
  FROM @csv_stage/transaction_lines.csv
  FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

-- Example inventory and supplier data ingestion
CREATE OR REPLACE TABLE ingredients_raw (
    ingredient_id STRING,
    ingredient_name STRING,
    cost_per_unit FLOAT,
    supplier_id STRING,
    shelf_life_days INT
);

COPY INTO ingredients_raw
  FROM @csv_stage/ingredients.csv
  FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

CREATE OR REPLACE TABLE suppliers_raw (
    supplier_id STRING,
    supplier_name STRING,
    contact_number STRING,
    address STRING,
    email STRING
);

COPY INTO suppliers_raw
  FROM @csv_stage/suppliers.csv
  FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


The bronze layer ensures that raw operational and supplier data are ingested with no alteration, providing a reliable foundation for all subsequent transformations. Data cleansing and integration are performed in the silver layer, where business rules and model logic are applied.

sql
-- Switch to the silver schema for transformation and integration
USE SCHEMA silver;

-- Create a user-defined function for standardizing Australian phone numbers
-- This enforces consistent formatting across supplier and store records
CREATE OR REPLACE FUNCTION format_phone_number(phone STRING)
RETURNS STRING
AS
$$
    REGEXP_REPLACE(phone, '[^0-9]', '')  -- remove non-numeric characters
$$;

-- Create the dimension table for stores using filtered and cleansed data
CREATE OR REPLACE TABLE dim_stores AS
SELECT DISTINCT
    store_id,
    store_name,
    address,
    format_phone_number(phone) AS phone_number,
    state,
    manager_name
FROM bronze.stores_raw
WHERE active = TRUE;

-- Create the dimension table for products with extracted JSON attributes
CREATE OR REPLACE TABLE dim_products AS
SELECT
    product_id,
    product_name,
    category,
    details:calories::FLOAT AS calories,
    details:popularity::FLOAT AS popularity_score,
    details:ingredients::ARRAY AS ingredient_list,
    base_price,
    cost_per_unit
FROM bronze.products_raw;

-- Create the dimension table for suppliers with standardized phone numbers
CREATE OR REPLACE TABLE dim_suppliers AS
SELECT
    supplier_id,
    supplier_name,
    format_phone_number(contact_number) AS contact_number,
    email,
    address
FROM bronze.suppliers_raw;

-- Create the fact table for sales transactions by joining header and line data
-- and calculating derived metrics for profitability analysis
CREATE OR REPLACE TABLE fact_sales_transactions AS
SELECT
    t.transaction_id,
    t.store_id,
    l.product_id,
    t.transaction_date,
    l.quantity,
    l.unit_price,
    l.total_price,
    t.discount,
    (l.total_price - (l.quantity * p.cost_per_unit)) AS gross_profit,
    ((l.total_price - (l.quantity * p.cost_per_unit)) / NULLIF(l.total_price, 0)) AS profit_margin
FROM bronze.transactions_raw AS t
JOIN bronze.transaction_lines_raw AS l
  ON t.transaction_id = l.transaction_id
JOIN silver.dim_products AS p
  ON l.product_id = p.product_id
WHERE t.total_amount > 0;

-- Create the date dimension with external weather enrichment
CREATE OR REPLACE TABLE dim_date AS
SELECT
    d.date_key,
    d.calendar_date,
    d.day_of_week,
    d.month,
    d.year,
    d.is_holiday,
    d.weather:temperature::FLOAT AS temperature,
    d.weather:precipitation::FLOAT AS precipitation
FROM bronze.dates_raw AS d;