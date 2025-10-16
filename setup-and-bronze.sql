-- create the database for this experiment
create or replace database kylesushi_exp2_db;

-- activate this database
use database kylesushi_exp2_db;

-- create schemas for each layer of medallion architecture
create or replace schema bronze comment = 'bronze layer of medallion';
create or replace schema silver comment = 'silver layer of medallion';
create or replace schema gold comment = 'gold layer of medallion';

use schema bronze;


-- create a new data warehouse with a small size
create or replace warehouse kylesushi_exp2_wh
    warehouse_size = 'small'
    auto_suspend = 60
    auto_resume = true
    initially_suspended = true
    warehouse_type = 'standard'
comment = 'experiment warehouse';

-- activate this data warehouse
use warehouse kylesushi_exp2_wh;


-- create a stage for loading csv files
create or replace stage bronze.csv_stage 
    file_format = (type = 'csv' 
                  field_delimiter = ',' 
                  skip_header = 1 
                  field_optionally_enclosed_by = '"'
                  escape_unenclosed_field = none);


-- first, load data onto bronze layer

-- use web ui upload csv files to csv stage, and check the result.
list @csv_stage;

-- source: data/ingredients.csv
create or replace table ingredients (
  ingredient_id varchar,
  ingredient_name varchar,
  category varchar,
  unit_of_measure varchar,
  cost_per_unit float,
  supplier_id varchar,
  shelf_life_days number,
  minimum_stock_level number,
  maximum_stock_level number,
  is_perishable boolean,
  storage_requirements varchar,
  created_at timestamp_ntz,
  is_active boolean,
  expiry_countdown_days number
);

-- source: data/inventory/part1.csv
create or replace table inventory_ledger (
  ledger_id varchar,
  store_id varchar,
  ingredient_id varchar,
  batch_id varchar,
  transaction_type varchar,
  quantity_change float,
  quantity_after float,
  unit_cost float,
  reference_id varchar,
  transaction_date date,
  created_at date
);


-- source: data/inventory_batches.csv
create or replace table inventory_batches (
  batch_id varchar,
  store_id varchar,
  ingredient_id varchar,
  quantity_received number,
  quantity_remaining float,
  cost_per_unit float,
  received_date date,
  expiry_date date,
  supplier_batch_number varchar,
  status varchar,
  created_at date
);

-- source: data/products.csv
create or replace table products (
  product_id varchar,
  product_name varchar,
  category varchar,
  cost_price float,
  selling_price float,
  prep_time_minutes number,
  is_available boolean,
  product_details variant,
  created_at timestamp_ntz,
  updated_at timestamp_ntz
);


-- source: data/purchase_order_lines.csv
create or replace table purchase_order_lines (
  purchase_order_line_id varchar,
  purchase_order_id varchar,
  ingredient_id varchar,
  quantity_ordered number,
  unit_cost float,
  line_total float,
  quantity_received number,
  received_date date,
  created_at timestamp_ntz
);

-- source: data/purchase_orders.csv
create or replace table purchase_orders (
  purchase_order_id varchar,
  store_id varchar,
  supplier_id varchar,
  order_date timestamp_ntz,
  expected_delivery_date date,
  status varchar,
  total_amount float,
  created_by varchar,
  created_at timestamp_ntz
);

-- source: data/recipe_ingredients.csv
create or replace table recipe_ingredients (
  recipe_ingredient_id varchar,
  recipe_id varchar,
  ingredient_id varchar,
  quantity_required float,
  unit_of_measure varchar,
  is_optional boolean,
  notes varchar,
  created_at timestamp_ntz
);

-- source: data/recipes.csv
create or replace table recipes (
  recipe_id varchar,
  product_id varchar,
  recipe_name varchar,
  version number,
  instructions varchar,
  prep_time_minutes number,
  serving_size number,
  created_at timestamp_ntz,
  is_active boolean
);



-- source: data/stores.csv
create or replace table stores (
  store_id varchar,
  store_name varchar,
  address varchar,
  suburb varchar,
  city varchar,
  state varchar,
  postcode number,
  phone varchar,
  manager_name varchar,
  opening_date date,
  store_config variant,
  created_at timestamp_ntz,
  is_active boolean,
  fixed_profit_rate float
);


-- source: data/suppliers.csv
create or replace table suppliers (
  supplier_id varchar,
  supplier_name varchar,
  supplier_type varchar,
  contact_person varchar,
  phone varchar,
  email varchar,
  address varchar,
  payment_terms varchar,
  delivery_days variant,
  specialties variant,
  is_active boolean,
  created_at timestamp_ntz
);


-- source: data/transaction_lines.csv
create or replace table transaction_lines (
  transaction_id varchar,
  product_id varchar,
  quantity number,
  unit_price float,
  line_total float,
  customization variant,
  preparation_notes varchar,
  created_at timestamp_ntz
);


-- source: data/transactions.csv
create or replace table transactions (
  transaction_id varchar,
  store_id varchar,
  staff_id varchar,
  transaction_datetime timestamp_ntz,
  total_amount float,
  tax_amount float,
  payment_info variant,
  order_source varchar,
  created_at timestamp_ntz
);


-- source: data/waste_tracking.csv
create or replace table waste_tracking (
  waste_id varchar,
  store_id varchar,
  ingredient_id varchar,
  batch_id varchar,
  waste_date date,
  quantity_wasted float,
  unit_cost float,
  total_cost float,
  waste_reason varchar,
  reported_by varchar,
  created_at date
);

create or replace table dates (
    date date primary key,
    date_display varchar(20),
    year number(4),
    quarter number(1),
    month number(2),
    month_name varchar(20),
    month_abbr varchar(3),
    day_of_week number(1),
    day_of_week_name varchar(20),
    day_of_week_abbr varchar(3),
    season varchar(10),
    is_weekend boolean,
    is_holiday boolean,
    holiday_name varchar(100),
    is_business_day boolean,
    is_today boolean,
    is_current_month boolean,
    is_current_quarter boolean,
    is_current_year boolean,
    days_from_today number,
    weather variant
);

copy into dates
from @csv_stage/dim_date_with_weather.csv
file_format = (type=csv field_optionally_enclosed_by='"' skip_header=1 null_if=('','null','null') escape_unenclosed_field=none)
on_error = continue;


copy into ingredients
  from @csv_stage/ingredients.csv
file_format = (type=csv field_optionally_enclosed_by='"' skip_header=1 null_if=('','null','null') escape_unenclosed_field=none)
on_error = continue;


copy into inventory_ledger
  from @csv_stage/inventory_ledger_part1.csv
file_format = (type=csv field_optionally_enclosed_by='"' skip_header=1 null_if=('','null','null') escape_unenclosed_field=none)
on_error = continue;

copy into inventory_ledger
  from @csv_stage/inventory_ledger_part2.csv
file_format = (type=csv field_optionally_enclosed_by='"' skip_header=1 null_if=('','null','null') escape_unenclosed_field=none)
on_error = continue;

copy into inventory_batches
  from @csv_stage/inventory_batches.csv
file_format = (type=csv field_optionally_enclosed_by='"' skip_header=1 null_if=('','null','null') escape_unenclosed_field=none)
on_error = continue;

copy into products
  from @csv_stage/products.csv
file_format = (type=csv field_optionally_enclosed_by='"' skip_header=1 null_if=('','null','null') escape_unenclosed_field=none)
on_error = continue;

copy into purchase_order_lines
  from @csv_stage/purchase_order_lines.csv
file_format = (type=csv field_optionally_enclosed_by='"' skip_header=1 null_if=('','null','null') escape_unenclosed_field=none)
on_error = continue;


copy into purchase_orders
  from @csv_stage/purchase_orders.csv
file_format = (type=csv field_optionally_enclosed_by='"' skip_header=1 null_if=('','null','null') escape_unenclosed_field=none)
on_error = continue;


copy into recipe_ingredients
  from @csv_stage/recipe_ingredients.csv
file_format = (type=csv field_optionally_enclosed_by='"' skip_header=1 null_if=('','null','null') escape_unenclosed_field=none)
on_error = continue;

copy into recipes
  from @csv_stage/recipes.csv
file_format = (type=csv field_optionally_enclosed_by='"' skip_header=1 null_if=('','null','null') escape_unenclosed_field=none)
on_error = continue;

copy into stores
  from @csv_stage/stores.csv
file_format = (type=csv field_optionally_enclosed_by='"' skip_header=1 null_if=('','null','null') escape_unenclosed_field=none)
on_error = continue;

copy into suppliers
  from @csv_stage/suppliers.csv
file_format = (type=csv field_optionally_enclosed_by='"' skip_header=1 null_if=('','null','null') escape_unenclosed_field=none)
on_error = continue;

copy into transaction_lines
  from @csv_stage/transaction_lines.csv
file_format = (type=csv field_optionally_enclosed_by='"' skip_header=1 null_if=('','null','null') escape_unenclosed_field=none)
on_error = continue;

copy into transactions
  from @csv_stage/transactions.csv
file_format = (type=csv field_optionally_enclosed_by='"' skip_header=1 null_if=('','null','null') escape_unenclosed_field=none)
on_error = continue;

copy into waste_tracking
  from @csv_stage/waste_tracking.csv
file_format = (type=csv field_optionally_enclosed_by='"' skip_header=1 null_if=('','null','null') escape_unenclosed_field=none)
on_error = continue;

