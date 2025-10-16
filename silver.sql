use database kylesushi_exp2_db;
use schema silver;
use warehouse kylesushi_exp2_wh;

create or replace function silver.format_phone_number(phone_raw varchar)
returns varchar
language sql
as
$$
  -- remove all non-digit characters (allowing + for international format)
  with extracted as (
    select regexp_replace(phone_raw, '[^0-9+]', '') as digits_only
  )
  select 
    case 
      when digits_only like '+61%' then 
        '+61 ' || substr(digits_only, 4, 1) || ' ' ||
        substr(digits_only, 5, 4) || ' ' || substr(digits_only, 9, 4)
      when digits_only like '61%' and length(digits_only) >= 10 then 
        '+61 ' || substr(digits_only, 3, 1) || ' ' ||
        substr(digits_only, 4, 4) || ' ' || substr(digits_only, 8, 4)
      when digits_only like '02%' and length(digits_only) >= 10 then 
        '+61 2 ' || substr(digits_only, 3, 4) || ' ' || substr(digits_only, 7, 4)
      when digits_only like '0%' and length(digits_only) >= 10 then 
        '+61 ' || substr(digits_only, 2, 1) || ' ' ||
        substr(digits_only, 3, 4) || ' ' || substr(digits_only, 7, 4)
      else 'invalid'
    end
  from extracted
$$;


create or replace table silver.dim_suppliers as
select 
  supplier_id,
  supplier_name,
  supplier_type,
  contact_person,
  silver.format_phone_number(phone) as phone,
  email,
  address,
  current_timestamp() as processed_at,
  current_date() as dw_date
from bronze.suppliers
where is_active = true
  and supplier_id is not null
qualify row_number() over (partition by supplier_id order by created_at desc) = 1;


create or replace table silver.dim_stores as
select 
  store_id,
  store_name,
  store_name || ' ' || city as unique_name,
  address,
  suburb,
  city,
  state,
  postcode,
  silver.format_phone_number(phone) as phone,
  manager_name,
  opening_date,
  store_config:profile::varchar as store_profile,
  store_config:seating_capacity::int as seating_capacity,
  store_config:waste_rate::float as waste_rate,
  fixed_profit_rate,
  current_timestamp() as processed_at,
  current_date() as dw_date
from bronze.stores
where is_active = true
  and store_id is not null
qualify row_number() over (partition by store_id order by created_at desc) = 1;     


create or replace table silver.dim_products as
select 
  product_id,
  product_name,
  category,
  cost_price,
  selling_price,
  coalesce(selling_price - cost_price, 0) as gross_profit,
  round((coalesce(selling_price - cost_price, 0) / nullif(selling_price, 0)) * 100, 2) as profit_margin_pct,
  prep_time_minutes,
  product_details:nutritional_info.calories::int as calories,
  product_details:nutritional_info.carbs::float as carbs,
  product_details:nutritional_info.protein::float as protein,
  product_details:spice_level::int as spice_level,
  product_details:popularity_score::float as popularity_score,
  current_timestamp() as processed_at,
  current_date() as dw_date
from bronze.products
where product_id is not null
qualify row_number() over (partition by product_id order by updated_at desc) = 1;

create or replace table silver.dim_ingredients as
select 
  ingredient_id,
  ingredient_name,
  category,
  unit_of_measure,
  cost_per_unit,
  supplier_id,
  shelf_life_days,
  minimum_stock_level,
  maximum_stock_level,
  is_perishable,
  storage_requirements,
  current_timestamp() as processed_at,
  current_date() as dw_date
from bronze.ingredients
where is_active = true
  and ingredient_id is not null
qualify row_number() over (partition by ingredient_id order by created_at desc) = 1;


create or replace table silver.dim_batches as
select 
  batch_id,
  store_id,
  ingredient_id,
  quantity_received,
  quantity_remaining,
  cost_per_unit,
  received_date,
  expiry_date,
  status,
  current_timestamp() as processed_at,
  current_date() as dw_date
from bronze.inventory_batches
where batch_id is not null
qualify row_number() over (partition by batch_id order by created_at desc) = 1;


create or replace table silver.dim_recipes as
select 
  recipe_id,
  product_id,
  recipe_name,
  version,
  instructions,
  current_timestamp() as processed_at,
  current_date() as dw_date
from bronze.recipes
where recipe_id is not null
qualify row_number() over (partition by recipe_id order by created_at desc) = 1;


create or replace table silver.fact_inventory_transactions as
select 
  ledger_id,
  store_id,
  ingredient_id,
  batch_id,
  transaction_type,
  quantity_change,
  quantity_after,
  unit_cost,
  quantity_change * unit_cost as transaction_value,
  transaction_date,
  current_timestamp() as processed_at,
  current_date() as dw_date
from bronze.inventory_ledger
where ledger_id is not null
  and transaction_date is not null;

create or replace table silver.fact_sales_transactions as
select 
  t.transaction_id,
  t.store_id,
  t.staff_id,
  tl.product_id,
  t.transaction_datetime,
  tl.quantity,
  tl.unit_price,
  tl.line_total,
  t.tax_amount * (tl.line_total / nullif(t.total_amount, 0)) as line_tax,
  tl.line_total + (t.tax_amount * (tl.line_total / nullif(t.total_amount, 0))) as line_total_with_tax,
  t.order_source,
  tl.preparation_notes,
  customization:spice_level::varchar as customization_spice_level,
  customization:no_rice::boolean as customization_no_rice,
  customization:extra_sauce::boolean as customization_extra_sauce,

  payment_info:method::varchar as payment_method,
  payment_info:card_type::varchar as payment_card_type,
  payment_info:mobile_pay_provider::varchar as payment_mobile_provider,
  current_timestamp() as processed_at,
  current_date() as dw_date
from bronze.transactions t
inner join bronze.transaction_lines tl on t.transaction_id = tl.transaction_id
where t.transaction_id is not null
  and tl.transaction_id is not null;

create or replace table silver.fact_purchase_orders as
select 
  pol.purchase_order_line_id,
  po.purchase_order_id,
  po.store_id,
  po.supplier_id,
  pol.ingredient_id,
  pol.quantity_ordered,
  pol.quantity_received,
  pol.unit_cost,
  pol.line_total,
  po.order_date,
  po.expected_delivery_date,
  pol.received_date,
  datediff(day, po.expected_delivery_date, coalesce(pol.received_date, current_date())) as days_variance,
  case 
    when pol.received_date <= po.expected_delivery_date then 'on_time'
    when pol.received_date is null then 'pending'
    else 'late'
  end as delivery_status,
  po.status,
  current_timestamp() as processed_at,
  current_date() as dw_date
from bronze.purchase_orders po
left join bronze.purchase_order_lines pol on po.purchase_order_id = pol.purchase_order_id
where po.purchase_order_id is not null;

create or replace table silver.fact_waste_tracking as
select
  waste_id,
  store_id,
  ingredient_id,
  batch_id,
  waste_date,
  quantity_wasted,
  unit_cost,
  total_cost,
  waste_reason,
  reported_by,
  current_timestamp() as processed_at,
  current_date() as dw_date
from bronze.waste_tracking
where waste_id is not null
  and waste_date is not null;


create or replace table silver.bridge_recipes_ingredients as
select 
  recipe_ingredient_id,
  recipe_id,
  ingredient_id,
  quantity_required,
  unit_of_measure,
  is_optional,
  notes,
  current_timestamp() as processed_at,
  current_date() as dw_date
from bronze.recipe_ingredients
where recipe_ingredient_id is not null;


create or replace table silver.dim_date as
select 
    date,
    date_display,
    year,
    quarter,
    month,
    month_name,
    month_abbr,
    day_of_week,
    day_of_week_name,
    day_of_week_abbr,
    season,
    is_weekend,
    is_holiday,
    holiday_name,
    is_business_day,
    is_today,
    is_current_month,
    is_current_quarter,
    is_current_year,
    days_from_today,
    weather:temp_max_c::float as max_temp,
    weather:temp_min_c::float as min_temp,
    weather:temp_mean_c::float as mean_temp,
    weather:precipitation_mm::float as precipitation_mm,
    case weather:weather_code::number
        when 0 then 'clear sky'
        when 1 then 'mainly clear'
        when 2 then 'partly cloudy'
        when 3 then 'overcast'
        when 45 then 'fog'
        when 48 then 'depositing rime fog'
        when 51 then 'light drizzle'
        when 53 then 'moderate drizzle'
        when 55 then 'dense drizzle'
        when 61 then 'slight rain'
        when 63 then 'moderate rain'
        when 65 then 'heavy rain'
        when 71 then 'slight snow'
        when 73 then 'moderate snow'
        when 75 then 'heavy snow'
        when 80 then 'slight rain showers'
        when 81 then 'moderate rain showers'
        when 82 then 'violent rain showers'
        when 95 then 'thunderstorm'
        when 96 then 'thunderstorm with slight hail'
        when 99 then 'thunderstorm with heavy hail'
        else 'unknown'
    end as weather_description
from bronze.dates;