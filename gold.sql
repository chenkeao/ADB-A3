USE WAREHOUSE KYLESUSHI_EXP2_WH;
USE DATABASE KYLESUSHI_EXP2_DB;
USE SCHEMA GOLD;

-- ===============================================================
-- [1] REVENUE & PROFIT ANALYSIS (Branch / Total)
-- ---------------------------------------------------------------
-- • Purpose: Calculate daily revenue and profit by branch and total.
-- • Data Source: SILVER.DIM_DATE, SILVER.DIM_STORES, FACT_SALES_TRANSACTIONS
-- • Key Logic:
--     - CROSS JOIN ensures every date-store pair exists.
--     - Profit = SUM(Line Total) × Fixed Profit Rate.
-- • Dashboard Use: “Sales by Branch” and “Total Profit Trend” charts.
-- ===============================================================


-- 1-1. Table Create: Total Revenue & Profit of 'Branch' by day
CREATE OR REPLACE VIEW GOLD.BRCS_REV_PRF_VIEW AS
SELECT TO_CHAR(T1.DATE, 'YYYYMMDD') || SUBSTR(T2.STORE_ID, 5) AS REV_PRF_ID
     , T1.DATE AS DATE
     , T2.STORE_ID
     , T2.STORE_NAME
     , COALESCE(SUM(T3.LINE_TOTAL), 0) AS TOT_REV
     , COALESCE(SUM(T3.LINE_TOTAL) * T2.FIXED_PROFIT_RATE, 0) AS TOT_PRF    -- Profit : Total revenue * Profit rate of each branches
  FROM SILVER.DIM_DATE T1
 CROSS JOIN SILVER.DIM_STORES T2
  LEFT JOIN SILVER.FACT_SALES_TRANSACTIONS T3
    ON T3.STORE_ID = T2.STORE_ID
   AND TO_CHAR(T3.TRANSACTION_DATETIME, 'YYYYMMDD') = TO_CHAR(T1.DATE, 'YYYYMMDD')
 GROUP BY TO_CHAR(T1.DATE, 'YYYYMMDD') || T2.STORE_ID
        , T1.DATE
        , T2.STORE_ID
        , T2.STORE_NAME
        , T2.FIXED_PROFIT_RATE
 ORDER BY TO_CHAR(T1.DATE, 'YYYYMMDD')
        , T1.DATE
        , T2.STORE_ID;

-- 1-2. Total Revenue and profit of the 'Kyle san sushi'
CREATE OR REPLACE VIEW GOLD.TOT_REV_PRF_VIEW AS
SELECT 'RP' || TO_CHAR(DATE, 'YYYYMMDD') AS REV_PRF_ID
     , DATE
     , SUM(TOT_REV) AS TOT_REV
     , SUM(TOT_PRF) AS TOT_PRF
  FROM GOLD.BRCS_REV_PRF_VIEW
 GROUP BY DATE
 ORDER BY DATE;

-- ===============================================================
-- [2] WASTE COST ANALYSIS
-- ---------------------------------------------------------------
-- • Purpose: Calculate waste cost, total waste quantity, and average unit cost.
-- • Data Source: SILVER.DIM_DATE, DIM_STORES, DIM_INGREDIENTS, FACT_WASTE_TRACKING
-- • Key Logic:
--     - CROSS JOIN generates complete combinations (date × store × ingredient).
--     - COALESCE ensures NULL-safe aggregation.
--     - Estimated waste cost = total cost × 1.1 (10% adjustment factor).
-- • Dashboard Use: Waste cost breakdown by branch, category, and ingredient.
-- ===============================================================

-- 2-1. WASTE_COST Analysis(Metrics)
CREATE OR REPLACE VIEW GOLD.WASTE_COST_METRICS_VIEW AS
SELECT D.DATE AS WASTE_DATE
     , S.STORE_ID
     , S.STORE_NAME
     , I.INGREDIENT_ID
     , I.INGREDIENT_NAME
     , I.CATEGORY
-- ====== Core Metrics ======
     , COALESCE(SUM(-F.UNIT_COST * F.QUANTITY_CHANGE), 0)   AS WASTE_COST
     , COALESCE(SUM(-F.QUANTITY_CHANGE), 0)                 AS TOTAL_WASTED
     , COALESCE(SUM(F.UNIT_COST * F.QUANTITY_CHANGE) / NULLIF(SUM(F.QUANTITY_CHANGE), 0), 0)
                                                            AS AVG_COST_PER_UNIT
     , CURRENT_TIMESTAMP()                                  AS PROCESSED_AT
  FROM SILVER.DIM_DATE D
 CROSS JOIN SILVER.DIM_STORES S
 CROSS JOIN SILVER.DIM_INGREDIENTS I
  LEFT JOIN SILVER.FACT_INVENTORY_TRANSACTIONS F
         ON F.TRANSACTION_DATE     = D.DATE
        AND F.STORE_ID             = S.STORE_ID
        AND F.INGREDIENT_ID        = I.INGREDIENT_ID
 WHERE F.TRANSACTION_TYPE = 'waste'
 GROUP BY D.DATE
        , S.STORE_ID
        , S.STORE_NAME
        , I.INGREDIENT_ID
        , I.INGREDIENT_NAME
        , I.CATEGORY
 ORDER BY D.DATE
        , S.STORE_ID
        , I.CATEGORY
        , I.INGREDIENT_ID;


-- 2-2. Derived waste cost views by dimension
-- Waste costs (Daily)
CREATE OR REPLACE VIEW GOLD.WASTE_COSTS_DAILY_VIEW AS
SELECT WASTE_DATE
     , COALESCE(SUM(WASTE_COST), 0) AS TOT_COST
  FROM GOLD.WASTE_COST_METRICS_VIEW
 GROUP BY WASTE_DATE
 ORDER BY WASTE_DATE DESC;
-- Waste costs by ingredient(Daily)
CREATE OR REPLACE VIEW GOLD.WASTE_COSTS_INGREDIENT_DAILY_VIEW AS
SELECT INGREDIENT_ID
     , INGREDIENT_NAME
     , WASTE_DATE
     , COALESCE(SUM(WASTE_COST), 0) AS TOT_COST
  FROM GOLD.WASTE_COST_METRICS_VIEW
 GROUP BY INGREDIENT_ID
        , INGREDIENT_NAME
        , WASTE_DATE
 ORDER BY WASTE_DATE
        , INGREDIENT_ID; 
-- Waste costs by category(Daily)
CREATE OR REPLACE VIEW GOLD.WASTE_COSTS_CATEGORY_DAILY_VIEW AS
SELECT CATEGORY
     , WASTE_DATE
     , COALESCE(SUM(WASTE_COST), 0) AS TOT_COST
  FROM GOLD.WASTE_COST_METRICS_VIEW
 GROUP BY CATEGORY
        , WASTE_DATE
 ORDER BY WASTE_DATE
        , CATEGORY;
-- Waste costs by branches(Daily)
CREATE OR REPLACE VIEW GOLD.WASTE_COSTS_BRANCH_DAILY_VIEW AS
SELECT STORE_ID
     , STORE_NAME
     , WASTE_DATE
     , COALESCE(SUM(WASTE_COST), 0) AS TOT_COST
  FROM GOLD.WASTE_COST_METRICS_VIEW
 GROUP BY STORE_ID
        , STORE_NAME
        , WASTE_DATE
 ORDER BY WASTE_DATE
        , STORE_ID;


-- ===============================================================
-- [3] EXPIRY COST ANALYSIS
-- ---------------------------------------------------------------
-- • Purpose: Identify ingredients nearing expiry and estimate related costs.
-- • Data Source: DIM_DATE, DIM_STORES, DIM_INGREDIENTS, DIM_BATCHES
-- • Key Logic:
--     - Calculates expiring cost within (expiry_date - countdown_days) to expiry_date.
--     - Includes remaining quantity and batch count near expiry.
-- • Dashboard Use: “Expiring Cost Trend” and “Top Ingredients Near Expiry”.
-- ===============================================================
CREATE OR REPLACE VIEW GOLD.EXPIRY_COST_METRICS_VIEW AS
SELECT D.DATE AS SNAPSHOT_DATE
     , S.STORE_ID
     , S.STORE_NAME
     , I.INGREDIENT_ID
     , I.INGREDIENT_NAME
     , I.CATEGORY
-- ====== Core Metrics ======
-- Expiring Costs: Costs of the expiring ingredient
-- (Expiry_date-Countdowndays ~ Expiry_date)
     , COALESCE(SUM(CASE WHEN D.DATE BETWEEN DATEADD(DAY, -I.EXPIRY_COUNTDOWN_DAYS, B.EXPIRY_DATE)
                                        AND B.EXPIRY_DATE THEN B.QUANTITY_REMAINING * B.COST_PER_UNIT ELSE 0 END), 0)  AS EXPIRING_COST
     , COALESCE(SUM(CASE WHEN D.DATE BETWEEN DATEADD(DAY, -I.EXPIRY_COUNTDOWN_DAYS, B.EXPIRY_DATE)
                                        AND B.EXPIRY_DATE THEN B.QUANTITY_REMAINING ELSE 0 END), 0)                    AS REMAINING_QUANTITY
     , COALESCE(SUM(CASE WHEN D.DATE BETWEEN DATEADD(DAY, -I.EXPIRY_COUNTDOWN_DAYS, B.EXPIRY_DATE)
                                        AND B.EXPIRY_DATE THEN 1 ELSE 0 END), 0)                                       AS BATCH_COUNT_NEAR_EXPIRY
     , CURRENT_TIMESTAMP()                                                                                             AS PROCESSED_AT
  FROM SILVER.DIM_DATE D
 CROSS JOIN SILVER.DIM_STORES S
 CROSS JOIN SILVER.DIM_INGREDIENTS I
  LEFT JOIN SILVER.DIM_BATCHES B
         ON B.STORE_ID      = S.STORE_ID
        AND B.INGREDIENT_ID = I.INGREDIENT_ID
        AND D.DATE BETWEEN DATEADD(DAY, -I.EXPIRY_COUNTDOWN_DAYS, B.EXPIRY_DATE)
                       AND B.EXPIRY_DATE
 GROUP BY D.DATE
        , S.STORE_ID
        , S.STORE_NAME
        , I.INGREDIENT_ID
        , I.INGREDIENT_NAME
        , I.CATEGORY
 ORDER BY D.DATE
        , S.STORE_ID
        , I.CATEGORY
        , I.INGREDIENT_ID;

-- ===============================================================
-- [4] STOCK LEVEL ANALYSIS
-- ---------------------------------------------------------------
-- • Purpose: Evaluate stock level health per ingredient and store.
-- • Data Source: DIM_BATCHES, DIM_STORES, DIM_INGREDIENTS
-- • Key Logic:
--     - Excludes expired batches.
--     - Defines stock status (LOW / OPTIMAL / OVER) based on min/max thresholds.
-- • Dashboard Use: “Stock Level Overview” table for inventory management.
-- ===============================================================

CREATE OR REPLACE VIEW GOLD.STOCK_LEVEL_METRICS_VIEW AS
SELECT DS.STORE_ID
     , DS.STORE_NAME
     , DI.INGREDIENT_ID
     , DI.INGREDIENT_NAME
     , DI.UNIT_OF_MEASURE
     , DI.CATEGORY
     , SUM(DB.QUANTITY_REMAINING)                       AS TOTAL_QUANTITY_ON_HAND
     , DI.MINIMUM_STOCK_LEVEL
     , DI.MAXIMUM_STOCK_LEVEL
     , CASE
            WHEN SUM(DB.QUANTITY_REMAINING) < DI.MINIMUM_STOCK_LEVEL THEN 'LOW_STOCK'
            WHEN SUM(DB.QUANTITY_REMAINING) > DI.MAXIMUM_STOCK_LEVEL THEN 'OVER_STOCK'
            ELSE 'OPTIMAL'
        END                                             AS STOCK_STATUS
     , SUM(DB.QUANTITY_REMAINING * DB.COST_PER_UNIT)    AS INVENTORY_VALUE
     , COUNT(DISTINCT DB.BATCH_ID)                      AS BATCH_COUNT
  FROM SILVER.DIM_BATCHES DB
  LEFT JOIN SILVER.DIM_STORES DS 
         ON DB.STORE_ID = DS.STORE_ID
  LEFT JOIN SILVER.DIM_INGREDIENTS DI 
         ON DB.INGREDIENT_ID = DI.INGREDIENT_ID
 WHERE DB.STATUS != 'EXPIRED'   -- Exclude Expired Ingredient
 GROUP BY DS.STORE_ID
        , DS.STORE_NAME
        , DI.INGREDIENT_ID
        , DI.INGREDIENT_NAME
        , DI.UNIT_OF_MEASURE
        , DI.CATEGORY
        , DI.MINIMUM_STOCK_LEVEL
        , DI.MAXIMUM_STOCK_LEVEL;
