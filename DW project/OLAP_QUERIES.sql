USE Metro_DW;

-- Query 1:
SELECT 
    P.PRODUCT_NAME,                       -- Product Name
    SUM(T.TOTAL_SALES) AS REVENUE,        -- Total Revenue
    MONTH(T.ORDER_DATE) AS MONTH,         -- Month of the Order
    CASE 
        WHEN DAYOFWEEK(T.ORDER_DATE) IN (1, 7) THEN 'Weekend'   -- 1 = Sunday, 7 = Saturday
        ELSE 'Weekday'                      -- Otherwise, it's a weekday
    END AS SALES_TYPE,                    -- Sales Type (Weekday or Weekend)
    YEAR(T.ORDER_DATE) AS YEAR             -- Year of the Order (2019)
FROM 
    FACT_TRANSACTION_OUTPUT T
JOIN 
    PRODUCTS_DIM P ON T.PRODUCT_ID = P.PRODUCT_ID    -- Join with Products Dimension
WHERE 
    YEAR(T.ORDER_DATE) = 2019               -- Filter for the year 2019
GROUP BY 
    P.PRODUCT_NAME, MONTH(T.ORDER_DATE), SALES_TYPE, YEAR(T.ORDER_DATE)   -- Group by product, month, and sales type
ORDER BY 
    REVENUE DESC                             -- Sort by revenue, descending order
LIMIT 5;                                    -- Limit to the top 5 products
-- --------------------------------------------------------------------------------------------------------------------


-- Query 2:

WITH QuarterlyRevenue AS (
    -- Calculate the total revenue per store per quarter for 2019
    SELECT 
        STORE_ID, 
        STORE_NAME, 
        YEAR(ORDER_DATE) AS REVENUE_YEAR, 
        QUARTER(ORDER_DATE) AS REVENUE_QUARTER, 
        ROUND(SUM(TOTAL_SALES), 0) AS QUARTERLY_REVENUE  -- Round the quarterly revenue to nearest integer
    FROM 
        FACT_TRANSACTION_OUTPUT
    WHERE 
        YEAR(ORDER_DATE) = 2019  -- Filter for the year 2019
    GROUP BY 
        STORE_ID, STORE_NAME, YEAR(ORDER_DATE), QUARTER(ORDER_DATE)
)
SELECT 
    SR.STORE_ID, 
    SR.STORE_NAME, 
    SR.REVENUE_YEAR, 
    SR.REVENUE_QUARTER, 
    SR.QUARTERLY_REVENUE AS CURRENT_QUARTER_REVENUE,  -- Current quarter revenue (already rounded)
    ROUND(LAG(SR.QUARTERLY_REVENUE) OVER (PARTITION BY SR.STORE_ID ORDER BY SR.REVENUE_QUARTER), 0) AS PREVIOUS_QUARTER_REVENUE,  -- Round previous quarter revenue
    -- Calculate revenue growth rate
    CASE 
        WHEN LAG(SR.QUARTERLY_REVENUE) OVER (PARTITION BY SR.STORE_ID ORDER BY SR.REVENUE_QUARTER) IS NOT NULL 
        THEN ROUND(
            (SR.QUARTERLY_REVENUE - LAG(SR.QUARTERLY_REVENUE) OVER (PARTITION BY SR.STORE_ID ORDER BY SR.REVENUE_QUARTER)) 
            / LAG(SR.QUARTERLY_REVENUE) OVER (PARTITION BY SR.STORE_ID ORDER BY SR.REVENUE_QUARTER) * 100, 0)  -- Rounded growth rate to nearest integer
        ELSE NULL
    END AS REVENUE_GROWTH_RATE
FROM 
    QuarterlyRevenue SR
ORDER BY 
    SR.STORE_ID, SR.REVENUE_QUARTER;
    
-- ------------------------------------------------------------------------------------------------------

-- Query 3:

SELECT 
    st.STORE_NAME,                               -- Store name from STORES_DIM
    p.SUPPLIER_NAME,                             -- Supplier name from PRODUCTS_DIM
    p.PRODUCT_NAME,                              -- Product name from PRODUCTS_DIM
    ROUND(SUM(T.TOTAL_SALES), 2) AS TOTAL_SALES  -- Rounded total sales for each store, supplier, and product
FROM 
    FACT_TRANSACTION_OUTPUT T
JOIN 
    PRODUCTS_DIM p ON T.PRODUCT_ID = p.PRODUCT_ID    -- Join with PRODUCTS_DIM to get supplier and product info
JOIN 
    STORES_DIM st ON T.STORE_ID = st.STORE_ID        -- Join with STORES_DIM to get store name
GROUP BY 
    st.STORE_NAME, p.SUPPLIER_NAME, p.PRODUCT_NAME   -- Group by store, supplier, and product name
ORDER BY 
    st.STORE_NAME, p.SUPPLIER_NAME, p.PRODUCT_NAME;  -- Order by store, then supplier, then product name


-- ----------------------------------------------------------------------------------------------------

-- Query 4:

SELECT 
    P.PRODUCT_NAME,                       -- Product Name
    SUM(T.TOTAL_SALES) AS TOTAL_SALES,    -- Total sales for each product
    CASE 
        WHEN MONTH(T.ORDER_DATE) IN (3, 4, 5) THEN 'Spring'  -- March, April, May
        WHEN MONTH(T.ORDER_DATE) IN (6, 7, 8) THEN 'Summer'  -- June, July, August
        WHEN MONTH(T.ORDER_DATE) IN (9, 10, 11) THEN 'Fall'   -- September, October, November
        WHEN MONTH(T.ORDER_DATE) IN (12, 1, 2) THEN 'Winter'  -- December, January, February
    END AS SEASON                       -- Assign season based on the month of the order
FROM 
    FACT_TRANSACTION_OUTPUT T
JOIN 
    PRODUCTS_DIM P ON T.PRODUCT_ID = P.PRODUCT_ID    -- Join with Products Dimension to get product names
GROUP BY 
    P.PRODUCT_NAME, SEASON                -- Group by product and season
ORDER BY 
    SEASON, TOTAL_SALES DESC;             -- Order by season and total sales in descending order


-- ----------------------------------------------------------------------------------------------------

-- Query 5:
WITH MonthlyRevenue AS (
    -- Step 1: Calculate the total monthly revenue per store and supplier
    SELECT 
        T.STORE_ID, 
        st.STORE_NAME, 
        T.SUPPLIER_ID, 
        p.SUPPLIER_NAME, 
        YEAR(T.ORDER_DATE) AS REVENUE_YEAR, 
        MONTH(T.ORDER_DATE) AS REVENUE_MONTH, 
        SUM(T.TOTAL_SALES) AS MONTHLY_REVENUE
    FROM 
        FACT_TRANSACTION_OUTPUT T
    JOIN 
        STORES_DIM st ON T.STORE_ID = st.STORE_ID
    JOIN 
        PRODUCTS_DIM p ON T.PRODUCT_ID = p.PRODUCT_ID
    WHERE 
        YEAR(T.ORDER_DATE) = 2019  -- Filter for the year 2019
    GROUP BY 
        T.STORE_ID, st.STORE_NAME, T.SUPPLIER_ID, p.SUPPLIER_NAME, YEAR(T.ORDER_DATE), MONTH(T.ORDER_DATE)
)
SELECT 
    MR.STORE_ID, 
    MR.STORE_NAME, 
    MR.SUPPLIER_ID, 
    MR.SUPPLIER_NAME, 
    MR.REVENUE_YEAR, 
    MR.REVENUE_MONTH, 
    ROUND(MR.MONTHLY_REVENUE, 2) AS CURRENT_MONTH_REVENUE,  -- Round the current month's revenue to 2 decimal places
    ROUND(LAG(MR.MONTHLY_REVENUE) OVER (PARTITION BY MR.STORE_ID, MR.SUPPLIER_ID ORDER BY MR.REVENUE_YEAR, MR.REVENUE_MONTH), 2) AS PREVIOUS_MONTH_REVENUE,  -- Round the previous month's revenue to 2 decimal places
    -- Step 2: Calculate the revenue volatility (percentage change)
    CASE 
        WHEN LAG(MR.MONTHLY_REVENUE) OVER (PARTITION BY MR.STORE_ID, MR.SUPPLIER_ID ORDER BY MR.REVENUE_YEAR, MR.REVENUE_MONTH) IS NOT NULL 
             AND LAG(MR.MONTHLY_REVENUE) OVER (PARTITION BY MR.STORE_ID, MR.SUPPLIER_ID ORDER BY MR.REVENUE_YEAR, MR.REVENUE_MONTH) != 0 
        THEN ROUND(
            (MR.MONTHLY_REVENUE - LAG(MR.MONTHLY_REVENUE) OVER (PARTITION BY MR.STORE_ID, MR.SUPPLIER_ID ORDER BY MR.REVENUE_YEAR, MR.REVENUE_MONTH)) 
            / LAG(MR.MONTHLY_REVENUE) OVER (PARTITION BY MR.STORE_ID, MR.SUPPLIER_ID ORDER BY MR.REVENUE_YEAR, MR.REVENUE_MONTH) * 100, 2)  -- Round volatility to 2 decimal places
        ELSE NULL
    END AS REVENUE_VOLATILITY_PERCENTAGE
FROM 
    MonthlyRevenue MR
ORDER BY 
    MR.STORE_ID, MR.SUPPLIER_ID, MR.REVENUE_YEAR, MR.REVENUE_MONTH;


    
-- -------------------------------------------------------------------------------------------------------------------------------
-- Query 6:

WITH ProductPairs AS (
    SELECT 
        A.ORDER_ID, 
        A.PRODUCT_ID AS Product1_ID, 
        B.PRODUCT_ID AS Product2_ID, 
        COUNT(*) AS Frequency
    FROM 
        FACT_TRANSACTION_OUTPUT A
    JOIN 
        FACT_TRANSACTION_OUTPUT B
        ON A.ORDER_ID = B.ORDER_ID  -- Same order
        AND A.PRODUCT_ID < B.PRODUCT_ID  -- Avoid self-joins and duplicates
    GROUP BY 
        A.ORDER_ID, A.PRODUCT_ID, B.PRODUCT_ID  -- Group by order and product pair
)
SELECT 
    P1.PRODUCT_NAME AS Product1,          -- First product in the pair
    P2.PRODUCT_NAME AS Product2,          -- Second product in the pair
    PP.Frequency                          -- Frequency of the product pair
FROM 
    ProductPairs PP
JOIN 
    PRODUCTS_DIM P1 ON PP.Product1_ID = P1.PRODUCT_ID   -- Get product names for Product1
JOIN 
    PRODUCTS_DIM P2 ON PP.Product2_ID = P2.PRODUCT_ID   -- Get product names for Product2
ORDER BY 
    PP.Frequency DESC                     -- Sort by frequency in descending order
LIMIT 5;                                  -- Get the top 5 product pairs

-- -----------------------------------------------------------------------------------------------------------------

-- Query 7:

SELECT 
    S.STORE_NAME,                            -- Store name
    P.SUPPLIER_NAME,                         -- Supplier name
    PR.PRODUCT_NAME,                         -- Product name
    YEAR(T.ORDER_DATE) AS REVENUE_YEAR,      -- Revenue year
    SUM(T.TOTAL_SALES) AS YEARLY_REVENUE      -- Total revenue for the given year
FROM 
    FACT_TRANSACTION_OUTPUT T
JOIN 
    STORES_DIM S ON T.STORE_ID = S.STORE_ID        -- Join with STORES_DIM for store name
JOIN 
    SUPPLIER_DIM P ON T.SUPPLIER_ID = P.SUPPLIER_ID  -- Join with SUPPLIER_DIM for supplier name
JOIN 
    PRODUCTS_DIM PR ON T.PRODUCT_ID = PR.PRODUCT_ID  -- Join with PRODUCTS_DIM for product name
GROUP BY 
    S.STORE_NAME, P.SUPPLIER_NAME, PR.PRODUCT_NAME, YEAR(T.ORDER_DATE)  -- Group by store, supplier, product, and year
WITH ROLLUP                                -- Use ROLLUP for cumulative totals
ORDER BY 
    S.STORE_NAME, P.SUPPLIER_NAME, PR.PRODUCT_NAME, YEAR(T.ORDER_DATE);
-- --------------------------------------------------------------------------------------------------------------------------

-- Query 8:
SELECT 
    P.PRODUCT_NAME,  -- Product name from PRODUCTS_DIM
    -- Total revenue and quantity sold in the first half (H1)
    SUM(CASE WHEN MONTH(T.ORDER_DATE) BETWEEN 1 AND 6 THEN T.TOTAL_SALES ELSE 0 END) AS H1_REVENUE,  
    SUM(CASE WHEN MONTH(T.ORDER_DATE) BETWEEN 1 AND 6 THEN T.QUANTITY_ORDERED ELSE 0 END) AS H1_QUANTITY,  
    -- Total revenue and quantity sold in the second half (H2)
    SUM(CASE WHEN MONTH(T.ORDER_DATE) BETWEEN 7 AND 12 THEN T.TOTAL_SALES ELSE 0 END) AS H2_REVENUE, 
    SUM(CASE WHEN MONTH(T.ORDER_DATE) BETWEEN 7 AND 12 THEN T.QUANTITY_ORDERED ELSE 0 END) AS H2_QUANTITY,  
    -- Total revenue and quantity sold for the entire year
    SUM(T.TOTAL_SALES) AS YEARLY_REVENUE,
    SUM(T.QUANTITY_ORDERED) AS YEARLY_QUANTITY  
FROM
    FACT_TRANSACTION_OUTPUT T
JOIN 
    PRODUCTS_DIM P ON T.PRODUCT_ID = P.PRODUCT_ID  -- Join with PRODUCTS_DIM to get product names
WHERE
    YEAR(T.ORDER_DATE) = 2019  -- Filter for the year 2019 (you can change the year as needed)
GROUP BY
    P.PRODUCT_NAME  -- Group by product name to aggregate for each product
ORDER BY
    P.PRODUCT_NAME;  -- Optionally order by product name
-- ---------------------------------------------------------------------------------------------------------------------

-- Query 9:

WITH DailySales AS (
    -- Step 1: Calculate daily sales for each product
    SELECT 
        T.PRODUCT_ID, 
        P.PRODUCT_NAME, 
        T.ORDER_DATE, 
        SUM(T.TOTAL_SALES) AS DAILY_SALES
    FROM 
        FACT_TRANSACTION_OUTPUT T
    JOIN 
        PRODUCTS_DIM P ON T.PRODUCT_ID = P.PRODUCT_ID
    GROUP BY 
        T.PRODUCT_ID, P.PRODUCT_NAME, T.ORDER_DATE
),
DailyAverage AS (
    -- Step 2: Calculate the daily average sales for each product
    SELECT 
        PRODUCT_ID, 
        PRODUCT_NAME, 
        AVG(DAILY_SALES) AS DAILY_AVG_SALES
    FROM 
        DailySales
    GROUP BY 
        PRODUCT_ID, PRODUCT_NAME
)
-- Step 3: Identify outliers (sales exceeding twice the daily average)
SELECT 
    DS.PRODUCT_NAME, 
    DS.ORDER_DATE, 
    DS.DAILY_SALES, 
    DA.DAILY_AVG_SALES, 
    CASE 
        WHEN DS.DAILY_SALES > 2 * DA.DAILY_AVG_SALES THEN 'OUTLIER'  -- Flag as outlier if sales exceed twice the daily average
        ELSE 'NORMAL'  -- Otherwise, mark as normal
    END AS SALES_FLAG
FROM 
    DailySales DS
JOIN 
    DailyAverage DA ON DS.PRODUCT_ID = DA.PRODUCT_ID
ORDER BY 
    DS.PRODUCT_NAME, DS.ORDER_DATE;
    
-- ----------------------------------------------------------------------------------------

-- Query 10:
DROP VIEW IF EXISTS STORE_QUARTERLY_SALES;
CREATE VIEW STORE_QUARTERLY_SALES AS
SELECT 
    S.STORE_NAME,                               -- Store name from STORES_DIM
    YEAR(T.ORDER_DATE) AS YEAR,                  -- Extract the year from ORDER_DATE
    QUARTER(T.ORDER_DATE) AS QUARTER,            -- Extract the quarter from ORDER_DATE
    SUM(T.TOTAL_SALES) AS QUARTERLY_SALES         -- Sum of total sales for each quarter and store
FROM 
    FACT_TRANSACTION_OUTPUT T
JOIN 
    STORES_DIM S ON T.STORE_ID = S.STORE_ID      -- Join with the STORES_DIM table to get store name
GROUP BY 
    S.STORE_NAME, YEAR(T.ORDER_DATE), QUARTER(T.ORDER_DATE)  -- Group by store, year, and quarter
ORDER BY 
    S.STORE_NAME, YEAR(T.ORDER_DATE), QUARTER(T.ORDER_DATE);  -- Order by store name, year, and quarter

SELECT * FROM STORE_QUARTERLY_SALES;

-- ---------------------------------------------------------------------------------------------------------







