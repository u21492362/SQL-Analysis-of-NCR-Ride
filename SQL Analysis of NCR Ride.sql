-- ==========================================
-- Assignment 3: SQL Analysis of NCR Ride Bookings
-- Full SQL Server Script (Corrected)
-- ==========================================

-- ===========================
-- Phase 1: Data Collection
-- ===========================

-- Step 1: Create staging table (all NVARCHAR to safely load CSV)
CREATE TABLE bookings_raw_str (
    Date NVARCHAR(50),
    Time NVARCHAR(50),
    Booking_ID NVARCHAR(50),
    Booking_Status NVARCHAR(50),
    Customer_ID NVARCHAR(50),
    Vehicle_Type NVARCHAR(50),
    Pickup_Location NVARCHAR(100),
    Drop_Location NVARCHAR(100),
    Avg_VTAT NVARCHAR(50),
    Avg_CTAT NVARCHAR(50),
    Cancelled_Rides_by_Customer NVARCHAR(50),
    Reason_for_cancelling_by_Customer NVARCHAR(255),
    Cancelled_Rides_by_Driver NVARCHAR(50),
    Driver_Cancellation_Reason NVARCHAR(255),
    Incomplete_Rides NVARCHAR(50),
    Incomplete_Rides_Reason NVARCHAR(255),
    Booking_Value NVARCHAR(50),
    Ride_Distance NVARCHAR(50),
    Driver_Ratings NVARCHAR(50),
    Customer_Rating NVARCHAR(50),
    Payment_Method NVARCHAR(50)
);

-- Step 2: Load CSV into staging table
BULK INSERT bookings_raw_str
FROM 'C:\Users\User\Desktop\Masters\Work\ZAIO\Assignment\Assignment 3\NCR_Ride_bookings.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    CODEPAGE = '65001'
);

-- Step 3: Create proper typed raw table
CREATE TABLE bookings_raw (
    Date NVARCHAR(50),
    Time NVARCHAR(50),
    Booking_ID NVARCHAR(50),
    Booking_Status NVARCHAR(50),
    Customer_ID NVARCHAR(50),
    Vehicle_Type NVARCHAR(50),
    Pickup_Location NVARCHAR(100),
    Drop_Location NVARCHAR(100),
    Avg_VTAT FLOAT,
    Avg_CTAT FLOAT,
    Cancelled_Rides_by_Customer INT,
    Reason_for_cancelling_by_Customer NVARCHAR(255),
    Cancelled_Rides_by_Driver INT,
    Driver_Cancellation_Reason NVARCHAR(255),
    Incomplete_Rides INT,
    Incomplete_Rides_Reason NVARCHAR(255),
    Booking_Value FLOAT,
    Ride_Distance FLOAT,
    Driver_Ratings FLOAT,
    Customer_Rating FLOAT,
    Payment_Method NVARCHAR(50)
);

-- Step 4: Insert into typed table using TRY_CAST for nulls
INSERT INTO bookings_raw
SELECT
    Date,
    Time,
    Booking_ID,
    Booking_Status,
    Customer_ID,
    Vehicle_Type,
    Pickup_Location,
    Drop_Location,
    TRY_CAST(NULLIF(Avg_VTAT,'null') AS FLOAT),
    TRY_CAST(NULLIF(Avg_CTAT,'null') AS FLOAT),
    TRY_CAST(NULLIF(Cancelled_Rides_by_Customer,'null') AS INT),
    Reason_for_cancelling_by_Customer,
    TRY_CAST(NULLIF(Cancelled_Rides_by_Driver,'null') AS INT),
    Driver_Cancellation_Reason,
    TRY_CAST(NULLIF(Incomplete_Rides,'null') AS INT),
    Incomplete_Rides_Reason,
    TRY_CAST(NULLIF(Booking_Value,'null') AS FLOAT),
    TRY_CAST(NULLIF(Ride_Distance,'null') AS FLOAT),
    TRY_CAST(NULLIF(Driver_Ratings,'null') AS FLOAT),
    TRY_CAST(NULLIF(Customer_Rating,'null') AS FLOAT),
    Payment_Method
FROM bookings_raw_str;

-- Step 5: Missing value percentage per column
SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN Booking_ID IS NULL THEN 1 ELSE 0 END) *100.0/COUNT(*) AS Booking_ID_missing_pct,
    SUM(CASE WHEN Date IS NULL THEN 1 ELSE 0 END) *100.0/COUNT(*) AS Date_missing_pct,
    SUM(CASE WHEN Time IS NULL THEN 1 ELSE 0 END) *100.0/COUNT(*) AS Time_missing_pct,
    SUM(CASE WHEN Booking_Status IS NULL THEN 1 ELSE 0 END) *100.0/COUNT(*) AS Booking_Status_missing_pct,
    SUM(CASE WHEN Booking_Value IS NULL THEN 1 ELSE 0 END) *100.0/COUNT(*) AS Booking_Value_missing_pct,
    SUM(CASE WHEN Pickup_Location IS NULL THEN 1 ELSE 0 END) *100.0/COUNT(*) AS Pickup_Location_missing_pct,
    SUM(CASE WHEN Drop_Location IS NULL THEN 1 ELSE 0 END) *100.0/COUNT(*) AS Drop_Location_missing_pct
FROM bookings_raw;

-- Step 6: Create cleaned table with critical fields not NULL
SELECT *
INTO bookings
FROM bookings_raw
WHERE Booking_ID IS NOT NULL
  AND Date IS NOT NULL
  AND Time IS NOT NULL
  AND Booking_Status IS NOT NULL
  AND Booking_Value IS NOT NULL
  AND Pickup_Location IS NOT NULL
  AND Drop_Location IS NOT NULL;

-- Show row counts
SELECT 
    (SELECT COUNT(*) FROM bookings_raw) AS raw_count,
    (SELECT COUNT(*) FROM bookings) AS cleaned_count;

-- ===========================
-- Phase 2: Data Preparation
-- ===========================

-- Remove duplicate Booking_ID keeping earliest Date + Time
;WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY Booking_ID ORDER BY Date, Time) AS rn
    FROM bookings
)
SELECT *
INTO bookings_dedup
FROM ranked
WHERE rn = 1;

-- Count of duplicates removed
SELECT COUNT(*) - (SELECT COUNT(*) FROM bookings_dedup) AS duplicates_removed
FROM bookings;

-- Feature engineering
SELECT *,
       CAST(Date + ' ' + Time AS DATETIME) AS pickup_ts,
       DATENAME(WEEKDAY, CAST(Date + ' ' + Time AS DATETIME)) AS Day_Of_Week,
       DATEPART(HOUR, CAST(Date + ' ' + Time AS DATETIME)) AS Hour_Of_Day,
       Pickup_Location + ' -> ' + Drop_Location AS Route,
       UPPER(LTRIM(RTRIM(Payment_Method))) AS Payment_Method_Norm
INTO bookings_clean
FROM bookings_dedup;

-- ===========================
-- Phase 3: Data Exploration
-- ===========================

-- 1. Booking_Value buckets
SELECT 
    CASE
        WHEN Booking_Value < 100 THEN '<100'
        WHEN Booking_Value >= 100 AND Booking_Value < 200 THEN '100-199.99'
        WHEN Booking_Value >= 200 AND Booking_Value < 300 THEN '200-299.99'
        ELSE '>=300'
    END AS fare_bucket,
    COUNT(*) AS count,
    CAST(COUNT(*)*100.0/(SELECT COUNT(*) FROM bookings_clean) AS DECIMAL(5,2)) AS pct
FROM bookings_clean
GROUP BY CASE
            WHEN Booking_Value < 100 THEN '<100'
            WHEN Booking_Value >= 100 AND Booking_Value < 200 THEN '100-199.99'
            WHEN Booking_Value >= 200 AND Booking_Value < 300 THEN '200-299.99'
            ELSE '>=300'
         END
ORDER BY fare_bucket;

-- 2. Top 10 Vehicle Type x Booking Status
SELECT TOP 10 Vehicle_Type, Booking_Status,
       COUNT(*) AS cnt,
       CAST(COUNT(*)*100.0/(SELECT COUNT(*) FROM bookings_clean) AS DECIMAL(5,2)) AS pct_total
FROM bookings_clean
GROUP BY Vehicle_Type, Booking_Status
ORDER BY cnt DESC;

-- 3. Ride Distance vs Booking Value components
SELECT
    COUNT(*) AS n,
    SUM(Ride_Distance) AS sum_x,
    SUM(Booking_Value) AS sum_y,
    SUM(Ride_Distance*Booking_Value) AS sum_xy,
    SUM(POWER(Ride_Distance,2)) AS sum_x2,
    SUM(POWER(Booking_Value,2)) AS sum_y2
FROM bookings_clean;

-- 4. Booking Value by Payment Method (approx percentiles using NTILE)
-- Booking Value percentiles by Payment Method
;WITH pct AS (
    SELECT *,
           PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY Booking_Value) OVER (PARTITION BY Payment_Method_Norm) AS Q1,
           PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY Booking_Value) OVER (PARTITION BY Payment_Method_Norm) AS median,
           PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY Booking_Value) OVER (PARTITION BY Payment_Method_Norm) AS Q3
    FROM bookings_clean
)
SELECT Payment_Method_Norm,
       MIN(Booking_Value) AS min_val,
       MAX(Booking_Value) AS max_val,
       AVG(Q1) AS Q1,
       AVG(median) AS median,
       AVG(Q3) AS Q3
FROM pct
GROUP BY Payment_Method_Norm
ORDER BY median DESC;


-- ===========================
-- Phase 4: Statistical Analysis
-- ===========================

-- 1. Descriptive stats
SELECT 
    AVG(Booking_Value) AS mean_booking_value,
    STDEV(Booking_Value) AS stddev_booking_value,
    MIN(Booking_Value) AS min_booking_value,
    MAX(Booking_Value) AS max_booking_value,
    AVG(Ride_Distance) AS mean_distance,
    STDEV(Ride_Distance) AS stddev_distance
FROM bookings_clean;

-- 2. IQR outlier detection for Booking_Value
;WITH stats AS (
    SELECT 
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY Booking_Value) OVER () AS Q1,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY Booking_Value) OVER () AS Q3
    FROM bookings_clean
)
SELECT TOP 20 Booking_ID, Route, Booking_Value
FROM bookings_clean, stats
WHERE Booking_Value > Q3 + 1.5*(Q3-Q1)
ORDER BY Booking_Value DESC;

-- ===========================
-- Phase 5: Advanced Analysis
-- ===========================

-- 1. Completion rate by Vehicle_Type
SELECT Vehicle_Type,
       CAST(SUM(CASE WHEN Booking_Status='Completed' THEN 1 ELSE 0 END) AS FLOAT) /
       SUM(CASE WHEN Booking_Status IN ('Completed','Cancelled','Incomplete') THEN 1 ELSE 0 END) AS completion_rate
FROM bookings_clean
GROUP BY Vehicle_Type
ORDER BY completion_rate DESC;

-- 2. Top 10 Routes by total Booking_Value
SELECT TOP 10 Route,
       SUM(Booking_Value) AS total_value,
       AVG(Booking_Value) AS avg_value,
       COUNT(*) AS ride_count
FROM bookings_clean
GROUP BY Route
ORDER BY total_value DESC;

-- 3. Top 5 cancellation reasons - Customer
SELECT TOP 5 COALESCE(NULLIF(Reason_for_cancelling_by_Customer,''),'Unspecified') AS cancel_reason,
       COUNT(*) AS cnt,
       CAST(COUNT(*)*100.0/(SELECT COUNT(*) FROM bookings_clean WHERE Booking_Status='Cancelled') AS DECIMAL(5,2)) AS pct
FROM bookings_clean
WHERE Booking_Status='Cancelled'
GROUP BY COALESCE(NULLIF(Reason_for_cancelling_by_Customer,''),'Unspecified')
ORDER BY cnt DESC;

-- Top 5 cancellation reasons - Driver
SELECT TOP 5 COALESCE(NULLIF(Driver_Cancellation_Reason,''),'Unspecified') AS cancel_reason,
       COUNT(*) AS cnt,
       CAST(COUNT(*)*100.0/(SELECT COUNT(*) FROM bookings_clean WHERE Booking_Status='Cancelled') AS DECIMAL(5,2)) AS pct
FROM bookings_clean
WHERE Booking_Status='Cancelled'
GROUP BY COALESCE(NULLIF(Driver_Cancellation_Reason,''),'Unspecified')
ORDER BY cnt DESC;

-- 4. Service levels by hour
SELECT TOP 3 Hour_Of_Day,
       AVG(TRY_CAST(Avg_VTAT AS FLOAT)) AS avg_vtat,
       AVG(TRY_CAST(Avg_CTAT AS FLOAT)) AS avg_ctat,
       COUNT(*) AS ride_count
FROM bookings_clean
GROUP BY Hour_Of_Day
ORDER BY ride_count DESC;

-- ===========================
-- End of Script
-- ===========================
