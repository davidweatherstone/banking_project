{{ 
    config(
        materialized='table'
    )
}}

with

DateSpine as(
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast(CURRENT_DATE + INTERVAL '2 years' as date)"
    )
    }}
)

SELECT 
    DATE_DAY AS TheDate,
    DATE_TRUNC('month', DATE_DAY) + INTERVAL '1 month' - INTERVAL '1 day' AS TheEndOfTheMonth,
    DATE_TRUNC('month', DATE_DAY) AS TheStartOfTheMonth,
    EXTRACT(DAY FROM DATE_DAY) AS TheDayOfTheMonth,
    EXTRACT(DOW FROM DATE_DAY) + 1 AS TheDayOfTheWeek, -- PostgreSQL starts with Sunday=0
    EXTRACT(WEEK FROM DATE_DAY) AS TheWeekOfTheYear,
    EXTRACT(YEAR FROM DATE_DAY) AS TheYear,
    EXTRACT(MONTH FROM DATE_DAY) AS TheMonth,
    EXTRACT(QUARTER FROM DATE_DAY) AS TheQuarter,
    MAKE_DATE(EXTRACT(YEAR FROM DATE_DAY)::int, 1, 1) AS TheStartOfTheYear,
    MAKE_DATE(EXTRACT(YEAR FROM DATE_DAY)::int, 12, 31) AS TheEndOfTheYear,

    -- Sorting Columns for Power BI
    TO_CHAR(DATE_DAY, 'YYYYMM') AS YYMM, -- Useful for sorting Year-Month
    TO_CHAR(DATE_DAY, 'YYYY-"Q"Q') AS YearQuarter, -- e.g., "2024-Q1"
    TO_CHAR(DATE_DAY, 'IYYY-"W"IW') AS YearWeek, -- ISO Year-Week

    -- Fiscal Year Columns (assuming fiscal year starts in April)
    CASE 
        WHEN EXTRACT(MONTH FROM DATE_DAY) >= 4 THEN EXTRACT(YEAR FROM DATE_DAY)
        ELSE EXTRACT(YEAR FROM DATE_DAY) - 1
    END AS FiscalYear,
    
    CASE 
        WHEN EXTRACT(MONTH FROM DATE_DAY) >= 4 THEN EXTRACT(MONTH FROM DATE_DAY) - 3
        ELSE EXTRACT(MONTH FROM DATE_DAY) + 9
    END AS FiscalMonth,

    CONCAT(
        CASE 
            WHEN EXTRACT(MONTH FROM DATE_DAY) >= 4 THEN EXTRACT(YEAR FROM DATE_DAY)
            ELSE EXTRACT(YEAR FROM DATE_DAY) - 1
        END, 
        LPAD(CAST(
            CASE 
                WHEN EXTRACT(MONTH FROM DATE_DAY) >= 4 THEN EXTRACT(MONTH FROM DATE_DAY) - 3
                ELSE EXTRACT(MONTH FROM DATE_DAY) + 9
            END AS TEXT), 2, '0')
    ) AS FiscalYYMM, -- Fiscal sorting
    
    -- Day of the Week Name
    TO_CHAR(DATE_DAY, 'DY') AS DayShort,
    TO_CHAR(DATE_DAY, 'FMDay') AS DayLong, -- FM removes leading spaces

    -- Workday Indicator (Assuming weekends are non-workdays)
    CASE 
        WHEN EXTRACT(DOW FROM DATE_DAY) IN (0, 6) THEN 'No' 
        ELSE 'Yes' 
    END AS IsWorkday

FROM DateSpine