--Question 1

SELECT DISTINCT animal_type, 
       COUNT(*) OVER (PARTITION BY animal_type) AS type_count
FROM animal_data;

--Question 2
WITH MultipleOutcomes AS (
    SELECT animal_id
    FROM outcome_events
    GROUP BY animal_id
    HAVING COUNT(*) > 1
)

SELECT COUNT(*) AS "More than 1 Outcome"
FROM MultipleOutcomes;

--Question 3
SELECT TO_CHAR(datetime, 'Month') AS month_name,
       COUNT(*) AS month_count
FROM outcome_events
GROUP BY TO_CHAR(datetime, 'Month')
ORDER BY month_count DESC
LIMIT 5;

--Question 4
SELECT TO_CHAR(datetime, 'Month') AS month_name,
       COUNT(*) AS month_count
FROM outcome_events
GROUP BY TO_CHAR(datetime, 'Month')
ORDER BY month_count DESC
LIMIT 5;

-- Question 5
SELECT
    date(datetime) AS "date",
    COUNT(*) AS "dailyOutcomes",
    SUM(COUNT(*)) OVER (ORDER BY date(datetime) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "Cumulative Total Outcomes"
FROM outcome_events
GROUP BY date(datetime)
ORDER BY date(datetime);
