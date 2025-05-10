WITH cleaned_data AS (
    SELECT DISTINCT
        user_id,
        timestamp,
        cumulative_steps,
        heart_rate,
        calories_burned,
        activity_type
    FROM fitness_tracker_dataset_dirty
    WHERE 
        heart_rate IS NOT NULL 
        AND calories_burned IS NOT NULL
),

user_medians AS (
    WITH ranked_heart_rates AS (
        SELECT 
            user_id,
            heart_rate,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY heart_rate) AS row_num,
            COUNT(*) OVER (PARTITION BY user_id) AS total_count
        FROM cleaned_data
    )
    SELECT 
        user_id,
        AVG(heart_rate) AS median_hr
    FROM ranked_heart_rates
    WHERE row_num IN (
        (total_count + 1) / 2,
        (total_count + 2) / 2
    )
    GROUP BY user_id
),


corrected_data AS (
    SELECT 
        c.user_id,
        c.timestamp,
        c.cumulative_steps,
        CASE 
            WHEN c.heart_rate < 40 OR c.heart_rate > 200 THEN m.median_hr
            ELSE c.heart_rate
        END AS heart_rate,
        c.calories_burned,
        c.activity_type
    FROM cleaned_data c
    JOIN user_medians m ON c.user_id = m.user_id
),

timestamp_table AS (
    SELECT
        user_id,
        timestamp,
        DATE(timestamp) AS date,
        CAST(STRFTIME('%H', timestamp) AS INTEGER) AS hour,
        heart_rate,
        calories_burned,
        cumulative_steps,
        activity_type
    FROM corrected_data
),

steps_calculated AS (
    SELECT 
        user_id,
        timestamp,
        date,
        hour,
        heart_rate,
        calories_burned,
        cumulative_steps,
        activity_type,
        CASE
            WHEN LAG(timestamp) OVER (PARTITION BY user_id, date ORDER BY timestamp) IS NULL
            THEN 0
            ELSE cumulative_steps - LAG(cumulative_steps) OVER (PARTITION BY user_id, date ORDER BY timestamp)
        END AS steps_this_hour
    FROM timestamp_table
)

SELECT 
    user_id,
    timestamp,
    cumulative_steps,
    ROUND(heart_rate, 2) AS heart_rate, 
    ROUND(calories_burned,2) AS calories_burned, 
    activity_type,
    date,
    hour,
    CASE 
        WHEN steps_this_hour IS NULL THEN 0  
        ELSE steps_this_hour
    END AS steps_this_hour,
    CASE  
        WHEN steps_this_hour IS NULL THEN 'low'
        WHEN steps_this_hour < 100 THEN 'low'
        WHEN steps_this_hour >= 1000 THEN 'high'
        ELSE 'medium'
    END AS activity_intensity
FROM steps_calculated
ORDER BY user_id, timestamp;
