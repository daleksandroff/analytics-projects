{{
    config(
        materialized='table',
        dist="month",
        sort="month"
    )
}}

WITH journeys AS (
	SELECT DISTINCT 
		   passenger_id
		 , DATE_TRUNC('month', fpj.passenger_first_completed_ts_loc)::DATE AS first_completed_month_loc
		 , DATE_TRUNC('month', fpj.completed_dt_loc)::DATE AS completed_month_loc
	  FROM {{ ref('f_passengers_journeys') }} AS fpj
	WHERE 1=1
	  AND fpj.completed_dt_loc < DATE_TRUNC('month', CURRENT_DATE)::DATE

)

-- Calculate statistics for each Passenger, Monthly
, passengers_stats AS (
	SELECT passenger_id
		 , completed_month_loc AS month
		 , LAG(completed_month_loc) OVER (PARTITION BY passenger_id ORDER BY completed_month_loc) AS prev_completed_month_loc
		 , LEAD(completed_month_loc) OVER (PARTITION BY passenger_id ORDER BY completed_month_loc) AS next_completed_month_loc
		 , CASE WHEN first_completed_month_loc = completed_month_loc THEN TRUE ELSE FALSE
		   END AS is_new
		 , CASE WHEN DATEDIFF('month', prev_completed_month_loc, completed_month_loc) = 1 
		 		THEN TRUE ELSE FALSE
		   END is_retained
		 , CASE WHEN DATEDIFF('month', prev_completed_month_loc, completed_month_loc) > 1 
		 		THEN TRUE ELSE FALSE
		   END is_reactivated
		 /* Mark passengers who has no completed journeys in next month */
		 , CASE WHEN next_completed_month_loc IS NULL THEN TRUE
		 		WHEN (DATEDIFF('month', completed_month_loc, next_completed_month_loc) <> 1) THEN TRUE
		 		ELSE FALSE
		   END is_churned_next_month
	  FROM journeys
)

-- Create additional rows for churned passengers with month when churned
, churned_stats AS (
	SELECT passenger_id
		 , DATEADD('month', 1, month)::DATE AS month
		 , is_churned_next_month AS is_churned
	  FROM passengers_stats
	 WHERE is_churned_next_month = TRUE
)

-- Combine statistics about New / Retained / Reactivated passengers with Churned
, stats_union AS (
	SELECT passenger_id
		 , month
		 , is_new
		 , is_retained
		 , is_reactivated
		 , FALSE AS is_churned
	  FROM passengers_stats
	  
	UNION 
	
	SELECT passenger_id
		 , month
		 , FALSE AS is_new
		 , FALSE AS is_retained
		 , FALSE AS is_reactivated
		 , is_churned
	FROM churned_stats
)

-- Calculate aggregated statistics for each City, Monthly
SELECT u.country_code AS country 
	 , u.city
	 , u.last_attribution_channel AS attribution_channel
	 , s.month
	 , COUNT(DISTINCT CASE WHEN is_new = TRUE THEN s.passenger_id END) AS new
	 , COUNT(DISTINCT CASE WHEN is_retained = TRUE THEN s.passenger_id END) AS retained
	 , COUNT(DISTINCT CASE WHEN is_reactivated = TRUE THEN s.passenger_id END) AS reactivated
	 , COUNT(DISTINCT CASE WHEN is_churned = TRUE THEN s.passenger_id END) AS churned
FROM stats_union AS s
INNER JOIN {{ ref('dim_users') }} AS u
ON s.passenger_id = u.user_id
-- exclude current incomplete month
WHERE s.month < DATE_TRUNC('month', CURRENT_DATE)::DATE
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 4, 3