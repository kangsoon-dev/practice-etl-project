/* Number of delivery jobs per day, peak delivery hours throughout a day, Past-30-day trend */

/* Top 5 peak hours in the day */
SELECT COUNT(job_id) as num_legs, date_trunc('hour',arrive_time) as hour, date_trunc('day',arrive_time) as sdate
FROM legs
WHERE date_trunc('day',arrive_time) >= CURRENT_DATE - 30
GROUP BY sdate, hour
ORDER BY num_legs DESC
LIMIT 5;

/*number of jobs in past 30 days*/
SELECT DATE_TRUNC('day', start_time) as sdate, COUNT(1) as jobs_per_day
FROM jobs
WHERE sdate >= CURRENT_DATE - 30
GROUP BY sdate
ORDER BY sdate;