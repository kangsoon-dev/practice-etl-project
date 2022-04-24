/*Median driving speed of each truck driver in all jobs handled by him / her 
  (driving speed = coordinate change between 2 consecutive legs, between starting point and leg_1, 
  or between the last leg and ending point, divided by time)
  Speed drivers (anyone who drives faster than 80km/h in any segment) */

WITH leg_stats AS (
    -- get speed for start to leg_1
    SELECT l.job_id,
        j.driver_id,
        1 as end_leg_id,
        SQRT(POW(start_xcoord - l.xcoord,2) + POW(start_ycoord - l.ycoord,2)) AS dist, 
        l.arrive_time - j.start_time AS travel_time
    FROM legs l
    INNER JOIN jobs j
    ON l.job_id = j.job_id
    WHERE l.leg_id = 1

    UNION ALL

    -- get speed for all intermediate legs
    SELECT job_id, driver_id, end_leg_id,dist,travel_time FROM (
        SELECT l.job_id, 
            j.driver_id,
            l.leg_id as end_leg_id,
            SQRT(POW(LAG(l.xcoord,1) OVER(PARTITION BY l.job_id ORDER BY l.leg_id) - l.xcoord,2) + POW(LAG(l.ycoord,1) OVER(PARTITION BY l.job_id ORDER BY l.leg_id) - l.ycoord,2)) AS dist, 
            l.arrive_time - LAG(l.leave_time,1) OVER(PARTITION BY l.job_id ORDER BY l.leg_id) AS travel_time
        FROM legs l
        INNER JOIN jobs j
        ON l.job_id = j.job_id
        
    ) t2
    WHERE end_leg_id != 1

    UNION ALL

    -- get speed for last leg to end
    SELECT job_id, driver_id, 999 AS end_leg_id, dist, travel_time FROM (
        SELECT l.job_id,
            j.driver_id,
            l.leg_id as end_leg_id,
            lead(l.leg_id,1) OVER(PARTITION BY l.job_id ORDER BY l.leg_id) AS lead_leg_id,
            SQRT(POW(l.xcoord-end_xcoord,2) + POW(l.ycoord-end_ycoord,2)) AS dist, 
            j.end_time - l.leave_time AS travel_time
        FROM legs l
        INNER JOIN jobs j
        ON l.job_id = j.job_id
    ) t3
    ORDER BY job_id,end_leg_id
)
SELECT driver_id, speed FROM (
       SELECT driver_id, dist/(DATE_PART('day',travel_time)*24 + DATE_PART('hour',travel_time) + DATE_PART('minutes',travel_time)/60) as speed
       FROM leg_stats
) t4
WHERE speed > 0.00001