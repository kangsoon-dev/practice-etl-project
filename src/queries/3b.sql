/* Median work speed of each truck driver in all jobs handled by him / her (work speed = time spent loading/unloading in a stop between two legs in a job, i.e. leave_time - arrive_time) 
   10 best performers in a day (lowest average work speed) */
SELECT j.driver_id, AVG(l.leave_time - l.arrive_time) as work_time
FROM legs l
INNER JOIN jobs j
ON l.job_id = j.job_id
GROUP BY j.driver_id
ORDER BY work_time DESC
LIMIT 10