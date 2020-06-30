SELECT job_execution_id,start_time,end_time,exit_code FROM batch_job_execution WHERE job_execution_id = 
(SELECT max(job_execution_id) FROM batch_job_request WHERE job_execution_id IS NOT NULL);