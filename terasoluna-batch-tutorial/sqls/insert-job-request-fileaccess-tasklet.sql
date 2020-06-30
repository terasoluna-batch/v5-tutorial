INSERT INTO batch_job_request(job_name,job_parameter,polling_status,create_date)
VALUES ('jobPointAddTasklet', 'inputFile=files/input/input-member-info-data.csv,outputFile=files/output/member_info_out.csv', 'INIT', current_timestamp);
