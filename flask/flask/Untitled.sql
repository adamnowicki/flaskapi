CREATE TABLE fp_grouped AS (
WITH fp_full AS(
SELECT
	fp.*
    , exp.Variation  
FROM vanguard.clean_fp AS fp
LEFT JOIN vanguard.clean_exp AS exp
USING (client_id)
)

SELECT 
	client_id
    , visit_id
    , date_time, process_step
  
	# , DATE_FORMAT(date_time, '%d-%m-%Y') AS formatted_date_time
    # , TIME_FORMAT(date_time, '%H:%i:%s') AS formatted_time
FROM fp_full
GROUP BY client_id, visit_id, date_time, process_step
ORDER BY  client_id, visit_id, date_time
);



drop view steps_time;
create view steps_time as 
SELECT *,
  LAG(date_time, 1) OVER
         (PARTITION BY visit_id ORDER BY date_time) AS step_ahead
from fp_grouped;

select * from steps_time ;




create table time_calculation as
with time_calc as (
SELECT *,
 TIMESTAMPDIFF(second, step_ahead, date_time) as diff
 from
    steps_time)
   
select * , LEAD(diff, 1)  OVER
         (PARTITION BY visit_id ORDER BY date_time) AS step_time
from time_calc;

    
select process_step , count(process_step)
from time_calculation
group by process_step;

select * from time_calculation;

