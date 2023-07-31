/* 
Methodology: use window functions for filtering duplicated columns
1st, generate row number in each row in each partition in the descending order of insert_timestamp.
So, the latest record in a parition will be assigned to row number 1 and so on.
We just need to keep row_number = 1 to keep the latest record
*/

SELECT * FROM {{ ref('events') }}
QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY insert_timestamp DESC) = 1