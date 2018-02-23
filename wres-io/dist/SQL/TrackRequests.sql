select pid, substring(query from 0 for 1000), state,state_change--, * 
from pg_stat_activity
WHERE client_port != -1
	AND application_name = 'PostgreSQL JDBC Driver'
	AND query != 'SHOW TRANSACTION ISOLATION LEVEL'
    AND query NOT LIKE 'SELECT e.typdelim%'
ORDER BY state_change
