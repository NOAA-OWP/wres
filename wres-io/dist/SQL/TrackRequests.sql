select pid, substring(query from 0 for 500), state,state_change--, * 
from pg_stat_activity
WHERE client_port != -1
	AND application_name = 'PostgreSQL JDBC Driver'
ORDER BY state_change
