select pg_cancel_backend(pid)
from pg_stat_activity
WHERE client_port != -1
	AND application_name = 'PostgreSQL JDBC Driver'
ORDER BY state_change
