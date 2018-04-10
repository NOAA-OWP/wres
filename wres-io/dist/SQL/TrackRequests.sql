select pid,
    query,
    state,
    CASE
        WHEN state = 'active' THEN age(now(), xact_start)
        ELSE age(state_change, xact_start)
    END,
    state_change
    --, * 
from pg_stat_activity
WHERE client_port != -1
	AND application_name = 'PostgreSQL JDBC Driver'
	AND query != 'SHOW TRANSACTION ISOLATION LEVEL'
    AND query NOT LIKE 'SELECT e.typdelim%'
ORDER BY state_change
