select pid, query, state,state_change, * 
from pg_stat_activity
WHERE client_port != -1
ORDER BY state_change
