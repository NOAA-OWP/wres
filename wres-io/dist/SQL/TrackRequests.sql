select pid, substring(query from 0 for 80), state,state_change--, * 
from pg_stat_activity
WHERE client_port != -1
ORDER BY state_change
