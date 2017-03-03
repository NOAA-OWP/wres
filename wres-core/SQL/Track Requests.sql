select query, datname, usename, backend_start, xact_start, query_start, state_change from pg_stat_activity where datname='wres' and state <> 'idle';
