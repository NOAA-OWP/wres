SELECT 'drop table if exists '||n.nspname ||'.'|| c.relname||';'
FROM pg_catalog.pg_class c
INNER JOIN pg_catalog.pg_namespace n
	ON N.oid = C.relnamespace
WHERE relchecks > 0
	AND (nspname = 'wres' OR nspname = 'partitions')
	AND relkind = 'r';


