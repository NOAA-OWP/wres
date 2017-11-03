
CREATE OR REPLACE FUNCTION _final_median(DOUBLE PRECISION[])
	RETURNS DOUBLE PRECISION AS
$$
	SELECT AVG(val)
	FROM (
		SELECT val
		FROM unnest($1) AS val
		ORDER BY 1
		LIMIT 2 - MOD(array_upper($1, 1), 2)
		OFFSET CEIL(array_upper($1, 1) / 2.0) - 1
	) AS sub;
$$
LANGUAGE 'sql' IMMUTABLE;

CREATE AGGREGATE median(DOUBLE PRECISION) (
	SFUNC=array_append,
	STYPE=DOUBLE PRECISION[],
	FINALFUNC=_final_median,
	INITCOND='{}'
);