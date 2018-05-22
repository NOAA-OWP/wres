-- View: public.ProjectExecutions

-- DROP VIEW public.ProjectExecutions;

CREATE OR REPLACE VIEW public.ProjectExecutions AS 
SELECT CASE
            WHEN failed = true THEN 'FAILED'
            ELSE 'SUCCEEDED'
    END AS status,
    substring(executionlog.project, '(?<=<project (label=".+" )?name=")[^"]+'::text) AS project_name,
    arguments,
    start_time,
    run_time,
    error,
    log_id
FROM public.ExecutionLog
WHERE arguments LIKE 'execute%'
ORDER BY start_time;

ALTER TABLE public.ProjectExecutions
  OWNER TO wres;
