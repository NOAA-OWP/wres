-- Table: public.IndexQueue

DROP TABLE IF EXISTS public.IndexQueue;

CREATE TABLE IF NOT EXISTS public.IndexQueue
(
	indexqueue_id SERIAL,
	table_name TEXT NOT NULL,
	column_definition TEXT NOT NULL,
	method TEXT DEFAULT 'btree'
)
WITH (
	OIDS=FALSE
);
ALTER TABLE public.IndexQueue
	OWNER to wres;