-- Table: public.variablenetcdfsource

-- DROP TABLE public.variablenetcdfsource;

CREATE TABLE public.variablenetcdfsource
(
  variable_id integer NOT NULL,
  netcdfsource_id INTEGER NOT NULL,
  CONSTRAINT variablenetcdfsource_variable_fk FOREIGN KEY (variable_id)
	REFERENCES public.variable (variable_id)
	ON UPDATE NO ACTION ON DELETE CASCADE,
  CONSTRAINT variablenetcdfsource_netcdfsource_fk FOREIGN KEY (netcdfsource_id)
	REFERENCES public.netcdfsource (netcdfsource_id)
	ON UPDATE NO ACTION ON DELETE CASCADE
)
WITH (
  OIDS=FALSE
);

-- Index: public.variablenetcdfsouce_netcdfsource_idx

-- DROP INDEX public.variablenetcdfsouce_netcdfsource_idx;

CREATE INDEX variablenetcdfsouce_netcdfsource_idx
  ON public.variablenetcdfsource
  USING btree
  (netcdfsource_id);

-- Index: public.variable_variable_name_idx

-- DROP INDEX public.variable_variable_name_idx;

CREATE INDEX variablenetcdfsouce_variable_idx
  ON public.variablenetcdfsource
  USING btree
  (variable_id);

