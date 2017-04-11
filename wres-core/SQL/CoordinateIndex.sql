-- Table: public.coordinateindex

DROP TABLE IF EXISTS public.coordinateindex CASCADE;

CREATE TABLE IF NOT EXISTS public.coordinateindex
(
  x_position integer NOT NULL,
  y_position integer NOT NULL,
  resolution integer NOT NULL,
  x_coordinate real NOT NULL,
  y_coordinate real NOT NULL,
  CONSTRAINT coordinateindex_pkey PRIMARY KEY (x_position, y_position, resolution)
)
WITH (
  OIDS=FALSE
);
