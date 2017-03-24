-- Table: public.coordinateindex

-- DROP TABLE public.coordinateindex;

CREATE TABLE public.coordinateindex
(
  x_position integer NOT NULL,
  y_position integer NOT NULL,
  resolution int NOT NULL,
  x_coordinate REAL NOT NULL,
  y_coordinate REAL NOT NULL,
  PRIMARY KEY (x_position, y_position, resolution)
)
WITH (
  OIDS=FALSE
);

