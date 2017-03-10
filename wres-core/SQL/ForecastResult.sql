-- Table: ForecastResult

DROP TABLE ForecastResult;

CREATE TABLE ForecastResult
(
  forecast_id INT NOT NULL,
  lead_time SMALLINT NOT NULL,
  measurements REAL[] NOT NULL,
  CONSTRAINT forecastresult_forecast_fk FOREIGN KEY (forecast_id)
	REFERENCES Forecast (forecast_id) MATCH SIMPLE
	ON UPDATE NO ACTION ON DELETE CASCADE
)
WITH (
  OIDS=FALSE
);
CREATE INDEX forecastresult_forecast_idx ON ForecastResult(forecast_id);
CREATE INDEX forecastresult_lead_idx ON ForecastResult(lead_time);
