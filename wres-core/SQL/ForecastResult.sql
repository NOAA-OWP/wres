-- Table: ForecastResult

--DELETE FROM Forecast;
--DROP TABLE ForecastResult;

CREATE TABLE ForecastResult
(
  forecast_id INT NOT NULL,
  lead_time SMALLINT NOT NULL,
  measurements REAL[] NOT NULL,
  observationlocation_id INT not null,
  CONSTRAINT forecastresult_forecast_fk FOREIGN KEY (forecast_id)
	REFERENCES Forecast (forecast_id) MATCH SIMPLE
	ON UPDATE NO ACTION ON DELETE CASCADE,
  CONSTRAINT forecastresult_observationlocation_fk FOREIGN KEY (observationlocation_id)
	REFERENCES ObservationLocation (observationlocation_id) MATCH SIMPLE
	ON UPDATE NO ACTION ON DELETE CASCADE
)
WITH (
  OIDS=FALSE
);
CREATE INDEX forecastresult_forecast_idx ON ForecastResult(forecast_id);
CREATE INDEX forecastresult_lead_idx ON ForecastResult(lead_time);
CREATE INDEX forecastresult_location_idx ON ForecastResult(observationlocation_id);