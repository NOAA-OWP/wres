package wres.io.ingesting;

import java.util.List;

import wres.datamodel.Ensemble;
import wres.datamodel.time.TimeSeries;
import wres.io.reading.DataSource;

/**
 * Ingests time-series data into a persistent store.
 * @author James Brown
 */

public interface TimeSeriesIngester
{
    /**
     * Ingests a time-series whose event values are {@link Double}.
     * 
     * @param timeSeries the time-series to ingest, not null
     * @param dataSource the data source to ingest, not null
     * @return the ingest results
     * @throws NullPointerException if any input is null
     */

    List<IngestResult> ingestSingleValuedTimeSeries( TimeSeries<Double> timeSeries, DataSource dataSource );

    /**
     * Ingests a time-series whose event values are {@link Ensemble}.
     * 
     * @param timeSeries the time-series to ingest, not null
     * @param dataSource the data source to ingest, not null
     * @return the ingest results
     * @throws NullPointerException if any input is null
     */

    List<IngestResult> ingestEnsembleTimeSeries( TimeSeries<Ensemble> timeSeries, DataSource dataSource );
}