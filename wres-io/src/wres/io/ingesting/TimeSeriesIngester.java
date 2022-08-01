package wres.io.ingesting;

import java.util.List;
import java.util.stream.Stream;

import wres.datamodel.time.TimeSeriesTuple;
import wres.io.reading.DataSource;

/**
 * Ingests time-series data into a persistent store.
 * @author James Brown
 */

public interface TimeSeriesIngester
{
    /**
     * Ingests a tuple of time-series.
     * 
     * @param timeSeriesTuple the time-series to ingest, not null
     * @param dataSource the data source to ingest, not null
     * @return the ingest results
     * @throws NullPointerException if any input is null
     */

    List<IngestResult> ingest( Stream<TimeSeriesTuple> timeSeriesTuple, DataSource dataSource );
}