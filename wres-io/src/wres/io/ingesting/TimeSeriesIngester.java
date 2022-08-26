package wres.io.ingesting;

import java.util.List;
import java.util.stream.Stream;

import wres.io.reading.DataSource;
import wres.io.reading.TimeSeriesTuple;

/**
 * Ingests time-series data into a persistent store.
 * @author James Brown
 */

public interface TimeSeriesIngester
{
    /**
     * Ingests a tuple of time-series, together with an outer data source. The outer data source may differ from the 
     * {@link TimeSeriesTuple#getDataSource()}. For example, each time-series within the stream may originate from a 
     * distinct data source contained within the outer data source.
     * 
     * @param timeSeriesTuple the time-series to ingest, not null
     * @param outerSource the outer data source, not null
     * @return the ingest results
     * @throws NullPointerException if any input is null
     */

    List<IngestResult> ingest( Stream<TimeSeriesTuple> timeSeriesTuple, DataSource outerSource );
}