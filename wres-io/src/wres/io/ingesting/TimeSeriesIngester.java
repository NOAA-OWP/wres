package wres.io.ingesting;

import java.util.List;
import java.util.stream.Stream;

import wres.reading.DataSource;
import wres.reading.TimeSeriesTuple;

/**
 * Ingests time-series data into a store, such as a database.
 * 
 * @author James Brown
 */

public interface TimeSeriesIngester
{
    /**
     * Ingests a tuple of time-series, together with descriptive metadata about the source of the time-series. The 
     * supplied data source may differ from the {@link TimeSeriesTuple#getDataSource()}. For example, it may represent 
     * an archive that contains many data sources, each one contributing to one or more time-series within the stream.
     * 
     * @param timeSeriesTuple the time-series to ingest, not null
     * @param outerSource the outer data source, not null
     * @return the ingest results
     * @throws NullPointerException if any input is null
     */

    List<IngestResult> ingest( Stream<TimeSeriesTuple> timeSeriesTuple, DataSource outerSource );
}