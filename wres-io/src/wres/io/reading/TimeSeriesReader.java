package wres.io.reading;

import java.io.InputStream;
import java.util.stream.Stream;

/**
 * <p>An API for reading different types of time-series data from a {@link DataSource}. <p>When reading from a stream 
 * supplied by a {@link TimeSeriesReader}, use a try-with-resources block to ensure that the underlying resources are 
 * closed. 
 * 
 * <p>Implementation notes:
 * 
 * <p>Reads are intended to be "one and done". When reading fails, then the reader should throw a {@link ReadException}, 
 * which is unchecked and, therefore, unrecoverable. 
 * 
 * <p>A {@link TimeSeriesReader} provides an open pipe or stream to an underlying data source. In other words, each new 
 * time-series obtained from the stream should translate into (at least) as many read operations on the underlying 
 * data source as necessary to acquire the next time-series. In practice, the read operations may not have exactly the
 * shape of one-time series (e.g., when buffering raw reads for efficiency), but the goal of a {@link TimeSeriesReader} 
 * is to translate raw reads from an underlying data source into time-series-shaped reads, while maintaining the lazy 
 * behavior of a {@link Stream}; that is supplying one time-series at a time, with each pull translating into an 
 * underlying read.
 * 
 * <p>If this implementation is followed, then production will be naturally throttled according to the rate of 
 * consumption allowed by the terminal operator or consumer. Conversely, supplying this class with streams that read 
 * through to an in-memory collection (where all time-series have been read into memory upfront) will consume more 
 * memory than strictly required when the consumer writes to a persistent store of time-series, such as a database. 
 * 
 * <p>Implementations that rely on closable resources should close resources by registering a callback via 
 * {@link Stream#onClose(Runnable)}.
 * 
 * @author James Brown
 */

public interface TimeSeriesReader
{
    /**
     * Reads and returns all time-series from the underlying source, creating a stream to read from the source. To read 
     * from an existing stream, use {@link #read(DataSource, InputStream)}.
     *  
     * @see #read(DataSource, InputStream)
     * @param dataSource the data source, required
     * @return the stream of time-series
     * @throws NullPointerException if the dataSource is null
     * @throws ReadException if the reading fails for any other reason
     */

    Stream<TimeSeriesTuple> read( DataSource dataSource );

    /**
     * Reads and returns all time-series from the prescribed stream using the data source only for a description of the 
     * source. To read from a source without an existing stream, use {@link #read(DataSource, InputStream)}.
     * @see #read(DataSource)
     * @param dataSource the data source, required
     * @param stream the input stream, required
     * @return the stream of time-series
     * @throws NullPointerException if the dataSource is null
     * @throws ReadException if the reading fails for any other reason
     */

    Stream<TimeSeriesTuple> read( DataSource dataSource, InputStream stream );
}
