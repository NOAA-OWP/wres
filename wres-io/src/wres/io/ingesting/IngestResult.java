package wres.io.ingesting;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import wres.config.generated.LeftOrRightOrBaseline;
import wres.io.reading.DataSource;

/**
 * High-level result for a single fragment of ingest, namely a time-series.
 */

public interface IngestResult
{
    /**
     * @return The surrogate key (the auto-incremented integer) for the source.
     */
    long getSurrogateKey();

    /**
     * @return The DataSource to use when retrying ingest.
     * @throws UnsupportedOperationException when requiresRetry() is false.
     */
    DataSource getDataSource();

    /**
     * @return Whether Left or Right or Baseline when retrying ingest.
     * @throws UnsupportedOperationException when requiresRetry() is false.
     */
    LeftOrRightOrBaseline getLeftOrRightOrBaseline();

    /**
     * Whether the data source existed already.
     * @return True when the data source was already in existence.
     */
    boolean wasFoundAlready();

    /**
     * Whether this data requires another try at ingest.
     * @return true when ingest needs to be retried.
     */

    boolean requiresRetry();

    /**
     * How many times the timeseries is included in the left dataset.
     * @return Count of left dataset associations.
     */
    short getLeftCount();

    /**
     * How many times the timeseries is included in the right dataset.
     * @return Count of right dataset associations.
     */
    short getRightCount();

    /**
     * How many times the timeseries is included in the baseline dataset.
     * @return Count of baseline dataset associations.
     */
    short getBaselineCount();


    private static IngestResult of( DataSource dataSource,
                                    long surrogateKey,
                                    boolean foundAlready,
                                    boolean requiresRetry )
    {
        if ( requiresRetry && !foundAlready )
        {
            throw new IllegalArgumentException( "If requiring retry, it must have been found already!" );
        }

        if ( requiresRetry )
        {
            return new IngestResultNeedingRetry( dataSource,
                                                 surrogateKey );
        }
        else
        {
            return new IngestResultCompact( dataSource,
                                            surrogateKey,
                                            foundAlready );
        }
    }



    /**
     * Get an IngestResult using the configuration elements, for convenience
     * @param dataSource the data source information
     * @param surrogateKey The surrogate key of the source data.
     * @param foundAlready true if found in the backing store, false otherwise
     * @param requiresRetry true if this requires retry, false otherwise
     * @return the IngestResult
     */
    private static IngestResult from( DataSource dataSource,
                                      long surrogateKey,
                                      boolean foundAlready,
                                      boolean requiresRetry )
    {
        return IngestResult.of( dataSource,
                                surrogateKey,
                                foundAlready,
                                requiresRetry );
    }


    /**
     * List with a single IngestResult from the given config, hash, foundAlready
     * <br>
     * For convenience (since this will be done all over the various ingesters).
     * @param dataSource the data source information
     * @param surrogateKey The surrogate key of the source data.
     * @param foundAlready true if found in the backing store, false otherwise
     * @param requiresRetry true if this requires retry, false otherwise
     * @return a list with a single IngestResult in it
     */

    static List<IngestResult> singleItemListFrom( DataSource dataSource,
                                                  long surrogateKey,
                                                  boolean foundAlready,
                                                  boolean requiresRetry )
    {
        IngestResult ingestResult = IngestResult.from( dataSource,
                                                       surrogateKey,
                                                       foundAlready,
                                                       requiresRetry );
        return List.of( ingestResult );
    }

    /**
     * Wrap a single item list of "already-found-in-backing-store" ingest result in
     * a Future for easy consumption by the ingest classes.
     * @param dataSource the data source information
     * @param surrogateKey The surrogate key of the source data.
     * @param requiresRetry true if this requires retry, false otherwise
     * @return an immediately-returning Future
     */

    static CompletableFuture<List<IngestResult>> fakeFutureSingleItemListFrom( DataSource dataSource,
                                                                               long surrogateKey,
                                                                               boolean requiresRetry )
    {
        return IngestResult.fakeFuturefrom( dataSource,
                                            surrogateKey,
                                            requiresRetry );
    }


    /**
     * For convenience of those clients needing an immediately-returning Future
     * this encapsulates an "already-found" IngestResult in a Future List.
     */

    private static CompletableFuture<List<IngestResult>> fakeFuturefrom( DataSource dataSource,
                                                                         long surrogateKey,
                                                                         boolean requiresRetry )
    {
        IngestResult results = IngestResult.from( dataSource,
                                                  surrogateKey,
                                                  true,
                                                  requiresRetry );
        return CompletableFuture.completedFuture( List.of( results ) );
    }
}
