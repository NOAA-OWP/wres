package wres.io.reading;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.ProjectConfig;
import wres.io.config.ConfigHelper;

/**
 * High-level result for a single fragment of ingest, namely a timeseries.
 * One can be used multiple times in a single dataset (witness scenario400).
 *
 * Due to the history of this class, it has been split into two implementations
 * and modes: one for retry and one not for retry. The caller must test by
 * calling requiresRetry() before calling getDataSource() or
 * getLeftOrRightOrBaseline() because when requiresRetry() is false, those
 * methods are unsupported and will throw UnsupportedOperationException.
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


    private static IngestResult of( LeftOrRightOrBaseline leftOrRightOrBaseline,
                                    DataSource dataSource,
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
            return new IngestResultNeedingRetry( leftOrRightOrBaseline,
                                                 dataSource,
                                                 surrogateKey );
        }
        else
        {
            return new IngestResultCompact( leftOrRightOrBaseline,
                                            dataSource,
                                            surrogateKey,
                                            foundAlready );
        }
    }



    /**
     * Get an IngestResult using the configuration elements, for convenience
     * @param projectConfig the ProjectConfig causing the ingest
     * @param dataSource the data source information
     * @param surrogateKey The surrogate key of the source data.
     * @param foundAlready true if found in the database, false otherwise
     * @param requiresRetry true if this requires retry, false otherwise
     * @return the IngestResult
     */
    private static IngestResult from( ProjectConfig projectConfig,
                                      DataSource dataSource,
                                      long surrogateKey,
                                      boolean foundAlready,
                                      boolean requiresRetry )
    {
        LeftOrRightOrBaseline leftOrRightOrBaseline =
                ConfigHelper.getLeftOrRightOrBaseline( projectConfig,
                                                       dataSource.getContext() );
        return IngestResult.of( leftOrRightOrBaseline,
                                dataSource,
                                surrogateKey,
                                foundAlready,
                                requiresRetry );
    }


    /**
     * List with a single IngestResult from the given config, hash, foundAlready
     * <br>
     * For convenience (since this will be done all over the various ingesters).
     * @param projectConfig the ProjectConfig causing the ingest
     * @param dataSource the data source information
     * @param surrogateKey The surrogate key of the source data.
     * @param foundAlready true if found in the database, false otherwise
     * @param requiresRetry true if this requires retry, false otherwise
     * @return a list with a single IngestResult in it
     */

    static List<IngestResult> singleItemListFrom( ProjectConfig projectConfig,
                                                  DataSource dataSource,
                                                  long surrogateKey,
                                                  boolean foundAlready,
                                                  boolean requiresRetry )
    {
        IngestResult ingestResult = IngestResult.from( projectConfig,
                                                       dataSource,
                                                       surrogateKey,
                                                       foundAlready,
                                                       requiresRetry );
        return List.of( ingestResult );
    }

    /**
     * Wrap a single item list of "already-found-in-database" ingest result in
     * a Future for easy consumption by the ingest classes.
     * @param projectConfig the project configuration
     * @param dataSource the data source information
     * @param surrogateKey The surrogate key of the source data.
     * @param requiresRetry true if this requires retry, false otherwise
     * @return an immediately-returning Future
     */

    static CompletableFuture<List<IngestResult>> fakeFutureSingleItemListFrom( ProjectConfig projectConfig,
                                                                               DataSource dataSource,
                                                                               long surrogateKey,
                                                                               boolean requiresRetry )
    {
        return IngestResult.fakeFuturefrom( projectConfig,
                                            dataSource,
                                            surrogateKey,
                                            requiresRetry );
    }


    /**
     * For convenience of those clients needing an immediately-returning Future
     * this encapsulates an "already-found" IngestResult in a Future List.
     */

    private static CompletableFuture<List<IngestResult>> fakeFuturefrom( ProjectConfig projectConfig,
                                                                         DataSource dataSource,
                                                                         long surrogateKey,
                                                                         boolean requiresRetry )
    {
        IngestResult results = IngestResult.from( projectConfig,
                                                  dataSource,
                                                  surrogateKey,
                                                  true,
                                                  requiresRetry );
        return CompletableFuture.completedFuture( List.of( results ) );
    }
}
