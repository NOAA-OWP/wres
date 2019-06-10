package wres.io.reading;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import wres.io.config.LeftOrRightOrBaseline;
import wres.config.generated.ProjectConfig;
import wres.io.config.ConfigHelper;

/**
 * High-level result for a single fragment of ingest.
 * Multiple fragments can be used in a single source collection, and one can
 * be used multiple times in a single source collection (witness scenario400)
 */

public class IngestResult
{
    private final LeftOrRightOrBaseline leftOrRightOrBaseline;
    private final DataSource dataSource;
    private final String hash;
    private final boolean foundAlready;
    private final boolean requiresRetry;

    private IngestResult( LeftOrRightOrBaseline leftOrRightOrBaseline,
                          DataSource dataSource,
                          String hash,
                          boolean foundAlready,
                          boolean requiresRetry )
    {
        Objects.requireNonNull( hash, "Ingester must include a hash." );
        Objects.requireNonNull( leftOrRightOrBaseline, "Ingester must include left/right/baseline" );
        Objects.requireNonNull( dataSource, "Ingester must include datasource information." );
        this.leftOrRightOrBaseline = leftOrRightOrBaseline;
        this.hash = hash;
        this.dataSource = dataSource;
        this.foundAlready = foundAlready;
        this.requiresRetry = requiresRetry;
    }

    public static IngestResult of( LeftOrRightOrBaseline leftOrRightOrBaseline,
                                   DataSource dataSource,
                                   String hash,
                                   boolean foundAlready,
                                   boolean requiresRetry )
    {
        return new IngestResult( leftOrRightOrBaseline, dataSource, hash, foundAlready, requiresRetry );
    }

    public static IngestResult of( LeftOrRightOrBaseline leftOrRightOrBaseline,
                                   String hash,
                                   DataSource dataSource,
                                   boolean foundAlready )
    {
        return IngestResult.of( leftOrRightOrBaseline, dataSource, hash, foundAlready, false );
    }


    /**
     * Get an IngestResult using the configuration elements, for convenience
     * @param projectConfig the ProjectConfig causing the ingest
     * @param dataSource the data source information
     * @param hash the hash of the data
     * @param foundAlready true if found in the database, false otherwise
     * @param requiresRetry true if this requires retry, false otherwise
     * @return the IngestResult
     */
    public static IngestResult from( ProjectConfig projectConfig,
                                     DataSource dataSource,
                                     String hash,
                                     boolean foundAlready,
                                     boolean requiresRetry )
    {
        LeftOrRightOrBaseline leftOrRightOrBaseline =
                ConfigHelper.getLeftOrRightOrBaseline( projectConfig,
                                                       dataSource.getContext() );
        return IngestResult.of( leftOrRightOrBaseline,
                                dataSource,
                                hash,
                                foundAlready,
                                requiresRetry );
    }

    /**
     * Get an IngestResult using the configuration elements, for convenience
     * @param projectConfig the ProjectConfig causing the ingest
     * @param dataSource the data source information
     * @param hash the hash of the data
     * @param foundAlready true if found in the database, false otherwise
     * @return the IngestResult
     */
    public static IngestResult from( ProjectConfig projectConfig,
                                     DataSource dataSource,
                                     String hash,
                                     boolean foundAlready )
    {
        LeftOrRightOrBaseline leftOrRightOrBaseline =
                ConfigHelper.getLeftOrRightOrBaseline( projectConfig,
                                                       dataSource.getContext() );
        return IngestResult.of( leftOrRightOrBaseline,
                                dataSource,
                                hash,
                                foundAlready,
                                false );
    }



    /**
     * List with a single IngestResult from the given config, hash, foundAlready
     * <br>
     * For convenience (since this will be done all over the various ingesters).
     * @param projectConfig the ProjectConfig causing the ingest
     * @param dataSource the data source information
     * @param hash the hash of the data
     * @param foundAlready true if found in the database, false otherwise
     * @param requiresRetry true if this requires retry, false otherwise
     * @return a list with a single IngestResult in it
     */

    public static List<IngestResult> singleItemListFrom( ProjectConfig projectConfig,
                                                         DataSource dataSource,
                                                         String hash,
                                                         boolean foundAlready,
                                                         boolean requiresRetry )
    {
        IngestResult ingestResult = IngestResult.from( projectConfig,
                                                       dataSource,
                                                       hash,
                                                       foundAlready,
                                                       requiresRetry );
        return List.of( ingestResult );
    }


    /**
     * List with a single IngestResult from the given config, hash, foundAlready
     * <br>
     * For convenience (since this will be done all over the various ingesters).
     * @param projectConfig the ProjectConfig causing the ingest
     * @param dataSource the data source information
     * @param hash the hash of the data
     * @param foundAlready true if found in the database, false otherwise
     * @return a list with a single IngestResult in it
     */

    public static List<IngestResult> singleItemListFrom( ProjectConfig projectConfig,
                                                         DataSource dataSource,
                                                         String hash,
                                                         boolean foundAlready )
    {
        IngestResult ingestResult = IngestResult.from( projectConfig,
                                                       dataSource,
                                                       hash,
                                                       foundAlready,
                                                       false );
        return List.of( ingestResult );
    }


    /**
     * Wrap a single item list of "already-found-in-database" ingest result in
     * a Future for easy consumption by the ingest classes.
     * @param projectConfig the project configuration
     * @param dataSource the data source information
     * @param hash the hash of the individual source
     * @param requiresRetry true if this requires retry, false otherwise
     * @return an immediately-returning Future
     */

    public static Future<List<IngestResult>> fakeFutureSingleItemListFrom( ProjectConfig projectConfig,
                                                                           DataSource dataSource,
                                                                           String hash,
                                                                           boolean requiresRetry )
    {
        return FakeFutureListOfIngestResults.from( projectConfig,
                                                   dataSource,
                                                   hash,
                                                   requiresRetry );
    }


    /**
     * Wrap a single item list of "already-found-in-database" ingest result in
     * a Future for easy consumption by the ingest classes.
     * @param projectConfig the project configuration
     * @param dataSource the data source information
     * @param hash the hash of the individual source
     * @return an immediately-returning Future
     */

    public static Future<List<IngestResult>> fakeFutureSingleItemListFrom( ProjectConfig projectConfig,
                                                                           DataSource dataSource,
                                                                           String hash )
    {
        return FakeFutureListOfIngestResults.from( projectConfig,
                                                   dataSource,
                                                   hash,
                                                   false );
    }

    public DataSource getDataSource()
    {
        return this.dataSource;
    }

    public LeftOrRightOrBaseline getLeftOrRightOrBaseline()
    {
        return this.leftOrRightOrBaseline;
    }

    public String getHash()
    {
        return this.hash;
    }

    public boolean wasFoundAlready()
    {
        return this.foundAlready;
    }

    public boolean requiresRetry()
    {
        return this.requiresRetry;
    }

    @Override
    public String toString()
    {
        return "DataSource:" + this.dataSource + ", hash: " + this.getHash() + ", "
               + "another party ingested? " + this.wasFoundAlready() + ", "
               + "l/r/b: " + getLeftOrRightOrBaseline().value();
    }


    /**
     * For convenience of those clients needing an immediately-returning Future
     * this encapsulates an "already-found" IngestResult in a Future List.
     */

    private static class FakeFutureListOfIngestResults implements Future<List<IngestResult>>
    {
        private final List<IngestResult> results;

        private FakeFutureListOfIngestResults( IngestResult result )
        {
            this.results = List.of( result );
        }

        public static FakeFutureListOfIngestResults from( ProjectConfig projectConfig,
                                                          DataSource dataSource,
                                                          String hash,
                                                          boolean requiresRetry )
        {
            IngestResult results = IngestResult.from( projectConfig,
                                                      dataSource,
                                                      hash,
                                                      true,
                                                      requiresRetry );
            return new FakeFutureListOfIngestResults( results );
        }

        @Override
        public boolean cancel( boolean b )
        {
            return false;
        }

        @Override
        public boolean isCancelled()
        {
            return false;
        }

        @Override
        public boolean isDone()
        {
            return true;
        }

        @Override
        public List<IngestResult> get()
                throws InterruptedException, ExecutionException
        {
            return this.getResults();
        }

        @Override
        public List<IngestResult> get( long l, TimeUnit timeUnit )
                throws InterruptedException, ExecutionException,
                TimeoutException
        {
            return get();
        }

        private List<IngestResult> getResults()
        {
            return this.results;
        }
    }
}
