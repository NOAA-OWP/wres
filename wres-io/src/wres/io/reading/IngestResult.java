package wres.io.reading;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.ProjectConfig;
import wres.io.config.ConfigHelper;

/**
 * High-level result for a single fragment of ingest.
 * Multiple fragments can be used in a single source collection, and one can
 * be used multiple times in a single source collection (witness scenario400)
 */

public class IngestResult
{
    private static final Logger LOGGER = LoggerFactory.getLogger( IngestResult.class );
    private final LeftOrRightOrBaseline leftOrRightOrBaseline;
    private final DataSource dataSource;
    private final int surrogateKey;
    private final boolean foundAlready;
    private final boolean requiresRetry;

    private IngestResult( LeftOrRightOrBaseline leftOrRightOrBaseline,
                          DataSource dataSource,
                          int surrogateKey,
                          boolean foundAlready,
                          boolean requiresRetry )
    {
        Objects.requireNonNull( leftOrRightOrBaseline, "Ingester must include left/right/baseline" );
        Objects.requireNonNull( dataSource, "Ingester must include datasource information." );

        if ( surrogateKey == 0 )
        {
            LOGGER.warn( "Suspicious surrogate key id=0 given for dataSource={} with l/r/b={} foundAlready={} requiresRetry={}",
                         dataSource, leftOrRightOrBaseline, foundAlready, requiresRetry );
        }

        if ( surrogateKey < 0 )
        {
            throw new IllegalArgumentException( "Auto-generated ids are usually positive, but given surrogateKey was "
                                                + surrogateKey );
        }

        this.leftOrRightOrBaseline = leftOrRightOrBaseline;
        this.surrogateKey = surrogateKey;
        this.dataSource = dataSource;
        this.foundAlready = foundAlready;
        this.requiresRetry = requiresRetry;
    }

    public static IngestResult of( LeftOrRightOrBaseline leftOrRightOrBaseline,
                                   DataSource dataSource,
                                   int surrogateKey,
                                   boolean foundAlready,
                                   boolean requiresRetry )
    {
        return new IngestResult( leftOrRightOrBaseline, dataSource, surrogateKey, foundAlready, requiresRetry );
    }

    public static IngestResult of( LeftOrRightOrBaseline leftOrRightOrBaseline,
                                   int surrogateKey,
                                   DataSource dataSource,
                                   boolean foundAlready )
    {
        return IngestResult.of( leftOrRightOrBaseline, dataSource, surrogateKey, foundAlready, false );
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
    public static IngestResult from( ProjectConfig projectConfig,
                                     DataSource dataSource,
                                     int surrogateKey,
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
     * Get an IngestResult using the configuration elements, for convenience
     * @param projectConfig the ProjectConfig causing the ingest
     * @param dataSource the data source information
     * @param surrogateKey The surrogate key of the source data.
     * @param foundAlready true if found in the database, false otherwise
     * @return the IngestResult
     */
    public static IngestResult from( ProjectConfig projectConfig,
                                     DataSource dataSource,
                                     int surrogateKey,
                                     boolean foundAlready )
    {
        LeftOrRightOrBaseline leftOrRightOrBaseline =
                ConfigHelper.getLeftOrRightOrBaseline( projectConfig,
                                                       dataSource.getContext() );
        return IngestResult.of( leftOrRightOrBaseline,
                                dataSource,
                                surrogateKey,
                                foundAlready,
                                false );
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

    public static List<IngestResult> singleItemListFrom( ProjectConfig projectConfig,
                                                         DataSource dataSource,
                                                         int surrogateKey,
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
     * List with a single IngestResult from the given config, hash, foundAlready
     * <br>
     * For convenience (since this will be done all over the various ingesters).
     * @param projectConfig the ProjectConfig causing the ingest
     * @param dataSource the data source information
     * @param surrogateKey The surrogate key of the source data.
     * @param foundAlready true if found in the database, false otherwise
     * @return a list with a single IngestResult in it
     */

    public static List<IngestResult> singleItemListFrom( ProjectConfig projectConfig,
                                                         DataSource dataSource,
                                                         int surrogateKey,
                                                         boolean foundAlready )
    {
        IngestResult ingestResult = IngestResult.from( projectConfig,
                                                       dataSource,
                                                       surrogateKey,
                                                       foundAlready,
                                                       false );
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

    public static Future<List<IngestResult>> fakeFutureSingleItemListFrom( ProjectConfig projectConfig,
                                                                           DataSource dataSource,
                                                                           int surrogateKey,
                                                                           boolean requiresRetry )
    {
        return FakeFutureListOfIngestResults.from( projectConfig,
                                                   dataSource,
                                                   surrogateKey,
                                                   requiresRetry );
    }


    public DataSource getDataSource()
    {
        return this.dataSource;
    }

    public LeftOrRightOrBaseline getLeftOrRightOrBaseline()
    {
        return this.leftOrRightOrBaseline;
    }

    public int getSurrogateKey()
    {
        return this.surrogateKey;
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
        return new ToStringBuilder( this )
                .append( "leftOrRightOrBaseline", leftOrRightOrBaseline )
                .append( "dataSource", dataSource )
                .append( "surrogateKey", surrogateKey )
                .append( "foundAlready", foundAlready )
                .append( "requiresRetry", requiresRetry )
                .toString();
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
                                                          int surrogateKey,
                                                          boolean requiresRetry )
        {
            IngestResult results = IngestResult.from( projectConfig,
                                                      dataSource,
                                                      surrogateKey,
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
