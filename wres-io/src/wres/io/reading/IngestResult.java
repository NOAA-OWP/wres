package wres.io.reading;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import wres.config.generated.DataSourceConfig;
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
    private final String hash;
    private final String name;
    private final boolean foundAlready;

    private IngestResult( LeftOrRightOrBaseline leftOrRightOrBaseline,
                          String hash,
                          String name,
                          boolean foundAlready )
    {
        Objects.requireNonNull( hash, "Ingester must include a hash." );
        Objects.requireNonNull( leftOrRightOrBaseline, "Ingester must include left/right/baseline" );
        this.leftOrRightOrBaseline = leftOrRightOrBaseline;
        this.hash = hash;
        this.name = name;
        this.foundAlready = foundAlready;
    }

    public static IngestResult of( LeftOrRightOrBaseline leftOrRightOrBaseline,
                                   String hash,
                                   String name,
                                   boolean foundAlready )
    {
        return new IngestResult( leftOrRightOrBaseline, hash, name, foundAlready );
    }

    /**
     * Get an IngestResult using the configuration elements, for convenience
     * @param projectConfig the ProjectConfig causing the ingest
     * @param dataSourceConfig the config element ingesting for
     * @param hash the hash of the data
     * @param foundAlready true if found in the database, false otherwise
     * @return the IngestResult
     */
    public static IngestResult from( ProjectConfig projectConfig,
                                     DataSourceConfig dataSourceConfig,
                                     String hash,
                                     String name,
                                     boolean foundAlready )
    {
        LeftOrRightOrBaseline leftOrRightOrBaseline =
                ConfigHelper.getLeftOrRightOrBaseline( projectConfig,
                                                       dataSourceConfig );
        return IngestResult.of( leftOrRightOrBaseline,
                                hash,
                                name,
                                foundAlready );
    }


    /**
     * List with a single IngestResult from the given config, hash, foundAlready
     * <br>
     * For convenience (since this will be done all over the various ingesters).
     * @param projectConfig the ProjectConfig causing the ingest
     * @param dataSourceConfig the config element ingesting for
     * @param hash the hash of the data
     * @param foundAlready true if found in the database, false otherwise
     * @return a list with a single IngestResult in it
     */

    public static List<IngestResult> singleItemListFrom( ProjectConfig projectConfig,
                                                         DataSourceConfig dataSourceConfig,
                                                         String hash,
                                                         String name,
                                                         boolean foundAlready )
    {
        List<IngestResult> result = new ArrayList<>( 1 );

        IngestResult ingestResult = IngestResult.from( projectConfig,
                                                       dataSourceConfig,
                                                       hash,
                                                       name,
                                                       foundAlready );
        result.add( ingestResult );

        return Collections.unmodifiableList( result );
    }


    /**
     * Wrap a single item list of "already-found-in-database" ingest result in
     * a Future for easy consumption by the ingest classes.
     * @param projectConfig the project configuration
     * @param dataSourceConfig the data source configuration
     * @param hash the hash of the individual source
     * @return an immediately-returning Future
     */

    public static Future<List<IngestResult>> fakeFutureSingleItemListFrom( ProjectConfig projectConfig,
                                                                           DataSourceConfig dataSourceConfig,
                                                                           String name,
                                                                           String hash )
    {
        return FakeFutureListOfIngestResults.from( projectConfig,
                                                   dataSourceConfig,
                                                   name,
                                                   hash );
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

    @Override
    public String toString()
    {
        return "Name:" + this.name + ", hash: " + this.getHash() + ", "
               + "db cache hit? " + this.wasFoundAlready() + ", "
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
            List<IngestResult> theResults = new ArrayList<>( 1 );
            theResults.add( result );
            this.results = Collections.unmodifiableList( theResults );
        }

        public static FakeFutureListOfIngestResults from( ProjectConfig projectConfig,
                                                          DataSourceConfig dataSourceConfig,
                                                          String name,
                                                          String hash )
        {
            IngestResult results = IngestResult.from( projectConfig,
                                                      dataSourceConfig,
                                                      hash,
                                                      name,
                                                      true );
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
