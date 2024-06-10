package wres.io.retrieving.database;

import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import wres.config.yaml.components.Variable;
import wres.datamodel.space.Feature;
import wres.statistics.MessageFactory;

/**
 * Helper for running retrieval tests.
 */
class RetrieverTestHelper
{
    /** A project surrogate id for testing */
    static final long PROJECT_ID = 1;
    /** A project natural id (hash) for testing */
    static final String PROJECT_HASH = "1c1d76049f6e433ded63b4f3a6ad82ab";
    static final Feature FEATURE = Feature.of( MessageFactory.getGeometry( "F" ) );
    static final String VARIABLE_NAME = "Q";
    static final Variable VARIABLE = new Variable( "Q", null, Set.of() );
    static final String UNIT = "[ft_i]3/s";
    static final Instant T2023_04_01T00_00_00Z = Instant.parse( "2023-04-01T00:00:00Z" );
    static final Instant T2023_04_01T01_00_00Z = Instant.parse( "2023-04-01T01:00:00Z" );
    static final Instant T2023_04_01T03_00_00Z = Instant.parse( "2023-04-01T03:00:00Z" );
    static final Instant T2023_04_01T04_00_00Z = Instant.parse( "2023-04-01T04:00:00Z" );
    static final Instant T2023_04_01T06_00_00Z = Instant.parse( "2023-04-01T06:00:00Z" );
    static final Instant T2023_04_01T07_00_00Z = Instant.parse( "2023-04-01T07:00:00Z" );
    static final Instant T2023_04_01T19_00_00Z = Instant.parse( "2023-04-01T19:00:00Z" );
    static final Instant T2023_04_01T17_00_00Z = Instant.parse( "2023-04-01T17:00:00Z" );

    /**
     * @return an ingest executor
     */
    static ExecutorService getIngestExecutor()
    {
        // Create an ingest executor
        ThreadFactory ingestFactory =
                new BasicThreadFactory.Builder().namingPattern( "Ingesting Thread %d" )
                                                .build();
        // Queue should be large enough to allow join() call to be reached with zero or few rejected submissions to the
        // executor service.
        BlockingQueue<Runnable> ingestQueue = new ArrayBlockingQueue<>( 7 );

        RejectedExecutionHandler ingestHandler = new ThreadPoolExecutor.CallerRunsPolicy();
        return new ThreadPoolExecutor( 7,
                                       7,
                                       30000,
                                       TimeUnit.MILLISECONDS,
                                       ingestQueue,
                                       ingestFactory,
                                       ingestHandler );
    }

    private RetrieverTestHelper()
    {
        // Disallow construction, only here for the constants
    }
}
