package wres.pipeline;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.ExecutionResult;
import wres.config.MultiDeclarationFactory;
import wres.config.yaml.DeclarationException;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.events.broker.BrokerConnectionFactory;
import wres.io.database.Database;
import wres.system.DatabaseLockManager;
import wres.system.DatabaseLockManagerNoop;
import wres.system.DatabaseType;
import wres.system.SystemSettings;

/**
 * A complete implementation of an evaluation pipeline that begins with a project declaration string.
 *
 * @author James Brown
 * @author Jesse Bickel
 */
public class Evaluator
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( Evaluator.class );

    /** System settings.*/
    private final SystemSettings systemSettings;

    /** Database instance.*/
    private final Database database;

    /** Broker connections.*/
    private final BrokerConnectionFactory brokerConnectionFactory;

    /**
     * Creates an instance.
     * @param systemSettings the system settings, not null
     * @param database the database, if required
     * @param brokerConnectionFactory a broker connection factory, not null
     * @throws NullPointerException if any required input is null
     */
    public Evaluator( SystemSettings systemSettings,
                      Database database,
                      BrokerConnectionFactory brokerConnectionFactory )
    {
        Objects.requireNonNull( systemSettings );
        Objects.requireNonNull( brokerConnectionFactory );

        if ( systemSettings.isInDatabase() )
        {
            Objects.requireNonNull( database );
        }

        this.systemSettings = systemSettings;
        this.database = database;
        this.brokerConnectionFactory = brokerConnectionFactory;
    }

    /**
     * Processes a declaration string or a path to a file that contains a declaration string.
     * @param declarationOrPath the declaration or path to a declaration string
     * @return the result of the execution
     * @throws UserInputException when WRES detects a problem with the declaration
     * @throws InternalWresException when WRES encounters an internal error, unrelated to the declaration
     */

    public ExecutionResult evaluate( String declarationOrPath )
    {
        if ( Objects.isNull( declarationOrPath ) )
        {
            throw new InternalWresException( "Expected a non-null declaration string or path." );
        }

        // Create a record of failure, but only commit if a failure actually occurs
        EvaluationEvent failure = EvaluationEvent.of();
        failure.begin();

        EvaluationDeclaration declaration;
        String rawDeclaration;
        try
        {
            FileSystem fileSystem = FileSystems.getDefault();
            rawDeclaration = MultiDeclarationFactory.getDeclarationString( declarationOrPath, fileSystem );
            declaration = MultiDeclarationFactory.from( rawDeclaration, fileSystem, true, true );
        }
        catch ( IOException e )
        {
            LOGGER.error( "Failed to unmarshal  evaluation declaration from command line argument.", e );
            UserInputException translated = new UserInputException( "The evaluation declaration was invalid.", e );
            failure.setFailed();
            failure.commit();
            return ExecutionResult.failure( translated );
        }

        SystemSettings innerSystemSettings = this.getSystemSettings();

        if ( innerSystemSettings.isInMemory() )
        {
            LOGGER.info( "Running evaluation in memory." );
        }

        return this.evaluate( declaration, rawDeclaration );
    }

    /**
     * Executes an evaluation.
     * @param declaration the declaration
     * @param rawDeclaration the raw declaration string to log
     * @return the result of the execution
     * @throws NullPointerException if the projectConfigPlus is null
     */

    private ExecutionResult evaluate( EvaluationDeclaration declaration,
                                      String rawDeclaration )
    {
        Objects.requireNonNull( declaration );

        EvaluationEvent monitor = EvaluationEvent.of();
        monitor.begin();

        // Build a processing pipeline whose work is performed by a collection of thread pools, one pool for each
        // conceptual activity. There are two activities not represented here, namely reading of time-series data from
        // source formats and ingest of time-series into a persistent data store, such as a database. The thread pools
        // used for reading and ingesting are managed by wres-io. For example, see wres.io.ingesting.SourceLoader and
        // wres.io.ingesting.database.DatabaseTimeSeriesIngester. In principle, those thread pools could be abstracted
        // here too, but ingest is implementation specific (e.g., in-memory ingest is an ingest facade and does not
        // require a thread pool) and some readers, notably archive readers, have their own thread pool
        ThreadFactory poolFactory = new BasicThreadFactory.Builder()
                .namingPattern( "Pool Thread %d" )
                .build();
        // Essential to use a separate thread pool for thresholds and metrics as ArrayBlockingQueue operates a FIFO
        // policy. If dependent tasks (thresholds) are queued ahead of independent ones (metrics) in the same pool,
        // there is a DEADLOCK probability. Likewise, use a separate thread pool for dispatching pools and completing
        // tasks within pools with the same number of threads in each.
        ThreadFactory thresholdFactory = new BasicThreadFactory.Builder()
                .namingPattern( "Threshold Thread %d" )
                .build();
        ThreadFactory metricFactory = new BasicThreadFactory.Builder()
                .namingPattern( "Metric Thread %d" )
                .build();
        ThreadFactory productFactory = new BasicThreadFactory.Builder()
                .namingPattern( "Format Writing Thread %d" )
                .build();

        SystemSettings innerSystemSettings = this.getSystemSettings();

        // Create some unbounded work queues. For evaluations that produce statistics faster than subscribers (e.g.,
        // statistics format writers) can consume them, production is flow controlled explicitly via the statistics
        // messaging. See #95867.
        BlockingQueue<Runnable> poolQueue = new LinkedBlockingQueue<>();
        BlockingQueue<Runnable> thresholdQueue = new LinkedBlockingQueue<>();
        BlockingQueue<Runnable> metricQueue = new LinkedBlockingQueue<>();
        BlockingQueue<Runnable> productQueue = new LinkedBlockingQueue<>();

        // Create some thread pools to perform the work required by different parts of the evaluation pipeline
        // Thread pool that processes pools of pairs
        ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor( innerSystemSettings.getMaximumPoolThreads(),
                                                                  innerSystemSettings.getMaximumPoolThreads(),
                                                                  innerSystemSettings.poolObjectLifespan(),
                                                                  TimeUnit.MILLISECONDS,
                                                                  poolQueue,
                                                                  poolFactory );
        // Thread pool that dispatches thresholds
        ThreadPoolExecutor thresholdExecutor = new ThreadPoolExecutor( innerSystemSettings.getMaximumThresholdThreads(),
                                                                       innerSystemSettings.getMaximumThresholdThreads(),
                                                                       0,
                                                                       TimeUnit.SECONDS,
                                                                       thresholdQueue,
                                                                       thresholdFactory );
        // Thread pool that processes metrics
        ThreadPoolExecutor metricExecutor = new ThreadPoolExecutor( innerSystemSettings.getMaximumMetricThreads(),
                                                                    innerSystemSettings.getMaximumMetricThreads(),
                                                                    innerSystemSettings.poolObjectLifespan(),
                                                                    TimeUnit.MILLISECONDS,
                                                                    metricQueue,
                                                                    metricFactory );
        // Thread pool that generates products, such as statistics formats
        ThreadPoolExecutor productExecutor = new ThreadPoolExecutor( innerSystemSettings.getMaximumProductThreads(),
                                                                     innerSystemSettings.getMaximumProductThreads(),
                                                                     innerSystemSettings.poolObjectLifespan(),
                                                                     TimeUnit.MILLISECONDS,
                                                                     productQueue,
                                                                     productFactory );

        // Create database services if needed
        DatabaseServices databaseServices = null;
        DatabaseLockManager lockManager;
        if ( innerSystemSettings.isInDatabase() )
        {
            Database innerDatabase = this.getDatabase();
            lockManager =
                    DatabaseLockManager.from( innerSystemSettings,
                                              innerDatabase::getRawConnection );

            databaseServices = new DatabaseServices( innerDatabase, lockManager );
        }
        else
        {
            // Dummy lock manager for in-memory evaluations
            lockManager = new DatabaseLockManagerNoop();
        }

        String projectHash;
        Set<Path> pathsWrittenTo = new TreeSet<>();
        ScheduledExecutorService monitoringService = null;
        try
        {
            // Mark the WRES as doing an evaluation.
            lockManager.lockShared( DatabaseType.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );

            // Reduce our set of executors to one object
            Executors executors = new Executors( poolExecutor,
                                                 thresholdExecutor,
                                                 metricExecutor,
                                                 productExecutor );

            // Monitor the task queues if required
            if ( System.getProperty( "wres.monitorTaskQueues" ) != null )
            {
                monitoringService = new ScheduledThreadPoolExecutor( 1 );
                QueueMonitor queueMonitor = new QueueMonitor( this.getDatabase(),
                                                              poolQueue,
                                                              thresholdQueue,
                                                              metricQueue,
                                                              productQueue );

                monitoringService.scheduleAtFixedRate( queueMonitor,
                                                       1,
                                                       500,
                                                       TimeUnit.MILLISECONDS );
            }

            // Perform the evaluation
            Pair<Set<Path>, String> innerPathsAndProjectHash =
                    EvaluationUtilities.evaluate( innerSystemSettings,
                                                  databaseServices,
                                                  declaration,
                                                  executors,
                                                  this.getBrokerConnectionFactory(),
                                                  monitor );
            pathsWrittenTo.addAll( innerPathsAndProjectHash.getLeft() );
            projectHash = innerPathsAndProjectHash.getRight();
            monitor.setDataHash( projectHash );
            monitor.setResources( pathsWrittenTo );

            lockManager.unlockShared( DatabaseType.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );
        }
        catch ( DeclarationException userException )
        {
            String message = "Please correct the project declaration.";
            UserInputException userInputException = new UserInputException( message, userException );
            monitor.setFailed();
            monitor.commit();
            return ExecutionResult.failure( declaration.label(),
                                            rawDeclaration,
                                            userInputException );
        }
        catch ( RuntimeException | IOException | SQLException internalException )
        {
            String message = "Could not complete project execution";
            InternalWresException internalWresException = new InternalWresException( message, internalException );
            monitor.setFailed();
            monitor.commit();
            return ExecutionResult.failure( declaration.label(),
                                            rawDeclaration,
                                            internalWresException );
        }
        // Shutdown
        finally
        {
            if ( Objects.nonNull( monitoringService ) )
            {
                Evaluator.shutDownGracefully( monitoringService );
            }
            Evaluator.shutDownGracefully( productExecutor );
            Evaluator.shutDownGracefully( metricExecutor );
            Evaluator.shutDownGracefully( thresholdExecutor );
            Evaluator.shutDownGracefully( poolExecutor );
            lockManager.shutdown();
        }

        monitor.setSucceeded();
        monitor.commit();
        return ExecutionResult.success( declaration.label(),
                                        rawDeclaration,
                                        projectHash,
                                        pathsWrittenTo );
    }

    /**
     * Kill off the executors passed in even if there are remaining tasks.
     *
     * @param executor the executor to shut down
     */
    private static void shutDownGracefully( final ExecutorService executor )
    {
        Objects.requireNonNull( executor );

        // Shutdown
        executor.shutdown();

        try
        {
            // Await termination after shutdown
            boolean died = executor.awaitTermination( 5, TimeUnit.SECONDS );

            if ( !died )
            {
                List<Runnable> tasks = executor.shutdownNow();

                if ( !tasks.isEmpty() && LOGGER.isInfoEnabled() )
                {
                    LOGGER.info( "Abandoned {} tasks from {}",
                                 tasks.size(),
                                 executor );
                }
            }
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted while shutting down {}", executor, ie );
            Thread.currentThread().interrupt();
        }
    }

    /**
     * @return the system settings.
     */
    private SystemSettings getSystemSettings()
    {
        return this.systemSettings;
    }

    /**
     * @return the database.
     */
    private Database getDatabase()
    {
        return this.database;
    }

    /**
     * @return the broker connections.
     */
    private BrokerConnectionFactory getBrokerConnectionFactory()
    {
        return this.brokerConnectionFactory;
    }

    /** Queue monitor. */
    private record QueueMonitor( Database database, Queue<?> poolQueue, Queue<?> thresholdQueue,
                                 Queue<?> metricQueue, Queue<?> productQueue ) implements Runnable
    {
        @Override
        public void run()
        {
            int poolCount = 0;
            int databaseCount = 0;
            int thresholdCount = 0;
            int metricCount = 0;
            int productCount = 0;

            if ( this.poolQueue != null )
            {
                poolCount = this.poolQueue.size();
            }

            if ( this.thresholdQueue != null )
            {
                thresholdCount = this.thresholdQueue.size();
            }

            if ( this.metricQueue != null )
            {
                metricCount = this.metricQueue.size();
            }

            if ( this.productQueue != null )
            {
                productCount = this.productQueue.size();
            }

            if ( this.database != null )
            {
                databaseCount = database.getDatabaseQueueTaskCount();
            }

            LOGGER.info( "PoolQ={}, DatabaseQ={}, ThresholdQ={}, MetricQ={}, ProductQ={}",
                         poolCount,
                         databaseCount,
                         thresholdCount,
                         metricCount,
                         productCount );
        }
    }

    /**
     * Small value object to collate a {@link Database} with an {@link DatabaseLockManager}. This may be disaggregated
     * for transparency if we can reduce the number of input arguments to some methods.
     * @param database The database instance.
     * @param databaseLockManager The Database lock manager instance.
     */

    record DatabaseServices( Database database, DatabaseLockManager databaseLockManager )
    {
    }

    /**
     * A value object that reduces count of args for some methods and provides names for those objects.
     */

    record Executors( ExecutorService poolExecutor, ExecutorService thresholdExecutor,
                      ExecutorService metricExecutor, ExecutorService productExecutor )
    {
    }
}
