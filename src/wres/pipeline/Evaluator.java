package wres.pipeline;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
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
import wres.io.database.locking.DatabaseLockManager;
import wres.io.database.locking.DatabaseLockManagerNoop;
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

        if ( systemSettings.isUseDatabase() )
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
     * @param canceller a cancellation callback to cancel the running evaluation
     * @return the result of the execution
     * @throws UserInputException when WRES detects a problem with the declaration
     * @throws InternalWresException when WRES encounters an internal error, unrelated to the declaration
     */

    public ExecutionResult evaluate( String declarationOrPath,
                                     Canceller canceller )
    {
        if ( Objects.isNull( declarationOrPath ) )
        {
            throw new InternalWresException( "Expected a non-null declaration string or path." );
        }

        if ( Objects.isNull( canceller ) )
        {
            throw new InternalWresException( "Expected a non-null cancellation callback." );
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
        catch ( DeclarationException | IOException e )
        {
            LOGGER.error( "Failed to read or validate an evaluation declaration from the command line argument.", e );
            UserInputException translated = new UserInputException( "The evaluation declaration was invalid.", e );
            failure.setFailed();
            failure.commit();
            return ExecutionResult.failure( translated, canceller.cancelled() );
        }

        SystemSettings settings = this.getSystemSettings();
        if ( !settings.isUseDatabase() )
        {
            LOGGER.info( "Running evaluation in memory." );
        }

        return this.evaluate( declaration, rawDeclaration, canceller );
    }

    /**
     * Executes an evaluation.
     * @param declaration the declaration
     * @param rawDeclaration the raw declaration string to log
     * @param canceller a cancellation callback to cancel the running evaluation, not null
     * @return the result of the execution
     * @throws NullPointerException if the projectConfigPlus is null
     */

    private ExecutionResult evaluate( EvaluationDeclaration declaration,
                                      String rawDeclaration,
                                      Canceller canceller )
    {
        Objects.requireNonNull( declaration );
        Objects.requireNonNull( canceller );

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
        // Use this thread pool for slicing pools and to dispatch metric tasks as ArrayBlockingQueue operates a FIFO
        // policy. If dependent tasks (slicing) are queued ahead of independent ones (metrics) in the same pool, there
        // is a DEADLOCK probability. Likewise, use a separate thread pool for dispatching pools and completing tasks
        // within pools with the same number of threads in each.

        // Inner readers may create additional thread factories (e.g., archives).
        ThreadFactory readingFactory = new BasicThreadFactory.Builder()
                .namingPattern( "Outer Reading Thread %d" )
                .build();
        ThreadFactory slicingFactory = new BasicThreadFactory.Builder()
                .namingPattern( "Slicing Thread %d" )
                .build();
        ThreadFactory metricFactory = new BasicThreadFactory.Builder()
                .namingPattern( "Metric Thread %d" )
                .build();
        ThreadFactory productFactory = new BasicThreadFactory.Builder()
                .namingPattern( "Format Writing Thread %d" )
                .build();

        SystemSettings settings = this.getSystemSettings();

        // Create some unbounded work queues. For evaluations that produce statistics faster than subscribers (e.g.,
        // statistics format writers) can consume them, production is flow controlled explicitly via the statistics
        // messaging. See #95867. There is a separate thread pool for each of several themed tasks within an evaluation
        // pipeline that are resource-intensive
        BlockingQueue<Runnable> poolQueue = new LinkedBlockingQueue<>();
        BlockingQueue<Runnable> slicingQueue = new LinkedBlockingQueue<>();
        BlockingQueue<Runnable> metricQueue = new LinkedBlockingQueue<>();
        BlockingQueue<Runnable> productQueue = new LinkedBlockingQueue<>();
        // Queue should be large enough to allow join call to be reached with zero or few rejected submissions to the
        // executor service.
        BlockingQueue<Runnable> readingQueue = new ArrayBlockingQueue<>( 100_000 );

        // Create some thread pools to perform the work required by different parts of the evaluation pipeline

        // Thread pool for reading formats
        RejectedExecutionHandler readingHandler = new ThreadPoolExecutor.CallerRunsPolicy();
        ExecutorService readingExecutor = new ThreadPoolExecutor( settings.getMaximumReadThreads(),
                                                                  settings.getMaximumReadThreads(),
                                                                  settings.getPoolObjectLifespan(),
                                                                  TimeUnit.MILLISECONDS,
                                                                  readingQueue,
                                                                  readingFactory,
                                                                  readingHandler );
        // Thread pool that processes pools of pairs
        ExecutorService poolExecutor = new ThreadPoolExecutor( settings.getMaximumPoolThreads(),
                                                               settings.getMaximumPoolThreads(),
                                                               settings.getPoolObjectLifespan(),
                                                               TimeUnit.MILLISECONDS,
                                                               poolQueue,
                                                               poolFactory );
        // Thread pool that performs slicing/dicing and transforming of pooled data
        ExecutorService slicingExecutor = new ThreadPoolExecutor( settings.getMaximumSlicingThreads(),
                                                                  settings.getMaximumSlicingThreads(),
                                                                  0,
                                                                  TimeUnit.SECONDS,
                                                                  slicingQueue,
                                                                  slicingFactory );
        // Thread pool that processes metrics
        ExecutorService metricExecutor = new ThreadPoolExecutor( settings.getMaximumMetricThreads(),
                                                                 settings.getMaximumMetricThreads(),
                                                                 settings.getPoolObjectLifespan(),
                                                                 TimeUnit.MILLISECONDS,
                                                                 metricQueue,
                                                                 metricFactory );
        // Thread pool that generates products, such as statistics formats
        ExecutorService productExecutor = new ThreadPoolExecutor( settings.getMaximumProductThreads(),
                                                                  settings.getMaximumProductThreads(),
                                                                  settings.getPoolObjectLifespan(),
                                                                  TimeUnit.MILLISECONDS,
                                                                  productQueue,
                                                                  productFactory );

        // Conditional executors
        ExecutorService samplingUncertaintyExecutor = null;
        ExecutorService ingestExecutor = null;

        if ( Objects.nonNull( declaration.sampleUncertainty() ) )
        {
            ThreadFactory resamplingFactory = new BasicThreadFactory.Builder()
                    .namingPattern( "Sampling Uncertainty Thread %d" )
                    .build();
            int threadCount = settings.getMaximumSamplingUncertaintyThreads();
            samplingUncertaintyExecutor = java.util.concurrent.Executors.newFixedThreadPool( threadCount,
                                                                                             resamplingFactory );
        }

        // Create database services if needed
        DatabaseServices databaseServices = null;
        DatabaseLockManager lockManager;
        if ( settings.isUseDatabase() )
        {
            Database innerDatabase = this.getDatabase();

            // Register for cancellation
            canceller.setDatabase( innerDatabase );

            lockManager = DatabaseLockManager.from( settings,
                                                    innerDatabase::getRawConnection );

            databaseServices = new DatabaseServices( innerDatabase, lockManager );

            // Create an ingest executor
            ThreadFactory ingestFactory =
                    new BasicThreadFactory.Builder().namingPattern( "Ingesting Thread %d" )
                                                    .build();
            // Queue should be large enough to allow join() call to be reached with zero or few rejected submissions to the
            // executor service.
            BlockingQueue<Runnable> ingestQueue = new ArrayBlockingQueue<>( settings.getMaximumIngestThreads() );

            RejectedExecutionHandler ingestHandler = new ThreadPoolExecutor.CallerRunsPolicy();
            ingestExecutor = new ThreadPoolExecutor( settings.getMaximumIngestThreads(),
                                                     settings.getMaximumIngestThreads(),
                                                     settings.getPoolObjectLifespan(),
                                                     TimeUnit.MILLISECONDS,
                                                     ingestQueue,
                                                     ingestFactory,
                                                     ingestHandler );
        }
        else
        {
            // Dummy lock manager for in-memory evaluations
            lockManager = new DatabaseLockManagerNoop();
        }

        // Reduce our set of executors to one object
        Executors executors = new Executors( readingExecutor,
                                             ingestExecutor,
                                             poolExecutor,
                                             slicingExecutor,
                                             metricExecutor,
                                             productExecutor,
                                             samplingUncertaintyExecutor );

        // Register the executors for cancellation
        canceller.setEvaluationExecutors( executors );

        String projectHash;
        Set<Path> pathsWrittenTo = new TreeSet<>();
        ScheduledExecutorService monitoringService = null;
        try
        {
            // Mark the WRES as doing an evaluation.
            lockManager.lockShared( DatabaseType.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );

            // Monitor the task queues if required
            if ( System.getProperty( "wres.monitorTaskQueues" ) != null )
            {
                monitoringService = new ScheduledThreadPoolExecutor( 1 );
                QueueMonitor queueMonitor = new QueueMonitor( this.getDatabase(),
                                                              poolQueue,
                                                              slicingQueue,
                                                              metricQueue,
                                                              productQueue );

                monitoringService.scheduleAtFixedRate( queueMonitor,
                                                       1,
                                                       500,
                                                       TimeUnit.MILLISECONDS );
            }

            // Perform the evaluation
            Pair<Set<Path>, String> innerPathsAndProjectHash =
                    EvaluationUtilities.evaluate( settings,
                                                  databaseServices,
                                                  declaration,
                                                  executors,
                                                  this.getBrokerConnectionFactory(),
                                                  monitor,
                                                  canceller );
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
                                            userInputException,
                                            canceller.cancelled() );
        }
        catch ( RuntimeException | IOException | SQLException internalException )
        {
            String message = "Could not complete project execution";
            InternalWresException internalWresException = new InternalWresException( message, internalException );
            monitor.setFailed();
            monitor.commit();
            return ExecutionResult.failure( declaration.label(),
                                            rawDeclaration,
                                            internalWresException,
                                            canceller.cancelled() );
        }
        // Shutdown
        finally
        {
            Canceller.closeGracefully( monitoringService );
            Canceller.closeGracefully( readingExecutor );
            Canceller.closeGracefully( ingestExecutor );
            Canceller.closeGracefully( productExecutor );
            Canceller.closeGracefully( metricExecutor );
            Canceller.closeGracefully( slicingExecutor );
            Canceller.closeGracefully( poolExecutor );
            Canceller.closeGracefully( samplingUncertaintyExecutor );
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
     *
     * @param readingExecutor the executor for reading data sources
     * @param ingestExecutor the executor for ingesting data into a database
     * @param poolExecutor the executor for completing pools
     * @param slicingExecutor the executor for slicing/dicing/transforming datasets
     * @param metricExecutor the executor for computing metrics
     * @param productExecutor the executor for writing products or formats
     * @param samplingUncertaintyExecutor the executor for calculating sampling uncertainties
     */

    record Executors( ExecutorService readingExecutor,
                      ExecutorService ingestExecutor,
                      ExecutorService poolExecutor,
                      ExecutorService slicingExecutor,
                      ExecutorService metricExecutor,
                      ExecutorService productExecutor,
                      ExecutorService samplingUncertaintyExecutor )
    {
    }
}
