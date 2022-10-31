package wres.pipeline;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import wres.config.ProjectConfigException;
import wres.config.ProjectConfigPlus;
import wres.config.generated.ProjectConfig;
import wres.events.broker.BrokerConnectionFactory;
import wres.config.Validation;
import wres.io.concurrency.Executor;
import wres.io.utilities.Database;
import wres.system.DatabaseLockManager;
import wres.system.DatabaseType;
import wres.system.SystemSettings;

/**
 * A complete implementation of a processing pipeline originating from one or more {@link ProjectConfig}.
 *
 * @author James Brown
 * @author Jesse Bickel
 */
public class Evaluator
{
    /** System settings.*/
    private final SystemSettings systemSettings;

    /** Database instance.*/
    private final Database database;

    /** Executor that executes i/o. TODO: reconsider this one.*/
    private final Executor executor;

    /** Broker connections.*/
    private final BrokerConnectionFactory brokerConnectionFactory;

    /**
     * Creates an instance.
     * @param systemSettings the system settings, not null
     * @param database the database, if required
     * @param executor an executor, not null
     * @param brokerConnectionFactory a broker connection factory, not null
     * @throws NullPointerException if any required input is null
     */
    public Evaluator( SystemSettings systemSettings,
                      Database database,
                      Executor executor,
                      BrokerConnectionFactory brokerConnectionFactory )
    {
        Objects.requireNonNull( systemSettings );
        Objects.requireNonNull( executor );
        Objects.requireNonNull( brokerConnectionFactory );

        if ( systemSettings.isInDatabase() )
        {
            Objects.requireNonNull( database );
        }

        this.systemSettings = systemSettings;
        this.database = database;
        this.executor = executor;
        this.brokerConnectionFactory = brokerConnectionFactory;
    }

    /**
     * Processes one or more projects whose paths are provided in the input arguments.
     * possible TODO: propagate exceptions and return void rather than Integer
     * @param args the paths to one or more project configurations
     * @return the result of the execution
     * @throws UserInputException when WRES detects problem with project config
     * @throws InternalWresException when WRES detects problem not with project
     */

    public ExecutionResult evaluate( final String[] args )
    {
        // Create a record of failure, but only commit if a failure actually occurs
        EvaluationEvent failure = EvaluationEvent.of();
        failure.begin();

        if ( args.length != 1 )
        {
            String message = "Please correct project configuration file name and "
                             + "pass it like this: "
                             + "bin/wres.bat execute c:/path/to/config1.xml";
            LOGGER.error( message );
            UserInputException e = new UserInputException( message );
            failure.setFailed();
            failure.commit();
            return ExecutionResult.failure( e ); // Or return 400 - Bad Request (see #41467)
        }

        String evaluationConfigArgument = args[0].trim();

        ProjectConfigPlus projectConfigPlus;

        // Declaration passed directly as an argument
        if ( evaluationConfigArgument.startsWith( "<?xml " ) )
        {
            // Successfully detected a project passed directly as an argument.
            try
            {
                projectConfigPlus = ProjectConfigPlus.from( evaluationConfigArgument,
                                                            "command line argument" );
            }
            catch ( IOException ioe )
            {
                String message = "Failed to unmarshal project configuration from command line argument.";
                LOGGER.error( message, ioe );
                UserInputException e = new UserInputException( message, ioe );
                failure.setFailed();
                failure.commit();
                return ExecutionResult.failure( e );
            }
        }
        else
        {
            Path configPath = Paths.get( evaluationConfigArgument );

            try
            {
                // Unmarshal the configuration
                projectConfigPlus = ProjectConfigPlus.from( configPath );
            }
            catch ( IOException ioe )
            {
                String message = "Failed to unmarshal project configuration from "
                                 + configPath.toString();
                LOGGER.error( message, ioe );
                UserInputException e = new UserInputException( message, ioe );
                failure.setFailed();
                failure.commit();
                return ExecutionResult.failure( e );
            }
        }

        // Display the raw configuration in logs. Issue #56900.
        LOGGER.info( "{}", projectConfigPlus.getRawConfig() );
        LOGGER.info( "Successfully unmarshalled project configuration from {}"
                     + ", validating further...",
                     projectConfigPlus );

        SystemSettings innerSystemSettings = this.getSystemSettings();

        if ( innerSystemSettings.isInMemory() )
        {
            LOGGER.info( "Running project {} in memory.", projectConfigPlus );
        }

        // Validate unmarshalled configurations
        final boolean validated =
                Validation.isProjectValid( innerSystemSettings, projectConfigPlus );

        if ( validated )
        {
            LOGGER.info( "Successfully validated project configuration from {}. "
                         + "Beginning execution...",
                         projectConfigPlus );
        }
        else
        {
            String message = "Validation failed for project configuration from "
                             + projectConfigPlus;
            LOGGER.error( message );
            UserInputException e = new UserInputException( message );
            failure.setFailed();
            failure.commit();
            // #108090
            return ExecutionResult.failure( projectConfigPlus.getProjectConfig()
                                                             .getName(),
                                            e );
        }

        return this.evaluate( projectConfigPlus );
    }


    /**
     * Runs a WRES project.
     * @param projectConfigPlus the project configuration to run
     * @return the result of the execution
     * @throws NullPointerException if the projectConfigPlus is null
     */

    public ExecutionResult evaluate( ProjectConfigPlus projectConfigPlus )
    {
        Objects.requireNonNull( projectConfigPlus );

        EvaluationEvent monitor = EvaluationEvent.of();
        monitor.begin();

        // Build a processing pipeline
        // Essential to use a separate thread pool for thresholds and metrics as ArrayBlockingQueue operates a FIFO 
        // policy. If dependent tasks (thresholds) are queued ahead of independent ones (metrics) in the same pool, 
        // there is a DEADLOCK probability. Likewise, use a separate thread pool for dispatching pools and completing
        // tasks within pools with the same number of threads in each.

        ThreadFactory poolFactory = new BasicThreadFactory.Builder()
                                                                    .namingPattern( "Pool Thread %d" )
                                                                    .build();

        ThreadFactory thresholdFactory = new BasicThreadFactory.Builder()
                                                                         .namingPattern( "Threshold Dispatch Thread %d" )
                                                                         .build();
        ThreadFactory metricFactory = new BasicThreadFactory.Builder()
                                                                      .namingPattern( "Metric Thread %d" )
                                                                      .build();
        ThreadFactory productFactory = new BasicThreadFactory.Builder()
                                                                       .namingPattern( "Product Thread %d" )
                                                                       .build();
        SystemSettings innerSystemSettings = this.getSystemSettings();

        // Create some unbounded work queues. For evaluations that produce faster than they consume, production is flow 
        // controlled explicitly via the statistics messaging. See #95867.
        BlockingQueue<Runnable> poolQueue = new LinkedBlockingQueue<>();
        BlockingQueue<Runnable> thresholdQueue = new LinkedBlockingQueue<>();
        BlockingQueue<Runnable> metricQueue = new LinkedBlockingQueue<>();
        BlockingQueue<Runnable> productQueue = new LinkedBlockingQueue<>();

        // Processes pools
        ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor( innerSystemSettings.getMaximumPoolThreads(),
                                                                  innerSystemSettings.getMaximumPoolThreads(),
                                                                  innerSystemSettings.poolObjectLifespan(),
                                                                  TimeUnit.MILLISECONDS,
                                                                  poolQueue,
                                                                  poolFactory );

        // Dispatches thresholds
        ThreadPoolExecutor thresholdExecutor = new ThreadPoolExecutor( innerSystemSettings.getMaximumThresholdThreads(),
                                                                       innerSystemSettings.getMaximumThresholdThreads(),
                                                                       0,
                                                                       TimeUnit.SECONDS,
                                                                       thresholdQueue,
                                                                       thresholdFactory );

        // Processes metrics
        ThreadPoolExecutor metricExecutor = new ThreadPoolExecutor( innerSystemSettings.getMaximumMetricThreads(),
                                                                    innerSystemSettings.getMaximumMetricThreads(),
                                                                    innerSystemSettings.poolObjectLifespan(),
                                                                    TimeUnit.MILLISECONDS,
                                                                    metricQueue,
                                                                    metricFactory );

        // Processes products
        ThreadPoolExecutor productExecutor = new ThreadPoolExecutor( innerSystemSettings.getMaximumProductThreads(),
                                                                     innerSystemSettings.getMaximumProductThreads(),
                                                                     innerSystemSettings.poolObjectLifespan(),
                                                                     TimeUnit.MILLISECONDS,
                                                                     productQueue,
                                                                     productFactory );

        ScheduledExecutorService monitoringService = new ScheduledThreadPoolExecutor( 1 );

        Database innerDatabase = this.getDatabase();
        Executor ioExecutor = this.getExecutor();
        QueueMonitor queueMonitor = new QueueMonitor( innerDatabase,
                                                      ioExecutor,
                                                      poolQueue,
                                                      thresholdQueue,
                                                      metricQueue,
                                                      productQueue );

        DatabaseLockManager lockManager =
                DatabaseLockManager.from( innerSystemSettings,
                                          () -> innerDatabase.getRawConnection() );

        // Compress database services into one object
        DatabaseServices databaseServices = new DatabaseServices( innerDatabase, lockManager );
        String projectHash = null;
        Set<Path> pathsWrittenTo = new TreeSet<>();
        try
        {
            // Mark the WRES as doing an evaluation.
            lockManager.lockShared( DatabaseType.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );

            // Reduce our set of executors to one object
            Executors executors = new Executors( ioExecutor,
                                                 poolExecutor,
                                                 thresholdExecutor,
                                                 metricExecutor,
                                                 productExecutor );

            if ( System.getProperty( "wres.monitorTaskQueues" ) != null )
            {
                monitoringService.scheduleAtFixedRate( queueMonitor,
                                                       1,
                                                       500,
                                                       TimeUnit.MILLISECONDS );
            }

            // Process the configuration
            Pair<Set<Path>, String> innerPathsAndProjectHash = ProcessorHelper.processEvaluation( innerSystemSettings,
                                                                                                  databaseServices,
                                                                                                  projectConfigPlus,
                                                                                                  executors,
                                                                                                  this.getBrokerConnectionFactory(),
                                                                                                  monitor );
            pathsWrittenTo.addAll( innerPathsAndProjectHash.getLeft() );
            projectHash = innerPathsAndProjectHash.getRight();
            monitor.setDataHash( projectHash );
            monitor.setResources( pathsWrittenTo );

            lockManager.unlockShared( DatabaseType.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );
        }
        catch ( ProjectConfigException userException )
        {
            String message = "Please correct the project configuration.";
            UserInputException userInputException = new UserInputException( message, userException );
            monitor.setFailed();
            monitor.commit();
            return ExecutionResult.failure( projectConfigPlus.getProjectConfig().getName(),
                                            userInputException );
        }
        catch ( RuntimeException | IOException | SQLException internalException )
        {
            String message = "Could not complete project execution";
            InternalWresException internalWresException = new InternalWresException( message, internalException );
            monitor.setFailed();
            monitor.commit();
            return ExecutionResult.failure( projectConfigPlus.getProjectConfig().getName(),
                                            internalWresException );
        }
        // Shutdown
        finally
        {
            shutDownGracefully( monitoringService );
            shutDownGracefully( productExecutor );
            shutDownGracefully( metricExecutor );
            shutDownGracefully( thresholdExecutor );
            shutDownGracefully( poolExecutor );
            lockManager.shutdown();
        }

        monitor.setSucceeded();
        monitor.commit();
        return ExecutionResult.success( projectConfigPlus.getProjectConfig()
                                                         .getName(),
                                        projectHash,
                                        pathsWrittenTo );
    }

    /**
     * Kill off the executors passed in even if there are remaining tasks.
     *
     * @param executor the executor to shutdown
     */
    private static void shutDownGracefully( final ExecutorService executor )
    {
        Objects.requireNonNull( executor );
        executor.shutdown();

        try
        {
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
     * Default logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( Evaluator.class );

    private static class QueueMonitor implements Runnable
    {
        private final Database database;
        private final Executor executor;
        private final Queue<?> poolQueue;
        private final Queue<?> thresholdQueue;
        private final Queue<?> metricQueue;
        private final Queue<?> productQueue;

        QueueMonitor( Database database,
                      Executor executor,
                      Queue<?> poolQueue,
                      Queue<?> thresholdQueue,
                      Queue<?> metricQueue,
                      Queue<?> productQueue )
        {
            this.database = database;
            this.executor = executor;
            this.poolQueue = poolQueue;
            this.thresholdQueue = thresholdQueue;
            this.metricQueue = metricQueue;
            this.productQueue = productQueue;
        }

        @Override
        public void run()
        {
            int poolCount = 0;
            int ioCount = executor.getIoExecutorQueueTaskCount();
            int databaseCount = database.getDatabaseQueueTaskCount();
            int hiPriCount = executor.getHiPriIoExecutorQueueTaskCount();
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

            LOGGER.info( "IoQ={}, IoHiPriQ={}, PoolQ={}, DatabaseQ={}, ThresholdQ={}, MetricQ={}, ProductQ={}",
                         ioCount,
                         hiPriCount,
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
     */

    static class DatabaseServices
    {
        /**The database instance.**/
        private final Database database;

        /**The Database lock manager instance.**/
        private final DatabaseLockManager databaseLockManager;

        /**
         * Build an instance.
         * 
         * @param database the database
         * @param databaseLockManager the database lock manager
         * @throws NullPointerException if either input is null
         */

        DatabaseServices( Database database, DatabaseLockManager databaseLockManager )
        {
            this.database = database;
            this.databaseLockManager = databaseLockManager;
        }

        /**
         * @return the database instance.
         */

        Database getDatabase()
        {
            return this.database;
        }

        /**
         * @Return the database lock manager.
         */

        DatabaseLockManager getDatabaseLockManager()
        {
            return this.databaseLockManager;
        }
    }

    /**
     * A value object that a) reduces count of args for some methods and
     * b) provides names for those objects. Can be removed if we can reduce the
     * count of dependencies in some of our methods, or if we prefer to see all
     * dependencies clearly laid out in the method signature.
     */

    static class Executors
    {
        /**
         * Executor for input/output operations, such as ingest.
         */

        private final Executor ioExecutor;

        /**
         * The pool executor.
         */
        private final ExecutorService poolExecutor;

        /**
         * The threshold executor.
         */
        private final ExecutorService thresholdExecutor;

        /**
         * The metric executor.
         */
        private final ExecutorService metricExecutor;

        /**
         * The product executor.
         */
        private final ExecutorService productExecutor;

        /**
         * Build. 
         * 
         * @param ioExecutor the executor for io operations
         * @param poolExecutor the outer pool executor
         * @param thresholdExecutor the threshold executor
         * @param metricExecutor the metric executor
         * @param productExecutor the product executor
         */
        Executors( Executor ioExecutor,
                   ExecutorService poolExecutor,
                   ExecutorService thresholdExecutor,
                   ExecutorService metricExecutor,
                   ExecutorService productExecutor )
        {
            this.ioExecutor = ioExecutor;
            this.poolExecutor = poolExecutor;
            this.thresholdExecutor = thresholdExecutor;
            this.metricExecutor = metricExecutor;
            this.productExecutor = productExecutor;
        }

        /**
         * Returns the {@link ExecutorService} for pool tasks.
         * @return the outer pool executor
         */

        ExecutorService getPoolExecutor()
        {
            return this.poolExecutor;
        }

        /**
         * Returns the {@link ExecutorService} for thresholds.
         * @return the threshold executor
         */

        ExecutorService getThresholdExecutor()
        {
            return this.thresholdExecutor;
        }

        /**
         * Returns the {@link ExecutorService} for metrics.
         * @return the metric executor
         */

        ExecutorService getMetricExecutor()
        {
            return this.metricExecutor;
        }

        /**
         * Returns the {@link ExecutorService} for products.
         * @return the product executor
         */

        ExecutorService getProductExecutor()
        {
            return this.productExecutor;
        }

        /**
         * Returns the {@link Executor} for io operations.
         * @return the io executor
         */

        Executor getIoExecutor()
        {
            return this.ioExecutor;
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
     * @return the executor.
     */
    private Executor getExecutor()
    {
        return this.executor;
    }

    /**
     * @return the broker connections.
     */
    private BrokerConnectionFactory getBrokerConnectionFactory()
    {
        return this.brokerConnectionFactory;
    }

}
