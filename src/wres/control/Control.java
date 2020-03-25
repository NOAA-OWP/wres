package wres.control;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.eclipse.persistence.internal.sessions.factories.model.project.ProjectConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.ProjectConfigPlus;
import wres.config.Validation;
import wres.control.ProcessorHelper.ExecutorServices;
import wres.io.concurrency.Executor;
import wres.io.utilities.Database;
import wres.io.utilities.NoDataException;
import wres.system.DatabaseConnectionSupplier;
import wres.system.DatabaseLockManager;
import wres.system.SystemSettings;

/**
 * A complete implementation of a processing pipeline originating from one or more {@link ProjectConfig}.
 *
 * @author james.brown@hydrosolved.com
 * @author jesse
 */
public class Control implements Function<String[], Integer>,
                                Consumer<ProjectConfigPlus>,
                                Supplier<Set<Path>>
{
    private final SystemSettings systemSettings;
    private final Database database;
    private final Executor executor;

    private final Set<Path> pathsWrittenTo = new HashSet<>();

    public Control( SystemSettings systemSettings,
                    Database database,
                    Executor executor )
    {
        Objects.requireNonNull( systemSettings );
        Objects.requireNonNull( database );
        Objects.requireNonNull( executor );
        this.systemSettings = systemSettings;
        this.database = database;
        this.executor = executor;
    }

    private SystemSettings getSystemSettings()
    {
        return this.systemSettings;
    }

    private Database getDatabase()
    {
        return this.database;
    }

    private Executor getExecutor()
    {
        return this.executor;
    }

    /**
     * Processes one or more projects whose paths are provided in the input arguments.
     * possible TODO: propagate exceptions and return void rather than Integer
     * @param args the paths to one or more project configurations
     * @throws UserInputException when WRES detects problem with project config
     * @throws InternalWresException when WRES detects problem not with project
     */

    @Override
    public Integer apply(final String[] args)
    {
        if ( args.length != 1 )
        {
            LOGGER.error( "Please correct project configuration file name and "
                          + "pass it like this: "
                          + "bin/wres.bat execute c:/path/to/config1.xml " );
            return 1; // Or return 400 - Bad Request (see #41467)
        }

        String evaluationConfigArgument = args[0].trim();

        ProjectConfigPlus projectConfigPlus;

        // Subject to team approval (if you would prefer to *not* overload
        // the "execute" command, add a new method instead)
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
                LOGGER.error( "Failed to unmarshal project configuration from command line argument.",
                              ioe );
                return 1;
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
                LOGGER.error( "Failed to unmarshal project configuration from {}.",
                              configPath, ioe );
                return 1; // Or return 400 - Bad Request (see #41467)
            }
        }

        // Display the raw configuration in logs. Issue #56900.
        LOGGER.info( "{}", projectConfigPlus.getRawConfig() );
        LOGGER.info( "Successfully unmarshalled project configuration from {}"
                     + ", validating further...",
                     projectConfigPlus );

        SystemSettings systemSettings = this.getSystemSettings();

        // Validate unmarshalled configurations
        final boolean validated =
                Validation.isProjectValid( systemSettings, projectConfigPlus );

        if ( validated )
        {
            LOGGER.info( "Successfully validated project configuration from {}. "
                         + "Beginning execution...",
                         projectConfigPlus );
        }
        else
        {
            LOGGER.error( "Validation failed for project configuration from {}.",
                          projectConfigPlus );
            return 1; // Or return 400 - Bad Request (see #41467)
        }

        this.accept( projectConfigPlus );

        return 0;
    }


    /**
     * Runs a WRES project.
     * @param projectConfigPlus the project configuration to run
     * @throws UserInputException when WRES detects problem with project config
     * @throws InternalWresException when WRES detects problem not with project
     */

    @Override
    public void accept( ProjectConfigPlus projectConfigPlus )
    {
        // Build a processing pipeline
        // Essential to use a separate thread pool for thresholds and metrics as ArrayBlockingQueue operates a FIFO 
        // policy. If dependent tasks (thresholds) are queued ahead of independent ones (metrics) in the same pool, 
        // there is a DEADLOCK probability
        ThreadFactory featureFactory = runnable -> new Thread( runnable, "Feature Thread" );
        ThreadFactory pairFactory = runnable -> new Thread( runnable, "Pair Thread" );
        ThreadFactory thresholdFactory = runnable -> new Thread( runnable, "Threshold Dispatch Thread" );
        ThreadFactory metricFactory = runnable -> new Thread( runnable, "Metric Thread" );
        ThreadFactory productFactory = runnable -> new Thread( runnable, "Product Thread" );

        SystemSettings systemSettings = this.getSystemSettings();

        // Name our queues in order to easily monitor them
        BlockingQueue<Runnable> featureQueue =new ArrayBlockingQueue<>( systemSettings
                                                                                .maximumThreadCount() * 5 );
        BlockingQueue<Runnable> pairQueue = new ArrayBlockingQueue<>( systemSettings.maximumThreadCount() * 5 );
        BlockingQueue<Runnable> thresholdQueue = new LinkedBlockingQueue<>();
        BlockingQueue<Runnable> metricQueue = new ArrayBlockingQueue<>( systemSettings.maximumThreadCount() * 5 );
        BlockingQueue<Runnable> productQueue = new ArrayBlockingQueue<>( systemSettings.maximumThreadCount() * 5 );

        // Processes features
        ThreadPoolExecutor featureExecutor = new ThreadPoolExecutor( systemSettings.maximumThreadCount(),
                                                                  systemSettings.maximumThreadCount(),
                                                                  systemSettings.poolObjectLifespan(),
                                                                  TimeUnit.MILLISECONDS,
                                                                  featureQueue,
                                                                  featureFactory );
        
        // Processes pairs       
        ThreadPoolExecutor pairExecutor = new ThreadPoolExecutor( systemSettings.maximumThreadCount(),
                                                                  systemSettings.maximumThreadCount(),
                                                                  systemSettings.poolObjectLifespan(),
                                                                  TimeUnit.MILLISECONDS,
                                                                  pairQueue,
                                                                  pairFactory );

        // Dispatches thresholds
        ThreadPoolExecutor thresholdExecutor = new ThreadPoolExecutor( 1,
                                                                       1,
                                                                       0,
                                                                       TimeUnit.SECONDS,
                                                                       thresholdQueue,
                                                                       thresholdFactory );

        // Processes metrics
        ThreadPoolExecutor metricExecutor = new ThreadPoolExecutor( systemSettings.maximumThreadCount(),
                                                                    systemSettings.maximumThreadCount(),
                                                                    systemSettings.poolObjectLifespan(),
                                                                    TimeUnit.MILLISECONDS,
                                                                    metricQueue,
                                                                    metricFactory );

        // Processes products
        ThreadPoolExecutor productExecutor = new ThreadPoolExecutor( systemSettings.maximumThreadCount(),
                                                                     systemSettings.maximumThreadCount(),
                                                                     systemSettings.poolObjectLifespan(),
                                                                     TimeUnit.MILLISECONDS,
                                                                     productQueue,
                                                                     productFactory );

        // Set the rejection policy to run in the caller, slowing producers           
        featureExecutor.setRejectedExecutionHandler( new ThreadPoolExecutor.CallerRunsPolicy() );
        pairExecutor.setRejectedExecutionHandler( new ThreadPoolExecutor.CallerRunsPolicy() );
        metricExecutor.setRejectedExecutionHandler( new ThreadPoolExecutor.CallerRunsPolicy() );
        productExecutor.setRejectedExecutionHandler( new ThreadPoolExecutor.CallerRunsPolicy() );

        ScheduledExecutorService monitoringService = new ScheduledThreadPoolExecutor( 1 );

        Database database = this.getDatabase();
        Executor executor = this.getExecutor();
        QueueMonitor queueMonitor = new QueueMonitor( database,
                                                      executor,
                                                      featureQueue,
                                                      pairQueue,
                                                      thresholdQueue,
                                                      metricQueue,
                                                      productQueue );

        Supplier<Connection> connectionSupplier = new DatabaseConnectionSupplier( systemSettings );
        DatabaseLockManager lockManager = new DatabaseLockManager( connectionSupplier );

        try
        {
            // Mark the WRES as doing an evaluation.
            lockManager.lockShared( DatabaseLockManager.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );

            // Reduce our set of executors to one object
            ExecutorServices executors = new ExecutorServices( featureExecutor,
                                                               pairExecutor,
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
            Set<Path> innerPathsWrittenTo =
                    ProcessorHelper.processProjectConfig( systemSettings,
                                                          database,
                                                          executor,
                                                          projectConfigPlus,
                                                          executors,
                                                          lockManager );

            this.pathsWrittenTo.addAll( innerPathsWrittenTo );
            LOGGER.info( "Wrote the following output: {}", this.pathsWrittenTo );
            lockManager.unlockShared( DatabaseLockManager.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );
        }
        catch ( WresProcessingException | IOException | NoDataException | SQLException internalException )
        {
            String message = "Could not complete project execution";
            LOGGER.error( "{} due to:", message, internalException );
            Control.addException( internalException );
            throw new InternalWresException( message, internalException );
        }
        catch ( ProjectConfigException userException )
        {
            String message = "Please correct the project configuration.";
            LOGGER.error( "{} Details:", message, userException );
            Control.addException( userException );
            throw new UserInputException( message, userException );
        }
        // Shutdown
        finally
        {
            shutDownGracefully( monitoringService );
            shutDownGracefully( productExecutor );
            shutDownGracefully( metricExecutor );
            shutDownGracefully( thresholdExecutor );
            shutDownGracefully( pairExecutor );
            shutDownGracefully( featureExecutor );
            lockManager.shutdown();
        }
    }

    @Override
    public Set<Path> get()
    {
        return Collections.unmodifiableSet( this.pathsWrittenTo );
    }

    /**
     * Kill off the executors passed in even if there are remaining tasks.
     *
     * @param executor the executor to shutdown
     */
    private static void shutDownGracefully(final ExecutorService executor)
    {
        if(Objects.isNull(executor))
        {
            return;
        }

        executor.shutdown();

        try
        {
            executor.awaitTermination( 5, TimeUnit.SECONDS );
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted while shutting down {}", executor, ie );
            Control.addException( ie );
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Default logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger(Control.class);

    private static final Object EXCEPTION_LOCK = new Object();
    private static List<Exception> encounteredExceptions;

    private static void addException(Exception recentException)
    {
        synchronized ( EXCEPTION_LOCK )
        {
            if (Control.encounteredExceptions == null)
            {
                Control.encounteredExceptions = new ArrayList<>(  );
            }

            Control.encounteredExceptions.add(recentException);
        }
    }

    public static List<Exception> getMostRecentException()
    {
        synchronized ( EXCEPTION_LOCK )
        {
            if (Control.encounteredExceptions == null)
            {
                Control.encounteredExceptions = new ArrayList<>(  );
            }

            return Collections.unmodifiableList(Control.encounteredExceptions);
        }
    }

    private static class QueueMonitor implements Runnable
    {
        private final Database database;
        private final Executor executor;
        private final Queue<?> featureQueue;
        private final Queue<?> pairQueue;
        private final Queue<?> thresholdQueue;
        private final Queue<?> metricQueue;
        private final Queue<?> productQueue;

        QueueMonitor( Database database,
                      Executor executor,
                      Queue<?> featureQueue,
                      Queue<?> pairQueue,
                      Queue<?> thresholdQueue,
                      Queue<?> metricQueue,
                      Queue<?> productQueue )
        {
            this.database = database;
            this.executor = executor;
            this.featureQueue = featureQueue;
            this.pairQueue = pairQueue;
            this.thresholdQueue = thresholdQueue;
            this.metricQueue = metricQueue;
            this.productQueue = productQueue;
        }

        @Override
        public void run()
        {
            int featureCount = 0;
            int pairCount = 0;
            int ioCount = executor.getIoExecutorQueueTaskCount();
            int databaseCount = database.getDatabaseQueueTaskCount();
            int hiPriCount = executor.getHiPriIoExecutorQueueTaskCount();
            int thresholdCount = 0;
            int metricCount = 0;
            int productCount = 0;

            if ( this.featureQueue != null )
            {
                featureCount = this.featureQueue.size();
            }

            if ( this.pairQueue != null )
            {
                pairCount = this.pairQueue.size();
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

            LOGGER.info( "IoQ={}, IoHiPriQ={}, FeatureQ={}, PairQ={}, DatabaseQ={}, ThresholdQ={}, MetricQ={}, ProductQ={}",
                         ioCount, hiPriCount, featureCount, pairCount, databaseCount, thresholdCount, metricCount, productCount );
        }
    }
}
