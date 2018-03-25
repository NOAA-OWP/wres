package wres.control;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.ProjectConfigPlus;
import wres.config.Validation;
import wres.config.generated.ProjectConfig;
import wres.io.config.SystemSettings;

/**
 * A complete implementation of a processing pipeline originating from one or more {@link ProjectConfig}.
 * 
 * @author james.brown@hydrosolved.com
 * @author jesse
 * @version 0.1
 * @since 0.1
 */
public class Control implements Function<String[], Integer>
{

    /**
     * Processes one or more projects whose paths are provided in the input arguments.
     * possible TODO: propagate exceptions and return void rather than Integer
     * @param args the paths to one or more project configurations
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

        Path configPath = Paths.get( args[0] );
        ProjectConfigPlus projectConfigPlus;

        try
        {
            // Unmarshal the configuration
             projectConfigPlus = ProjectConfigPlus.from( configPath );
        }
        catch ( IOException ioe )
        {
            LOGGER.error( "Failed to unmarshal project configuration at {}.",
                          ioe );
            return 1; // Or return 400 - Bad Request (see #41467)
        }

        LOGGER.info( "Successfully unmarshalled project configuration at {}"
                     + ", validating further...",
                     projectConfigPlus );

        // Validate unmarshalled configurations
        final boolean validated = Validation.isProjectValid( projectConfigPlus );

        if ( validated )
        {
            LOGGER.info( "Successfully validated project configuration at {}. "
                         + "Beginning execution...",
                         projectConfigPlus );
        }
        else
        {
            LOGGER.error( "Validation failed for project configuration at {}.",
                          projectConfigPlus);
            return 1; // Or return 400 - Bad Request (see #41467)
        }

        // Build a processing pipeline
        // Essential to use a separate thread pool for thresholds and metrics as ArrayBlockingQueue operates a FIFO 
        // policy. If dependent tasks (thresholds) are queued ahead of independent ones (metrics) in the same pool, 
        // there is a DEADLOCK probability       
        ThreadFactory pairFactory = runnable -> new Thread( runnable, "Pair Thread" );
        ThreadFactory thresholdFactory = runnable -> new Thread( runnable, "Threshold Dispatch Thread" );
        ThreadFactory metricFactory = runnable -> new Thread( runnable, "Metric Thread" );
        // Processes pairs       
        ThreadPoolExecutor pairExecutor = new ThreadPoolExecutor( SystemSettings.maximumThreadCount(),
                                                                  SystemSettings.maximumThreadCount(),
                                                                  SystemSettings.poolObjectLifespan(),
                                                                  TimeUnit.MILLISECONDS,
                                                                  new ArrayBlockingQueue<>( SystemSettings.maximumThreadCount()
                                                                                            * 5 ),
                                                                  pairFactory );
        // Dispatches thresholds
        ExecutorService thresholdExecutor = Executors.newSingleThreadExecutor( thresholdFactory );
        // Processes metrics
        ThreadPoolExecutor metricExecutor = new ThreadPoolExecutor( SystemSettings.maximumThreadCount(),
                                                                    SystemSettings.maximumThreadCount(),
                                                                    SystemSettings.poolObjectLifespan(),
                                                                    TimeUnit.MILLISECONDS,
                                                                    new ArrayBlockingQueue<>( SystemSettings.maximumThreadCount()
                                                                                              * 5 ),
                                                                    metricFactory );
        // Set the rejection policy to run in the caller, slowing producers
        pairExecutor.setRejectedExecutionHandler( new ThreadPoolExecutor.CallerRunsPolicy() );
        metricExecutor.setRejectedExecutionHandler( new ThreadPoolExecutor.CallerRunsPolicy() );

        try
        {
            // Process the configuration
            ProcessorHelper.processProjectConfig( projectConfigPlus,
                                                  pairExecutor,
                                                  thresholdExecutor,
                                                  metricExecutor );
            return 0; // Or return 200 - OK (see #41467)
        }
        catch ( WresProcessingException | IOException internalException)
        {
            LOGGER.error( "Could not complete project execution due to:", internalException );
            Control.addException( internalException );
            return -1; // Or return 500 - Internal Server Error (see #41467)
        }
        catch ( ProjectConfigException userException )
        {
            LOGGER.error( "Please correct the project configuration. Details:", userException );
            Control.addException( userException );
            return -1; // Or return 400 - Bad Request (see #41467)
        }
        // Shutdown
        finally
        {
            shutDownGracefully(metricExecutor);
            shutDownGracefully(thresholdExecutor);
            shutDownGracefully(pairExecutor);
        }
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
            Control.addException( ie );
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Default logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger(Control.class);

    /**
     * System property used to retrieve max thread count, passed as -D
     */

    public static final String MAX_THREADS_PROP_NAME = "wres.maxThreads";

    /**
     * Maximum threads.
     */

    public static final int MAX_THREADS;

    // Figure out the max threads from property or by default rule.
    // Ideally priority order would be: -D, SystemSettings, default rule.
    static
    {
        final String maxThreadsStr = System.getProperty(MAX_THREADS_PROP_NAME);
        int maxThreads;
        try
        {
            maxThreads = Integer.parseInt(maxThreadsStr);
        }
        catch(final NumberFormatException nfe)
        {
            maxThreads = SystemSettings.maximumThreadCount();
        }

        // Since ForkJoinPool goes ape when this is 2 or less...
        if ( maxThreads >= 3 )
        {
            MAX_THREADS = maxThreads;
        }
        else
        {
            LOGGER.warn( "Java -D property {} or SystemSettings "
                         + "maximumThreadCount was likely less than 1, setting "
                         + " Control.MAX_THREADS to 3",
                         MAX_THREADS_PROP_NAME );

            MAX_THREADS = 3;
        }
    }

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

            Control.encounteredExceptions.addAll( WresProcessingException.getOccurrences() );
            Control.encounteredExceptions.addAll(ProcessorHelper.getEncounteredExceptions());

            return Collections.unmodifiableList(Control.encounteredExceptions);
        }
    }

}
