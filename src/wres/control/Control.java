package wres.control;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.Validation;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.engine.statistics.metric.ConfigMapper;
import wres.engine.statistics.metric.MetricConfigurationException;
import wres.io.config.ConfigHelper;
import wres.io.config.ProjectConfigPlus;
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
     *
     * @param args the paths to one or more project configurations
     */
    @Override
    public Integer apply(final String[] args)
    {
        // Unmarshal the configurations
        final List<ProjectConfigPlus> projectConfiggies = getProjects(args);

        if ( !projectConfiggies.isEmpty() )
        {
            LOGGER.info( "Successfully unmarshalled {} project "
                         + "configuration(s),  validating further...",
                         projectConfiggies.size() );
        }
        else
        {
            LOGGER.error( "Please correct project configuration files and pass "
                          + "them in the command line like this: "
                          + "bin/wres.bat execute c:/path/to/config1.xml "
                          + "c:/path/to/config2.xml" );
            return 1;
        }

        // Validate unmarshalled configurations
        final boolean validated = Validation.validateProjects(projectConfiggies);
        if ( validated )
        {
            LOGGER.info( "Successfully validated {} project configuration(s). "
                         + "Beginning execution...",
                         projectConfiggies.size() );
        }
        else
        {
            LOGGER.error( "One or more projects did not pass validation.");
            return 1;
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
            // Iterate through the configurations
            for(final ProjectConfigPlus projectConfigPlus: projectConfiggies)
            {
                // Process the next configuration
                ProcessorHelper.processProjectConfig( projectConfigPlus,
                                                      pairExecutor,
                                                      thresholdExecutor,
                                                      metricExecutor );

            }

            return 0;
        }
        catch ( WresProcessingException | IOException e )
        {

            LOGGER.error( "Could not complete project execution:", e );

            return -1;
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
     * Get project configurations from command line file args. If there are no command line args, look in System
     * Settings for directory to scan for configurations.
     *
     * @param args the paths to the projects
     * @return the successfully found, read, unmarshalled project configs
     */
    private static List<ProjectConfigPlus> getProjects(final String[] args)
    {
        final List<Path> existingProjectFiles = new ArrayList<>();

        if(args.length > 0)
        {
            for(final String arg: args)
            {
                final Path path = Paths.get(arg);
                if(path.toFile().exists())
                {
                    existingProjectFiles.add(path);
                }
                else
                {
                    LOGGER.warn("Project configuration file {} does not exist!", path);
                }
            }
        }

        final List<ProjectConfigPlus> projectConfiggies = new ArrayList<>();

        for(final Path path: existingProjectFiles)
        {
            try
            {
                final ProjectConfigPlus projectConfigPlus = ProjectConfigPlus.from(path);
                projectConfiggies.add(projectConfigPlus);
            }
            catch(final IOException ioe)
            {
                LOGGER.error("Could not read project configuration: ", ioe);
            }
        }
        return projectConfiggies;
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
            Thread.currentThread().interrupt();
        }
    }


    /**
     * Locates the metric configuration corresponding to the input {@link MetricConstants} or null if no corresponding
     * configuration could be found. If the configuration contains a {@link MetricConfigName#ALL_VALID}, the 
     * prescribed metric identifier is ignored and the configuration is returned for 
     * {@link MetricConfigName#ALL_VALID}.
     * 
     * @param metric the metric
     * @param config the project configuration
     * @return the metric configuration or null
     */

    private static MetricConfig getNamedConfigOrAllValid( final MetricConstants metric, final ProjectConfig config )
    {
        // Deal with MetricConfigName.ALL_VALID first
        MetricConfig allValid = ConfigHelper.getMetricConfigByName( config, MetricConfigName.ALL_VALID );
        if ( allValid != null )
        {
            return allValid;
        }
        // Find the corresponding configuration
        final Optional<MetricConfig> returnMe = config.getOutputs().getMetric().stream().filter( a -> {
            try
            {
                return metric.equals( ConfigMapper.from( a.getName() ) );
            }
            catch ( final MetricConfigurationException e )
            {
                LOGGER.error( "Could not map metric name '{}' to metric configuration.", metric, e );
                return false;
            }
        } ).findFirst();
        return returnMe.isPresent() ? returnMe.get() : null;
    }


    /**
     * Returns true if the given config has one or more of given output type.
     * @param config the config to search
     * @param type the type of output to look for
     * @return true if the output type is present, false otherwise
     */

    private static boolean configNeedsThisTypeOfOutput( ProjectConfig config,
                                                        DestinationType type )
    {
        if ( config.getOutputs() == null
             || config.getOutputs().getDestination() == null )
        {
            LOGGER.debug( "No destinations specified for config {}", config );
            return false;
        }

        for ( DestinationConfig d : config.getOutputs().getDestination() )
        {
            if ( d.getType().equals( type ) )
            {
                return true;
            }
        }

        return false;
    }

    /**
     * Default logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger(Control.class);

    /**
     * Default data factory.
     */

    private static final DataFactory DATA_FACTORY = DefaultDataFactory.getInstance();

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

}
