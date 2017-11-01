package wres;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ohd.hseb.charter.ChartEngine;
import ohd.hseb.charter.ChartEngineException;
import ohd.hseb.hefs.utils.xml.GenericXMLReadingHandlerException;
import wres.config.ProjectConfigException;
import wres.config.Validation;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.Feature;
import wres.config.generated.MetricConfig;
import wres.config.generated.PlotTypeSelection;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MapKey;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.MetricInput;
import wres.datamodel.MetricOutput;
import wres.datamodel.MetricOutputForProjectByLeadThreshold;
import wres.datamodel.MetricOutputMapByLeadThreshold;
import wres.datamodel.MetricOutputMetadata;
import wres.datamodel.MetricOutputMultiMapByLeadThreshold;
import wres.datamodel.MultiVectorOutput;
import wres.datamodel.ScalarOutput;
import wres.datamodel.VectorOutput;
import wres.engine.statistics.metric.MetricConfigurationException;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricProcessor;
import wres.engine.statistics.metric.MetricProcessorByLeadTime;
import wres.io.Operations;
import wres.io.config.ConfigHelper;
import wres.io.config.ProjectConfigPlus;
import wres.io.config.SystemSettings;
import wres.io.utilities.InputGenerator;
import wres.io.writing.ChartWriter;
import wres.io.writing.ChartWriter.ChartWritingException;
import wres.io.writing.CommaSeparated;
import wres.util.ProgressMonitor;
import wres.vis.ChartEngineFactory;

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

        String projectRawConfig = "";

        try
        {
            // Iterate through the configurations
            for(final ProjectConfigPlus projectConfigPlus: projectConfiggies)
            {
                projectRawConfig = projectConfigPlus.getRawConfig();

                // Process the next configuration
                processProjectConfig( projectConfigPlus,
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
     * Processes a {@link ProjectConfigPlus} using a prescribed {@link ExecutorService} for each of the pairs, 
     * thresholds and metrics.
     * 
     * @param projectConfigPlus the project configuration
     * @param pairExecutor the {@link ExecutorService} for processing pairs
     * @param thresholdExecutor the {@link ExecutorService} for processing thresholds
     * @param metricExecutor the {@link ExecutorService} for processing metrics
     * @throws IOException when an issue occurs during ingest
     * @throws WresProcessingException when an issue occurs during processing
     */

    private void   processProjectConfig( final ProjectConfigPlus projectConfigPlus,
                                         final ExecutorService pairExecutor,
                                         final ExecutorService thresholdExecutor,
                                         final ExecutorService metricExecutor )
            throws IOException
    {

        final ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();

        ProgressMonitor.setShowStepDescription( true );
        ProgressMonitor.resetMonitor();

        LOGGER.debug( "Beginning ingest for project {}...",
                      projectConfigPlus.getPath() );

        // Need to ingest first
        Operations.ingest(projectConfig);

        LOGGER.debug( "Finished ingest for project {}...",
                      projectConfigPlus.getPath() );

        // TODO: Implement way of iterating through features correctly
        Feature feature = projectConfig.getPair()
                                       .getFeature()
                                       .get( 0 );

        ProgressMonitor.setShowStepDescription( false );
        ProgressMonitor.resetMonitor();
        processFeature( feature,
                        projectConfigPlus,
                        pairExecutor,
                        thresholdExecutor,
                        metricExecutor );
    }

    /**
     * Processes a {@link ProjectConfigPlus} for a specific {@link Feature} using a prescribed {@link ExecutorService}
     * for each of the pairs, thresholds and metrics.
     * 
     * @param feature the feature to process
     * @param projectConfigPlus the project configuration
     * @param pairExecutor the {@link ExecutorService} for processing pairs
     * @param thresholdExecutor the {@link ExecutorService} for processing thresholds
     * @param metricExecutor the {@link ExecutorService} for processing metrics
     * @throws WresProcessingException when an error occurs during processing
     */
    private void processFeature( final Feature feature,
                                 final ProjectConfigPlus projectConfigPlus,
                                 final ExecutorService pairExecutor,
                                 final ExecutorService thresholdExecutor,
                                 final ExecutorService metricExecutor )
    {

        final ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();

        if(LOGGER.isInfoEnabled())
        {
            // This will NOT work if we are using any other feature format
            LOGGER.info( "Processing feature '{}'", feature.getLid() );
        }

        // Sink for the results: the results are added incrementally to an immutable store via a builder
        // Some output types are processed at the end of the pipeline, others after each input is processed
        // Construct a processor that retains all output types required at the end of the pipeline: SCALAR and VECTOR
        // TODO: support additional processor types
        MetricProcessorByLeadTime processor = null;
        try
        {
            processor = MetricFactory.getInstance(DATA_FACTORY).getMetricProcessorByLeadTime(projectConfig,
                                                                                   thresholdExecutor,          
                                                                                   metricExecutor,
                                                                                   MetricOutputGroup.SCALAR,
                                                                                   MetricOutputGroup.VECTOR);
        }
        catch(final MetricConfigurationException e)
        {
            throw new WresProcessingException( "While processing the metric configuration:",
                                               e );
        }

        InputGenerator metricInputs;

        // Build an InputGenerator for the next feature
        if ( projectConfig.getInputs().getBaseline() != null )
        {
            metricInputs = Operations.getInputs( projectConfig,
                                                 feature );
        }
        else
        {
            metricInputs = Operations.getInputs( projectConfig,
                                                 feature );
        }

        // Queue the various tasks by lead time (lead time is the pooling dimension for metric calculation here)
        final List<CompletableFuture<?>> listOfFutures = new ArrayList<>(); //List of futures to test for completion

        // Iterate
        for(final Future<MetricInput<?>> nextInput: metricInputs)
        {
            // Complete all tasks asynchronously:
            // 1. Get some pairs from the database
            // 2. Compute the metrics
            // 3. Process any intermediate verification results
            final CompletableFuture<Void> c =
                                            CompletableFuture.supplyAsync(new PairsByLeadProcessor(nextInput),
                                                                          pairExecutor)
                                                             .thenApplyAsync(processor, pairExecutor)
                                                             .thenAcceptAsync( new IntermediateResultProcessor( feature,
                                                                                                                projectConfigPlus ),
                                                                              pairExecutor)
                                                             .thenAccept(
                                                                         aVoid -> ProgressMonitor.completeStep() );

            //Add the future to the list
            listOfFutures.add(c);
        }

        // Complete all tasks or one exceptionally: join() is blocking, representing a final sink for the results
        try
        {
            doAllOrException(listOfFutures).join();
        }
        catch(final CompletionException e)
        {
            String message = "Error while processing feature "
                             + ConfigHelper.getFeatureDescription( feature );
            throw new WresProcessingException( message, e );
        }

        // Generated stored output if available
        if ( processor.hasStoredMetricOutput() )
        {
            processCachedProducts( projectConfigPlus, processor, feature );
        }
        else if ( LOGGER.isInfoEnabled() )
        {
            String description = ConfigHelper.getFeatureDescription( feature );
            LOGGER.info( "No cached outputs to generate for feature: '"
                         + description + "'" );
        }
    }

    /**
     * Completes the processing of products, including graphical and numerical products, at the end of a processing 
     * pipeline using the cached {@link MetricOutput} stored in the {@link MetricProcessor}, and in keeping with 
     * the supplied {@link ProjectConfig}.
     * 
     * @param projectConfigPlus the project configuration
     * @param processor the processor with cached metric outputs
     * @param feature the feature being processed
     */

    private static void   processCachedProducts( ProjectConfigPlus projectConfigPlus,
                                               MetricProcessorByLeadTime processor,
                                               Feature feature )
    {
        ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();
        //Generate graphical output
        if ( configNeedsThisTypeOfOutput( projectConfig,
                                          DestinationType.GRAPHIC ) )
        {
            LOGGER.debug( "Beginning to build charts for feature {}...",
                          feature.getLid() );

            processCachedCharts( feature,
                                 projectConfigPlus,
                                 processor,
                                 MetricOutputGroup.SCALAR,
                                 MetricOutputGroup.VECTOR );

            LOGGER.debug( "Finished building charts for feature {}.",
                          feature.getLid() );
        }

        //Generate numerical output
        if ( configNeedsThisTypeOfOutput( projectConfig,
                                          DestinationType.NUMERIC ) )
        {
            LOGGER.debug( "Beginning to write numeric output for feature {}...",
                          feature.getLid() );

            try
            {
                CommaSeparated.writeOutputFiles( projectConfig,
                                                 feature,
                                                 processor.getStoredMetricOutput() );

            }
            catch ( final ProjectConfigException pce )
            {
                throw new WresProcessingException(
                                                   "Please include valid numeric output clause(s) in"
                                                   + " project configuration. Example: <destination>"
                                                   + "<path>c:/Users/myname/wres_output/</path>"
                                                   + "</destination>",
                                                   pce );
            }
            catch ( IOException e )
            {
                throw new WresProcessingException( "While writing output files: ",
                                                   e );
            }

            LOGGER.debug( "Finished writing numeric output for feature {}.",
                          feature.getLid() );
        }

        if ( LOGGER.isInfoEnabled() )
        {
            LOGGER.info( "Completed processing of feature '{}'.", feature.getLid() );
        }
    }

    /**
     * Processes all charts for which metric outputs were cached across successive calls to a {@link MetricProcessor}.
     * 
     * @param feature the feature for which the charts are defined
     * @param projectConfigPlus the project configuration
     * @param processor the {@link MetricProcessor} that contains the results for all chart types
     * @param outGroup the {@link MetricOutputGroup} for which charts are required
     * @throws WresProcessingException when an error occurs during processing
     */

    private static void   processCachedCharts( final Feature feature,
                                               final ProjectConfigPlus projectConfigPlus,
                                               final MetricProcessorByLeadTime processor,
                                               final MetricOutputGroup... outGroup)
    {
        if(!processor.hasStoredMetricOutput())
        {
            LOGGER.warn( "No cached outputs to process. ");
            return;
        }

        try
        {
            for(final MetricOutputGroup nextGroup: outGroup)
            {
                if(processor.getStoredMetricOutput().hasOutput(nextGroup))
                {
                    switch(nextGroup)
                    {
                        case SCALAR:
                            processScalarCharts( feature,
                                                 projectConfigPlus,
                                                 processor.getStoredMetricOutput()
                                                          .getScalarOutput() );
                            break;
                        case VECTOR:
                            processVectorCharts( feature,
                                                 projectConfigPlus,
                                                 processor.getStoredMetricOutput()
                                                          .getVectorOutput() );
                            break;
                        case MULTIVECTOR:
                            processMultiVectorCharts( feature,
                                                      projectConfigPlus,
                                                      processor.getStoredMetricOutput()
                                                               .getMultiVectorOutput() );
                            break;
                        default:
                            LOGGER.warn( "Unsupported chart type {}.",
                                         nextGroup );
                            return;
                    }
                }
                else
                {
                    //Return silently: chart type not necessarily configured
                    return;
                }
            }
        }
        catch(final InterruptedException e)
        {
            LOGGER.warn( "Interrupted while processing charts.", e );
            Thread.currentThread().interrupt();
        }
        catch(final ExecutionException e)
        {
            throw new WresProcessingException( "Error while processing charts:", e);
        }
    }

    /**
     * Processes a set of charts associated with {@link ScalarOutput} across multiple metrics, forecast lead times, and
     * thresholds, stored in a {@link MetricOutputMultiMapByLeadThreshold}.
     * 
     * @param feature the feature for which the chart is defined
     * @param projectConfigPlus the project configuration
     * @param scalarResults the scalar outputs
     * @throws WresProcessingException when an error occurs during processing
     */

    private static void   processScalarCharts( final Feature feature,
                                               final ProjectConfigPlus projectConfigPlus,
                                               final MetricOutputMultiMapByLeadThreshold<ScalarOutput> scalarResults)
    {

        //Check for results
        if(Objects.isNull(scalarResults))
        {
            LOGGER.warn("No scalar outputs from which to generate charts.");
            return;
        }

        ProjectConfig config = projectConfigPlus.getProjectConfig();

        // Build charts
        try
        {
            for(final Map.Entry<MapKey<MetricConstants>, MetricOutputMapByLeadThreshold<ScalarOutput>> e: scalarResults.entrySet())
            {
                List<DestinationConfig> destinations =
                        ConfigHelper.getGraphicalDestinations( config );

                for ( DestinationConfig dest : destinations )
                {
                    final String graphicsString = projectConfigPlus.getGraphicsStrings().get(dest);
                    // Build the chart engine
                    final MetricConfig nextConfig = getMetricConfiguration(e.getKey().getKey(), config);
                    PlotTypeSelection plotType = null;
                    String templateResourceName = null;
                    if(!Objects.isNull(nextConfig))
                    {
                        plotType = nextConfig.getPlotType();
                        templateResourceName = nextConfig.getTemplateResourceName();
                    }
                    final ChartEngine engine = ChartEngineFactory.buildGenericScalarOutputChartEngine(e.getValue(),
                                                                                                      DATA_FACTORY,
                                                                                                      plotType,
                                                                                                      templateResourceName,
                                                                                                      graphicsString);
                    //Build the output
                    File destDir = ConfigHelper.getDirectoryFromDestinationConfig( dest );
                    Path outputImage = Paths.get( destDir.toString(),
                                                  ConfigHelper.getFeatureDescription( feature )
                                                  + "_"
                                                  + e.getKey()
                                                  .getKey().name()
                                                  + "_"
                                                  + config.getInputs()
                                                  .getRight()
                                                  .getVariable()
                                                  .getValue()
                                                  + ".png");
                    ChartWriter.writeChart(outputImage, engine, dest);
                }
            }
        }
        catch( ChartEngineException
                | GenericXMLReadingHandlerException
                | ChartWritingException
                | ProjectConfigException e )
        {
            throw new WresProcessingException( "Error while generating scalar charts:", e);
        }
    }

    /**
     * Processes a set of charts associated with {@link VectorOutput} across multiple metrics, forecast lead times, and
     * thresholds, stored in a {@link MetricOutputMultiMapByLeadThreshold}. these.
     * 
     * @param feature the feature for which the chart is defined
     * @param projectConfigPlus the project configuration
     * @param vectorResults the scalar outputs
     * @throws WresProcessingException when an error occurs during processing
     */

    private static void   processVectorCharts( final Feature feature,
                                               final ProjectConfigPlus projectConfigPlus,
                                               final MetricOutputMultiMapByLeadThreshold<VectorOutput> vectorResults)
    {
        //Check for results
        if(Objects.isNull(vectorResults))
        {
            LOGGER.warn("No vector outputs from which to generate charts.");
            return;
        }

        ProjectConfig config = projectConfigPlus.getProjectConfig();

        // Build charts
        try
        {
            for(final Map.Entry<MapKey<MetricConstants>, MetricOutputMapByLeadThreshold<VectorOutput>> e: vectorResults.entrySet())
            {
                List<DestinationConfig> destinations =
                        ConfigHelper.getGraphicalDestinations( config );

                for ( DestinationConfig dest : destinations )
                {
                    final String graphicsString = projectConfigPlus.getGraphicsStrings().get(dest);
                    // Build the chart engine
                    final MetricConfig nextConfig = getMetricConfiguration(e.getKey().getKey(), config);
                    PlotTypeSelection plotType = null;
                    String templateResourceName = null;
                    if(!Objects.isNull(nextConfig))
                    {
                        plotType = nextConfig.getPlotType();
                        templateResourceName = nextConfig.getTemplateResourceName();
                    }
                    final Map<Object, ChartEngine> engines =
                        ChartEngineFactory.buildVectorOutputChartEngine(e.getValue(),
                                                                        DATA_FACTORY,
                                                                        plotType,
                                                                        templateResourceName,
                                                                        graphicsString);
                    // Build the outputs
                    for(final Map.Entry<Object, ChartEngine> nextEntry: engines.entrySet())
                    {
                        // Build the output file name
                        File destDir = ConfigHelper.getDirectoryFromDestinationConfig( dest );
                        Path outputImage = Paths.get( destDir.toString(),
                                                      ConfigHelper.getFeatureDescription( feature )
                                                      + "_"
                                                      + e.getKey()
                                                      .getKey().name()
                                                      + "_"
                                                      + config.getInputs()
                                                      .getRight()
                                                      .getVariable()
                                                      .getValue()
                                                      + "_"
                                                      + ((MetricConstants)nextEntry.getKey()).name()
                                                      + ".png" );
                        ChartWriter.writeChart(outputImage, nextEntry.getValue(), dest);
                    }
                }
            }
        }
        catch ( ChartEngineException
                | GenericXMLReadingHandlerException
                | ChartWritingException
                | ProjectConfigException e )
        {
            throw new WresProcessingException( "Error while generating vector charts:", e );
        }
    }

    /**
     * Processes a set of charts associated with {@link MultiVectorOutput} across multiple metrics, forecast lead times,
     * and thresholds, stored in a {@link MetricOutputMultiMapByLeadThreshold}.
     * 
     * @param feature the feature for which the chart is defined
     * @param projectConfigPlus the project configuration
     * @param multiVectorResults the scalar outputs
     * @throws WresProcessingException if the processing completed unsuccessfully
     */

    private static void   processMultiVectorCharts( final Feature feature,
                                                    final ProjectConfigPlus projectConfigPlus,
                                                    final MetricOutputMultiMapByLeadThreshold<MultiVectorOutput> multiVectorResults)
    {
        //Check for results
        if(Objects.isNull(multiVectorResults))
        {
            LOGGER.warn( "No multi-vector outputs from which to generate charts." );
            return;
        }

        ProjectConfig config = projectConfigPlus.getProjectConfig();

        // Build charts
        try
        {
            // Build the charts for each metric
            for(final Map.Entry<MapKey<MetricConstants>, MetricOutputMapByLeadThreshold<MultiVectorOutput>> e: multiVectorResults.entrySet())
            {
                List<DestinationConfig> destinations =
                        ConfigHelper.getGraphicalDestinations( config );

                for ( DestinationConfig dest : destinations )
                {
                    final String graphicsString = projectConfigPlus.getGraphicsStrings().get(dest);
                    // Build the chart engine
                    final MetricConfig nextConfig = getMetricConfiguration(e.getKey().getKey(), config);
                    PlotTypeSelection plotType = null;
                    String templateResourceName = null;
                    if(!Objects.isNull(nextConfig))
                    {
                        plotType = nextConfig.getPlotType();
                        templateResourceName = nextConfig.getTemplateResourceName();
                    }

                    final Map<Object, ChartEngine> engines =
                        ChartEngineFactory.buildMultiVectorOutputChartEngine(e.getValue(),
                                                                             DATA_FACTORY,
                                                                             plotType,
                                                                             templateResourceName,
                                                                             graphicsString);
                    // Build the outputs
                    for(final Map.Entry<Object, ChartEngine> nextEntry: engines.entrySet())
                    {
                        // Build the output file name
                        File destDir = ConfigHelper.getDirectoryFromDestinationConfig( dest );
                        Path outputImage = Paths.get( destDir.toString(),
                                                      ConfigHelper.getFeatureDescription( feature )
                                                      + "_"
                                                      + e.getKey()
                                                      .getKey().name()
                                                      + "_"
                                                      + config.getInputs()
                                                      .getRight()
                                                      .getVariable()
                                                      .getValue()
                                                      + "_"
                                                      + nextEntry.getKey()
                                                      + ".png" );
                        ChartWriter.writeChart(outputImage, nextEntry.getValue(), dest);
                    }
                }
            }
        }
        catch ( ChartEngineException
                | ChartWritingException
                | ProjectConfigException e)
        {
            throw new WresProcessingException( "Error while generating multi-vector charts:", e );
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

                    // TODO: Needs to be temporary; used to log execution information
                    if(path.toFile().isFile())
                    {
                        MainFunctions.setProjectPath(path.toAbsolutePath().toString());
                    }
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
     * Task that waits for pairs to arrive and then returns them.
     */
    private static class PairsByLeadProcessor implements Supplier<MetricInput<?>>
    {
        /**
         * The future metric input.
         */
        private final Future<MetricInput<?>> input;

        /**
         * Construct.
         * 
         * @param input the future metric input
         */

        private PairsByLeadProcessor(final Future<MetricInput<?>> input)
        {
            this.input = input;
        }

        @Override
        public MetricInput<?> get()
        {
            MetricInput<?> nextInput = null;
            try
            {
                nextInput = input.get();
                if(LOGGER.isDebugEnabled())
                {
                    LOGGER.debug("Completed processing of pairs for feature '{}' at lead time {}.",
                                nextInput.getMetadata().getIdentifier().getGeospatialID(),
                                nextInput.getMetadata().getLeadTimeInHours());
                }
            }
            catch(final InterruptedException e)
            {
                LOGGER.warn( "Interrupted while processing pairs.", e );
                Thread.currentThread().interrupt();
            }
            catch( ExecutionException e )
            {
                throw new WresProcessingException( "While processing pairs:", e );
            }

            return nextInput;
        }
    }

    /**
     * A task the processes an intermediate set of results.
     */

    private static class IntermediateResultProcessor implements Consumer<MetricOutputForProjectByLeadThreshold>
    {

        /**
         * The project configuration.
         */

        private final ProjectConfigPlus projectConfigPlus;

        /**
         * The feature.
         */

        private final Feature feature;

        /**
         * Construct.
         * 
         * @param feature the feature
         * @param projectConfigPlus the project configuration
         */

        private IntermediateResultProcessor(final Feature feature, final ProjectConfigPlus projectConfigPlus)
        {
            this.projectConfigPlus = projectConfigPlus;
            this.feature = feature;
        }

        @Override
        public void accept(final MetricOutputForProjectByLeadThreshold input)
        {
            MetricOutputMetadata meta = null;
            try
            {
                if ( input.hasOutput( MetricOutputGroup.MULTIVECTOR )
                     && configNeedsThisTypeOfOutput( projectConfigPlus.getProjectConfig(),
                                                     DestinationType.GRAPHIC ) )
                {
                    processMultiVectorCharts(feature,
                                             projectConfigPlus,
                                             input.getMultiVectorOutput());
                    meta = input.getMultiVectorOutput().entrySet().iterator().next().getValue().getMetadata();
                    if(LOGGER.isDebugEnabled())
                    {
                        LOGGER.debug("Completed processing of intermediate metrics results for feature '{}' at lead time {}.",
                                     meta.getIdentifier().getGeospatialID(),
                                     meta.getLeadTimeInHours());
                    }
                }
            }
            catch( InterruptedException ie)
            {
                LOGGER.warn( "Interrupted while processing intermediate results" );
                Thread.currentThread().interrupt();
            }
            catch ( ExecutionException ee )
            {
                throw new WresProcessingException(
                        "While processing intermediate results: ",
                        ee );
            }
        }
    }

    /**
     * An exception representing that execution of a step failed.
     * Needed because Java 8 Function world does not
     * deal kindly with checked Exceptions.
     */
    private static class WresProcessingException extends RuntimeException
    {
        WresProcessingException( String message, Throwable cause )
        {
            super( message, cause );
        }

        WresProcessingException( String message )
        {
            super( message );
        }
    }

    /**
     * Composes a list of {@link CompletableFuture} so that execution completes when all futures are completed normally
     * or any one future completes exceptionally. None of the {@link CompletableFuture} passed to this utility method
     * should already handle exceptions otherwise the exceptions will not be caught here (i.e. all futures will process
     * to completion).
     *
     * @param futures the futures to compose
     * @return the composed futures
     * @throws CompletionException if completing exceptionally 
     */

    private static CompletableFuture<?> doAllOrException(final List<CompletableFuture<?>> futures)
    {
        //Complete when all futures are completed
        final CompletableFuture<?> allDone =
                                           CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
        //Complete when any of the underlying futures completes exceptionally
        final CompletableFuture<?> oneExceptional = new CompletableFuture<>();
        //Link the two
        for(final CompletableFuture<?> completableFuture: futures)
        {
            //When one completes exceptionally, propagate
            completableFuture.exceptionally(exception -> {
                oneExceptional.completeExceptionally(exception);
                return null;
            });
        }
        //Either all done OR one completes exceptionally
        return CompletableFuture.anyOf(allDone, oneExceptional);
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
     * configuration could be found.
     * 
     * @param metric the metric
     * @param config the project configuration
     * @return the metric configuration or null
     */

    private static MetricConfig getMetricConfiguration(final MetricConstants metric, final ProjectConfig config)
    {
        // Find the corresponding configuration
        final Optional<MetricConfig> returnMe = config.getOutputs().getMetric().stream().filter(a -> {
            try
            {
                return metric.equals(MetricProcessor.fromMetricConfigName(a.getName()));
            }
            catch(final MetricConfigurationException e)
            {
                LOGGER.error("Could not map metric name '{}' to metric configuration.", metric, e);
                return false;
            }
        }).findFirst();
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
     * Log interval.
     */

    public static final long LOG_PROGRESS_INTERVAL_MILLIS = 2000;

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
