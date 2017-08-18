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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ohd.hseb.charter.ChartEngine;
import ohd.hseb.charter.ChartEngineException;
import ohd.hseb.charter.ChartTools;
import ohd.hseb.charter.datasource.XYChartDataSourceException;
import ohd.hseb.hefs.utils.xml.GenericXMLReadingHandlerException;
import wres.config.ProjectConfigException;
import wres.config.Validation;
import wres.config.generated.Conditions.Feature;
import wres.config.generated.DestinationConfig;
import wres.config.generated.MetricConfig;
import wres.config.generated.ProjectConfig;
import wres.datamodel.metric.DataFactory;
import wres.datamodel.metric.DefaultDataFactory;
import wres.datamodel.metric.MapBiKey;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricConstants.MetricOutputGroup;
import wres.datamodel.metric.MetricInput;
import wres.datamodel.metric.MetricOutputForProjectByLeadThreshold;
import wres.datamodel.metric.MetricOutputMapByLeadThreshold;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.datamodel.metric.MetricOutputMultiMapByLeadThreshold;
import wres.datamodel.metric.MultiVectorOutput;
import wres.datamodel.metric.ScalarOutput;
import wres.datamodel.metric.VectorOutput;
import wres.engine.statistics.metric.MetricConfigurationException;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricProcessor;
import wres.io.Operations;
import wres.io.config.ProjectConfigPlus;
import wres.io.config.SystemSettings;
import wres.io.utilities.InputGenerator;
import wres.io.writing.CommaSeparated;
import wres.vis.ChartEngineFactory;

/**
 * A complete implementation of a processing pipeline originating from one or more {@link ProjectConfig}. The processing
 * pipeline is constructed with a {@link ForkJoinPool} and uses {@link CompletableFuture} to chain together tasks
 * asynchronously. This allows for work-stealing from I/O tasks that are waiting, and allows for consumers to continue
 * operating as pairs become available, thereby avoiding deadlock and OutOfMemoryException with large datasets.
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
                          + "wres executeConfigProject c:/path/to/config1.xml "
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

        // Process the configurations with a ForkJoinPool

        // Build a processing pipeline

        final ExecutorService processPairExecutor = new ForkJoinPool( MAX_THREADS );
        final ExecutorService metricExecutor = new ForkJoinPool( MAX_THREADS );

        try
        {
            // Iterate through the configurations
            for(final ProjectConfigPlus projectConfigPlus: projectConfiggies)
            {
                // Process the next configuration
                final boolean processed = processProjectConfig(projectConfigPlus, processPairExecutor, metricExecutor);
                if(!processed)
                {
                    return -1;
                }
            }
            return 0;
        }
        // Shutdown
        finally
        {
            shutDownGracefully(processPairExecutor);
            shutDownGracefully(metricExecutor);
        }
    }

    /**
     * Processes a {@link ProjectConfigPlus} using a prescribed {@link ExecutorService}.
     * 
     * @param projectConfigPlus the project configuration
     * @param processPairExecutor the {@link ExecutorService}
     * @param metricExecutor an optional {@link ExecutorService} for processing metrics
     * @return true if the project processed successfully, false otherwise
     */

    private boolean processProjectConfig(final ProjectConfigPlus projectConfigPlus,
                                         final ExecutorService processPairExecutor,
                                         final ExecutorService metricExecutor)
    {

        final ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();

        // Need to ingest first
        final boolean ingestResult = Operations.ingest(projectConfig);

        if(!ingestResult)
        {
            LOGGER.error("Error while attempting to ingest data.");
            return false;
        }

        // Obtain the features
        final List<Feature> features = projectConfig.getConditions().getFeature();

        // Iterate through the features and process them
        for(final Feature nextFeature: features)
        {
            if(!processFeature(nextFeature, projectConfigPlus, processPairExecutor, metricExecutor))
            {
                return false;
            }
        }
        return true;
    }

    /**
     * Processes a {@link ProjectConfigPlus} for a specific {@link Feature} using a prescribed {@link ExecutorService}.
     * 
     * @param feature the feature to process
     * @param projectConfigPlus the project configuration
     * @param processPairExecutor the {@link ExecutorService}
     * @param metricExecutor an optional {@link ExecutorService} for processing metrics
     * @return true if the project processed successfully, false otherwise
     */

    private boolean processFeature(final Feature feature,
                                   final ProjectConfigPlus projectConfigPlus,
                                   final ExecutorService processPairExecutor,
                                   final ExecutorService metricExecutor)
    {

        final ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();

        if(LOGGER.isInfoEnabled())
        {
            LOGGER.info("Processing feature '{}'.", feature.getLocation().getLid());
        }

        // Sink for the results: the results are added incrementally to an immutable store via a builder
        // Some output types are processed at the end of the pipeline, others after each input is processed
        // Construct a processor that retains all output types required at the end of the pipeline: SCALAR and VECTOR
        MetricProcessor processor = null;
        try
        {
            processor = MetricFactory.getInstance(DATA_FACTORY).getMetricProcessor(projectConfig,
                                                                                   metricExecutor,
                                                                                   MetricOutputGroup.SCALAR,
                                                                                   MetricOutputGroup.VECTOR);
        }
        catch(final MetricConfigurationException e)
        {
            LOGGER.error("While processing the metric configuration:", e);
            return false;
        }

        // Build an InputGenerator for the next feature
        final InputGenerator metricInputs = Operations.getInputs(projectConfig, feature);

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
                                                                          processPairExecutor)
                                                             .thenApplyAsync(processor, processPairExecutor)
                                                             .thenAcceptAsync(new IntermediateResultProcessor(feature,
                                                                                                              projectConfigPlus),
                                                                              processPairExecutor);

            //Add the future to the list
            listOfFutures.add(c);
        }

        // Complete all tasks or one exceptionally: join() is blocking, representing a final sink for the results
        try
        {
            doAllOrException(listOfFutures).join();
        }
        catch(final Exception e)
        {
            LOGGER.error("While processing feature '{}':", feature.getLocation().getLid(), e);
            return false;
        }

        // Complete the end-of-pipeline processing
        if(processor.hasStoredMetricOutput())
        {
            processCachedCharts(feature,
                                projectConfigPlus,
                                processor,
                                MetricOutputGroup.SCALAR,
                                MetricOutputGroup.VECTOR);

            try
            {
                CommaSeparated.writeOutputFiles( projectConfig,
                                                 feature,
                                                 processor.getStoredMetricOutput() );

            }
            catch ( final ProjectConfigException pce )
            {
                LOGGER.error( "Please include valid numeric output clause(s) in"
                              + " project configuration. Example: <destination>"
                              + "<path>c:/Users/myname/wres_output/</path>"
                              + "</destination>",
                              pce );
                return false;
            }
            catch ( final InterruptedException ie )
            {
                LOGGER.warn("Interrupted while writing output files.");
                Thread.currentThread().interrupt();
            }
            catch ( IOException | ExecutionException e)
            {
                LOGGER.error("While writing output files: ", e);
                return false;
            }

            if(LOGGER.isInfoEnabled())
            {
                LOGGER.info("Completed processing of feature '{}'.", feature.getLocation().getLid());
            }
        }
        else if(LOGGER.isInfoEnabled())
        {
            LOGGER.info("No cached outputs to generate for feature '{}'.", feature.getLocation().getLid());
        }
        return true;
    }

    /**
     * Processes all charts for which metric outputs were cached across successive calls to a {@link MetricProcessor}.
     * 
     * @param feature the feature for which the charts are defined
     * @param projectConfigPlus the project configuration
     * @param processor the {@link MetricProcessor} that contains the results for all chart types
     * @param outGroup the {@link MetricOutputGroup} for which charts are required
     * @return true it the processing completed successfully, false otherwise
     */

    private static boolean processCachedCharts(final Feature feature,
                                               final ProjectConfigPlus projectConfigPlus,
                                               final MetricProcessor processor,
                                               final MetricOutputGroup... outGroup)
    {
        if(!processor.hasStoredMetricOutput())
        {
            LOGGER.error("No cached outputs to process.");
            return false;
        }

        // True until failed
        boolean returnMe = true;
        try
        {
            for(final MetricOutputGroup nextGroup: outGroup)
            {
                if(processor.getStoredMetricOutput().hasOutput(nextGroup))
                {
                    switch(nextGroup)
                    {
                        case SCALAR:
                            returnMe = returnMe
                                && processScalarCharts(feature,
                                                       projectConfigPlus,
                                                       processor.getStoredMetricOutput().getScalarOutput());
                            break;
                        case VECTOR:
                            returnMe = returnMe
                                && processVectorCharts(feature,
                                                       projectConfigPlus,
                                                       processor.getStoredMetricOutput().getVectorOutput());
                            break;
                        case MULTIVECTOR:
                            returnMe = returnMe
                                && processMultiVectorCharts(feature,
                                                            projectConfigPlus,
                                                            processor.getStoredMetricOutput().getMultiVectorOutput());
                            break;
                        default:
                            LOGGER.error("Unsupported chart type {}.", nextGroup);
                            returnMe = false;
                    }
                }
                else
                {
                    //Return silently: chart type not necessarily configured
                    returnMe = false;
                }
            }
        }
        catch(final InterruptedException e)
        {
            LOGGER.error("Interrupted while processing charts.", e);
            Thread.currentThread().interrupt();
            returnMe = false;
        }
        catch(final ExecutionException e)
        {
            LOGGER.error("While processing charts:", e);
            returnMe = false;
        }
        return returnMe;
    }

    /**
     * Processes a set of charts associated with {@link ScalarOutput} across multiple metrics, forecast lead times, and
     * thresholds, stored in a {@link MetricOutputMultiMapByLeadThreshold}.
     * 
     * @param feature the feature for which the chart is defined
     * @param projectConfigPlus the project configuration
     * @param scalarResults the scalar outputs
     * @return true it the processing completed successfully, false otherwise
     */

    private static boolean processScalarCharts(final Feature feature,
                                               final ProjectConfigPlus projectConfigPlus,
                                               final MetricOutputMultiMapByLeadThreshold<ScalarOutput> scalarResults)
    {

        //Check for results
        if(Objects.isNull(scalarResults))
        {
            LOGGER.error("No scalar outputs from which to generate charts.");
            return false;
        }

        // Build charts
        try
        {
            for(final Map.Entry<MapBiKey<MetricConstants, MetricConstants>, MetricOutputMapByLeadThreshold<ScalarOutput>> e: scalarResults.entrySet())
            {
                final ProjectConfig config = projectConfigPlus.getProjectConfig();
                final DestinationConfig dest = config.getOutputs().getDestination().get(1);
                final String graphicsString = projectConfigPlus.getGraphicsStrings().get(dest);
                // Build the chart engine
                final MetricConfig nextConfig = getMetricConfiguration(e.getKey().getFirstKey(), config);
                if(Objects.isNull(nextConfig))
                {
                    LOGGER.error(MISSING_CONFIGURATION, e.getKey().getFirstKey());
                    return false;
                }
                final ChartEngine engine = ChartEngineFactory.buildGenericScalarOutputChartEngine(e.getValue(),
                                                                                                  DATA_FACTORY,
                                                                                                  nextConfig.getPlotType(),
                                                                                                  nextConfig.getTemplateResourceName(),
                                                                                                  graphicsString);
                //Build the output
                final StringBuilder pathBuilder = new StringBuilder();
                pathBuilder.append(dest.getPath())
                           .append(feature.getLocation().getLid())
                           .append("_")
                           .append(e.getKey().getFirstKey())
                           .append("_")
                           .append(e.getKey().getSecondKey())
                           .append("_")
                           .append(projectConfigPlus.getProjectConfig().getInputs().getRight().getVariable().getValue())
                           .append(".png");
                final Path outputImage = Paths.get(pathBuilder.toString());
                writeChart(outputImage, engine, dest);
            }
        }
        catch(ChartEngineException | GenericXMLReadingHandlerException | XYChartDataSourceException | IOException e)
        {
            LOGGER.error("While generating scalar charts:", e);
            return false;
        }
        return true;
    }

    /**
     * Processes a set of charts associated with {@link VectorOutput} across multiple metrics, forecast lead times, and
     * thresholds, stored in a {@link MetricOutputMultiMapByLeadThreshold}. these.
     * 
     * @param feature the feature for which the chart is defined
     * @param projectConfigPlus the project configuration
     * @param vectorResults the scalar outputs
     * @return true it the processing completed successfully, false otherwise
     */

    private static boolean processVectorCharts(final Feature feature,
                                               final ProjectConfigPlus projectConfigPlus,
                                               final MetricOutputMultiMapByLeadThreshold<VectorOutput> vectorResults)
    {
        //Check for results
        if(Objects.isNull(vectorResults))
        {
            LOGGER.error("No vector outputs from which to generate charts.");
            return false;
        }

        // Build charts
        try
        {
            for(final Map.Entry<MapBiKey<MetricConstants, MetricConstants>, MetricOutputMapByLeadThreshold<VectorOutput>> e: vectorResults.entrySet())
            {
                final ProjectConfig config = projectConfigPlus.getProjectConfig();
                final DestinationConfig dest = config.getOutputs().getDestination().get(1);
                final String graphicsString = projectConfigPlus.getGraphicsStrings().get(dest);
                // Build the chart engine
                final MetricConfig nextConfig = getMetricConfiguration(e.getKey().getFirstKey(), config);
                if(Objects.isNull(nextConfig))
                {
                    LOGGER.error(MISSING_CONFIGURATION, e.getKey().getFirstKey());
                    return false;
                }
                final Map<Object, ChartEngine> engines =
                                                       ChartEngineFactory.buildVectorOutputChartEngine(e.getValue(),
                                                                                                       DATA_FACTORY,
                                                                                                       nextConfig.getPlotType(),
                                                                                                       nextConfig.getTemplateResourceName(),
                                                                                                       graphicsString);
                // Build the outputs
                for(final Map.Entry<Object, ChartEngine> nextEntry: engines.entrySet())
                {
                    // Build the output file name
                    final StringBuilder pathBuilder = new StringBuilder();
                    pathBuilder.append(dest.getPath())
                               .append(feature.getLocation().getLid())
                               .append("_")
                               .append(e.getKey().getFirstKey())
                               .append("_")
                               .append(e.getKey().getSecondKey())
                               .append("_")
                               .append(projectConfigPlus.getProjectConfig()
                                                        .getInputs()
                                                        .getRight()
                                                        .getVariable()
                                                        .getValue())
                               .append("_")
                               .append(nextEntry.getKey())
                               .append(".png");
                    final Path outputImage = Paths.get(pathBuilder.toString());
                    writeChart(outputImage, nextEntry.getValue(), dest);
                }
            }
        }
        catch(ChartEngineException | GenericXMLReadingHandlerException | XYChartDataSourceException | IOException e)
        {
            LOGGER.error("While generating vector charts:", e);
            return false;
        }
        return true;
    }

    /**
     * Processes a set of charts associated with {@link MultiVectorOutput} across multiple metrics, forecast lead times,
     * and thresholds, stored in a {@link MetricOutputMultiMapByLeadThreshold}.
     * 
     * @param feature the feature for which the chart is defined
     * @param projectConfigPlus the project configuration
     * @param multiVectorResults the scalar outputs
     * @return true it the processing completed successfully, false otherwise
     */

    private static boolean processMultiVectorCharts(final Feature feature,
                                                    final ProjectConfigPlus projectConfigPlus,
                                                    final MetricOutputMultiMapByLeadThreshold<MultiVectorOutput> multiVectorResults)
    {
        //Check for results
        if(Objects.isNull(multiVectorResults))
        {
            LOGGER.error("No multi-vector outputs from which to generate charts.");
            return false;
        }

        // Build charts
        try
        {
            // Build the charts for each metric
            for(final Map.Entry<MapBiKey<MetricConstants, MetricConstants>, MetricOutputMapByLeadThreshold<MultiVectorOutput>> e: multiVectorResults.entrySet())
            {
                final ProjectConfig config = projectConfigPlus.getProjectConfig();
                final DestinationConfig dest = config.getOutputs().getDestination().get(1);
                final String graphicsString = projectConfigPlus.getGraphicsStrings().get(dest);
                // Build the chart engine
                final MetricConfig nextConfig = getMetricConfiguration(e.getKey().getFirstKey(), config);
                if(Objects.isNull(nextConfig))
                {
                    LOGGER.error(MISSING_CONFIGURATION, e.getKey().getFirstKey());
                    return false;
                }
                final Map<Object, ChartEngine> engines =
                                                       ChartEngineFactory.buildMultiVectorOutputChartEngine(e.getValue(),
                                                                                                            DATA_FACTORY,
                                                                                                            nextConfig.getPlotType(),
                                                                                                            nextConfig.getTemplateResourceName(),
                                                                                                            graphicsString);
                // Build the outputs
                for(final Map.Entry<Object, ChartEngine> nextEntry: engines.entrySet())
                {
                    // Build the output file name
                    final StringBuilder pathBuilder = new StringBuilder();
                    pathBuilder.append(dest.getPath())
                               .append(feature.getLocation().getLid())
                               .append("_")
                               .append(e.getKey().getFirstKey())
                               .append("_")
                               .append(e.getKey().getSecondKey())
                               .append("_")
                               .append(projectConfigPlus.getProjectConfig()
                                                        .getInputs()
                                                        .getRight()
                                                        .getVariable()
                                                        .getValue())
                               .append("_")
                               .append(nextEntry.getKey())
                               .append(".png");
                    final Path outputImage = Paths.get(pathBuilder.toString());
                    writeChart(outputImage, nextEntry.getValue(), dest);
                }
            }
        }
        catch(ChartEngineException | GenericXMLReadingHandlerException | XYChartDataSourceException | IOException e)
        {
            LOGGER.error("While generating multi-vector charts:", e);
            return false;
        }
        return true;
    }

    /**
     * Writes an output chart to a specified path.
     * 
     * @param outputImage the path to the output image
     * @param engine the chart engine
     * @param dest the destination configuration
     * @throws XYChartDataSourceException
     * @throws ChartEngineException
     * @throws IOException
     */

    private static void writeChart(final Path outputImage,
                                   final ChartEngine engine,
                                   final DestinationConfig dest) throws IOException,
                                                                 ChartEngineException,
                                                                 XYChartDataSourceException
    {
        if(LOGGER.isWarnEnabled() && outputImage.toFile().exists())
        {
            LOGGER.warn("File {} already existed and is being overwritten.", outputImage);
        }

        final File outputImageFile = outputImage.toFile();

        int width = SystemSettings.getDefaultChartWidth();
        int height = SystemSettings.getDefaultChartHeight();

        if(dest.getGraphical() != null && dest.getGraphical().getWidth() != null)
        {
            width = dest.getGraphical().getWidth();
        }
        if(dest.getGraphical() != null && dest.getGraphical().getHeight() != null)
        {
            height = dest.getGraphical().getHeight();
        }
        ChartTools.generateOutputImageFile(outputImageFile, engine.buildChart(), width, height);
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
     * Task that waits for pairs to arrive, computes metrics from them, and then produces any intermediate
     * (within-pipeline) products.
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
                if(LOGGER.isInfoEnabled())
                {
                    LOGGER.info("Completed processing of pairs for feature '{}' at lead time {}.",
                                nextInput.getMetadata().getIdentifier().getGeospatialID(),
                                nextInput.getMetadata().getLeadTime());
                }
            }
            catch(final InterruptedException e)
            {
                LOGGER.error("Interrupted while processing pairs.", e);
                Thread.currentThread().interrupt();
            }
            catch(final Exception e)
            {
                LOGGER.error("While processing pairs:", e);
            }
            return nextInput;
        }
    }

    /**
     * A function that does something with a set of results (in this case, prints to a logger).
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
                if(input.hasOutput(MetricOutputGroup.MULTIVECTOR))
                {
                    processMultiVectorCharts(feature, projectConfigPlus, input.getMultiVectorOutput());
                    meta = input.getMultiVectorOutput().entrySet().iterator().next().getValue().getMetadata();
                    if(LOGGER.isDebugEnabled())
                    {
                        LOGGER.debug("Completed processing of intermediate metrics results for feature '{}' at lead time {}.",
                                     meta.getIdentifier().getGeospatialID(),
                                     meta.getLeadTime());
                    }
                }
            }
            catch(final Exception e)
            {
                LOGGER.error("While processing intermediate results:", e);
            }
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
        // (There are probably better ways to do this, e.g. awaitTermination)
        int processingSkipped = 0;
        int i = 0;
        boolean deathTime = false;
        while(!executor.isTerminated())
        {
            if(i == 0)
            {
                LOGGER.info("Some processing is finishing up before exit.");
            }

            if(i > 10)
            {
                deathTime = true;
            }

            try
            {
                Thread.sleep(500);
            }
            catch(final InterruptedException ie)
            {
                deathTime = true;
                Thread.currentThread().interrupt();
            }
            finally
            {
                if(deathTime)
                {
                    LOGGER.info("Forcing shutdown.");
                    processingSkipped += executor.shutdownNow().size();
                }
            }
            i++;
        }

        if(processingSkipped > 0)
        {
            LOGGER.info("Abandoned {} processing tasks.", processingSkipped);
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
    
    /**
     * Error message for missing configuration.
     */
    
    private static final String MISSING_CONFIGURATION = "Could not locate the metric configuration for metric {}.";

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
