package wres.control;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.ProjectConfigPlus;
import wres.config.ProjectConfigs;
import wres.config.generated.*;
import wres.control.Control.DatabaseServices;
import wres.datamodel.FeatureTuple;
import wres.datamodel.MetricConstants.StatisticType;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.events.Consumers;
import wres.events.Evaluation;
import wres.eventsbroker.BrokerConnectionFactory;
import wres.io.Operations;
import wres.io.concurrency.Executor;
import wres.io.config.ConfigHelper;
import wres.io.geography.FeatureFinder;
import wres.io.project.Project;
import wres.io.retrieval.UnitMapper;
import wres.io.thresholds.ThresholdReader;
import wres.io.utilities.NoDataException;
import wres.io.writing.SharedSampleDataWriters;
import wres.io.writing.SharedStatisticsWriters;
import wres.io.writing.SharedStatisticsWriters.SharedWritersBuilder;
import wres.io.writing.commaseparated.pairs.PairsWriter;
import wres.io.writing.netcdf.NetcdfOutputWriter;
import wres.io.writing.protobuf.ProtobufWriter;
import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.Outputs;
import wres.system.ProgressMonitor;
import wres.system.SystemSettings;

/**
 * Class with functions to help in generating metrics and processing metric products.
 *
 * TODO: abstract away the functions used for graphical processing to a separate helper, GraphicalProductsHelper.
 *
 * @author james.brown@hydrosolved.com
 * @author jesse.bickel@***REMOVED***
 */
class ProcessorHelper
{
    private static final Logger LOGGER = LoggerFactory.getLogger( ProcessorHelper.class );

    /**
     * Processes a {@link ProjectConfigPlus} using a prescribed {@link ExecutorService} for each of the pairs, 
     * thresholds and metrics.
     *
     * Assumes that a shared lock for evaluation has already been obtained.
     * @param systemSettings the system settings
     * @param databaseServices the database services
     * @param projectConfigPlus the project configuration
     * @param executors the executors
     * @param connections broker connections
     * @return the paths to which outputs were written
     * @throws WresProcessingException if the evaluation processing fails
     * @throws ProjectConfigException if the declaration is incorrect
     * @throws NullPointerException if any input is null
     * @throws IOException if the creation of outputs fails
     */

    static Set<Path> processEvaluation( SystemSettings systemSettings,
                                        DatabaseServices databaseServices,
                                        ProjectConfigPlus projectConfigPlus,
                                        Executors executors,
                                        BrokerConnectionFactory connections )
            throws IOException
    {
        Objects.requireNonNull( systemSettings );
        Objects.requireNonNull( databaseServices );
        Objects.requireNonNull( projectConfigPlus );
        Objects.requireNonNull( executors );
        Objects.requireNonNull( connections );

        Set<Path> returnMe = new TreeSet<>();

        // Create output directory
        Path outputDirectory = ProcessorHelper.createTempOutputDirectory();

        ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();

        // Create a description of the evaluation
        wres.statistics.generated.Evaluation evaluationDescription =
                ProcessorHelper.getEvaluationDescription( systemSettings,
                                                          projectConfigPlus );

        // Create some shared writers
        SharedWriters sharedWriters = ProcessorHelper.getSharedWriters( systemSettings,
                                                                        executors.getIoExecutor(),
                                                                        projectConfig,
                                                                        evaluationDescription,
                                                                        outputDirectory );

        // Obtain any external subscribers that are required for this evaluation.
        Map<DestinationType, Set<String>> externalSubscribers =
                ProcessorHelper.getExternalSubscribers( evaluationDescription,
                                                        systemSettings );

        // Write some statistic types and formats as soon as they become available. Everything else is written on 
        // group/feature completion.
        // During the pipeline, only write types that are not end-of-pipeline types unless they refer to
        // a format that can be written incrementally. Duration scores are always written last because they are not
        // computed until the end of the pipeline.
        // Also ignore formats for which external subscribers are available.
        BiPredicate<StatisticType, DestinationType> incrementalTypes =
                ( type, format ) -> ( type == StatisticType.BOXPLOT_PER_PAIR
                                      || ConfigHelper.getIncrementalFormats( projectConfig ).contains( format ) )
                                    && type != StatisticType.DURATION_SCORE
                                    && !externalSubscribers.containsKey( format );

        // All others types, but again ignoring those for which external subscribers are available 
        BiPredicate<StatisticType, DestinationType> nonIncrementalTypes =
                ( type, format ) -> !externalSubscribers.containsKey( format )
                                    && incrementalTypes.negate().test( type, format );

        // Incremental consumer
        StatisticsConsumer incrementalConsumer = StatisticsConsumer.of( evaluationDescription,
                                                                        projectConfigPlus,
                                                                        incrementalTypes,
                                                                        sharedWriters.getStatisticsWriters(),
                                                                        outputDirectory );

        // Group consumer (group = feature for now)
        StatisticsConsumer groupConsumer = StatisticsConsumer.of( evaluationDescription,
                                                                  projectConfigPlus,
                                                                  nonIncrementalTypes,
                                                                  sharedWriters.getStatisticsWriters(),
                                                                  outputDirectory );

        // TODO: currently there are only logging consumers for both evaluation description events and evaluation status 
        // events. Both of these things could be exposed via the web service API and a corresponding consumer 
        // provided as input to this method, allowing them to bubble up to that API. Also consider adding the 
        // declaration parsing status events into the ProjectConfigPlus instance so they can be published once the 
        // evaluation is created below. These need to be mapped from the ProjectConfigPlus::getValidationEvents.
        Consumers.Builder consumerBuilder = new Consumers.Builder();
        // Add the external subscribers
        externalSubscribers.values()
                           .stream()
                           .flatMap( Set::stream )
                           .forEach( consumerBuilder::addExternalSubscriber );
        // Add the remaining subscribers and build
        Consumers consumerGroup =
                consumerBuilder.addStatusConsumer( ProcessorHelper.getLoggerConsumerForStatusEvents() )
                               .addEvaluationConsumer( ProcessorHelper.getLoggerConsumerForEvaluationEvents() )
                               // Add a regular consumer for statistics that are neither grouped by time window
                               // nor feature. These include box plots per pair and statistics that can be 
                               // written incrementally, such as netCDF and Protobuf
                               .addStatisticsConsumer( next -> incrementalConsumer.accept( List.of( next ) ) )
                               // Add a grouped consumer for statistics that are grouped by feature
                               .addGroupedStatisticsConsumer( groupConsumer )
                               .build();

        // Open an evaluation, to be closed on completion or stopped on exception
        Evaluation evaluation = Evaluation.open( evaluationDescription,
                                                 connections,
                                                 consumerGroup,
                                                 outputDirectory );

        try
        {
            Set<Path> pathsWritten = ProcessorHelper.processProjectConfig( evaluation,
                                                                           systemSettings,
                                                                           databaseServices,
                                                                           projectConfigPlus,
                                                                           executors,
                                                                           sharedWriters,
                                                                           outputDirectory );

            returnMe.addAll( pathsWritten );

            // Wait for the evaluation to conclude
            evaluation.await();

            // Since the shared writers are created here, they should be destroyed here. An attempt should be made to 
            // close them before the finally block because some of these writers may employ a delayed write, which could 
            // still fail exceptionally. Such a failure should stop the evaluation exceptionally. For further context 
            // see #81790-21 and the detailed description in Evaluation.await(), which clarifies that awaiting for an 
            // evaluation to complete does not mean that all consumers have finished their work, only that they have 
            // received all expected messages. If this contract is insufficient (e.g., because of a delayed write
            // implementation), then it may be necessary to promote the underlying consumer/s to an external/outer 
            // subscriber that is responsible for messaging its own lifecycle, rather than delegating that to the 
            // Evaluation instance (which adopts the limited contract described here). An external subscriber within 
            // this jvm/process has the same contract as an external subscriber running in another process/jvm. It 
            // should only report completion when consumption is "done done".
            sharedWriters.close();

            return Collections.unmodifiableSet( returnMe );
        }
        // Allow a user-error to be distinguished separately
        catch ( ProjectConfigException userError )
        {
            // Stop forcibly
            evaluation.stop( userError );

            throw userError;
        }
        // Internal error
        catch ( RuntimeException internalError )
        {
            String evaluationId = "unknown";

            // Stop forcibly
            evaluation.stop( internalError );
            evaluationId = evaluation.getEvaluationId();

            // Decorate and rethrow
            throw new WresProcessingException( "Encountered an error while processing evaluation '"
                                               + evaluationId
                                               + "': ",
                                               internalError );
        }
        finally
        {
            // Close the evaluation always (even if stopped on exception)
            try
            {
                evaluation.close();
            }
            catch ( IOException e )
            {
                LOGGER.error( "Failed to close evaluation {}.", evaluation.getEvaluationId() );
            }

            // Close the shared writers if they weren't closed already
            try
            {
                sharedWriters.close();
            }
            catch ( IOException e )
            {
                LOGGER.error( "Failed to close the shared writers for evaluation {}.", evaluation.getEvaluationId() );
            }

            // Close the other closable consumers
            try
            {
                groupConsumer.close();
            }
            catch ( IOException e )
            {
                LOGGER.error( "Failed to close a group consumer for evaluation {}.", evaluation.getEvaluationId() );
            }
            try
            {
                incrementalConsumer.close();
            }
            catch ( IOException e )
            {
                LOGGER.error( "Failed to close an incremental consumer for evaluation {}.",
                              evaluation.getEvaluationId() );
            }

            // Add the consumer paths written, since consumers are now out-of-band to producers
            returnMe.addAll( incrementalConsumer.get() );
            returnMe.addAll( groupConsumer.get() );

            // Add the paths written by external subscribers
            returnMe.addAll( evaluation.getPathsWrittenByExternalSubscribers() );

            // Clean-up an empty output directory: #67088
            try ( Stream<Path> outputs = Files.list( outputDirectory ) )
            {
                if ( outputs.count() == 0 )
                {
                    // Will only succeed for an empty directory
                    boolean status = Files.deleteIfExists( outputDirectory );

                    LOGGER.debug( "Attempted to remove empty output directory {} with success status: {}",
                                  outputDirectory,
                                  status );
                }
            }

            LOGGER.info( "Wrote the following output: {}", returnMe );
        }
    }

    /**
     * Processes a {@link ProjectConfigPlus} using a prescribed {@link ExecutorService} for each of the pairs, 
     * thresholds and metrics.
     *
     * Assumes that a shared lock for evaluation has already been obtained.
     * @param evaluation the evaluation
     * @param systemSettings the system settings
     * @param databaseServices the database services
     * @param projectConfigPlus the project configuration
     * @param executors the executors
     * @param sharedWriters for writing
     * @param outputDirectory the output directory
     * @throws WresProcessingException if the processing failed for any reason
     * @return the paths to which outputs were written
     */

    private static Set<Path> processProjectConfig( Evaluation evaluation,
                                                   SystemSettings systemSettings,
                                                   DatabaseServices databaseServices,
                                                   ProjectConfigPlus projectConfigPlus,
                                                   Executors executors,
                                                   SharedWriters sharedWriters,
                                                   Path outputDirectory )
    {
        Set<Path> pathsWrittenTo = new HashSet<>();

        try
        {
            final ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();
            ProgressMonitor.setShowStepDescription( false );
            ProgressMonitor.resetMonitor();

            // Get a unit mapper for the declared measurement units
            PairConfig pairConfig = projectConfig.getPair();
            String desiredMeasurementUnit = pairConfig.getUnit();
            UnitMapper unitMapper = UnitMapper.of( databaseServices.getDatabase(), desiredMeasurementUnit );

            // Look up any needed feature correlations, generate a new declaration.
            ProjectConfig featurefulProjectConfig = FeatureFinder.fillFeatures( projectConfig );
            LOGGER.debug( "Filled out features for project. Before: {} After: {}",
                          projectConfig,
                          featurefulProjectConfig );

            LOGGER.debug( "Beginning ingest for project {}...", projectConfigPlus );

            // Need to ingest first
            Project project = Operations.ingest( systemSettings,
                                                 databaseServices.getDatabase(),
                                                 executors.getIoExecutor(),
                                                 featurefulProjectConfig,
                                                 databaseServices.getDatabaseLockManager() );

            Operations.prepareForExecution( project );

            LOGGER.debug( "Finished ingest for project {}...", projectConfigPlus );

            ProgressMonitor.setShowStepDescription( false );

            Set<FeatureTuple> decomposedFeatures = ProcessorHelper.getDecomposedFeatures( project );

            // Read external thresholds from the configuration, per feature
            // Compare on left dataset's feature name only.
            // TODO: consider how better to transmit these thresholds
            // to wres-metrics, given that they are resolved by project configuration that is
            // passed separately to wres-metrics. Options include moving MetricProcessor* to
            // wres-control, since they make processing decisions, or passing ResolvedProject onwards
            ThresholdReader thresholdReader = new ThresholdReader( systemSettings,
                                                                   projectConfig,
                                                                   unitMapper,
                                                                   decomposedFeatures,
                                                                   LeftOrRightOrBaseline.LEFT );
            Map<FeatureTuple, ThresholdsByMetric> thresholds = thresholdReader.read();

            // Features having thresholds as reported by the threshold reader.
            Set<FeatureTuple> havingThresholds = thresholdReader.getEvaluatableFeatures();

            // If the left dataset name exists in thresholds, keep it in the set.
            decomposedFeatures = Collections.unmodifiableSet( havingThresholds );

            if ( decomposedFeatures.isEmpty() )
            {
                throw new NoDataException( "There were data correlated by "
                                           + "geographic features specified "
                                           + "available for evaluation but "
                                           + "there were no thresholds available "
                                           + "for any of those features." );
            }

            // Create any netcdf blobs for writing. See #80267-137.
            if ( sharedWriters.getStatisticsWriters().contains( DestinationType.NETCDF ) )
            {
                sharedWriters.getStatisticsWriters()
                             .getNetcdfOutputWriter()
                             .createBlobsForWriting( thresholds );
            }

            // The project code - ideally project hash
            String projectIdentifier = String.valueOf( project.getInputCode() );

            ResolvedProject resolvedProject = ResolvedProject.of(
                                                                  projectConfigPlus,
                                                                  decomposedFeatures,
                                                                  projectIdentifier,
                                                                  thresholds,
                                                                  outputDirectory );

            // Tasks for features
            List<CompletableFuture<Void>> featureTasks = new ArrayList<>();

            // Report on the completion state of all features
            // Report detailed state by default (final arg = true)
            // TODO: demote to summary report (final arg = false) for >> feature count
            FeatureReporter featureReport = new FeatureReporter( projectConfigPlus, decomposedFeatures.size(), true );

            // Deactivate progress monitoring within features, as features are processed asynchronously - the internal
            // completion state of features has no value when reported in this way
            ProgressMonitor.deactivate();

            // Create one task per feature
            for ( FeatureTuple feature : decomposedFeatures )
            {
                Supplier<FeatureProcessingResult> featureProcessor = new FeatureProcessor( evaluation,
                                                                                           feature,
                                                                                           resolvedProject,
                                                                                           project,
                                                                                           unitMapper,
                                                                                           executors,
                                                                                           sharedWriters );

                CompletableFuture<Void> nextFeatureTask = CompletableFuture.supplyAsync( featureProcessor,
                                                                                         executors.getFeatureExecutor() )
                                                                           .thenAccept( featureReport );

                // Add to list of tasks
                featureTasks.add( nextFeatureTask );
            }

            // Run the tasks, and join on all tasks. The main thread will wait until all are completed successfully
            // or one completes exceptionally for reasons other than lack of data
            // Complete the feature tasks
            ProcessorHelper.doAllOrException( featureTasks ).join();

            // Report that all publication was completed. At this stage, a message is sent indicating the expected 
            // message count for all message types, thereby allowing consumers to know when they are done/
            evaluation.markPublicationCompleteReportedSuccess();

            // Find the paths written to by writers
            pathsWrittenTo.addAll( featureReport.getPathsWrittenTo() );

            // Find the paths written to by shared writers
            pathsWrittenTo.addAll( sharedWriters.getStatisticsWriters().get() );

            if ( sharedWriters.hasSharedSampleWriters() )
            {
                pathsWrittenTo.addAll( sharedWriters.getSampleDataWriters().get() );
            }
            if ( sharedWriters.hasSharedBaselineSampleWriters() )
            {
                pathsWrittenTo.addAll( sharedWriters.getBaselineSampleDataWriters().get() );
            }

            // Report on the features
            featureReport.report();
        }
        catch ( CompletionException | IllegalArgumentException | IOException | NoDataException e )
        {
            throw new WresProcessingException( "Project failed to complete with the following error: ", e );
        }

        return Collections.unmodifiableSet( pathsWrittenTo );
    }

    /**
     * Returns the decomposed features.
     * 
     * @param project the project
     * @throws NoDataException if no features could be retrieved
     */

    private static Set<FeatureTuple> getDecomposedFeatures( Project project )
    {

        Set<FeatureTuple> decomposedFeatures;

        try
        {
            decomposedFeatures = project.getFeatures();
        }
        catch ( SQLException e )
        {
            throw new NoDataException( "Failed to retrieve the set of features.", e );
        }

        if ( decomposedFeatures.isEmpty() )
        {
            throw new NoDataException( "There were no data correlated by "
                                       + " geographic features specified "
                                       + "available for evaluation." );
        }

        return decomposedFeatures;
    }

    /**
     * Returns an instance of {@link SharedWriters} for shared writing.
     * 
     * @param systemSettings the system settings
     * @param executor the executor
     * @param projectConfig the project declaration
     * @param evaluationDescription the evaluation description
     * @param outputDirectory the output directory for writing
     * @return the shared writer instance
     * @throws IOException if the shared writer could not be created
     */

    private static SharedWriters getSharedWriters( SystemSettings systemSettings,
                                                   Executor executor,
                                                   ProjectConfig projectConfig,
                                                   wres.statistics.generated.Evaluation evaluationDescription,
                                                   Path outputDirectory )
            throws IOException
    {

        // Obtain the duration units for outputs: #55441
        String durationUnitsString = projectConfig.getOutputs()
                                                  .getDurationFormat()
                                                  .value()
                                                  .toUpperCase();
        ChronoUnit durationUnits = ChronoUnit.valueOf( durationUnitsString );

        // Build any writers of incremental formats that are shared across features
        SharedWritersBuilder sharedWritersBuilder = new SharedWritersBuilder();
        Set<DestinationType> incrementalFormats = ConfigHelper.getIncrementalFormats( projectConfig );

        if ( incrementalFormats.contains( DestinationType.NETCDF ) )
        {
            // Use the gridded netcdf writer.
            NetcdfOutputWriter netcdfWriter = NetcdfOutputWriter.of(
                                                                     systemSettings,
                                                                     executor,
                                                                     projectConfig,
                                                                     durationUnits,
                                                                     outputDirectory );

            sharedWritersBuilder.setNetcdfOutputWriter( netcdfWriter );

            LOGGER.debug( "Added a shared netcdf writer for statistics to the evaluation." );
        }

        if ( incrementalFormats.contains( DestinationType.PROTOBUF ) )
        {
            // Use a standard name for the protobuf
            // Eventually, this should probably correspond to the unique evaluation identifier
            Path protobufPath = outputDirectory.resolve( "evaluation.pb3" );
            sharedWritersBuilder.setProtobufWriter( ProtobufWriter.of( protobufPath, evaluationDescription ) );

            LOGGER.debug( "Added a shared protobuf writer to the evaluation." );
        }

        SharedSampleDataWriters sharedSampleWriters = null;
        SharedSampleDataWriters sharedBaselineSampleWriters = null;

        // If there are multiple destinations for pairs, ignore these. The system chooses the destination.
        // Writing the same pairs, more than once, to that single destination does not make sense.
        // See #55948-12 and #55948-13. Ultimate solution is to improve the schema to prevent multiple occurrences.
        List<DestinationConfig> pairDestinations = ProjectConfigs.getDestinationsOfType( projectConfig,
                                                                                         DestinationType.PAIRS );
        if ( !pairDestinations.isEmpty() )
        {
            DecimalFormat decimalFormatter = null;
            if ( Objects.nonNull( pairDestinations.get( 0 ).getDecimalFormat() ) )
            {
                decimalFormatter = ConfigHelper.getDecimalFormatter( pairDestinations.get( 0 ) );
            }

            sharedSampleWriters =
                    SharedSampleDataWriters.of( Paths.get( outputDirectory.toString(), PairsWriter.DEFAULT_PAIRS_NAME ),
                                                durationUnits,
                                                decimalFormatter );
            // Baseline writer?
            if ( Objects.nonNull( projectConfig.getInputs().getBaseline() ) )
            {
                sharedBaselineSampleWriters = SharedSampleDataWriters.of( Paths.get( outputDirectory.toString(),
                                                                                     PairsWriter.DEFAULT_BASELINE_PAIRS_NAME ),
                                                                          durationUnits,
                                                                          decimalFormatter );
            }
        }

        // Iterate the features, closing any shared writers on completion
        SharedStatisticsWriters sharedStatisticsWriters = sharedWritersBuilder.build();

        return SharedWriters.of( sharedStatisticsWriters,
                                 sharedSampleWriters,
                                 sharedBaselineSampleWriters );
    }

    /**
     * Creates a temporary directory for the outputs with the correct permissions. 
     *
     * @return the path to the temporary output directory
     * @throws IOException if the temporary directory cannot be created
     */

    private static Path createTempOutputDirectory() throws IOException
    {
        // Where outputs files will be written
        Path outputDirectory = null;

        // POSIX-compliant
        if ( FileSystems.getDefault().supportedFileAttributeViews().contains( "posix" ) )
        {
            // Permissions for temp directory require group read so that the tasker
            // may give the output to the client on GET. Write so that the tasker
            // may remove the output on client DELETE. Execute for dir reads.            
            Set<PosixFilePermission> permissions = EnumSet.of( PosixFilePermission.OWNER_READ,
                                                               PosixFilePermission.OWNER_WRITE,
                                                               PosixFilePermission.OWNER_EXECUTE,
                                                               PosixFilePermission.GROUP_READ,
                                                               PosixFilePermission.GROUP_WRITE,
                                                               PosixFilePermission.GROUP_EXECUTE );

            FileAttribute<Set<PosixFilePermission>> fileAttribute =
                    PosixFilePermissions.asFileAttribute( permissions );

            outputDirectory = Files.createTempDirectory( "wres_evaluation_output_",
                                                         fileAttribute );
        }
        // Not POSIX-compliant
        else
        {
            outputDirectory = Files.createTempDirectory( "wres_evaluation_output_" );
        }

        if ( !outputDirectory.isAbsolute() )
        {
            return outputDirectory.toAbsolutePath();
        }

        return outputDirectory;
    }

    /**
     * Composes a list of {@link CompletableFuture} so that execution completes when all futures are completed normally
     * or any one future completes exceptionally. None of the {@link CompletableFuture} passed to this utility method
     * should already handle exceptions otherwise the exceptions will not be caught here (i.e. all futures will process
     * to completion).
     *
     * @param <T> the type of future
     * @param futures the futures to compose
     * @return the composed futures
     * @throws CompletionException if completing exceptionally 
     */

    static <T> CompletableFuture<Object> doAllOrException( final List<CompletableFuture<T>> futures )
    {
        //Complete when all futures are completed
        final CompletableFuture<Void> allDone =
                CompletableFuture.allOf( futures.toArray( new CompletableFuture[futures.size()] ) );
        //Complete when any of the underlying futures completes exceptionally
        final CompletableFuture<T> oneExceptional = new CompletableFuture<>();
        //Link the two
        for ( final CompletableFuture<T> completableFuture : futures )
        {
            //When one completes exceptionally, propagate
            completableFuture.exceptionally( exception -> {
                oneExceptional.completeExceptionally( exception );
                return null;
            } );
        }
        //Either all done OR one completes exceptionally
        return CompletableFuture.anyOf( allDone, oneExceptional );
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
         * The feature executor.
         */
        private final ExecutorService featureExecutor;

        /**
         * The pair executor.
         */
        private final ExecutorService pairExecutor;

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
         * @param featureExecutor the feature executor
         * @param pairExecutor the pair executor
         * @param thresholdExecutor the threshold executor
         * @param metricExecutor the metric executor
         * @param productExecutor the product executor
         */
        Executors( Executor ioExecutor,
                   ExecutorService featureExecutor,
                   ExecutorService pairExecutor,
                   ExecutorService thresholdExecutor,
                   ExecutorService metricExecutor,
                   ExecutorService productExecutor )
        {
            this.ioExecutor = ioExecutor;
            this.featureExecutor = featureExecutor;
            this.pairExecutor = pairExecutor;
            this.thresholdExecutor = thresholdExecutor;
            this.metricExecutor = metricExecutor;
            this.productExecutor = productExecutor;
        }

        /**
         * Returns the {@link ExecutorService} for features.
         * @return the metric executor
         */

        ExecutorService getFeatureExecutor()
        {
            return this.featureExecutor;
        }

        /**
         * Returns the {@link ExecutorService} for pairs.
         * @return the pair executor
         */

        ExecutorService getPairExecutor()
        {
            return this.pairExecutor;
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
     * A value object for shared writers.
     */

    static class SharedWriters implements Closeable
    {
        /**
         * Shared writers for statstics.
         */

        private final SharedStatisticsWriters sharedStatisticsWriters;

        /**
         * Shared writers for sample data.
         */

        private final SharedSampleDataWriters sharedSampleWriters;

        /**
         * Shared writers for baseline sampled data.
         */

        private final SharedSampleDataWriters sharedBaselineSampleWriters;

        /**
         * Returns an instance.
         * 
         * @param sharedStatisticsWriters
         * @param sharedSampleWriters
         * @param sharedBaselineSampleWriters
         */
        static SharedWriters of( SharedStatisticsWriters sharedStatisticsWriters,
                                 SharedSampleDataWriters sharedSampleWriters,
                                 SharedSampleDataWriters sharedBaselineSampleWriters )

        {
            return new SharedWriters( sharedStatisticsWriters, sharedSampleWriters, sharedBaselineSampleWriters );
        }

        /**
         * Returns the shared statistics writers.
         * 
         * @return the shared statistics writers.
         */

        SharedStatisticsWriters getStatisticsWriters()
        {
            return this.sharedStatisticsWriters;
        }

        /**
         * Returns the shared sample data writers.
         * 
         * @return the shared sample data writers.
         */

        SharedSampleDataWriters getSampleDataWriters()
        {
            return this.sharedSampleWriters;
        }

        /**
         * Returns the shared sample data writers for baseline data.
         * 
         * @return the shared sample data writers  for baseline data.
         */

        SharedSampleDataWriters getBaselineSampleDataWriters()
        {
            return this.sharedBaselineSampleWriters;
        }

        /**
         * Returns <code>true</code> if shared statistics writers are available, otherwise <code>false</code>.
         * 
         * @return true if shared statistics writers are available
         */

        boolean hasSharedStatisticsWriters()
        {
            return Objects.nonNull( this.sharedStatisticsWriters );
        }

        /**
         * Returns <code>true</code> if shared sample writers are available, otherwise <code>false</code>.
         * 
         * @return true if shared sample writers are available
         */

        boolean hasSharedSampleWriters()
        {
            return Objects.nonNull( this.sharedSampleWriters );
        }

        /**
         * Returns <code>true</code> if shared sample writers are available for the baseline samples, otherwise 
         * <code>false</code>.
         * 
         * @return true if shared sample writers are available for the baseline samples
         */

        boolean hasSharedBaselineSampleWriters()
        {
            return Objects.nonNull( this.sharedBaselineSampleWriters );
        }

        /**
         * Attempts to close all shared writers.
         * @throws IOException when a resource could not be closed
         */

        public void close() throws IOException
        {
            if ( this.hasSharedStatisticsWriters() )
            {
                this.getStatisticsWriters().close();
            }

            if ( this.hasSharedSampleWriters() )
            {
                this.getSampleDataWriters().close();
            }

            if ( this.hasSharedBaselineSampleWriters() )
            {
                this.getBaselineSampleDataWriters().close();
            }
        }

        /**
         * Hidden constructor.
         * 
         * @param sharedStatisticsWriters
         * @param sharedSampleWriters
         * @param sharedBaselineSampleWriters
         */
        private SharedWriters( SharedStatisticsWriters sharedStatisticsWriters,
                               SharedSampleDataWriters sharedSampleWriters,
                               SharedSampleDataWriters sharedBaselineSampleWriters )
        {
            this.sharedStatisticsWriters = sharedStatisticsWriters;
            this.sharedSampleWriters = sharedSampleWriters;
            this.sharedBaselineSampleWriters = sharedBaselineSampleWriters;
        }

    }

    /**
     * Returns a consumer that logs evaluation status messages. TODO: replace this with a real consumer that exposes 
     * the status messages via the web service API. Most likely, this consumer should be supplied by 
     * {@link wres.server.ProjectService}.
     * 
     * @param evaluationId the evaluation identifier
     * @return a logger consumer for evaluation status messages
     */

    private static Consumer<EvaluationStatus> getLoggerConsumerForStatusEvents()
    {
        return statusMessage -> LOGGER.debug( "Encountered an evaluation status message: {}", statusMessage );
    }

    /**
     * Returns a consumer that logs evaluation description messages. TODO: replace this with a real consumer that 
     * exposes the evaluation description messages via the web service API. Most likely, this consumer should be 
     * supplied by {@link wres.server.ProjectService}.
     * 
     * @param evaluationId the evaluation identifier
     * @return a logger consumer for evaluation status messages
     */

    private static Consumer<wres.statistics.generated.Evaluation> getLoggerConsumerForEvaluationEvents()
    {
        return evaluationMessage -> LOGGER.debug( "Encountered an evaluation description message: {}",
                                                  evaluationMessage );
    }

    /**
     * Returns a map of external subscribers that are required by the evaluation provided.
     * 
     * @param evaluationDescription the evaluation description
     * @param systemSettings the system settings where external subscribers are registered
     * @return the external subscribers
     */

    private static Map<DestinationType, Set<String>>
            getExternalSubscribers( wres.statistics.generated.Evaluation evaluationDescription,
                                    SystemSettings systemSettings )
    {
        Objects.requireNonNull( evaluationDescription );
        Objects.requireNonNull( systemSettings );

        Map<DestinationType, Set<String>> returnMe = new EnumMap<>( DestinationType.class );

        // Only add subscribers when a subscription is required
        if ( MessageFactory.hasGraphicsTypes( evaluationDescription.getOutputs() ) )
        {
            Set<String> graphics = systemSettings.getGraphicsSubscribers();

            if ( !graphics.isEmpty() )
            {
                // Add the subscribers for each graphics type
                Set<DestinationType> types = MessageFactory.getGraphicsTypes( evaluationDescription.getOutputs() );
                types.forEach( type -> returnMe.put( type, graphics ) );
            }
        }

        return Collections.unmodifiableMap( returnMe );
    }

    /**
     * @param systemSettings the system settings to help resolve the path
     * @param projectConfigPlus the project declaration with graphics information
     * @return a description of the evaluation.
     */

    private static wres.statistics.generated.Evaluation getEvaluationDescription( SystemSettings systemSettings,
                                                                                  ProjectConfigPlus projectConfigPlus )
    {
        wres.statistics.generated.Evaluation returnMe = MessageFactory.parse( projectConfigPlus );

        Outputs outputs = returnMe.getOutputs();

        if ( outputs.hasPng() && outputs.getPng().hasOptions() )
        {
            String template = outputs.getPng().getOptions().getTemplateName();

            template = ProcessorHelper.getAbsolutePathFromRelativePath( systemSettings, template );

            Outputs.Builder builder = outputs.toBuilder();
            builder.getPngBuilder()
                   .getOptionsBuilder()
                   .setTemplateName( template );
            returnMe = returnMe.toBuilder()
                               .setOutputs( builder )
                               .build();
        }

        if ( outputs.hasSvg() && outputs.getSvg().hasOptions() )
        {
            String template = outputs.getSvg().getOptions().getTemplateName();

            template = ProcessorHelper.getAbsolutePathFromRelativePath( systemSettings, template );

            Outputs.Builder builder = outputs.toBuilder();
            builder.getSvgBuilder()
                   .getOptionsBuilder()
                   .setTemplateName( template );
            returnMe = returnMe.toBuilder()
                               .setOutputs( builder )
                               .build();
        }

        return returnMe;
    }

    /**
     * @param systemSettings the system settings to help resolve the path
     * @return an absolute path string from a relative one.
     */

    private static String getAbsolutePathFromRelativePath( SystemSettings systemSettings, String pathString )
    {
        if ( Objects.isNull( pathString ) || pathString.isBlank() )
        {
            return pathString;
        }

        Path path = Path.of( pathString );

        if ( !path.isAbsolute() )
        {
            path = systemSettings.getDataDirectory()
                                 .getParent()
                                 .resolve( path );
        }

        return path.toString();
    }

    private ProcessorHelper()
    {
        // Helper class with static methods therefore no construction allowed.
    }
}
