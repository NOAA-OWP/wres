package wres.control;

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
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.ProjectConfigPlus;
import wres.config.generated.*;
import wres.datamodel.FeatureTuple;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.events.Consumers;
import wres.events.Evaluation;
import wres.events.EvaluationEventException;
import wres.eventsbroker.BrokerConnectionFactory;
import wres.io.Operations;
import wres.io.concurrency.Executor;
import wres.io.config.ConfigHelper;
import wres.io.geography.FeatureFinder;
import wres.io.project.Project;
import wres.io.retrieval.UnitMapper;
import wres.io.thresholds.ThresholdReader;
import wres.io.utilities.Database;
import wres.io.utilities.NoDataException;
import wres.io.writing.SharedSampleDataWriters;
import wres.io.writing.SharedStatisticsWriters;
import wres.io.writing.SharedStatisticsWriters.SharedWritersBuilder;
import wres.io.writing.commaseparated.pairs.PairsWriter;
import wres.io.writing.netcdf.NetcdfOutputWriter;
import wres.io.writing.protobuf.ProtobufWriter;
import wres.system.DatabaseLockManager;
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
     * @param projectConfigPlus the project configuration
     * @param executors the executors
     * @param connections broker connections
     * @return the paths to which outputs were written
     * @throws WresProcessingException if the evaluation fails for any reason
     * @throws NullPointerException if any input is null
     */

    static Set<Path> processEvaluation( SystemSettings systemSettings,
                                        Database database,
                                        Executor executor,
                                        ProjectConfigPlus projectConfigPlus,
                                        ExecutorServices executors,
                                        DatabaseLockManager lockManager,
                                        BrokerConnectionFactory connections )
            throws IOException
    {
        Objects.requireNonNull( systemSettings );
        Objects.requireNonNull( database );
        Objects.requireNonNull( executor );
        Objects.requireNonNull( projectConfigPlus );
        Objects.requireNonNull( executors );
        Objects.requireNonNull( lockManager );

        Set<Path> returnMe = new TreeSet<>();

        ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();

        // Create a description of the evaluation
        wres.statistics.generated.Evaluation evaluationDescription = MessageFactory.parse( projectConfig );

        // Create the consumers
        // Create a container for all the consumers
        // TODO: populate with real consumers, not no-op consumers.
        // Statistics that are consumed by feature should be published by feature group
        // Statistics that are consumed by feature and time window could be published by feature and time window group
        // or by feature group. The advantage of the former is more atomic consumption.
        // Statistic types for the consumer are set on construction. For example, see:
        // Set<StatisticType> mergeSet = MetricConfigHelper.getCacheListFromProjectConfig( config );
        // This provides the set of statistics types that are grouped by feature
        Consumers consumerGroup =
                new Consumers.Builder().addStatusConsumer( Function.identity()::apply )
                                       .addEvaluationConsumer( Function.identity()::apply )
                                       // Add a regular consumer for statistics that are neither grouped by time window
                                       // nor feature. These include box plots per pair and statistics that can be 
                                       // written incrementally, such as netCDF and Protobuf
                                       .addStatisticsConsumer( Function.identity()::apply )
                                       // Add a grouped consumer for statistics that are grouped by feature
                                       .addGroupedStatisticsConsumer( Function.identity()::apply )
                                       .build();

        // Create and start a broker and open an evaluation, closing on completion
        Evaluation evaluation = null;
        try
        {
            evaluation = Evaluation.open( evaluationDescription,
                                          connections,
                                          consumerGroup );

            Set<Path> pathsWritten = ProcessorHelper.processProjectConfig( evaluation,
                                                                           systemSettings,
                                                                           database,
                                                                           executor,
                                                                           projectConfigPlus,
                                                                           executors,
                                                                           lockManager );
            returnMe.addAll( pathsWritten );
        }
        catch ( IOException | ProjectConfigException | WresProcessingException
                | IllegalArgumentException
                | EvaluationEventException e )
        {
            String evaluationId = "unknown";

            if ( Objects.nonNull( evaluation ) )
            {
                evaluation.stopOnException( e );
                evaluationId = evaluation.getEvaluationId();
            }

            // Rethrow
            throw new WresProcessingException( "Encountered an error while processing evaluation '"
                                               + evaluationId
                                               + "': ",
                                               e );
        }
        finally
        {
            // Close the evaluation
            if ( Objects.nonNull( evaluation ) )
            {
                evaluation.close();
            }
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Processes a {@link ProjectConfigPlus} using a prescribed {@link ExecutorService} for each of the pairs, 
     * thresholds and metrics.
     *
     * Assumes that a shared lock for evaluation has already been obtained.
     * @param projectConfigPlus the project configuration
     * @param executors the executors
     * @throws IOException when an issue occurs during ingest
     * @throws ProjectConfigException if the project configuration is invalid
     * @throws WresProcessingException when an issue occurs during processing
     * @return the paths to which outputs were written
     */

    private static Set<Path> processProjectConfig( Evaluation evaluation,
                                                   SystemSettings systemSettings,
                                                   Database database,
                                                   Executor executor,
                                                   ProjectConfigPlus projectConfigPlus,
                                                   ExecutorServices executors,
                                                   DatabaseLockManager lockManager )
            throws IOException
    {
        final ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();
        ProgressMonitor.setShowStepDescription( false );
        ProgressMonitor.resetMonitor();

        // Create output directory prior to ingest, fails early when it fails.
        Path outputDirectory = ProcessorHelper.createTempOutputDirectory();

        // Get a unit mapper for the declared measurement units
        PairConfig pairConfig = projectConfig.getPair();
        String desiredMeasurementUnit = pairConfig.getUnit();
        UnitMapper unitMapper = UnitMapper.of( database, desiredMeasurementUnit );

        // Look up any needed feature correlations, generate a new declaration.
        ProjectConfig featurefulProjectConfig = FeatureFinder.fillFeatures( projectConfig );
        LOGGER.debug( "Filled out features for project. Before: {} After: {}",
                      projectConfig,
                      featurefulProjectConfig );

        LOGGER.debug( "Beginning ingest for project {}...", projectConfigPlus );

        // Need to ingest first
        Project project = Operations.ingest( systemSettings,
                                             database,
                                             executor,
                                             featurefulProjectConfig,
                                             lockManager );

        Operations.prepareForExecution( project );

        LOGGER.debug( "Finished ingest for project {}...", projectConfigPlus );

        ProgressMonitor.setShowStepDescription( false );

        Set<FeatureTuple> decomposedFeatures;

        try
        {
            decomposedFeatures = project.getFeatures();
        }
        catch ( SQLException e )
        {
            throw new IOException( "Failed to retrieve the set of features.", e );
        }

        if ( decomposedFeatures.isEmpty() )
        {
            throw new NoDataException( "There were no data correlated by "
                                       + " geographic features specified "
                                       + "available for evaluation." );
        }

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

        // The project code - ideally project hash
        String projectIdentifier = String.valueOf( project.getInputCode() );

        ResolvedProject resolvedProject = ResolvedProject.of(
                                                              projectConfigPlus,
                                                              decomposedFeatures,
                                                              projectIdentifier,
                                                              thresholds,
                                                              outputDirectory );

        Set<Path> pathsWrittenTo = new HashSet<>();

        // Tasks for features
        List<CompletableFuture<Void>> featureTasks = new ArrayList<>();

        // Report on the completion state of all features
        // Report detailed state by default (final arg = true)
        // TODO: demote to summary report (final arg = false) for >> feature count
        FeatureReporter featureReport = new FeatureReporter( projectConfigPlus, decomposedFeatures.size(), true );

        // Deactivate progress monitoring within features, as features are processed asynchronously - the internal
        // completion state of features has no value when reported in this way
        ProgressMonitor.deactivate();

        // Shared writers
        SharedWriters sharedWriters = ProcessorHelper.getSharedWriters( systemSettings,
                                                                        executor,
                                                                        project,
                                                                        projectConfig,
                                                                        thresholds,
                                                                        outputDirectory );

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
        try
        {
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
        catch ( CompletionException e )
        {
            throw new WresProcessingException( "Project failed to complete with the following error: ", e );
        }
        finally
        {
            // Clean up by closing shared writers
            sharedWriters.close();

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
        }

        return Collections.unmodifiableSet( pathsWrittenTo );
    }

    /**
     * Returns an instance of {@link SharedWriters} for shared writing.
     * 
     * @param systemSettings the system settings
     * @param executor the executor
     * @param project the project that is aware of ingest
     * @param projectConfig the project declaration
     * @param thresholds the thresholds
     * @param outputDirectory the output directory for writing
     * @return the shared writer instance
     * @throws IOException if the shared writer could not be created
     */

    private static SharedWriters getSharedWriters( SystemSettings systemSettings,
                                                   Executor executor,
                                                   Project project,
                                                   ProjectConfig projectConfig,
                                                   Map<FeatureTuple, ThresholdsByMetric> thresholds,
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
            // Use the gridded netcdf writer
            sharedWritersBuilder.setNetcdfOutputWriter(
                                                        NetcdfOutputWriter.of(
                                                                               systemSettings,
                                                                               executor,
                                                                               projectConfig,
                                                                               durationUnits,
                                                                               outputDirectory,
                                                                               thresholds ) );

            LOGGER.debug( "Added a shared netcdf writer for statistics to the evaluation." );
        }

        if ( incrementalFormats.contains( DestinationType.PROTOBUF ) )
        {
            // TODO: abstract the creation of an evaluation description to the outermost caller that creates
            // an evaluation. For now, it is only used here.

            wres.statistics.generated.Evaluation evaluation = MessageFactory.parse( projectConfig );

            // Use a standard name for the protobuf
            // Eventually, this should probably correspond to the unique evaluation identifier
            Path protobufPath = outputDirectory.resolve( "evaluation.pb3" );
            sharedWritersBuilder.setProtobufWriter( ProtobufWriter.of( protobufPath, evaluation ) );

            LOGGER.debug( "Added a shared protobuf writer to the evaluation." );
        }

        SharedSampleDataWriters sharedSampleWriters = null;
        SharedSampleDataWriters sharedBaselineSampleWriters = null;

        // If there are multiple destinations for pairs, ignore these. The system chooses the destination.
        // Writing the same pairs, more than once, to that single destination does not make sense.
        // See #55948-12 and #55948-13. Ultimate solution is to improve the schema to prevent multiple occurrences.
        if ( !project.getPairDestinations().isEmpty() )
        {
            DecimalFormat decimalFormatter = null;
            if ( Objects.nonNull( project.getPairDestinations().get( 0 ).getDecimalFormat() ) )
            {
                decimalFormatter = ConfigHelper.getDecimalFormatter( project.getPairDestinations().get( 0 ) );
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

    static class ExecutorServices
    {

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
         * @param featureExecutor the feature executor
         * @param pairExecutor the pair executor
         * @param thresholdExecutor the threshold executor
         * @param metricExecutor the metric executor
         * @param productExecutor the product executor
         */
        ExecutorServices( ExecutorService featureExecutor,
                          ExecutorService pairExecutor,
                          ExecutorService thresholdExecutor,
                          ExecutorService metricExecutor,
                          ExecutorService productExecutor )
        {
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
    }

    /**
     * A value object for shared writers.
     */

    static class SharedWriters
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

        void close() throws IOException
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

    private ProcessorHelper()
    {
        // Helper class with static methods therefore no construction allowed.
    }
}
