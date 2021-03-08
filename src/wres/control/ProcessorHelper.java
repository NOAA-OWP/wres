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
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.events.Evaluation;
import wres.events.subscribe.ConsumerFactory;
import wres.events.subscribe.EvaluationSubscriber;
import wres.events.subscribe.SubscriberApprover;
import wres.eventsbroker.BrokerConnectionFactory;
import wres.io.Operations;
import wres.io.concurrency.Executor;
import wres.io.concurrency.Pipelines;
import wres.io.config.ConfigHelper;
import wres.io.geography.FeatureFinder;
import wres.io.project.Project;
import wres.io.retrieval.UnitMapper;
import wres.io.thresholds.ThresholdReader;
import wres.io.utilities.NoDataException;
import wres.io.writing.SharedSampleDataWriters;
import wres.io.writing.WriteException;
import wres.io.writing.commaseparated.pairs.PairsWriter;
import wres.io.writing.netcdf.NetcdfOutputWriter;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Consumer.Format;
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
     * Unique identifier for this instance of the core messaging client.
     */

    private static final String CLIENT_ID = Evaluation.getUniqueId();

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

        // Get a unique evaluation identifier
        String evaluationId = Evaluation.getUniqueId();

        // Create output directory
        Path outputDirectory = ProcessorHelper.createTempOutputDirectory( evaluationId );

        ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();

        // Create a description of the evaluation
        wres.statistics.generated.Evaluation evaluationDescription =
                ProcessorHelper.getEvaluationDescription( projectConfigPlus );

        // Create some shared writers
        SharedWriters sharedWriters =
                ProcessorHelper.getSharedWriters( projectConfig,
                                                  outputDirectory );

        // Create netCDF writers
        List<NetcdfOutputWriter> netcdfWriters =
                ProcessorHelper.getNetcdfWriters( projectConfig,
                                                  systemSettings,
                                                  executors.getIoExecutor(),
                                                  outputDirectory );

        // Obtain any formats delivered by out-of-process subscribers.
        Set<Format> externalFormats = ProcessorHelper.getFormatsDeliveredByExternalSubscribers();

        LOGGER.debug( "These formats will be delivered by external subscribers: {}.", externalFormats );

        // Formats delivered by within-process subscribers, in a mutable list
        Set<Format> internalFormats = MessageFactory.getDeclaredFormats( evaluationDescription.getOutputs() );

        internalFormats = new HashSet<>( internalFormats );
        internalFormats.removeAll( externalFormats );

        LOGGER.debug( "These formats will be delivered by internal subscribers: {}.", internalFormats );

        // Create a subscriber for the format writers that are within-process
        String consumerId = Evaluation.getUniqueId();
        ConsumerFactory consumerFactory = new StatisticsConsumerFactory( consumerId,
                                                                         new HashSet<>( internalFormats ),
                                                                         netcdfWriters,
                                                                         projectConfig );

        EvaluationSubscriber formatsSubscriber = EvaluationSubscriber.of( consumerFactory,
                                                                          executors.getProductExecutor(),
                                                                          connections );

        // Restrict the subscribers for internally-delivered formats otherwise core clients may steal format writing
        // work from each other. This is expected insofar as all subscribers are par. However, core clients currently 
        // run in short-running processes, we want to estimate resources for core clients effectively, and some format
        // writers are stateful (e.g., netcdf), hence this is currently a bad thing. Goal: place all format writers in
        // long running processes instead. See #88262 and #88267.
        SubscriberApprover subscriberApprover = new SubscriberApprover.Builder().addApprovedSubscriber( internalFormats,
                                                                                                        consumerId )
                                                                                .build();

        // Open an evaluation, to be closed on completion or stopped on exception
        Evaluation evaluation = Evaluation.of( evaluationDescription,
                                               connections,
                                               ProcessorHelper.CLIENT_ID,
                                               evaluationId,
                                               subscriberApprover );

        try
        {
            Set<Path> pathsWritten = ProcessorHelper.processProjectConfig( evaluation,
                                                                           systemSettings,
                                                                           databaseServices,
                                                                           projectConfigPlus,
                                                                           executors,
                                                                           sharedWriters,
                                                                           netcdfWriters,
                                                                           outputDirectory );
            returnMe.addAll( pathsWritten );

            // Wait for the evaluation to conclude
            evaluation.await();

            // Since the consumer resources are created here, they should be destroyed here. An attempt should be made 
            // to close them before the finally block because some of these writers may employ a delayed write, which 
            // could still fail exceptionally. Such a failure should stop the evaluation exceptionally. For further 
            // context see #81790-21 and the detailed description in Evaluation.await(), which clarifies that awaiting 
            // for an evaluation to complete does not mean that all consumers have finished their work, only that they 
            // have received all expected messages. If this contract is insufficient (e.g., because of a delayed write
            // implementation), then it may be necessary to promote the underlying consumer/s to an external/outer 
            // subscriber that is responsible for messaging its own lifecycle, rather than delegating that to the 
            // Evaluation instance (which adopts the limited contract described here). An external subscriber within 
            // this jvm/process has the same contract as an external subscriber running in another process/jvm. It 
            // should only report completion when consumption is "done done".
            sharedWriters.close();

            if ( !netcdfWriters.isEmpty() )
            {
                LOGGER.debug( "Finishing up writing netCDF data..." );

                for ( NetcdfOutputWriter writer : netcdfWriters )
                {
                    writer.close();
                }
            }

            return Collections.unmodifiableSet( returnMe );
        }
        // Allow a user-error to be distinguished separately
        catch ( ProjectConfigException userError )
        {
            LOGGER.debug( "Forcibly stopping evaluation {} upon encountering a user error.", evaluationId );

            // Stop forcibly
            evaluation.stop( userError );

            throw userError;
        }
        // Internal error
        catch ( RuntimeException internalError )
        {
            // Stop forcibly
            LOGGER.debug( "Forcibly stopping evaluation {} upon encountering an internal error.", evaluationId );

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
                String message = "Failed to close evaluation " + evaluationId + ".";
                LOGGER.warn( message, e );
            }

            // Close the pairs writers if they weren't closed already
            try
            {
                sharedWriters.close();
            }
            catch ( IOException e )
            {
                String message = "Failed to close the pair writers.";
                LOGGER.warn( message, e );
            }

            // Close the format writers
            try
            {
                consumerFactory.close();
            }
            catch ( IOException e )
            {
                String message = "Failed to close the format writers.";
                LOGGER.warn( message, e );
            }

            // Close the netCDF writers if not closed
            for ( NetcdfOutputWriter writer : netcdfWriters )
            {
                try
                {
                    writer.close();
                }
                catch ( WriteException we )
                {
                    LOGGER.warn( "Failed to close a netcdf writer.", we );
                }
            }

            // Close the formats subscriber
            try
            {
                formatsSubscriber.close();
            }
            catch ( IOException e )
            {
                String message = "Failed to close formats subscriber " + formatsSubscriber.getClientId() + ".";
                LOGGER.warn( message, e );
            }

            // Add the paths written by external subscribers
            returnMe.addAll( evaluation.getPathsWrittenBySubscribers() );

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
     * @param netcdfWriters netCDF writers
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
                                                   List<NetcdfOutputWriter> netcdfWriters,
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
            ThresholdReader thresholdReader = new ThresholdReader(
                                                                   systemSettings,
                                                                   projectConfig,
                                                                   unitMapper,
                                                                   decomposedFeatures );
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
            if ( !netcdfWriters.isEmpty() )
            {
                for ( NetcdfOutputWriter writer : netcdfWriters )
                {
                    writer.createBlobsForWriting( decomposedFeatures,
                                                  thresholds );
                }
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
            Pipelines.doAllOrException( featureTasks ).join();

            // Report that all publication was completed. At this stage, a message is sent indicating the expected 
            // message count for all message types, thereby allowing consumers to know when they are done/
            evaluation.markPublicationCompleteReportedSuccess();

            // Find the paths written to by writers
            pathsWrittenTo.addAll( featureReport.getPathsWrittenTo() );

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
     * @param projectConfig the project declaration
     * @param outputDirectory the output directory for writing
     * @return the shared writer instance
     */

    private static SharedWriters getSharedWriters( ProjectConfig projectConfig,
                                                   Path outputDirectory )
    {
        // Obtain the duration units for outputs: #55441
        String durationUnitsString = projectConfig.getOutputs()
                                                  .getDurationFormat()
                                                  .value()
                                                  .toUpperCase();
        ChronoUnit durationUnits = ChronoUnit.valueOf( durationUnitsString );

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

        return SharedWriters.of( sharedSampleWriters,
                                 sharedBaselineSampleWriters );
    }


    /**
     * Get the netCDF writers requested by this project declaration.
     *
     * @param projectConfig The declaration.
     * @param systemSettings The system settings.
     * @param executor The executor to pass to NetcdfOutputWriters.
     * @param outputDirectory The output directory into which to write.
     * @return A list of netCDF writers, zero to two.
     */

    private static List<NetcdfOutputWriter> getNetcdfWriters( ProjectConfig projectConfig,
                                                              SystemSettings systemSettings,
                                                              Executor executor,
                                                              Path outputDirectory )
    {
        List<NetcdfOutputWriter> writers = new ArrayList<>( 2 );

        // Obtain the duration units for outputs: #55441
        String durationUnitsString = projectConfig.getOutputs()
                                                  .getDurationFormat()
                                                  .value()
                                                  .toUpperCase();
        ChronoUnit durationUnits = ChronoUnit.valueOf( durationUnitsString );

        DestinationConfig firstDeprecatedNetcdf = null;
        DestinationConfig firstNetcdf2 = null;

        for ( DestinationConfig destination : projectConfig.getOutputs()
                                                           .getDestination() )
        {
            if ( destination.getType()
                            .equals( DestinationType.NETCDF )
                 && Objects.isNull( firstDeprecatedNetcdf ) )
            {
                firstDeprecatedNetcdf = destination;
            }

            if ( destination.getType()
                            .equals( DestinationType.NETCDF_2 )
                 && Objects.isNull( firstNetcdf2 ) )
            {
                firstNetcdf2 = destination;
            }
        }

        if ( Objects.nonNull( firstDeprecatedNetcdf ) )
        {
            // Use the template-based netcdf writer.
            NetcdfOutputWriter netcdfWriterDeprecated = NetcdfOutputWriter.of(
                                                                               systemSettings,
                                                                               executor,
                                                                               projectConfig,
                                                                               firstDeprecatedNetcdf,
                                                                               durationUnits,
                                                                               outputDirectory,
                                                                               true );
            writers.add( netcdfWriterDeprecated );
            LOGGER.warn( "Added a deprecated netcdf writer for statistics to the evaluation. Please update your declaration to use the newer netCDF output." );
        }

        if ( Objects.nonNull( firstNetcdf2 ) )
        {
            // Use the newer from-scratch netcdf writer.
            NetcdfOutputWriter netcdfWriter = NetcdfOutputWriter.of(
                                                                     systemSettings,
                                                                     executor,
                                                                     projectConfig,
                                                                     firstNetcdf2,
                                                                     durationUnits,
                                                                     outputDirectory,
                                                                     false );
            writers.add( netcdfWriter );
            LOGGER.debug( "Added a shared netcdf writer for statistics to the evaluation." );
        }

        return Collections.unmodifiableList( writers );
    }

    /**
     * Creates a temporary directory for the outputs with the correct permissions. 
     *
     * @param evaluationId the unique evaluation identifier
     * @return the path to the temporary output directory
     * @throws IOException if the temporary directory cannot be created     
     * @throws NullPointerException if the evaluationId is null 
     */

    private static Path createTempOutputDirectory( String evaluationId ) throws IOException
    {
        Objects.requireNonNull( evaluationId );

        // Where outputs files will be written
        Path outputDirectory = null;
        String tempDir = System.getProperty( "java.io.tmpdir" );

        // Is this instance running in a context that uses a wres job identifier?
        // If so, create a directory corresponding to the job identifier. See #84942.
        String jobId = System.getProperty( "wres.jobId" );
        if ( Objects.nonNull( jobId ) )
        {
            LOGGER.debug( "Discovered system property {} with value {}.", "wres.jobId", jobId );
            tempDir = tempDir + System.getProperty( "file.separator" ) + jobId;
        }

        Path namedPath = Paths.get( tempDir, "wres_evaluation_" + evaluationId );

        // POSIX-compliant    
        if ( FileSystems.getDefault().supportedFileAttributeViews().contains( "posix" ) )
        {
            Set<PosixFilePermission> permissions = EnumSet.of( PosixFilePermission.OWNER_READ,
                                                               PosixFilePermission.OWNER_WRITE,
                                                               PosixFilePermission.OWNER_EXECUTE,
                                                               PosixFilePermission.GROUP_READ,
                                                               PosixFilePermission.GROUP_WRITE,
                                                               PosixFilePermission.GROUP_EXECUTE );

            FileAttribute<Set<PosixFilePermission>> fileAttribute =
                    PosixFilePermissions.asFileAttribute( permissions );

            // Create if not exists
            outputDirectory = Files.createDirectories( namedPath, fileAttribute );
        }
        // Not POSIX-compliant
        else
        {
            outputDirectory = Files.createDirectories( namedPath );
        }

        if ( !outputDirectory.isAbsolute() )
        {
            return outputDirectory.toAbsolutePath();
        }

        return outputDirectory;
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
         * @param sharedSampleWriters shared writer of pairs
         * @param sharedBaselineSampleWriters shared writer of baseline pairs
         */
        static SharedWriters of( SharedSampleDataWriters sharedSampleWriters,
                                 SharedSampleDataWriters sharedBaselineSampleWriters )

        {
            return new SharedWriters( sharedSampleWriters, sharedBaselineSampleWriters );
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
         * @param sharedSampleWriters the shared writer for pairs
         * @param sharedBaselineSampleWriters the shared writer for baseline pairs
         */
        private SharedWriters( SharedSampleDataWriters sharedSampleWriters,
                               SharedSampleDataWriters sharedBaselineSampleWriters )
        {
            this.sharedSampleWriters = sharedSampleWriters;
            this.sharedBaselineSampleWriters = sharedBaselineSampleWriters;
        }

    }

    /**
     * Returns a set of formats that are delivered by external subscribers, according to relevant system properties.
     * 
     * @return the formats delivered by external subscribers
     */

    private static Set<Format> getFormatsDeliveredByExternalSubscribers()
    {
        String externalGraphics = System.getProperty( "wres.externalGraphics" );

        Set<Format> formats = new HashSet<>();

        // Add external graphics if required
        if ( Objects.nonNull( externalGraphics ) && "true".equalsIgnoreCase( externalGraphics ) )
        {
            formats.add( Format.PNG );
            formats.add( Format.SVG );
        }

        return Collections.unmodifiableSet( formats );
    }

    /**
     * @param projectConfigPlus the project declaration with graphics information
     * @return a description of the evaluation.
     * @deprecated for removal in 5.1 where templates should not be configurable
     */

    @Deprecated( since = "5.0", forRemoval = true )
    private static wres.statistics.generated.Evaluation getEvaluationDescription( ProjectConfigPlus projectConfigPlus )
    {
        wres.statistics.generated.Evaluation returnMe = MessageFactory.parse( projectConfigPlus );

        Outputs outputs = returnMe.getOutputs();

        if ( outputs.hasPng() && outputs.getPng().hasOptions() )
        {
            String template = outputs.getPng().getOptions().getTemplateName();

            template = ProcessorHelper.getAbsolutePathFromRelativePath( template );

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

            template = ProcessorHelper.getAbsolutePathFromRelativePath( template );

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
     * @return an absolute path string from a relative one.
     */

    private static String getAbsolutePathFromRelativePath( String pathString )
    {
        if ( Objects.isNull( pathString ) || pathString.isBlank() )
        {
            return pathString;
        }

        Path path = Path.of( pathString );

        if ( !path.isAbsolute() )
        {
            return path.toAbsolutePath().toString();
        }

        return pathString;
    }
    
    private ProcessorHelper()
    {
        // Helper class with static methods therefore no construction allowed.
    }
}
