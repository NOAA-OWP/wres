package wres.pipeline;

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
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.ProjectConfigPlus;
import wres.config.ProjectConfigs;
import wres.config.generated.DatasourceType;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.MetricsConfig;
import wres.config.generated.ProjectConfig;
import wres.datamodel.Ensemble;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolRequest;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.datamodel.thresholds.ThresholdsByMetricAndFeature;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindowOuter;
import wres.events.Evaluation;
import wres.events.EvaluationEventUtilities;
import wres.events.subscribe.ConsumerFactory;
import wres.events.subscribe.EvaluationSubscriber;
import wres.events.subscribe.SubscriberApprover;
import wres.eventsbroker.BrokerConnectionFactory;
import wres.io.Operations;
import wres.io.concurrency.Executor;
import wres.io.concurrency.Pipelines;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.MeasurementUnits;
import wres.io.geography.FeatureFinder;
import wres.io.pooling.PoolFactory;
import wres.io.project.Project;
import wres.io.retrieval.EnsembleRetrieverFactory;
import wres.io.retrieval.RetrieverFactory;
import wres.io.retrieval.SingleValuedRetrieverFactory;
import wres.io.retrieval.UnitMapper;
import wres.io.thresholds.ThresholdReader;
import wres.io.writing.SharedSampleDataWriters;
import wres.io.writing.WriteException;
import wres.io.writing.commaseparated.pairs.PairsWriter;
import wres.io.writing.netcdf.NetcdfOutputWriter;
import wres.pipeline.Evaluator.DatabaseServices;
import wres.pipeline.Evaluator.Executors;
import wres.pipeline.statistics.MetricProcessor;
import wres.pipeline.statistics.MetricProcessorByTimeEnsemblePairs;
import wres.pipeline.statistics.MetricProcessorByTimeSingleValuedPairs;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Consumer.Format;
import wres.system.ProgressMonitor;
import wres.system.SystemSettings;

/**
 * Class with functions to help in generating metrics and processing metric products.
 *
 * TODO: abstract away the functions used for graphical processing to a separate helper, GraphicalProductsHelper.
 *
 * @author James Brown
 * @author Jesse Bickel
 */
class ProcessorHelper
{
    private static final Logger LOGGER = LoggerFactory.getLogger( ProcessorHelper.class );

    /**
     * Unique identifier for this instance of the core messaging client.
     */

    private static final String CLIENT_ID = EvaluationEventUtilities.getId();

    /** A function that estimates the trace count of a pool that contains ensemble traces. */
    private static final ToIntFunction<Pool<TimeSeries<Pair<Double, Ensemble>>>> ENSEMBLE_TRACE_COUNT_ESTIMATOR =
            ProcessorHelper.getEnsembleTraceCountEstimator();

    /** A function that estimates the trace count of a pool that contains single-valued traces. */
    private static final ToIntFunction<Pool<TimeSeries<Pair<Double, Double>>>> SINGLE_VALUED_TRACE_COUNT_ESTIMATOR =
            ProcessorHelper.getSingleValuedTraceCountEstimator();

    /**
     * Processes an evaluation.
     *
     * Assumes that a shared lock for evaluation has already been obtained.
     * @param systemSettings the system settings
     * @param databaseServices the database services
     * @param projectConfigPlus the project configuration
     * @param executors the executors
     * @param connections broker connections
     * @param monitor an event that monitors the life cycle of the evaluation, not null
     * @return the resources written and the hash of the project data
     * @throws WresProcessingException if the evaluation processing fails
     * @throws ProjectConfigException if the declaration is incorrect
     * @throws NullPointerException if any input is null
     * @throws IOException if the creation of outputs fails
     */

    static Pair<Set<Path>, String> processEvaluation( SystemSettings systemSettings,
                                                      DatabaseServices databaseServices,
                                                      ProjectConfigPlus projectConfigPlus,
                                                      Executors executors,
                                                      BrokerConnectionFactory connections,
                                                      EvaluationEvent monitor )
            throws IOException
    {
        Objects.requireNonNull( systemSettings );
        Objects.requireNonNull( databaseServices );
        Objects.requireNonNull( projectConfigPlus );
        Objects.requireNonNull( executors );
        Objects.requireNonNull( connections );
        Objects.requireNonNull( monitor );

        Set<Path> resources = new TreeSet<>();
        String projectHash = null;

        // Get a unique evaluation identifier
        String evaluationId = EvaluationEventUtilities.getId();
        monitor.setEvaluationId( evaluationId );

        // Create output directory
        Path outputDirectory = ProcessorHelper.createTempOutputDirectory( evaluationId );

        ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();

        // Create a description of the evaluation
        wres.statistics.generated.Evaluation evaluationDescription =
                ProcessorHelper.getEvaluationDescription( projectConfigPlus );

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

        String consumerId = EvaluationEventUtilities.getId();

        // Moving this into the try-with-resources would require a different approach than notifying the evaluation to 
        // stop( Exception e ) on encountering an error that is not visible to it. See discussion in #90292.
        Evaluation evaluation = null;

        try ( SharedWriters sharedWriters = ProcessorHelper.getSharedWriters( projectConfig,
                                                                              outputDirectory );
              // Create a subscriber for the format writers that are within-process. The subscriber is built for this
              // evaluation only, and should not serve other evaluations, else there is a risk that short-running
              // subscribers die without managing to serve the evaluations they promised to serve. This complexity 
              // disappears when all subscribers are moved to separate, long-running, processes: #89868
              ConsumerFactory consumerFactory = new StatisticsConsumerFactory( consumerId,
                                                                               new HashSet<>( internalFormats ),
                                                                               netcdfWriters,
                                                                               projectConfig );
              EvaluationSubscriber formatsSubscriber = EvaluationSubscriber.of( consumerFactory,
                                                                                executors.getProductExecutor(),
                                                                                connections,
                                                                                evaluationId ); )
        {
            // Restrict the subscribers for internally-delivered formats otherwise core clients may steal format writing
            // work from each other. This is expected insofar as all subscribers are par. However, core clients currently 
            // run in short-running processes, we want to estimate resources for core clients effectively, and some format
            // writers are stateful (e.g., netcdf), hence this is currently a bad thing. Goal: place all format writers in
            // long running processes instead. See #88262 and #88267.
            SubscriberApprover subscriberApprover =
                    new SubscriberApprover.Builder().addApprovedSubscriber( internalFormats,
                                                                            consumerId )
                                                    .build();

            // Package the details needed to build the evaluation
            EvaluationDetails evaluationDetails = new EvaluationDetails( projectConfigPlus,
                                                                         evaluationDescription,
                                                                         evaluationId,
                                                                         subscriberApprover,
                                                                         connections,
                                                                         monitor );

            // Open an evaluation, to be closed on completion or stopped on exception
            Pair<Evaluation, String> evaluationAndProjectHash =
                    ProcessorHelper.processProjectConfig( evaluationDetails,
                                                          systemSettings,
                                                          databaseServices,
                                                          executors,
                                                          sharedWriters,
                                                          netcdfWriters,
                                                          outputDirectory );
            evaluation = evaluationAndProjectHash.getLeft();
            projectHash = evaluationAndProjectHash.getRight();

            // Wait for the evaluation to conclude
            evaluation.await();

            // Since the netcdf consumers are created here, they should be destroyed here. An attempt should be made to 
            // close the netcdf writers before the finally block because these writers employ a delayed write, which 
            // could still fail exceptionally. Such a failure should stop the evaluation exceptionally. For further 
            // context see #81790-21 and the detailed description in Evaluation.await(), which clarifies that awaiting 
            // for an evaluation to complete does not mean that all consumers have finished their work, only that they 
            // have received all expected messages. If this contract is insufficient (e.g., because of a delayed write
            // implementation), then it may be necessary to promote the underlying consumer/s to an external/outer 
            // subscriber that is responsible for messaging its own lifecycle, rather than delegating that to the 
            // Evaluation instance (which adopts the limited contract described here). An external subscriber within 
            // this jvm/process has the same contract as an external subscriber running in another process/jvm. It 
            // should only report completion when consumption is "done done".
            for ( NetcdfOutputWriter writer : netcdfWriters )
            {
                writer.close();
            }

            // Add the paths written by shared writers
            if ( sharedWriters.hasSharedSampleWriters() )
            {
                resources.addAll( sharedWriters.getSampleDataWriters().get() );
            }
            if ( sharedWriters.hasSharedBaselineSampleWriters() )
            {
                resources.addAll( sharedWriters.getBaselineSampleDataWriters().get() );
            }

            return Pair.of( Collections.unmodifiableSet( resources ), projectHash );
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
            if ( Objects.nonNull( evaluation ) )
            {
                // Stop forcibly
                LOGGER.debug( "Forcibly stopping evaluation {} upon encountering an internal error.", evaluationId );

                evaluation.stop( internalError );
            }

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
                if ( Objects.nonNull( evaluation ) )
                {
                    evaluation.close();
                }
            }
            catch ( IOException e )
            {
                String message = "Failed to close evaluation " + evaluationId + ".";
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

            // Add the paths written by external subscribers
            if ( Objects.nonNull( evaluation ) )
            {
                resources.addAll( evaluation.getPathsWrittenBySubscribers() );
            }

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

            LOGGER.info( "Wrote the following output: {}", resources );
        }
    }

    /**
     * Processes a {@link ProjectConfigPlus} using a prescribed {@link ExecutorService} for each of the pairs, 
     * thresholds and metrics.
     *
     * Assumes that a shared lock for evaluation has already been obtained.
     * @param evaluationDetails the evaluation details
     * @param systemSettings the system settings
     * @param databaseServices the database services
     * @param executors the executors
     * @param netcdfWriters netCDF writers
     * @param sharedWriters for writing
     * @param outputDirectory the output directory
     * @throws WresProcessingException if the processing failed for any reason
     * @return the evaluation and the hash of the project data
     * @throws IOException if an attempt was made to close the evaluation and it failed
     */

    private static Pair<Evaluation, String> processProjectConfig( EvaluationDetails evaluationDetails,
                                                                  SystemSettings systemSettings,
                                                                  DatabaseServices databaseServices,
                                                                  Executors executors,
                                                                  SharedWriters sharedWriters,
                                                                  List<NetcdfOutputWriter> netcdfWriters,
                                                                  Path outputDirectory )
            throws IOException
    {
        Evaluation evaluation = null;
        String projectHash = null;
        try
        {
            ProjectConfigPlus projectConfigPlus = evaluationDetails.getProjectConfigPlus();
            ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();
            ProgressMonitor.setShowStepDescription( false );
            ProgressMonitor.resetMonitor();

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

            LOGGER.debug( "Finished ingest for project {}...", projectConfigPlus );

            Operations.prepareForExecution( project );

            evaluationDetails.setProject( project );
            projectHash = project.getHash();

            // Get a unit mapper for the declared or analyzed measurement units
            String desiredMeasurementUnit = project.getMeasurementUnit();
            MeasurementUnits measurementUnitsCache =
                    new MeasurementUnits( databaseServices.getDatabase() );
            UnitMapper unitMapper = UnitMapper.of( measurementUnitsCache,
                                                   desiredMeasurementUnit,
                                                   projectConfig );
            // Update the evaluation description with any analyzed units and variable names
            wres.statistics.generated.Evaluation evaluationDescription =
                    ProcessorHelper.setAnalyzedUnitsAndVariableNames( evaluationDetails.getEvaluationDescription(),
                                                                      project );

            // Build the evaluation. In future, there may be a desire to build the evaluation prior to ingest, in order 
            // to message the status of ingest. In order to build an evaluation before ingest, those parts of the 
            // evaluation description that depend on the data would need to be part of the pool description instead 
            // (e.g., the measurement units). Indeed, the time scale is part of the pool description for this reason.
            evaluation = Evaluation.of( evaluationDescription,
                                        evaluationDetails.getBrokerConnections(),
                                        ProcessorHelper.CLIENT_ID,
                                        evaluationDetails.getEvaluationId(),
                                        evaluationDetails.getSubscriberApprover() );
            evaluationDetails.setEvaluation( evaluation );

            ProgressMonitor.setShowStepDescription( false );

            // Acquire the individual feature tuples to correlate with thresholds
            Set<FeatureTuple> features = project.getFeatures();

            // Read external thresholds from the configuration, per feature
            List<ThresholdsByMetricAndFeature> thresholdsByMetricAndFeature = new ArrayList<>();
            Set<FeatureTuple> featuresWithExplicitThresholds = new TreeSet<>();
            for ( MetricsConfig metricsConfig : projectConfig.getMetrics() )
            {
                ThresholdReader thresholdReader = new ThresholdReader(
                                                                       systemSettings,
                                                                       projectConfig,
                                                                       metricsConfig,
                                                                       unitMapper,
                                                                       features );

                Map<FeatureTuple, ThresholdsByMetric> nextThresholds = thresholdReader.read();
                Set<FeatureTuple> innerFeaturesWithExplicitThresholds = thresholdReader.getEvaluatableFeatures();

                int minimumSampleSize = ProcessorHelper.getMinimumSampleSize( metricsConfig.getMinimumSampleSize() );
                ThresholdsByMetricAndFeature nextMetrics = ThresholdsByMetricAndFeature.of( nextThresholds,
                                                                                            minimumSampleSize );
                thresholdsByMetricAndFeature.add( nextMetrics );
                featuresWithExplicitThresholds.addAll( innerFeaturesWithExplicitThresholds );
            }

            // Render the bags of thresholds and features immutable
            thresholdsByMetricAndFeature = Collections.unmodifiableList( thresholdsByMetricAndFeature );
            featuresWithExplicitThresholds = Collections.unmodifiableSet( featuresWithExplicitThresholds );

            // Create the feature groups
            Set<FeatureGroup> featureGroups = ProcessorHelper.getFeatureGroups( project,
                                                                                featuresWithExplicitThresholds );

            // Create any netcdf blobs for writing. See #80267-137.
            if ( !netcdfWriters.isEmpty() )
            {
                for ( NetcdfOutputWriter writer : netcdfWriters )
                {
                    writer.createBlobsForWriting( featureGroups,
                                                  thresholdsByMetricAndFeature );
                }
            }

            // The project code - ideally project hash
            String projectIdentifier = project.getHash();

            ResolvedProject resolvedProject = ResolvedProject.of( projectConfigPlus,
                                                                  projectIdentifier,
                                                                  thresholdsByMetricAndFeature,
                                                                  outputDirectory );
            evaluationDetails.setResolvedProject( resolvedProject );

            // Deactivate progress monitoring within features, as features are processed asynchronously - the internal
            // completion state of features has no value when reported in this way
            ProgressMonitor.deactivate();

            List<PoolRequest> poolRequests = ProcessorHelper.getPoolRequests( evaluationDescription, project );

            int poolCount = poolRequests.size();
            EvaluationEvent monitor = evaluationDetails.getMonitor();
            monitor.setPoolCount( poolCount );

            // Report on the completion state of all pools
            PoolReporter poolReporter = new PoolReporter( projectConfigPlus, poolCount, true );

            // Get a message group tracker to notify the completion of groups that encompass several pools. Currently, 
            // this is feature-group shaped, but additional shapes may be desired in future
            PoolGroupTracker groupTracker = PoolGroupTracker.ofFeatureGroupTracker( evaluation, poolRequests );

            // Create the atomic tasks for this evaluation pipeline, i.e., pools
            List<CompletableFuture<Void>> poolTasks = ProcessorHelper.getPoolTasks( evaluationDetails,
                                                                                    sharedWriters,
                                                                                    unitMapper,
                                                                                    poolRequests,
                                                                                    executors,
                                                                                    poolReporter,
                                                                                    groupTracker );

            // Run the pool tasks, and join on all tasks. The main thread will wait until all are completed successfully
            // or one completes exceptionally (for reasons other than lack of data)
            Pipelines.doAllOrException( poolTasks )
                     .join();

            // Report that all publication was completed. At this stage, a message is sent indicating the expected 
            // message count for all message types, thereby allowing consumers to know when all messages have arrived.
            evaluation.markPublicationCompleteReportedSuccess();

            // Report on the pools
            poolReporter.report();

            // Return an evaluation that was opened
            return Pair.of( evaluation, projectHash );
        }
        catch ( IOException | SQLException | RuntimeException internalError )
        {
            if ( Objects.nonNull( evaluation ) )
            {
                LOGGER.debug( "Forcibly stopping evaluation {} upon encountering an internal error.",
                              evaluation.getEvaluationId() );

                evaluation.stop( internalError );
            }

            throw new WresProcessingException( "Project failed to complete with the following error: ", internalError );
        }
        // Close an evaluation that failed
        finally
        {
            if ( Objects.nonNull( evaluation ) && evaluation.isFailed() )
            {
                evaluation.close();
            }
        }
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
     * @deprecated for removal as templates should not be configurable
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

    /**
     * @param project the project
     * @param featuresWithExplicitThresholds features with explicit thresholds (not the implicit "all data" threshold)
     * @return the feature groups
     */

    private static Set<FeatureGroup> getFeatureGroups( Project project,
                                                       Set<FeatureTuple> featuresWithExplicitThresholds )
    {
        Objects.requireNonNull( project );
        Objects.requireNonNull( featuresWithExplicitThresholds );

        // Get the baseline groups in a sorted set
        Set<FeatureGroup> featureGroups = new TreeSet<>( project.getFeatureGroups() );

        // Log a warning about any discrepancies between features with thresholds and features to evaluate
        if ( LOGGER.isWarnEnabled() )
        {
            Map<String, Set<String>> missing = new HashMap<>();

            // Check that every group has one or more thresholds for every tuple, else warn
            for ( FeatureGroup nextGroup : featureGroups )
            {
                if ( nextGroup.getFeatures().size() > 1
                     && !featuresWithExplicitThresholds.containsAll( nextGroup.getFeatures() ) )
                {
                    Set<FeatureTuple> missingFeatures = new HashSet<>();
                    missingFeatures.addAll( nextGroup.getFeatures() );
                    missingFeatures.removeAll( featuresWithExplicitThresholds );

                    // Show abbreviated information only
                    missing.put( nextGroup.getName(),
                                 missingFeatures.stream()
                                                .map( FeatureTuple::toStringShort )
                                                .collect( Collectors.toSet() ) );
                }
            }

            // Warn about groups without thresholds, which will be skipped
            if ( !missing.isEmpty() )
            {
                LOGGER.warn( "While correlating thresholds with the features contained in feature groups, "
                             + "discovered {} feature groups that did not have thresholds for every feature within the "
                             + "group. These groups will be evaluated, but the grouped statistics will not include the "
                             + "pairs associated with the features that have missing thresholds (for the thresholds "
                             + "that are missing). By default, the \"all data\" threshold is added to every feature "
                             + "and the statistics for this threshold will not be impacted. The features with missing "
                             + "thresholds and their associated feature groups are: {}.",
                             missing.size(),
                             missing );
            }
        }

        return Collections.unmodifiableSet( featureGroups );
    }

    /**
     * Obtain the minimum sample size from a possible null input. If null, return zero.
     * 
     * @param minimumSampleSize the minimum sample size, which is nullable and defaults to zero
     */
    private static int getMinimumSampleSize( Integer minimumSampleSize )
    {
        // Defaults to zero
        if ( Objects.isNull( minimumSampleSize ) )
        {
            LOGGER.debug( "Setting the minimum sample size to zero. " );
            return 0;
        }
        else
        {
            return minimumSampleSize;
        }
    }

    /**
     * @param evaluation the evaluation description
     * @param project the project
     * @return an evaluation description with analyzed measurement units and variables, as needed
     * @throws SQLException if the analyzed unit could not be obtained from the project
     */

    private static wres.statistics.generated.Evaluation
            setAnalyzedUnitsAndVariableNames( wres.statistics.generated.Evaluation evaluation,
                                              Project project )
                    throws SQLException
    {
        String desiredMeasurementUnit = project.getMeasurementUnit();
        wres.statistics.generated.Evaluation.Builder builder = evaluation.toBuilder()
                                                                         .setMeasurementUnit( desiredMeasurementUnit );

        // Only set the names with analyzed names if the existing names are empty
        if ( "".equals( evaluation.getLeftVariableName() ) )
        {
            builder.setLeftVariableName( project.getLeftVariableName() );
        }
        if ( "".equals( evaluation.getRightVariableName() ) )
        {
            builder.setLeftVariableName( project.getRightVariableName() );
        }
        if ( project.hasBaseline() && "".equals( evaluation.getBaselineVariableName() ) )
        {
            builder.setBaselineVariableName( project.getBaselineVariableName() );
        }

        return builder.build();
    }

    /**
     * Creates the pool requests from the project.
     * 
     * @param evaluationDescription the evaluation description
     * @param project the project
     * @return the pool requests
     */

    private static List<PoolRequest> getPoolRequests( wres.statistics.generated.Evaluation evaluationDescription,
                                                      Project project )
    {
        List<PoolRequest> poolRequests = PoolFactory.getPoolRequests( evaluationDescription, project );

        // Log some information about the pools
        if ( LOGGER.isInfoEnabled() )
        {
            Set<FeatureGroup> features = new TreeSet<>();
            Set<TimeWindowOuter> timeWindows = new TreeSet<>();

            for ( PoolRequest nextRequest : poolRequests )
            {
                FeatureGroup nextFeature = nextRequest.getMetadata()
                                                      .getFeatureGroup();
                features.add( nextFeature );

                TimeWindowOuter nextTimeWindow = nextRequest.getMetadata()
                                                            .getTimeWindow();

                timeWindows.add( nextTimeWindow );
            }

            LOGGER.info( "Created {} pool requests, which included {} features groups and {} time windows. "
                         + "The feature groups were: {}. The time windows were: {}.",
                         poolRequests.size(),
                         features.size(),
                         timeWindows.size(),
                         PoolReporter.getPoolItemDescription( features, FeatureGroup::getName ),
                         PoolReporter.getPoolItemDescription( timeWindows, TimeWindowOuter::toString ) );
        }

        return poolRequests;
    }

    /**
     * Creates one pool task for each pool request.
     * 
     * @param evaluationDetails the evaluation details
     * @param sharedWriters the shared writers
     * @param unitMapper the unit mapper
     * @param poolRequests the pool requests
     * @param executors the executor services
     * @param poolReporter the pool reporter that reports on a pool execution
     * @param poolGroupTracker the group publication tracker
     * @return the pool execution tasks
     */

    private static List<CompletableFuture<Void>> getPoolTasks( EvaluationDetails evaluationDetails,
                                                               SharedWriters sharedWriters,
                                                               UnitMapper unitMapper,
                                                               List<PoolRequest> poolRequests,
                                                               Executors executors,
                                                               PoolReporter poolReporter,
                                                               PoolGroupTracker poolGroupTracker )
    {

        List<CompletableFuture<Void>> poolTasks = new ArrayList<>();

        DatasourceType type = evaluationDetails.getProject()
                                               .getRight()
                                               .getType();

        // Ensemble pairs
        if ( type == DatasourceType.ENSEMBLE_FORECASTS )
        {
            List<PoolProcessor<Double, Ensemble>> poolProcessors =
                    ProcessorHelper.getEnsemblePoolProcessors( evaluationDetails,
                                                               poolRequests,
                                                               sharedWriters,
                                                               unitMapper,
                                                               executors,
                                                               poolGroupTracker );

            List<CompletableFuture<Void>> nextPoolTasks =
                    ProcessorHelper.getPoolTasks( poolProcessors,
                                                  executors.getPoolExecutor(),
                                                  poolReporter );
            poolTasks.addAll( nextPoolTasks );
        }
        // All other single-valued types
        else
        {
            List<PoolProcessor<Double, Double>> poolProcessors =
                    ProcessorHelper.getSingleValuedPoolProcessors( evaluationDetails,
                                                                   poolRequests,
                                                                   sharedWriters,
                                                                   unitMapper,
                                                                   executors,
                                                                   poolGroupTracker );

            List<CompletableFuture<Void>> nextPoolTasks =
                    ProcessorHelper.getPoolTasks( poolProcessors,
                                                  executors.getPoolExecutor(),
                                                  poolReporter );
            poolTasks.addAll( nextPoolTasks );
        }

        return Collections.unmodifiableList( poolTasks );
    }

    /**
     * Returns a list of processors for processing single-valued pools, one for each pool request.
     * @param evaluationDetails the evaluation details
     * @param poolRequests the pool requests
     * @param sharedWriters the shared writers
     * @param unitMapper the unit mapper
     * @param executors the executors
     * @param groupId the group identifier for message groups
     * @param groupPublicationTracker the group publication tracker
     * @return the single-valued processors
     */

    private static List<PoolProcessor<Double, Double>>
            getSingleValuedPoolProcessors( EvaluationDetails evaluationDetails,
                                           List<PoolRequest> poolRequests,
                                           SharedWriters sharedWriters,
                                           UnitMapper unitMapper,
                                           Executors executors,
                                           PoolGroupTracker groupPublicationTracker )
    {
        Project project = evaluationDetails.getProject();

        List<MetricProcessor<Pool<TimeSeries<Pair<Double, Double>>>>> processors =
                ProcessorHelper.getSingleValuedProcessors( evaluationDetails.getResolvedProject()
                                                                            .getThresholdsByMetricAndFeature(),
                                                           executors.getThresholdExecutor(),
                                                           executors.getMetricExecutor() );

        // Create a retriever factory to support retrieval for this project
        RetrieverFactory<Double, Double> retrieverFactory = SingleValuedRetrieverFactory.of( project,
                                                                                             unitMapper );

        // Create the pool suppliers for all pools in this evaluation
        List<Supplier<Pool<TimeSeries<Pair<Double, Double>>>>> poolSuppliers =
                PoolFactory.getSingleValuedPools( project,
                                                  poolRequests,
                                                  retrieverFactory );

        // Stand-up the pair writers
        PairsWriter<Double, Double> pairsWriter = null;
        PairsWriter<Double, Double> basePairsWriter = null;
        if ( sharedWriters.hasSharedSampleWriters() )
        {
            pairsWriter = sharedWriters.getSampleDataWriters().getSingleValuedWriter();
        }
        if ( sharedWriters.hasSharedBaselineSampleWriters() )
        {
            basePairsWriter = sharedWriters.getBaselineSampleDataWriters().getSingleValuedWriter();
        }

        List<PoolProcessor<Double, Double>> poolProcessors = new ArrayList<>();

        int count = poolSuppliers.size();
        for ( int i = 0; i < count; i++ )
        {
            PoolRequest poolRequest = poolRequests.get( i );
            Supplier<Pool<TimeSeries<Pair<Double, Double>>>> poolSupplier = poolSuppliers.get( i );

            PoolProcessor<Double, Double> poolProcessor =
                    new PoolProcessor.Builder<Double, Double>().setPairsWriter( pairsWriter )
                                                               .setBasePairsWriter( basePairsWriter )
                                                               .setMetricProcessors( processors )
                                                               .setPoolRequest( poolRequest )
                                                               .setPoolSupplier( poolSupplier )
                                                               .setEvaluation( evaluationDetails.getEvaluation() )
                                                               .setMonitor( evaluationDetails.getMonitor() )
                                                               .setTraceCountEstimator( SINGLE_VALUED_TRACE_COUNT_ESTIMATOR )
                                                               .setProjectConfig( project.getProjectConfig() )
                                                               .setPoolGroupTracker( groupPublicationTracker )
                                                               .build();

            poolProcessors.add( poolProcessor );
        }

        return Collections.unmodifiableList( poolProcessors );
    }

    /**
     * Returns a list of processors for processing ensemble pools, one for each pool request.
     * @param evaluationDetails the evaluation details
     * @param poolRequests the pool requests
     * @param sharedWriters the shared writers
     * @param unitMapper the unit mapper
     * @param executors the executors
     * @param groupId the group identifier for message groups
     * @param groupPublicationTracker the group publication tracker
     * @return the ensemble processors
     */

    private static List<PoolProcessor<Double, Ensemble>>
            getEnsemblePoolProcessors( EvaluationDetails evaluationDetails,
                                       List<PoolRequest> poolRequests,
                                       SharedWriters sharedWriters,
                                       UnitMapper unitMapper,
                                       Executors executors,
                                       PoolGroupTracker groupPublicationTracker )
    {
        Project project = evaluationDetails.getProject();

        List<MetricProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>>> processors =
                ProcessorHelper.getEnsembleProcessors( evaluationDetails.getResolvedProject()
                                                                        .getThresholdsByMetricAndFeature(),
                                                       executors.getThresholdExecutor(),
                                                       executors.getMetricExecutor() );

        // Create a retriever factory to support retrieval for this project
        RetrieverFactory<Double, Ensemble> retrieverFactory = EnsembleRetrieverFactory.of( project,
                                                                                           unitMapper );

        // Create the pool suppliers for all pools in this evaluation
        List<Supplier<Pool<TimeSeries<Pair<Double, Ensemble>>>>> poolSuppliers =
                PoolFactory.getEnsemblePools( project,
                                              poolRequests,
                                              retrieverFactory );

        // Stand-up the pair writers
        PairsWriter<Double, Ensemble> pairsWriter = null;
        PairsWriter<Double, Ensemble> basePairsWriter = null;
        if ( sharedWriters.hasSharedSampleWriters() )
        {
            pairsWriter = sharedWriters.getSampleDataWriters().getEnsembleWriter();
        }
        if ( sharedWriters.hasSharedBaselineSampleWriters() )
        {
            basePairsWriter = sharedWriters.getBaselineSampleDataWriters().getEnsembleWriter();
        }

        List<PoolProcessor<Double, Ensemble>> poolProcessors = new ArrayList<>();

        int count = poolSuppliers.size();
        for ( int i = 0; i < count; i++ )
        {
            PoolRequest poolRequest = poolRequests.get( i );
            Supplier<Pool<TimeSeries<Pair<Double, Ensemble>>>> poolSupplier = poolSuppliers.get( i );

            PoolProcessor<Double, Ensemble> poolProcessor =
                    new PoolProcessor.Builder<Double, Ensemble>().setPairsWriter( pairsWriter )
                                                                 .setBasePairsWriter( basePairsWriter )
                                                                 .setMetricProcessors( processors )
                                                                 .setPoolRequest( poolRequest )
                                                                 .setPoolSupplier( poolSupplier )
                                                                 .setEvaluation( evaluationDetails.getEvaluation() )
                                                                 .setMonitor( evaluationDetails.getMonitor() )
                                                                 .setTraceCountEstimator( ENSEMBLE_TRACE_COUNT_ESTIMATOR )
                                                                 .setProjectConfig( project.getProjectConfig() )
                                                                 .setPoolGroupTracker( groupPublicationTracker )
                                                                 .build();

            poolProcessors.add( poolProcessor );
        }

        return Collections.unmodifiableList( poolProcessors );
    }

    /**
     * @param <L> the left type of pooled data
     * @param <R> the right type of pooled data
     * @param poolProcessors the pool processors
     * @param poolExecutor the pool executor
     * @param poolReporter the pool reporter
     * @return the pool tasks
     */

    private static <L, R> List<CompletableFuture<Void>> getPoolTasks( List<PoolProcessor<L, R>> poolProcessors,
                                                                      ExecutorService poolExecutor,
                                                                      PoolReporter poolReporter )
    {
        List<CompletableFuture<Void>> poolTasks = new ArrayList<>();

        for ( PoolProcessor<L, R> nextProcessor : poolProcessors )
        {
            CompletableFuture<Void> nextPoolTask = CompletableFuture.supplyAsync( nextProcessor,
                                                                                  poolExecutor )
                                                                    .thenAccept( poolReporter );

            poolTasks.add( nextPoolTask );
        }

        return Collections.unmodifiableList( poolTasks );
    }

    /**
     * @param metrics the metrics
     * @param thresholdExecutor the threshold executor
     * @param metricExecutor the metric executor
     * @return the single-valued processors
     */

    private static List<MetricProcessor<Pool<TimeSeries<Pair<Double, Double>>>>>
            getSingleValuedProcessors( List<ThresholdsByMetricAndFeature> metrics,
                                       ExecutorService thresholdExecutor,
                                       ExecutorService metricExecutor )
    {
        List<MetricProcessor<Pool<TimeSeries<Pair<Double, Double>>>>> processors = new ArrayList<>();

        for ( ThresholdsByMetricAndFeature nextMetrics : metrics )
        {
            MetricProcessor<Pool<TimeSeries<Pair<Double, Double>>>> nextProcessor =
                    new MetricProcessorByTimeSingleValuedPairs( nextMetrics,
                                                                thresholdExecutor,
                                                                metricExecutor );
            processors.add( nextProcessor );
        }

        return Collections.unmodifiableList( processors );
    }

    /**
     * @param metrics the metrics
     * @param thresholdExecutor the threshold executor
     * @param metricExecutor the metric executor
     * @return the single-valued processors
     */

    private static List<MetricProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>>>
            getEnsembleProcessors( List<ThresholdsByMetricAndFeature> metrics,
                                   ExecutorService thresholdExecutor,
                                   ExecutorService metricExecutor )
    {
        List<MetricProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>>> processors = new ArrayList<>();

        for ( ThresholdsByMetricAndFeature nextMetrics : metrics )
        {
            MetricProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>> nextProcessor =
                    new MetricProcessorByTimeEnsemblePairs( nextMetrics,
                                                            thresholdExecutor,
                                                            metricExecutor );
            processors.add( nextProcessor );
        }

        return Collections.unmodifiableList( processors );
    }

    /**
     * @return a function that estimates the number of traces in a pool of single-valued time-series
     */

    private static ToIntFunction<Pool<TimeSeries<Pair<Double, Double>>>> getSingleValuedTraceCountEstimator()
    {
        return pool -> 2 * pool.get().size();
    }

    /**
     * @return a function that estimates the number of traces in a pool of ensemble time-series
     */

    private static ToIntFunction<Pool<TimeSeries<Pair<Double, Ensemble>>>> getEnsembleTraceCountEstimator()
    {
        return pool -> pool.get()
                           .size()
                       * ( pool.get()
                               .get( 0 )
                               .getEvents()
                               .first()
                               .getValue()
                               .getRight()
                               .size()
                           + 1 );
    }

    /**
     * Small value class to collect together variables needed to instantiate an evaluation.
     */

    private static class EvaluationDetails
    {
        /** Project configuration. */
        private final ProjectConfigPlus projectConfigPlus;
        /** Evaluation description. */
        private final wres.statistics.generated.Evaluation evaluationDescription;
        /** Unique evaluation identifier. */
        private final String evaluationId;
        /** Approves format writer subscriptions that attempt to serve an evaluation. */
        private final SubscriberApprover subscriberApprover;
        /** Broker connections. */
        private final BrokerConnectionFactory connections;
        /** Monitor. */
        private final EvaluationEvent monitor;
        /** The project, possibly null. */
        private Project project;
        /** The resolved project, possibly null. */
        private ResolvedProject resolvedProject;
        /** The messaging component of an evaluation, possibly null. */
        private Evaluation evaluation;

        /**
         * @return the project configuration
         */
        private ProjectConfigPlus getProjectConfigPlus()
        {
            return projectConfigPlus;
        }

        /**
         * @return the evaluation description
         */
        private wres.statistics.generated.Evaluation getEvaluationDescription()
        {
            return evaluationDescription;
        }

        /**
         * @return the evaluation identifier
         */
        private String getEvaluationId()
        {
            return evaluationId;
        }

        /**
         * @return the subscriber approver
         */
        private SubscriberApprover getSubscriberApprover()
        {
            return subscriberApprover;
        }

        /**
         * @return the broker connection factory
         */
        private BrokerConnectionFactory getBrokerConnections()
        {
            return connections;
        }

        /**
         * @return the monitor
         */

        private EvaluationEvent getMonitor()
        {
            return this.monitor;
        }

        /**
         * @return the project, possibly null
         */

        private Project getProject()
        {
            return this.project;
        }

        /**
         * @return the resolvedProject, possibly null
         */

        private ResolvedProject getResolvedProject()
        {
            return this.resolvedProject;
        }

        /**
         * @return the evaluation, possibly null
         */

        private Evaluation getEvaluation()
        {
            return this.evaluation;
        }

        /**
         * Set the project, not null.
         * @param project the project
         * @throws NullPointerException if the project is null
         */

        private void setProject( Project project )
        {
            Objects.requireNonNull( project );

            this.project = project;
        }

        /**
         * Set the resolved project, not null.
         * @param resolvedProject the resolved project
         * @throws NullPointerException if the resolvedProject is null
         */

        private void setResolvedProject( ResolvedProject resolvedProject )
        {
            Objects.requireNonNull( resolvedProject );

            this.resolvedProject = resolvedProject;
        }

        /**
         * Set the evaluation, not null.
         * @param evaluation the evaluation
         * @throws NullPointerException if the evaluation is null
         */

        private void setEvaluation( Evaluation evaluation )
        {
            Objects.requireNonNull( evaluation );

            this.evaluation = evaluation;
        }

        /**
         * Builds an instance.
         * 
         * @param projectConfigPlus the project declaration
         * @param evaluationDescription the evaluation description
         * @param evaluationId the evaluation identifier
         * @param subscriberApprover the subscriber approver
         * @param connections the broker connections
         */

        private EvaluationDetails( ProjectConfigPlus projectConfigPlus,
                                   wres.statistics.generated.Evaluation evaluationDescription,
                                   String evaluationId,
                                   SubscriberApprover subscriberApprover,
                                   BrokerConnectionFactory connections,
                                   EvaluationEvent monitor )
        {
            this.projectConfigPlus = projectConfigPlus;
            this.evaluationDescription = evaluationDescription;
            this.evaluationId = evaluationId;
            this.subscriberApprover = subscriberApprover;
            this.connections = connections;
            this.monitor = monitor;
        }
    }

    /**
     * A value object for shared writers.
     */

    private static class SharedWriters implements Closeable
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
        private static SharedWriters of( SharedSampleDataWriters sharedSampleWriters,
                                         SharedSampleDataWriters sharedBaselineSampleWriters )

        {
            return new SharedWriters( sharedSampleWriters, sharedBaselineSampleWriters );
        }

        /**
         * Returns the shared sample data writers.
         * 
         * @return the shared sample data writers.
         */

        private SharedSampleDataWriters getSampleDataWriters()
        {
            return this.sharedSampleWriters;
        }

        /**
         * Returns the shared sample data writers for baseline data.
         * 
         * @return the shared sample data writers  for baseline data.
         */

        private SharedSampleDataWriters getBaselineSampleDataWriters()
        {
            return this.sharedBaselineSampleWriters;
        }

        /**
         * Returns <code>true</code> if shared sample writers are available, otherwise <code>false</code>.
         * 
         * @return true if shared sample writers are available
         */

        private boolean hasSharedSampleWriters()
        {
            return Objects.nonNull( this.sharedSampleWriters );
        }

        /**
         * Returns <code>true</code> if shared sample writers are available for the baseline samples, otherwise 
         * <code>false</code>.
         * 
         * @return true if shared sample writers are available for the baseline samples
         */

        private boolean hasSharedBaselineSampleWriters()
        {
            return Objects.nonNull( this.sharedBaselineSampleWriters );
        }

        /**
         * Attempts to close all shared writers.
         * @throws IOException when a resource could not be closed
         */
        @Override
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

    private ProcessorHelper()
    {
        // Helper class with static methods therefore no construction allowed.
    }

}
