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
import java.text.DecimalFormat;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.soabase.recordbuilder.core.RecordBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.MetricConstants;
import wres.config.yaml.DeclarationException;
import wres.config.yaml.DeclarationInterpolator;
import wres.config.yaml.DeclarationUtilities;
import wres.config.yaml.DeclarationValidator;
import wres.config.yaml.components.DataType;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.GeneratedBaselines;
import wres.datamodel.Ensemble;
import wres.datamodel.bootstrap.BlockSizeEstimator;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolRequest;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.thresholds.MetricsAndThresholds;
import wres.datamodel.thresholds.ThresholdSlicer;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.time.TimeSeriesStore;
import wres.datamodel.time.TimeWindowOuter;
import wres.events.EvaluationMessager;
import wres.events.EvaluationEventUtilities;
import wres.events.broker.BrokerConnectionFactory;
import wres.events.subscribe.ConsumerFactory;
import wres.events.subscribe.EvaluationSubscriber;
import wres.events.subscribe.SubscriberApprover;
import wres.io.database.caching.DatabaseCaches;
import wres.io.reading.netcdf.grid.GriddedFeatures;
import wres.io.ingesting.IngestResult;
import wres.io.ingesting.SourceLoader;
import wres.io.ingesting.TimeSeriesIngester;
import wres.io.ingesting.TimeSeriesTracker;
import wres.io.ingesting.database.DatabaseTimeSeriesIngester;
import wres.io.ingesting.memory.InMemoryTimeSeriesIngester;
import wres.io.project.Projects;
import wres.io.reading.ReaderUtilities;
import wres.io.retrieving.database.EnsembleSingleValuedRetrieverFactory;
import wres.io.retrieving.memory.EnsembleSingleValuedRetrieverFactoryInMemory;
import wres.pipeline.pooling.PoolFactory;
import wres.pipeline.pooling.PoolParameters;
import wres.datamodel.units.UnitMapper;
import wres.io.project.Project;
import wres.io.retrieving.RetrieverFactory;
import wres.io.retrieving.database.EnsembleRetrieverFactory;
import wres.io.retrieving.database.SingleValuedRetrieverFactory;
import wres.io.retrieving.memory.EnsembleRetrieverFactoryInMemory;
import wres.io.retrieving.memory.SingleValuedRetrieverFactoryInMemory;
import wres.io.writing.SharedSampleDataWriters;
import wres.io.writing.csv.pairs.PairsWriter;
import wres.io.writing.netcdf.NetcdfOutputWriter;
import wres.pipeline.Evaluator.DatabaseServices;
import wres.pipeline.Evaluator.Executors;
import wres.pipeline.pooling.PoolGroupTracker;
import wres.pipeline.pooling.PoolProcessor;
import wres.pipeline.pooling.PoolReporter;
import wres.pipeline.statistics.StatisticsProcessor;
import wres.pipeline.statistics.EnsembleStatisticsProcessor;
import wres.pipeline.statistics.SingleValuedStatisticsProcessor;
import wres.statistics.generated.Consumer.Format;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.Outputs;
import wres.system.SystemSettings;

/**
 * Utility class with functions to help execute an evaluation.
 *
 * @author James Brown
 * @author Jesse Bickel
 */
class EvaluationUtilities
{
    /** Re-used error message. */
    private static final String FORCIBLY_STOPPING_EVALUATION_UPON_ENCOUNTERING_AN_INTERNAL_ERROR =
            "Forcibly stopping evaluation {} upon encountering an internal error.";

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( EvaluationUtilities.class );

    /** Unique identifier for this instance of the core messaging client. */

    private static final String CLIENT_ID = EvaluationEventUtilities.getId();

    /** A function that estimates the trace count of a pool that contains ensemble traces. */
    private static final ToIntFunction<Pool<TimeSeries<Pair<Double, Ensemble>>>> ENSEMBLE_TRACE_COUNT_ESTIMATOR =
            EvaluationUtilities.getEnsembleTraceCountEstimator();

    /** A function that estimates the trace count of a pool that contains single-valued traces. */
    private static final ToIntFunction<Pool<TimeSeries<Pair<Double, Double>>>> SINGLE_VALUED_TRACE_COUNT_ESTIMATOR =
            EvaluationUtilities.getSingleValuedTraceCountEstimator();

    /** A block size estimator for the stationary bootstrap as applied to single-valued pools.*/
    private static final ToLongFunction<Pool<TimeSeries<Pair<Double, Double>>>> SINGLE_VALUED_BLOCK_SIZE_ESTIMATOR =
            EvaluationUtilities::getOptimalBlockSizeForStationaryBootstrap;

    /** A block size estimator for the stationary bootstrap as applied to ensemble pools.*/
    private static final ToLongFunction<Pool<TimeSeries<Pair<Double, Ensemble>>>> ENSEMBLE_BLOCK_SIZE_ESTIMATOR =
            EvaluationUtilities::getOptimalBlockSizeForStationaryBootstrap;

    /** Re-used string. */
    private static final String PERFORMING_RETRIEVAL_WITH_AN_IN_MEMORY_RETRIEVER_FACTORY =
            "Performing retrieval with an in-memory retriever factory.";

    /** Re-used string. */
    private static final String PERFORMING_RETRIEVAL_WITH_A_RETRIEVER_FACTORY_BACKED_BY_A_PERSISTENT_STORE =
            "Performing retrieval with a retriever factory backed by a persistent store.";

    /**
     * Executes an evaluation.
     *
     * @param systemSettings the system settings
     * @param databaseServices the database services
     * @param declaration the project declaration
     * @param executors the executors
     * @param connections broker connections
     * @param monitor an event that monitors the life cycle of the evaluation, not null
     * @return the resources written and the hash of the project data
     * @throws WresProcessingException if the evaluation processing fails
     * @throws DeclarationException if the declaration is incorrect
     * @throws NullPointerException if any input is null
     * @throws IOException if the creation of outputs fails
     */

    static Pair<Set<Path>, String> evaluate( SystemSettings systemSettings,
                                             DatabaseServices databaseServices,
                                             EvaluationDeclaration declaration,
                                             Executors executors,
                                             BrokerConnectionFactory connections,
                                             EvaluationEvent monitor )
            throws IOException
    {
        Objects.requireNonNull( systemSettings );
        Objects.requireNonNull( declaration );
        Objects.requireNonNull( executors );
        Objects.requireNonNull( connections );
        Objects.requireNonNull( monitor );

        if ( systemSettings.isInDatabase() )
        {
            Objects.requireNonNull( databaseServices );
        }

        Set<Path> resources = new TreeSet<>();
        String projectHash;

        // Get a unique evaluation identifier
        String evaluationId = EvaluationEventUtilities.getId();
        monitor.setEvaluationId( evaluationId );

        // Create output directory
        Path outputDirectory = EvaluationUtilities.createTempOutputDirectory( evaluationId );

        // Create netCDF writers
        List<NetcdfOutputWriter> netcdfWriters =
                EvaluationUtilities.getNetcdfWriters( declaration,
                                                      systemSettings,
                                                      outputDirectory );

        // Obtain any formats delivered by out-of-process subscribers.
        Set<Format> externalFormats = EvaluationUtilities.getFormatsDeliveredByExternalSubscribers();

        LOGGER.debug( "These formats will be delivered by external subscribers: {}.", externalFormats );

        // Formats delivered by within-process subscribers, in a mutable list
        Set<Format> internalFormats = wres.statistics.MessageFactory.getDeclaredFormats( declaration.formats()
                                                                                                    .outputs() );

        internalFormats = new HashSet<>( internalFormats );
        internalFormats.removeAll( externalFormats );

        LOGGER.debug( "These formats will be delivered by internal subscribers: {}.", internalFormats );

        String consumerId = EvaluationEventUtilities.getId();

        // Moving this into the try-with-resources would require a different approach than notifying the evaluation to
        // stop( Exception e ) on encountering an error that is not visible to it. See discussion in #90292.
        EvaluationMessager evaluation = null;

        try ( SharedWriters sharedWriters = EvaluationUtilities.getSharedWriters( declaration,
                                                                                  outputDirectory );
              // Create a subscriber for the format writers that are within-process. The subscriber is built for this
              // evaluation only, and should not serve other evaluations, else there is a risk that short-running
              // subscribers die without managing to serve the evaluations they promised to serve. This complexity
              // disappears when all subscribers are moved to separate, long-running, processes: #89868
              ConsumerFactory consumerFactory = new StatisticsConsumerFactory( consumerId,
                                                                               new HashSet<>( internalFormats ),
                                                                               netcdfWriters,
                                                                               declaration );
              // Out-of-band statistics format subscriber/writer, ignored locally
              EvaluationSubscriber ignoredFormatsSubscriber = EvaluationSubscriber.of( consumerFactory,
                                                                                       executors.productExecutor(),
                                                                                       connections,
                                                                                       evaluationId ) )
        {
            // Restrict the subscribers for internally-delivered formats otherwise core clients may steal format
            // writing work from each other. This is expected insofar as all subscribers are par. However, core clients
            // currently run in short-running processes, we want to estimate resources for core clients effectively,
            // and some format writers are stateful (e.g., netcdf), hence this is currently a bad thing. Goal: place
            // all format writers in long running processes instead. See #88262 and #88267.
            SubscriberApprover subscriberApprover =
                    new SubscriberApprover.Builder().addApprovedSubscriber( internalFormats,
                                                                            consumerId )
                                                    .build();

            // Package the details needed to build the evaluation
            EvaluationDetails evaluationDetails =
                    EvaluationUtilitiesEvaluationDetailsBuilder.builder()
                                                               .systemSettings( systemSettings )
                                                               .declaration( declaration )
                                                               .evaluationId( evaluationId )
                                                               .subscriberApprover( subscriberApprover )
                                                               .monitor( monitor )
                                                               .databaseServices( databaseServices )
                                                               .build();

            // Open an evaluation, to be closed on completion or stopped on exception
            Pair<EvaluationMessager, String> evaluationAndProjectHash =
                    EvaluationUtilities.evaluate( evaluationDetails,
                                                  databaseServices,
                                                  executors,
                                                  connections,
                                                  sharedWriters,
                                                  netcdfWriters );
            evaluation = evaluationAndProjectHash.getLeft();
            projectHash = evaluationAndProjectHash.getRight();

            // Wait for the evaluation to conclude
            evaluation.await();

            // Since the netcdf consumers are created here, they should be destroyed here. An attempt should be made to 
            // close the netcdf writers before the finally block because these writers employ a delayed write, which 
            // could still fail exceptionally. Such a failure should stop the evaluation exceptionally. For further 
            // context see #81790-21 and the detailed description in EvaluationMessager.await(), which clarifies that
            // awaiting for an evaluation to complete does not mean that all consumers have finished their work, only
            // that they have received all expected messages. If this contract is insufficient (e.g., because of a
            // delayed write implementation), then it may be necessary to promote the underlying consumer/s to an
            // external/outer subscriber that is responsible for messaging its own lifecycle, rather than delegating
            // that to the EvaluationMessager instance (which adopts the limited contract described here). An external
            // subscriber within this jvm/process has the same contract as an external subscriber running in another
            // process/jvm. It should only report completion when consumption is "really done".
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
        catch ( DeclarationException userError )
        {
            EvaluationUtilities.forceStop( evaluation, userError, evaluationId );

            // Rethrow
            throw userError;
        }
        // Internal error
        catch ( RuntimeException internalError )
        {
            EvaluationUtilities.forceStop( evaluation, internalError, evaluationId );

            // Decorate and rethrow
            throw new WresProcessingException( "Encountered an error while processing evaluation '"
                                               + evaluationId
                                               + "': ",
                                               internalError );
        }
        finally
        {
            // Close the netCDF writers if not closed
            EvaluationUtilities.closeNetcdfWriters( netcdfWriters, evaluation, evaluationId );

            // Clean-up an empty output directory: #67088
            try ( Stream<Path> outputs = Files.list( outputDirectory ) )
            {
                if ( outputs.findAny().isEmpty() )
                {
                    // Will only succeed for an empty directory
                    boolean status = Files.deleteIfExists( outputDirectory );

                    LOGGER.debug( "Attempted to remove empty output directory {} with success status: {}",
                                  outputDirectory,
                                  status );
                }
            }

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

            // Add the paths written by external subscribers
            if ( Objects.nonNull( evaluation ) )
            {
                resources.addAll( evaluation.getPathsWrittenBySubscribers() );
            }

            LOGGER.info( "Wrote the following output: {}", resources );
        }
    }

    /**
     * Executes and evaluation.
     *
     * @param evaluationDetails the evaluation details
     * @param databaseServices the database services
     * @param executors the executors
     * @param connections the broker connections
     * @param netcdfWriters netCDF writers
     * @param sharedWriters for writing
     * @throws WresProcessingException if the processing failed for any reason
     * @return the evaluation and the hash of the project data
     * @throws IOException if the evaluation could not be closed
     */

    private static Pair<EvaluationMessager, String> evaluate( EvaluationDetails evaluationDetails,
                                                              DatabaseServices databaseServices,
                                                              Executors executors,
                                                              BrokerConnectionFactory connections,
                                                              SharedWriters sharedWriters,
                                                              List<NetcdfOutputWriter> netcdfWriters )
            throws IOException
    {
        EvaluationMessager evaluation = null;
        String projectHash;
        try
        {
            EvaluationDeclaration declaration = evaluationDetails.declaration();

            // Look up any needed feature correlations and thresholds, generate a new declaration. These are needed for
            // reading and ingest, as well as subsequent steps, so perform this upfront: #116208
            EvaluationDeclaration declarationWithFeatures = ReaderUtilities.readAndFillFeatures( declaration );
            // Update the small bag-o-state
            evaluationDetails = EvaluationUtilitiesEvaluationDetailsBuilder.builder( evaluationDetails )
                                                                           .declaration( declarationWithFeatures )
                                                                           .build();
            // Gridded features cache, if required. See #51232.
            GriddedFeatures.Builder griddedFeaturesBuilder =
                    EvaluationUtilities.getGriddedFeaturesCache( declarationWithFeatures );

            LOGGER.debug( "Beginning ingest of time-series data..." );

            Project project;
            SystemSettings systemSettings = evaluationDetails.systemSettings();
            // Track the time-series through ingest
            TimeSeriesTracker timeSeriesTracker = TimeSeriesTracker.of();

            // Is the evaluation in a database? If so, use implementations that support a database
            if ( systemSettings.isInDatabase() )
            {
                // Build the database caches/ORMs, if required
                DatabaseCaches caches = DatabaseCaches.of( databaseServices.database() );
                // Set the caches
                evaluationDetails = EvaluationUtilitiesEvaluationDetailsBuilder.builder( evaluationDetails )
                                                                               .caches( caches )
                                                                               .build();
                try ( DatabaseTimeSeriesIngester databaseIngester =
                              new DatabaseTimeSeriesIngester.Builder().setSystemSettings( evaluationDetails.systemSettings() )
                                                                      .setDatabase( databaseServices.database() )
                                                                      .setCaches( caches )
                                                                      .setTimeSeriesTracker( timeSeriesTracker )
                                                                      .setLockManager( databaseServices.databaseLockManager() )
                                                                      .build() )
                {
                    List<IngestResult> ingestResults = SourceLoader.load( databaseIngester,
                                                                          evaluationDetails.systemSettings(),
                                                                          declarationWithFeatures,
                                                                          griddedFeaturesBuilder );

                    declarationWithFeatures =
                            EvaluationUtilities.interpolateMissingDataTypes( declarationWithFeatures,
                                                                             timeSeriesTracker.getDataTypes() );

                    // Create the gridded features cache if needed
                    GriddedFeatures griddedFeatures = null;
                    if ( Objects.nonNull( griddedFeaturesBuilder ) )
                    {
                        griddedFeatures = griddedFeaturesBuilder.build();
                    }

                    // Get the project, which provides an interface to the underlying store of time-series data
                    project = Projects.getProject( databaseServices.database(),
                                                   declarationWithFeatures,
                                                   caches,
                                                   griddedFeatures,
                                                   ingestResults );
                }
            }
            // In-memory evaluation
            else
            {
                // Builder for an in-memory store of time-series
                TimeSeriesStore.Builder timeSeriesStoreBuilder = new TimeSeriesStore.Builder();

                // Ingester that ingests into the in-memory store
                TimeSeriesIngester timeSeriesIngester = InMemoryTimeSeriesIngester.of( timeSeriesStoreBuilder,
                                                                                       timeSeriesTracker );

                // Load the sources using the ingester and create the ingest results to share
                List<IngestResult> ingestResults = SourceLoader.load( timeSeriesIngester,
                                                                      evaluationDetails.systemSettings(),
                                                                      declarationWithFeatures,
                                                                      griddedFeaturesBuilder );

                // Interpolate any missing elements of the declaration that depend on the data types
                declarationWithFeatures =
                        EvaluationUtilities.interpolateMissingDataTypes( declarationWithFeatures,
                                                                         timeSeriesTracker.getDataTypes() );

                // The immutable collection of in-memory time-series
                TimeSeriesStore timeSeriesStore = timeSeriesStoreBuilder.build();
                // Set the store
                evaluationDetails = EvaluationUtilitiesEvaluationDetailsBuilder.builder( evaluationDetails )
                                                                               .timeSeriesStore( timeSeriesStore )
                                                                               .build();
                project = Projects.getProject( declarationWithFeatures,
                                               timeSeriesStore,
                                               ingestResults );
            }

            LOGGER.debug( "Finished ingest of time-series data." );

            // Set the project hash for identification
            projectHash = project.getHash();

            // Get a unit mapper for the declared or analyzed measurement units
            String desiredMeasurementUnit = project.getMeasurementUnit();
            UnitMapper unitMapper = UnitMapper.of( desiredMeasurementUnit,
                                                   declaration.unitAliases() );

            // Read external thresholds into the declaration
            EvaluationDeclaration declarationWithFeaturesAndThresholds =
                    ReaderUtilities.readAndFillThresholds( declarationWithFeatures, unitMapper );

            // Update the small bag-o-state
            evaluationDetails = EvaluationUtilitiesEvaluationDetailsBuilder.builder( evaluationDetails )
                                                                           .declaration(
                                                                                   declarationWithFeaturesAndThresholds )
                                                                           .build();
            // Get the features, as described in the ingested time-series data, which may differ in number and details
            // from the declared features. For example, they are filtered for data availability, spatial mask etc. and
            // may include extra descriptive information, such as a geometry or location description.
            Set<FeatureTuple> features = project.getFeatures();
            Set<GeometryTuple> unwrappedFeatures = features.stream()
                                                           .map( FeatureTuple::getGeometryTuple )
                                                           .collect( Collectors.toUnmodifiableSet() );

            // Get the atomic metrics and thresholds for processing, each group representing a distinct processing task.
            // Ensure that named features correspond to the features associated with the data rather than declaration
            Set<MetricsAndThresholds> metricsAndThresholds =
                    ThresholdSlicer.getMetricsAndThresholdsForProcessing( declarationWithFeaturesAndThresholds,
                                                                          unwrappedFeatures );

            // Create the feature groups
            Set<FeatureGroup> featureGroups = EvaluationUtilities.getFeatureGroups( project, features );

            // Create any netcdf blobs for writing. See #80267-137.
            if ( !netcdfWriters.isEmpty() )
            {
                // TODO: eliminate these log messages when legacy netcdf is removed
                LOGGER.info( "Creating Netcdf blobs for statistics. This can take a while..." );

                for ( NetcdfOutputWriter writer : netcdfWriters )
                {
                    writer.createBlobsForWriting( featureGroups,
                                                  metricsAndThresholds );
                }

                LOGGER.info( "Finished creating Netcdf blobs, which are now ready to accept statistics." );
            }

            // Create the evaluation description with any analyzed units and variable names that happen post-ingest
            // This is akin to a post-ingest interpolation/augmentation of the declared project. Earlier stages of
            // interpolation include interpolation of missing declaration and service calls to interpolate features and
            // thresholds. This is the latest step in that process of combining the declaration and data
            Evaluation evaluationDescription = MessageFactory.parse( declaration );
            evaluationDescription = EvaluationUtilities.setAnalyzedUnitsAndVariableNames( evaluationDescription,
                                                                                          project );

            // Build the evaluation description for messaging. In future, there may be a desire to build the evaluation
            // description prior to ingest, in order to message the status of ingest to client applications. In order
            // to build an evaluation description before ingest, those parts of the evaluation description that depend
            // on the data would need to be part of the pool description instead (e.g., the measurement units). Indeed,
            // the time-scale is part of the pool description for this reason.
            evaluation = EvaluationMessager.of( evaluationDescription,
                                                connections,
                                                EvaluationUtilities.CLIENT_ID,
                                                evaluationDetails.evaluationId(),
                                                evaluationDetails.subscriberApprover() );

            // Set the project and evaluation, metrics and thresholds
            evaluationDetails = EvaluationUtilitiesEvaluationDetailsBuilder.builder( evaluationDetails )
                                                                           .project( project )
                                                                           .evaluation( evaluation )
                                                                           .metricsAndThresholds( metricsAndThresholds )
                                                                           .build();

            PoolFactory poolFactory = PoolFactory.of( project );
            List<PoolRequest> poolRequests = EvaluationUtilities.getPoolRequests( poolFactory, evaluationDescription );

            int poolCount = poolRequests.size();
            EvaluationEvent monitor = evaluationDetails.monitor();
            monitor.setPoolCount( poolCount );

            // Report on the completion state of all pools
            PoolReporter poolReporter = new PoolReporter( declarationWithFeaturesAndThresholds,
                                                          poolCount,
                                                          true );

            // Get a message group tracker to notify the completion of groups that encompass several pools. Currently, 
            // this is feature-group shaped, but additional shapes may be desired in future
            PoolGroupTracker groupTracker = PoolGroupTracker.ofFeatureGroupTracker( evaluation, poolRequests );

            // Create one or more pool tasks and complete them
            EvaluationUtilities.completePoolTasks( poolFactory,
                                                   evaluationDetails,
                                                   sharedWriters,
                                                   poolRequests,
                                                   executors,
                                                   poolReporter,
                                                   groupTracker );

            // Report that all publication was completed. At this stage, a message is sent indicating the expected 
            // message count for all message types, thereby allowing consumers to know when all messages have arrived.
            evaluation.markPublicationCompleteReportedSuccess();

            // Report on the pools
            poolReporter.report();

            // Return an evaluation that was opened
            return Pair.of( evaluation, projectHash );
        }
        catch ( RuntimeException internalError )
        {
            if ( Objects.nonNull( evaluation ) )
            {
                LOGGER.debug( FORCIBLY_STOPPING_EVALUATION_UPON_ENCOUNTERING_AN_INTERNAL_ERROR,
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
     * Interpolates any missing data types and validates the interpolated declaration for internal consistency.
     * @param declaration the declaration with missing data types
     * @param dataTypes the data types detected through ingest
     * @return the interpolated declaration
     * @throws DeclarationException if the declaration is inconsistent with the inferred types
     */

    private static EvaluationDeclaration interpolateMissingDataTypes( EvaluationDeclaration declaration,
                                                                      Map<DatasetOrientation, DataType> dataTypes )
    {
        // Interpolate any missing elements of the declaration that depend on the data types
        if ( DeclarationUtilities.hasMissingDataTypes( declaration ) )
        {
            // If the ingested types differ from any existing types, this will throw an exception
            declaration = DeclarationInterpolator.interpolate( declaration,
                                                               dataTypes.get( DatasetOrientation.LEFT ),
                                                               dataTypes.get( DatasetOrientation.RIGHT ),
                                                               dataTypes.get( DatasetOrientation.BASELINE ),
                                                               true );

            // Validate the declaration in relation to the interpolated data types only
            DeclarationValidator.validateTypes( declaration );
        }

        return declaration;
    }

    /**
     * Forcibly stops an evaluation messager on encountering an error, if already created.
     * @param evaluation the evaluation messager
     * @param error the error
     * @param evaluationId the evaluation identifier
     */
    private static void forceStop( EvaluationMessager evaluation, RuntimeException error, String evaluationId )
    {
        if ( Objects.nonNull( evaluation ) )
        {
            // Stop forcibly
            LOGGER.debug( FORCIBLY_STOPPING_EVALUATION_UPON_ENCOUNTERING_AN_INTERNAL_ERROR, evaluationId );

            evaluation.stop( error );
        }
    }

    /**
     * Closes the netcdf writers.
     * @param netcdfWriters the writers to close
     * @param evaluation the evaluation messager
     * @param evaluationId the evaluation identifier
     */

    private static void closeNetcdfWriters( List<NetcdfOutputWriter> netcdfWriters,
                                            EvaluationMessager evaluation,
                                            String evaluationId )
    {
        for ( NetcdfOutputWriter writer : netcdfWriters )
        {
            try
            {
                writer.close();
            }
            catch ( IOException we )
            {
                if ( Objects.nonNull( evaluation ) )
                {
                    LOGGER.debug( FORCIBLY_STOPPING_EVALUATION_UPON_ENCOUNTERING_AN_INTERNAL_ERROR,
                                  evaluationId );

                    evaluation.stop( we );
                }
                LOGGER.warn( "Failed to close a netcdf writer.", we );
            }
        }
    }

    /**
     * Chunks the pooling tasks into chains and evaluates them.
     *
     * @param poolFactory the pool factory
     * @param evaluationDetails the evaluation details
     * @param sharedWriters the shared writers
     * @param poolRequests the pool requests
     * @param executors the executor services
     * @param poolReporter the pool reporter that reports on a pool execution
     * @param poolGroupTracker the group publication tracker
     */

    private static void completePoolTasks( PoolFactory poolFactory,
                                           EvaluationDetails evaluationDetails,
                                           SharedWriters sharedWriters,
                                           List<PoolRequest> poolRequests,
                                           Executors executors,
                                           PoolReporter poolReporter,
                                           PoolGroupTracker poolGroupTracker )
    {
        LOGGER.info( "Submitting {} pool tasks for execution, which are awaiting completion. This can take a "
                     + "while...",
                     poolRequests.size() );

        // Sampling uncertainty declaration?
        if ( Objects.nonNull( evaluationDetails.declaration()
                                               .sampleUncertainty() ) )
        {
            LOGGER.warn( "Estimating the sampling uncertainties of the evaluation statistics with the stationary "
                         + "bootstrap. This evaluation may take much longer than usual..." );
        }

        // Create the atomic tasks for this evaluation pipeline, i.e., pools. There are as many tasks as pools and
        // they are composed into an asynchronous "chain" such that all pools complete successfully or one pool 
        // completes exceptionally, whichever happens first
        CompletableFuture<Object> poolTaskChain = EvaluationUtilities.getPoolTasks( poolFactory,
                                                                                    evaluationDetails,
                                                                                    sharedWriters,
                                                                                    poolRequests,
                                                                                    executors,
                                                                                    poolReporter,
                                                                                    poolGroupTracker );

        // Wait for the pool chain to complete
        poolTaskChain.join();
    }

    /**
     * Returns an instance of {@link SharedWriters} for shared writing.
     *
     * @param declaration the project declaration
     * @param outputDirectory the output directory for writing
     * @return the shared writer instance
     */

    private static SharedWriters getSharedWriters( EvaluationDeclaration declaration,
                                                   Path outputDirectory )
    {
        // Obtain the duration units for outputs: #55441
        ChronoUnit durationUnits = declaration.durationFormat();

        SharedSampleDataWriters sharedSampleWriters = null;
        SharedSampleDataWriters sharedBaselineSampleWriters = null;

        Outputs outputs = declaration.formats()
                                     .outputs();
        if ( outputs.hasPairs() )
        {
            DecimalFormat decimalFormatter = declaration.decimalFormat();

            sharedSampleWriters =
                    SharedSampleDataWriters.of( Paths.get( outputDirectory.toString(),
                                                           PairsWriter.DEFAULT_PAIRS_ZIP_NAME ),
                                                durationUnits,
                                                decimalFormatter );
            // Baseline writer?
            if ( DeclarationUtilities.hasBaseline( declaration ) )
            {
                sharedBaselineSampleWriters = SharedSampleDataWriters.of( Paths.get( outputDirectory.toString(),
                                                                                     PairsWriter.DEFAULT_BASELINE_PAIRS_ZIP_NAME ),
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
     * @param declaration the declaration
     * @param systemSettings the system settings
     * @param outputDirectory the output directory into which to write
     * @return a list of netCDF writers, zero to two
     */

    private static List<NetcdfOutputWriter> getNetcdfWriters( EvaluationDeclaration declaration,
                                                              SystemSettings systemSettings,
                                                              Path outputDirectory )
    {
        List<NetcdfOutputWriter> writers = new ArrayList<>( 2 );

        // Obtain the duration units for outputs: #55441
        ChronoUnit durationUnits = declaration.durationFormat();

        Outputs outputs = declaration.formats()
                                     .outputs();

        if ( outputs.hasNetcdf() )
        {
            // Use the template-based netcdf writer.
            NetcdfOutputWriter netcdfWriterDeprecated = NetcdfOutputWriter.of( systemSettings,
                                                                               declaration,
                                                                               durationUnits,
                                                                               outputDirectory );
            writers.add( netcdfWriterDeprecated );
            LOGGER.warn(
                    "Added a deprecated netcdf writer for statistics to the evaluation. Please update your declaration to use the newer netCDF output." );
        }

        if ( outputs.hasNetcdf2() )
        {
            // Use the newer from-scratch netcdf writer.
            NetcdfOutputWriter netcdfWriter = NetcdfOutputWriter.of( systemSettings,
                                                                     declaration,
                                                                     durationUnits,
                                                                     outputDirectory );
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
        Path outputDirectory;
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
                    Set<FeatureTuple> missingFeatures = new HashSet<>( nextGroup.getFeatures() );
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
     * @param evaluation the evaluation description
     * @param project the project
     * @return an evaluation description with analyzed measurement units and variables, as needed
     */

    private static Evaluation setAnalyzedUnitsAndVariableNames( Evaluation evaluation,
                                                                Project project )
    {
        String desiredMeasurementUnit = project.getMeasurementUnit();
        Evaluation.Builder builder = evaluation.toBuilder()
                                               .setMeasurementUnit( desiredMeasurementUnit );

        // Only set the names with analyzed names if the existing names are empty
        if ( evaluation.getLeftVariableName()
                       .isBlank() )
        {
            builder.setLeftVariableName( project.getVariableName( DatasetOrientation.LEFT ) );
        }
        if ( evaluation.getRightVariableName()
                       .isBlank() )
        {
            builder.setRightVariableName( project.getVariableName( DatasetOrientation.RIGHT ) );
        }
        if ( project.hasBaseline()
             && evaluation.getBaselineVariableName()
                          .isBlank() )
        {
            builder.setBaselineVariableName( project.getVariableName( DatasetOrientation.BASELINE ) );
        }

        return builder.build();
    }

    /**
     * Creates the pool requests from the project.
     *
     * @param evaluationDescription the evaluation description
     * @param poolFactory the pool factory
     * @return the pool requests
     */

    private static List<PoolRequest> getPoolRequests( PoolFactory poolFactory,
                                                      Evaluation evaluationDescription )
    {
        List<PoolRequest> poolRequests = poolFactory.getPoolRequests( evaluationDescription );

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

            LOGGER.info( "Created {} pool requests, which include {} features groups and {} time windows. "
                         + "The feature groups are: {}. The time windows are: {}.",
                         poolRequests.size(),
                         features.size(),
                         timeWindows.size(),
                         PoolReporter.getPoolItemDescription( features, FeatureGroup::getName ),
                         PoolReporter.getPoolItemDescription( timeWindows, TimeWindowOuter::toString ) );
        }

        // Log some detailed information about the pools, if required
        if ( LOGGER.isTraceEnabled() )
        {
            for ( PoolRequest nextRequest : poolRequests )
            {
                if ( nextRequest.hasBaseline() )
                {
                    LOGGER.trace( "Pool request {}/{} is: {}.",
                                  nextRequest.getMetadata()
                                             .getPool()
                                             .getPoolId(),
                                  nextRequest.getMetadataForBaseline()
                                             .getPool()
                                             .getPoolId(),
                                  nextRequest );
                }
                else
                {
                    LOGGER.trace( "Pool request {} is: {}.",
                                  nextRequest.getMetadata()
                                             .getPool()
                                             .getPoolId(),
                                  nextRequest );
                }
            }
        }

        return poolRequests;
    }

    /**
     * Creates one pool task for each pool request and then chains them together, such that all of the pools complete 
     * nominally or one completes exceptionally.
     *
     * @param poolFactory the pool factory
     * @param evaluationDetails the evaluation details
     * @param sharedWriters the shared writers
     * @param poolRequests the pool requests
     * @param executors the executor services
     * @param poolReporter the pool reporter that reports on a pool execution
     * @param poolGroupTracker the group publication tracker
     * @return the pool task chain
     */

    private static CompletableFuture<Object> getPoolTasks( PoolFactory poolFactory,
                                                           EvaluationDetails evaluationDetails,
                                                           SharedWriters sharedWriters,
                                                           List<PoolRequest> poolRequests,
                                                           Executors executors,
                                                           PoolReporter poolReporter,
                                                           PoolGroupTracker poolGroupTracker )
    {

        CompletableFuture<Object> poolTasks;

        DataType type = evaluationDetails.project()
                                         .getDeclaredDataset( DatasetOrientation.RIGHT )
                                         .type();

        SystemSettings settings = evaluationDetails.systemSettings();
        PoolParameters poolParameters =
                new PoolParameters.Builder().setFeatureBatchThreshold( settings.getFeatureBatchThreshold() )
                                            .setFeatureBatchSize( settings.getFeatureBatchSize() )
                                            .build();

        // Ensemble pairs
        if ( type == DataType.ENSEMBLE_FORECASTS )
        {
            List<PoolProcessor<Double, Ensemble>> poolProcessors =
                    EvaluationUtilities.getEnsemblePoolProcessors( poolFactory,
                                                                   evaluationDetails,
                                                                   poolRequests,
                                                                   sharedWriters,
                                                                   executors,
                                                                   poolGroupTracker,
                                                                   poolParameters );

            poolTasks = EvaluationUtilities.getPoolTaskChain( poolProcessors,
                                                              executors.poolExecutor(),
                                                              poolReporter );
        }
        // All other single-valued types
        else
        {
            List<PoolProcessor<Double, Double>> poolProcessors =
                    EvaluationUtilities.getSingleValuedPoolProcessors( poolFactory,
                                                                       evaluationDetails,
                                                                       poolRequests,
                                                                       sharedWriters,
                                                                       executors,
                                                                       poolGroupTracker,
                                                                       poolParameters );

            poolTasks = EvaluationUtilities.getPoolTaskChain( poolProcessors,
                                                              executors.poolExecutor(),
                                                              poolReporter );
        }

        return poolTasks;
    }

    /**
     * Returns a list of processors for processing single-valued pools, one for each pool request.
     * @param poolFactory the pool factory
     * @param evaluationDetails the evaluation details
     * @param poolRequests the pool requests
     * @param sharedWriters the shared writers
     * @param executors the executors
     * @param groupPublicationTracker the group publication tracker
     * @param poolParameters the pool parameters
     * @return the single-valued processors
     */

    private static List<PoolProcessor<Double, Double>> getSingleValuedPoolProcessors( PoolFactory poolFactory,
                                                                                      EvaluationDetails evaluationDetails,
                                                                                      List<PoolRequest> poolRequests,
                                                                                      SharedWriters sharedWriters,
                                                                                      Executors executors,
                                                                                      PoolGroupTracker groupPublicationTracker,
                                                                                      PoolParameters poolParameters )
    {
        Project project = evaluationDetails.project();

        // Separate metrics for a baseline?
        boolean separateMetrics = EvaluationUtilities.hasSeparateMetricsForBaseline( project );

        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Double>>>>> processors =
                EvaluationUtilities.getSingleValuedProcessors( evaluationDetails.metricsAndThresholds(),
                                                               executors.slicingExecutor(),
                                                               executors.metricExecutor() );

        // Get a separate set of processors for sampling uncertainty, excluding metrics whose uncertainties should not
        // be estimated
        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Double>>>>> sampleProcessors =
                EvaluationUtilities.getSingleValuedProcessorsForSamplingUncertainty( evaluationDetails.metricsAndThresholds(),
                                                                                     executors.slicingExecutor(),
                                                                                     executors.metricExecutor() );

        // Create a retriever factory to support retrieval for this project
        RetrieverFactory<Double, Double, Double> retrieverFactory;
        if ( evaluationDetails.hasInMemoryStore() )
        {
            LOGGER.debug( PERFORMING_RETRIEVAL_WITH_AN_IN_MEMORY_RETRIEVER_FACTORY );
            retrieverFactory = SingleValuedRetrieverFactoryInMemory.of( evaluationDetails.project(),
                                                                        evaluationDetails.timeSeriesStore() );
        }
        else
        {
            LOGGER.debug( PERFORMING_RETRIEVAL_WITH_A_RETRIEVER_FACTORY_BACKED_BY_A_PERSISTENT_STORE );
            retrieverFactory = SingleValuedRetrieverFactory.of( project,
                                                                evaluationDetails.databaseServices()
                                                                                 .database(),
                                                                evaluationDetails.caches() );
        }

        // Create the pool suppliers for all pools in this evaluation
        List<Pair<PoolRequest, Supplier<Pool<TimeSeries<Pair<Double, Double>>>>>> poolSuppliers =
                poolFactory.getSingleValuedPools( poolRequests,
                                                  retrieverFactory,
                                                  poolParameters );

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

        for ( Pair<PoolRequest, Supplier<Pool<TimeSeries<Pair<Double, Double>>>>> next : poolSuppliers )
        {
            PoolRequest poolRequest = next.getKey();
            Supplier<Pool<TimeSeries<Pair<Double, Double>>>> poolSupplier = next.getValue();

            PoolProcessor<Double, Double> poolProcessor =
                    new PoolProcessor.Builder<Double, Double>()
                            .setPairsWriter( pairsWriter )
                            .setBasePairsWriter( basePairsWriter )
                            .setMetricProcessors( processors )
                            .setSamplingUncertaintyMetricProcessors( sampleProcessors )
                            .setSamplingUncertaintyDeclaration( evaluationDetails.declaration()
                                                                                 .sampleUncertainty() )
                            .setSamplingUncertaintyBlockSize( SINGLE_VALUED_BLOCK_SIZE_ESTIMATOR )
                            .setSamplingUncertaintyExecutor( executors.samplingUncertaintyExecutor() )
                            .setPoolRequest( poolRequest )
                            .setPoolSupplier( poolSupplier )
                            .setEvaluation( evaluationDetails.evaluation() )
                            .setMonitor( evaluationDetails.monitor() )
                            .setTraceCountEstimator( SINGLE_VALUED_TRACE_COUNT_ESTIMATOR )
                            .setSeparateMetricsForBaseline( separateMetrics )
                            .setPoolGroupTracker( groupPublicationTracker )
                            .build();

            poolProcessors.add( poolProcessor );
        }

        return Collections.unmodifiableList( poolProcessors );
    }

    /**
     * Returns a list of processors for processing ensemble pools, one for each pool request.
     * @param poolFactory the pool factory
     * @param evaluationDetails the evaluation details
     * @param poolRequests the pool requests
     * @param sharedWriters the shared writers
     * @param executors the executors
     * @param groupPublicationTracker the group publication tracker
     * @param poolParameters the pool parameters
     * @return the ensemble processors
     */

    private static List<PoolProcessor<Double, Ensemble>> getEnsemblePoolProcessors( PoolFactory poolFactory,
                                                                                    EvaluationDetails evaluationDetails,
                                                                                    List<PoolRequest> poolRequests,
                                                                                    SharedWriters sharedWriters,
                                                                                    Executors executors,
                                                                                    PoolGroupTracker groupPublicationTracker,
                                                                                    PoolParameters poolParameters )
    {
        Project project = evaluationDetails.project();
        EvaluationDeclaration declaration = evaluationDetails.declaration();

        // Separate metrics for a baseline?
        boolean separateMetrics = EvaluationUtilities.hasSeparateMetricsForBaseline( project );

        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>>> processors =
                EvaluationUtilities.getEnsembleProcessors( evaluationDetails.metricsAndThresholds(),
                                                           executors.slicingExecutor(),
                                                           executors.metricExecutor() );

        // Get a separate set of processors for sampling uncertainty, excluding metrics whose uncertainties should not
        // be estimated
        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>>> sampleProcessors =
                EvaluationUtilities.getEnsembleProcessorsForSamplingUncertainty( evaluationDetails.metricsAndThresholds(),
                                                                                 executors.slicingExecutor(),
                                                                                 executors.metricExecutor() );

        List<Pair<PoolRequest, Supplier<Pool<TimeSeries<Pair<Double, Ensemble>>>>>> poolSuppliers;

        // Create the pool suppliers, depending on the types of data to be retrieved
        if ( project.hasGeneratedBaseline() )
        {
            GeneratedBaselines method = declaration.baseline()
                                                   .generatedBaseline()
                                                   .method();
            if ( !method.isEnsemble() )
            {
                List<GeneratedBaselines> supported = Arrays.stream( GeneratedBaselines.values() )
                                                           .filter( GeneratedBaselines::isEnsemble )
                                                           .toList();
                throw new DeclarationException( "Discovered an evaluation with ensemble forecasts and a generated "
                                                + "'baseline' with a 'method' of '"
                                                + method
                                                + "'. However, this 'method' produces single-valued forecasts, which "
                                                + "is not allowed. Please declare a baseline that contains ensemble "
                                                + "forecasts and try again. The following 'method' options support "
                                                + "ensemble forecasts: "
                                                + supported );
            }

            RetrieverFactory<Double, Ensemble, Double> retrieverFactory;
            if ( evaluationDetails.hasInMemoryStore() )
            {
                LOGGER.debug( PERFORMING_RETRIEVAL_WITH_AN_IN_MEMORY_RETRIEVER_FACTORY );
                retrieverFactory = EnsembleSingleValuedRetrieverFactoryInMemory.of( evaluationDetails.project(),
                                                                                    evaluationDetails.timeSeriesStore() );
            }
            else
            {
                LOGGER.debug( PERFORMING_RETRIEVAL_WITH_A_RETRIEVER_FACTORY_BACKED_BY_A_PERSISTENT_STORE );
                retrieverFactory = EnsembleSingleValuedRetrieverFactory.of( project,
                                                                            evaluationDetails.databaseServices()
                                                                                             .database(),
                                                                            evaluationDetails.caches() );
            }

            // Create the pool suppliers for all pools in this evaluation
            poolSuppliers = poolFactory.getEnsemblePoolsWithGeneratedBaseline( poolRequests,
                                                                               retrieverFactory,
                                                                               poolParameters );
        }
        else
        {
            RetrieverFactory<Double, Ensemble, Ensemble> retrieverFactory;
            if ( evaluationDetails.hasInMemoryStore() )
            {
                LOGGER.debug( PERFORMING_RETRIEVAL_WITH_AN_IN_MEMORY_RETRIEVER_FACTORY );
                retrieverFactory = EnsembleRetrieverFactoryInMemory.of( evaluationDetails.project(),
                                                                        evaluationDetails.timeSeriesStore() );
            }
            else
            {
                LOGGER.debug( PERFORMING_RETRIEVAL_WITH_A_RETRIEVER_FACTORY_BACKED_BY_A_PERSISTENT_STORE );
                retrieverFactory = EnsembleRetrieverFactory.of( project,
                                                                evaluationDetails.databaseServices()
                                                                                 .database(),
                                                                evaluationDetails.caches() );
            }

            // Create the pool suppliers for all pools in this evaluation
            poolSuppliers = poolFactory.getEnsemblePools( poolRequests,
                                                          retrieverFactory,
                                                          poolParameters );
        }

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

        for ( Pair<PoolRequest, Supplier<Pool<TimeSeries<Pair<Double, Ensemble>>>>> next : poolSuppliers )
        {
            PoolRequest poolRequest = next.getKey();

            Supplier<Pool<TimeSeries<Pair<Double, Ensemble>>>> poolSupplier = next.getValue();

            PoolProcessor<Double, Ensemble> poolProcessor =
                    new PoolProcessor.Builder<Double, Ensemble>()
                            .setPairsWriter( pairsWriter )
                            .setBasePairsWriter( basePairsWriter )
                            .setMetricProcessors( processors )
                            .setSamplingUncertaintyMetricProcessors( sampleProcessors )
                            .setSamplingUncertaintyDeclaration( evaluationDetails.declaration()
                                                                                 .sampleUncertainty() )
                            .setSamplingUncertaintyBlockSize( ENSEMBLE_BLOCK_SIZE_ESTIMATOR )
                            .setSamplingUncertaintyExecutor( executors.samplingUncertaintyExecutor() )
                            .setPoolRequest( poolRequest )
                            .setPoolSupplier( poolSupplier )
                            .setEvaluation( evaluationDetails.evaluation() )
                            .setMonitor( evaluationDetails.monitor() )
                            .setTraceCountEstimator( ENSEMBLE_TRACE_COUNT_ESTIMATOR )
                            .setSeparateMetricsForBaseline( separateMetrics )
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

    private static <L, R> CompletableFuture<Object> getPoolTaskChain( List<PoolProcessor<L, R>> poolProcessors,
                                                                      ExecutorService poolExecutor,
                                                                      PoolReporter poolReporter )
    {
        // Create the composition of pool tasks for completion
        List<CompletableFuture<Void>> poolTasks = new ArrayList<>();

        // Create a future that completes when any one pool task completes exceptionally
        CompletableFuture<Void> oneExceptional = new CompletableFuture<>();

        for ( PoolProcessor<L, R> nextProcessor : poolProcessors )
        {
            CompletableFuture<Void> nextPoolTask = CompletableFuture.supplyAsync( nextProcessor,
                                                                                  poolExecutor )
                                                                    .thenAccept( poolReporter )
                                                                    // When one pool completes exceptionally, propagate
                                                                    // Once chained below, all others that have not
                                                                    // excepted will get a RejectedExecutionException
                                                                    .exceptionally( exception -> {
                                                                        oneExceptional.completeExceptionally( exception );
                                                                        return null;
                                                                    } );

            poolTasks.add( nextPoolTask );
        }

        // Create a future that completes when all pool tasks succeed
        CompletableFuture<Void> allDone =
                CompletableFuture.allOf( poolTasks.toArray( new CompletableFuture[0] ) );

        // Chain the two futures together so that either: 1) all pool tasks succeed; or 2) one fails exceptionally.
        return CompletableFuture.anyOf( allDone, oneExceptional );
    }

    /**
     * @param metricsAndThresholds the metrics and thresholds, one for each atomic processing operation
     * @param slicingExecutor the pool slicing/dicing/transforming executor
     * @param metricExecutor the metric executor
     * @return the single-valued processors
     */

    private static List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Double>>>>>
    getSingleValuedProcessors( Set<MetricsAndThresholds> metricsAndThresholds,
                               ExecutorService slicingExecutor,
                               ExecutorService metricExecutor )
    {
        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Double>>>>> processors = new ArrayList<>();

        for ( MetricsAndThresholds nextMetrics : metricsAndThresholds )
        {
            StatisticsProcessor<Pool<TimeSeries<Pair<Double, Double>>>> nextProcessor =
                    new SingleValuedStatisticsProcessor( nextMetrics,
                                                         slicingExecutor,
                                                         metricExecutor );
            processors.add( nextProcessor );
        }

        return Collections.unmodifiableList( processors );
    }

    /**
     * @param metricsAndThresholds the metrics and thresholds, one for each atomic processing operation
     * @param slicingExecutor the pool slicing/dicing/transforming executor
     * @param metricExecutor the metric executor
     * @return the single-valued processors for sampling uncertainty calculations
     */

    private static List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Double>>>>>
    getSingleValuedProcessorsForSamplingUncertainty( Set<MetricsAndThresholds> metricsAndThresholds,
                                                     ExecutorService slicingExecutor,
                                                     ExecutorService metricExecutor )
    {
        Set<MetricsAndThresholds> overallFiltered =
                EvaluationUtilities.getMetricsForSamplingUncertainty( metricsAndThresholds );
        return EvaluationUtilities.getSingleValuedProcessors( overallFiltered,
                                                              slicingExecutor,
                                                              metricExecutor );
    }

    /**
     * @param metricsAndThresholds the metrics and thresholds, one for each atomic processing operation
     * @param slicingExecutor the pool slicing/dicing/transforming executor
     * @param metricExecutor the metric executor
     * @return the ensemble processors
     */

    private static List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>>>
    getEnsembleProcessors( Set<MetricsAndThresholds> metricsAndThresholds,
                           ExecutorService slicingExecutor,
                           ExecutorService metricExecutor )
    {
        List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>>> processors = new ArrayList<>();

        for ( MetricsAndThresholds nextMetrics : metricsAndThresholds )
        {
            StatisticsProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>> nextProcessor =
                    new EnsembleStatisticsProcessor( nextMetrics,
                                                     slicingExecutor,
                                                     metricExecutor );
            processors.add( nextProcessor );
        }

        return Collections.unmodifiableList( processors );
    }

    /**
     * @param metricsAndThresholds the metrics and thresholds, one for each atomic processing operation
     * @param slicingExecutor the pool slicing/dicing/transforming executor
     * @param metricExecutor the metric executor
     * @return the ensemble processors for sampling uncertainty calculations
     */

    private static List<StatisticsProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>>>
    getEnsembleProcessorsForSamplingUncertainty( Set<MetricsAndThresholds> metricsAndThresholds,
                                                 ExecutorService slicingExecutor,
                                                 ExecutorService metricExecutor )
    {
        Set<MetricsAndThresholds> overallFiltered =
                EvaluationUtilities.getMetricsForSamplingUncertainty( metricsAndThresholds );
        return EvaluationUtilities.getEnsembleProcessors( overallFiltered,
                                                          slicingExecutor,
                                                          metricExecutor );
    }

    /**
     * @param metrics the metrics and thresholds to filter
     * @return the metrics and thresholds containing only metrics for which sampling uncertainties can be estimated
     */

    private static Set<MetricsAndThresholds> getMetricsForSamplingUncertainty( Set<MetricsAndThresholds> metrics )
    {
        Set<MetricsAndThresholds> overallFiltered = new HashSet<>();
        for ( MetricsAndThresholds next : metrics )
        {
            Set<MetricConstants> nextMetrics = next.metrics();
            Set<MetricConstants> filtered = nextMetrics.stream()
                                                       .filter( MetricConstants::isSamplingUncertaintyAllowed )
                                                       .collect( Collectors.toUnmodifiableSet() );
            MetricsAndThresholds adjusted = new MetricsAndThresholds( filtered,
                                                                      next.thresholds(),
                                                                      next.minimumSampleSize(),
                                                                      next.ensembleAverageType() );
            overallFiltered.add( adjusted );
        }

        return Collections.unmodifiableSet( overallFiltered );
    }

    /**
     * @param project the project to inspect
     * @return whether the evaluation should contain separate metrics for a baseline.
     */

    private static boolean hasSeparateMetricsForBaseline( Project project )
    {
        return project.hasBaseline() && project.getDeclaration()
                                               .baseline()
                                               .separateMetrics();
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
        return pool -> {

            // Estimate the number of traces using the first time-series in the pool only
            int traceCount = pool.get()
                                 .stream()
                                 .mapToInt( next -> next.getEvents()
                                                        .first()
                                                        .getValue()
                                                        .getValue()
                                                        .size() )
                                 .max()
                                 .orElse( 0 );

            return traceCount * pool.get()
                                    .size();
        };
    }

    /**
     * @param declaration the project declaration
     * @return a gridded feature cache or null if none is required
     */

    private static GriddedFeatures.Builder getGriddedFeaturesCache( EvaluationDeclaration declaration )
    {
        GriddedFeatures.Builder griddedFeatures = null;

        if ( Objects.nonNull( declaration.spatialMask() ) )
        {
            griddedFeatures = new GriddedFeatures.Builder( declaration.spatialMask() );
        }

        return griddedFeatures;
    }

    /**
     * Estimates the optimal block size for each left-ish time-series in each mini-pool and returns the average of the
     * optimal block sizes across all time-series.
     * @param <R> the type of right-ish time-series data
     * @param pool the pool
     * @return the optimal block size for the stationary bootstrap
     */
    private static <R> long getOptimalBlockSizeForStationaryBootstrap( Pool<TimeSeries<Pair<Double, R>>> pool )
    {
        List<Pool<TimeSeries<Pair<Double, R>>>> miniPools = pool.getMiniPools();

        List<Long> blockSizes = new ArrayList<>();
        for ( Pool<TimeSeries<Pair<Double, R>>> next : miniPools )
        {
            long nextMain = EvaluationUtilities.getOptimalBlockSizesForStationaryBootstrap( next.get() );
            blockSizes.add( nextMain );
            if ( next.hasBaseline() )
            {
                List<TimeSeries<Pair<Double, R>>> baseline = next.getBaselineData()
                                                                 .get();
                long nextBaseline = EvaluationUtilities.getOptimalBlockSizesForStationaryBootstrap( baseline );
                blockSizes.add( nextBaseline );
            }
        }

        double optimalBlockSizeReal =
                blockSizes.stream()
                          .mapToLong( Long::longValue )
                          .average()
                          .orElseThrow( () -> new WresProcessingException( "Failed to estimate the optimal block size "
                                                                           + "for the stationary bootstrap." ) );

        long optimalBlockSize = ( long ) Math.ceil( optimalBlockSizeReal );

        LOGGER.debug( "Determined an optimal block size of {} timesteps for applying the stationary bootstrap to the "
                      + "pool with metadata: {}. This is an average of the optimal block sizes across all observed "
                      + "time-series within the pool, which included the following block sizes: {}.",
                      optimalBlockSize,
                      pool.getMetadata(),
                      blockSizes );

        return optimalBlockSize;
    }

    /**
     * Estimates the optimal block size for the consolidated left-ish time-series in the input.
     * @param <T> the type of time-series data
     * @param pool the pool
     * @return the optimal block size for the stationary bootstrap
     */
    private static <T> long getOptimalBlockSizesForStationaryBootstrap( List<TimeSeries<Pair<Double, T>>> pool )
    {
        Function<Pair<Double, T>, Double> doubleMapper = Pair::getLeft;
        List<TimeSeries<Double>> leftSeries = pool.stream()
                                                  .map( n -> TimeSeriesSlicer.transform( n,
                                                                                         doubleMapper,
                                                                                         null ) )
                                                  .toList();
        TimeSeries<Double> consolidatedLeft = TimeSeriesSlicer.consolidate( leftSeries );
        double[] data = consolidatedLeft.getEvents()
                                        .stream()
                                        .mapToDouble( Event::getValue )
                                        .toArray();

        return BlockSizeEstimator.getOptimalBlockSize( data );
    }

    /**
     * Small value class to collect together variables needed to instantiate an evaluation.
     *
     * @param systemSettings the system settings
     * @param declaration the project declaration
     * @param evaluationId the evaluation identifier
     * @param subscriberApprover the subscriber approver
     * @param monitor the evaluation event monitor
     * @param databaseServices the database services
     * @param caches the database caches/ORMs
     * @param metricsAndThresholds the metrics and thresholds
     * @param project the project
     * @param evaluation the evaluation
     * @param timeSeriesStore the time-series data store
     */

    @RecordBuilder
    record EvaluationDetails( SystemSettings systemSettings,
                              EvaluationDeclaration declaration,
                              String evaluationId,
                              SubscriberApprover subscriberApprover,
                              EvaluationEvent monitor,
                              DatabaseServices databaseServices,
                              DatabaseCaches caches,
                              Set<MetricsAndThresholds> metricsAndThresholds,
                              Project project,
                              EvaluationMessager evaluation,
                              TimeSeriesStore timeSeriesStore )
    {
        /**
         * @return true if there is an in-memory store of time-series, false otherwise.
         */

        private boolean hasInMemoryStore()
        {
            return Objects.nonNull( this.timeSeriesStore );
        }
    }

    /**
     * A value object for shared writers.
     * @param sharedSampleWriters Shared writers for sample data.
     * @param sharedBaselineSampleWriters Shared writers for baseline sampled data.
     */

    private record SharedWriters( SharedSampleDataWriters sharedSampleWriters,
                                  SharedSampleDataWriters sharedBaselineSampleWriters ) implements Closeable
    {
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
    }

    private EvaluationUtilities()
    {
        // Helper class with static methods therefore no construction allowed.
    }

}