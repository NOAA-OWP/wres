package wres.pipeline;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.ExecutionResult;
import wres.config.MultiDeclarationFactory;
import wres.config.yaml.DeclarationException;
import wres.config.yaml.DeclarationUtilities;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.FeatureGroups;
import wres.config.yaml.components.Features;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.PoolRequest;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.thresholds.MetricsAndThresholds;
import wres.datamodel.thresholds.ThresholdSlicer;
import wres.datamodel.time.TimeSeriesStore;
import wres.datamodel.units.UnitMapper;
import wres.events.EvaluationEventUtilities;
import wres.events.EvaluationMessager;
import wres.events.broker.BrokerConnectionFactory;
import wres.events.subscribe.ConsumerFactory;
import wres.events.subscribe.EvaluationSubscriber;
import wres.events.subscribe.SubscriberApprover;
import wres.io.database.Database;
import wres.io.database.caching.DatabaseCaches;
import wres.io.database.locking.DatabaseLockManager;
import wres.io.database.locking.DatabaseLockManagerNoop;
import wres.io.ingesting.IngestResult;
import wres.io.ingesting.SourceLoader;
import wres.io.ingesting.TimeSeriesIngester;
import wres.io.ingesting.database.DatabaseTimeSeriesIngester;
import wres.io.ingesting.memory.InMemoryTimeSeriesIngester;
import wres.io.project.Project;
import wres.io.project.Projects;
import wres.reading.ReaderUtilities;
import wres.reading.netcdf.grid.GriddedFeatures;
import wres.writing.netcdf.NetcdfOutputWriter;
import wres.metrics.SummaryStatisticsCalculator;
import wres.pipeline.pooling.PoolFactory;
import wres.pipeline.pooling.PoolGroupTracker;
import wres.pipeline.pooling.PoolReporter;
import wres.statistics.generated.Consumer;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.GeometryTuple;
import wres.system.DatabaseType;
import wres.system.SystemSettings;

/**
 * A complete implementation of an evaluation pipeline that begins with a project declaration string.
 *
 * @author James Brown
 * @author Jesse Bickel
 */
public class Evaluator
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( Evaluator.class );

    /** Unique identifier for this instance of the core messaging client. */

    private static final String CLIENT_ID = EvaluationEventUtilities.getId();

    /** Number of items in a reading queue. Enough to allow join to be called with few or no rejected executions. */
    private static final int READ_QUEUE_LENGTH = 100_000;

    /** System settings.*/
    private final SystemSettings systemSettings;

    /** Database instance.*/
    private final Database database;

    /** Broker connections.*/
    private final BrokerConnectionFactory brokerConnectionFactory;

    /**
     * Creates an instance.
     * @param systemSettings the system settings, not null
     * @param database the database, if required
     * @param brokerConnectionFactory a broker connection factory, not null
     * @throws NullPointerException if any required input is null
     */
    public Evaluator( SystemSettings systemSettings,
                      Database database,
                      BrokerConnectionFactory brokerConnectionFactory )
    {
        Objects.requireNonNull( systemSettings );
        Objects.requireNonNull( brokerConnectionFactory );

        if ( systemSettings.isUseDatabase() )
        {
            Objects.requireNonNull( database );
        }

        this.systemSettings = systemSettings;
        this.database = database;
        this.brokerConnectionFactory = brokerConnectionFactory;
    }

    /**
     * Processes a declaration string or a path to a file that contains a declaration string.
     * @param declarationOrPath the declaration or path to a declaration string
     * @param canceller a cancellation callback to cancel the running evaluation
     * @return the result of the execution
     * @throws UserInputException when WRES detects a problem with the declaration
     * @throws InternalWresException when WRES encounters an internal error, unrelated to the declaration
     */

    public ExecutionResult evaluate( String declarationOrPath,
                                     Canceller canceller )
    {
        if ( Objects.isNull( declarationOrPath ) )
        {
            throw new InternalWresException( "Expected a non-null declaration string or path." );
        }

        if ( Objects.isNull( canceller ) )
        {
            throw new InternalWresException( "Expected a non-null cancellation callback." );
        }

        // Get a unique evaluation identifier
        String evaluationId = EvaluationEventUtilities.getId();

        // Create a record of failure, but only commit if a failure actually occurs
        EvaluationEvent failure = EvaluationEvent.of();
        failure.begin();

        EvaluationDeclaration declaration;
        String rawDeclaration;
        try
        {
            FileSystem fileSystem = FileSystems.getDefault();
            rawDeclaration = MultiDeclarationFactory.getDeclarationString( declarationOrPath, fileSystem );
            declaration = MultiDeclarationFactory.from( rawDeclaration, fileSystem, true, true );
        }
        catch ( DeclarationException | IOException e )
        {
            LOGGER.error( "Failed to read or validate an evaluation declaration from the command line argument.", e );
            UserInputException translated = new UserInputException( "The evaluation declaration was invalid.", e );
            failure.setFailed();
            failure.commit();
            return ExecutionResult.failure( translated, canceller.cancelled(), evaluationId );
        }

        SystemSettings settings = this.getSystemSettings();
        if ( !settings.isUseDatabase() )
        {
            LOGGER.info( "Running evaluation in memory." );
        }

        return this.evaluate( declaration, rawDeclaration, canceller, evaluationId );
    }

    /**
     * Executes an evaluation.
     * @param declaration the declaration
     * @param rawDeclaration the raw declaration string to log
     * @param canceller a cancellation callback to cancel the running evaluation, not null
     * @return the result of the execution
     * @throws NullPointerException if the projectConfigPlus is null
     */

    private ExecutionResult evaluate( EvaluationDeclaration declaration,
                                      String rawDeclaration,
                                      Canceller canceller,
                                      String evaluationId )
    {
        Objects.requireNonNull( declaration );
        Objects.requireNonNull( canceller );

        EvaluationEvent monitor = EvaluationEvent.of();
        monitor.begin();

        // Build a processing pipeline whose work is performed by a collection of thread pools, one pool for each
        // conceptual activity. There are two activities not represented here, namely reading of time-series data from
        // source formats and ingest of time-series into a persistent data store, such as a database. The thread pools
        // used for reading and ingesting are managed by wres-io. For example, see wres.io.ingesting.SourceLoader and
        // wres.io.ingesting.database.DatabaseTimeSeriesIngester. In principle, those thread pools could be abstracted
        // here too, but ingest is implementation specific (e.g., in-memory ingest is an ingest facade and does not
        // require a thread pool) and some readers, notably archive readers, have their own thread pool
        ThreadFactory poolFactory = new BasicThreadFactory.Builder()
                .namingPattern( "Pool Thread %d" )
                .build();
        // Use this thread pool for slicing pools and to dispatch metric tasks as ArrayBlockingQueue operates a FIFO
        // policy. If dependent tasks (slicing) are queued ahead of independent ones (metrics) in the same pool, there
        // is a DEADLOCK probability. Likewise, use a separate thread pool for dispatching pools and completing tasks
        // within pools with the same number of threads in each.

        // Inner readers may create additional thread factories (e.g., archives).
        ThreadFactory readingFactory = new BasicThreadFactory.Builder()
                .namingPattern( "Outer Reading Thread %d" )
                .build();
        ThreadFactory slicingFactory = new BasicThreadFactory.Builder()
                .namingPattern( "Slicing Thread %d" )
                .build();
        ThreadFactory metricFactory = new BasicThreadFactory.Builder()
                .namingPattern( "Metric Thread %d" )
                .build();
        ThreadFactory productFactory = new BasicThreadFactory.Builder()
                .namingPattern( "Format Writing Thread %d" )
                .build();

        SystemSettings settings = this.getSystemSettings();

        // Create some unbounded work queues. For evaluations that produce statistics faster than subscribers (e.g.,
        // statistics format writers) can consume them, production is flow controlled explicitly via the statistics
        // messaging. See #95867. There is a separate thread pool for each of several themed tasks within an evaluation
        // pipeline that are resource-intensive
        BlockingQueue<Runnable> poolQueue = new LinkedBlockingQueue<>();
        BlockingQueue<Runnable> slicingQueue = new LinkedBlockingQueue<>();
        BlockingQueue<Runnable> metricQueue = new LinkedBlockingQueue<>();
        BlockingQueue<Runnable> productQueue = new LinkedBlockingQueue<>();
        BlockingQueue<Runnable> readingQueue = new ArrayBlockingQueue<>( READ_QUEUE_LENGTH );

        // Create some thread pools to perform the work required by different parts of the evaluation pipeline

        // Thread pool for reading formats
        RejectedExecutionHandler readingHandler = new ThreadPoolExecutor.CallerRunsPolicy();
        ExecutorService readingExecutor = new ThreadPoolExecutor( settings.getMaximumReadThreads(),
                                                                  settings.getMaximumReadThreads(),
                                                                  settings.getPoolObjectLifespan(),
                                                                  TimeUnit.MILLISECONDS,
                                                                  readingQueue,
                                                                  readingFactory,
                                                                  readingHandler );
        // Thread pool that processes pools of pairs
        ExecutorService poolExecutor = new ThreadPoolExecutor( settings.getMaximumPoolThreads(),
                                                               settings.getMaximumPoolThreads(),
                                                               settings.getPoolObjectLifespan(),
                                                               TimeUnit.MILLISECONDS,
                                                               poolQueue,
                                                               poolFactory );
        // Thread pool that performs slicing/dicing and transforming of pooled data
        ExecutorService slicingExecutor = new ThreadPoolExecutor( settings.getMaximumSlicingThreads(),
                                                                  settings.getMaximumSlicingThreads(),
                                                                  0,
                                                                  TimeUnit.SECONDS,
                                                                  slicingQueue,
                                                                  slicingFactory );
        // Thread pool that processes metrics
        ExecutorService metricExecutor = new ThreadPoolExecutor( settings.getMaximumMetricThreads(),
                                                                 settings.getMaximumMetricThreads(),
                                                                 settings.getPoolObjectLifespan(),
                                                                 TimeUnit.MILLISECONDS,
                                                                 metricQueue,
                                                                 metricFactory );
        // Thread pool that generates products, such as statistics formats
        ExecutorService productExecutor = new ThreadPoolExecutor( settings.getMaximumProductThreads(),
                                                                  settings.getMaximumProductThreads(),
                                                                  settings.getPoolObjectLifespan(),
                                                                  TimeUnit.MILLISECONDS,
                                                                  productQueue,
                                                                  productFactory );

        // Conditional executors
        ExecutorService samplingUncertaintyExecutor = null;
        ExecutorService ingestExecutor = null;

        if ( Objects.nonNull( declaration.sampleUncertainty() ) )
        {
            ThreadFactory resamplingFactory = new BasicThreadFactory.Builder()
                    .namingPattern( "Sampling Uncertainty Thread %d" )
                    .build();
            int threadCount = settings.getMaximumSamplingUncertaintyThreads();
            samplingUncertaintyExecutor = java.util.concurrent.Executors.newFixedThreadPool( threadCount,
                                                                                             resamplingFactory );
        }

        // Create database services if needed
        DatabaseServices databaseServices = null;
        DatabaseLockManager lockManager;
        if ( settings.isUseDatabase() )
        {
            Database innerDatabase = this.getDatabase();

            // Register for cancellation
            canceller.setDatabase( innerDatabase );

            lockManager = DatabaseLockManager.from( settings,
                                                    innerDatabase::getRawConnection );

            databaseServices = new DatabaseServices( innerDatabase, lockManager );

            // Create an ingest executor
            ThreadFactory ingestFactory =
                    new BasicThreadFactory.Builder().namingPattern( "Ingesting Thread %d" )
                                                    .build();
            // Queue should be large enough to allow join() call to be reached with zero or few rejected submissions to the
            // executor service.
            BlockingQueue<Runnable> ingestQueue = new ArrayBlockingQueue<>( settings.getMaximumIngestThreads() );

            RejectedExecutionHandler ingestHandler = new ThreadPoolExecutor.CallerRunsPolicy();
            ingestExecutor = new ThreadPoolExecutor( settings.getMaximumIngestThreads(),
                                                     settings.getMaximumIngestThreads(),
                                                     settings.getPoolObjectLifespan(),
                                                     TimeUnit.MILLISECONDS,
                                                     ingestQueue,
                                                     ingestFactory,
                                                     ingestHandler );
        }
        else
        {
            // Dummy lock manager for in-memory evaluations
            lockManager = new DatabaseLockManagerNoop();
        }

        // Reduce our set of executors to one object
        EvaluationExecutors executors = new EvaluationExecutors( readingExecutor,
                                                                 ingestExecutor,
                                                                 poolExecutor,
                                                                 slicingExecutor,
                                                                 metricExecutor,
                                                                 productExecutor,
                                                                 samplingUncertaintyExecutor );

        // Register the executors for cancellation
        canceller.setEvaluationExecutors( executors );

        String projectHash;
        Set<Path> pathsWrittenTo = new TreeSet<>();
        ScheduledExecutorService monitoringService = null;

        try
        {
            // Mark the WRES as doing an evaluation.
            lockManager.lockShared( DatabaseType.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );

            // Monitor the task queues if required
            if ( System.getProperty( "wres.monitorTaskQueues" ) != null )
            {
                monitoringService = new ScheduledThreadPoolExecutor( 1 );
                QueueMonitor queueMonitor = new QueueMonitor( this.getDatabase(),
                                                              poolQueue,
                                                              slicingQueue,
                                                              metricQueue,
                                                              productQueue );

                monitoringService.scheduleAtFixedRate( queueMonitor,
                                                       1,
                                                       500,
                                                       TimeUnit.MILLISECONDS );
            }

            // Perform the evaluation
            Pair<Set<Path>, String> innerPathsAndProjectHash =
                    this.evaluate( settings,
                                   databaseServices,
                                   declaration,
                                   executors,
                                   monitor,
                                   canceller,
                                   evaluationId );
            pathsWrittenTo.addAll( innerPathsAndProjectHash.getLeft() );
            projectHash = innerPathsAndProjectHash.getRight();
            monitor.setDataHash( projectHash );
            monitor.setResources( pathsWrittenTo );

            lockManager.unlockShared( DatabaseType.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );
        }
        catch ( DeclarationException userException )
        {
            String message = "Please correct the project declaration.";
            UserInputException userInputException = new UserInputException( message, userException );
            monitor.setFailed();
            monitor.commit();
            return ExecutionResult.failure( declaration.label(),
                                            rawDeclaration,
                                            userInputException,
                                            canceller.cancelled(),
                                            evaluationId );
        }
        catch ( RuntimeException | IOException | SQLException internalException )
        {
            String message = "Could not complete project execution";
            InternalWresException internalWresException = new InternalWresException( message, internalException );
            monitor.setFailed();
            monitor.commit();
            return ExecutionResult.failure( declaration.label(),
                                            rawDeclaration,
                                            internalWresException,
                                            canceller.cancelled(),
                                            evaluationId );
        }
        // Shutdown
        finally
        {
            Canceller.closeGracefully( monitoringService );
            Canceller.closeGracefully( readingExecutor );
            Canceller.closeGracefully( ingestExecutor );
            Canceller.closeGracefully( productExecutor );
            Canceller.closeGracefully( metricExecutor );
            Canceller.closeGracefully( slicingExecutor );
            Canceller.closeGracefully( poolExecutor );
            Canceller.closeGracefully( samplingUncertaintyExecutor );
            lockManager.shutdown();
        }

        monitor.setSucceeded();
        monitor.commit();
        return ExecutionResult.success( declaration.label(),
                                        rawDeclaration,
                                        projectHash,
                                        pathsWrittenTo,
                                        evaluationId );
    }

    /**
     * Executes an evaluation.
     *
     * @param systemSettings the system settings
     * @param databaseServices the database services
     * @param declaration the project declaration
     * @param executors the executors
     * @param monitor an event that monitors the life cycle of the evaluation, not null
     * @param canceller a callback to allow for cancellation of the running evaluation, not null
     * @param evaluationId the id of the evaluation we are executing
     * @return the resources written and the hash of the project data
     * @throws WresProcessingException if the evaluation processing fails
     * @throws DeclarationException if the declaration is incorrect
     * @throws NullPointerException if any input is null
     * @throws IOException if the creation of outputs fails
     */

    private Pair<Set<Path>, String> evaluate( SystemSettings systemSettings,
                                              DatabaseServices databaseServices,
                                              EvaluationDeclaration declaration,
                                              EvaluationExecutors executors,
                                              EvaluationEvent monitor,
                                              Canceller canceller,
                                              String evaluationId )
            throws IOException
    {
        Objects.requireNonNull( systemSettings );
        Objects.requireNonNull( declaration );
        Objects.requireNonNull( executors );
        Objects.requireNonNull( monitor );
        Objects.requireNonNull( canceller );

        // Database needed?
        if ( systemSettings.isUseDatabase() )
        {
            Objects.requireNonNull( databaseServices );
        }

        BrokerConnectionFactory connections = this.getBrokerConnectionFactory();

        Set<Path> resources = new TreeSet<>();
        String projectHash;

        // Set the identifier where needed
        monitor.setEvaluationId( evaluationId );
        canceller.setEvaluationId( evaluationId );

        // Create output directory
        Path outputDirectory = EvaluationUtilities.createTempOutputDirectory( evaluationId );

        // Create netCDF writers
        List<NetcdfOutputWriter> netcdfWriters =
                EvaluationUtilities.getNetcdfWriters( declaration,
                                                      systemSettings,
                                                      outputDirectory );

        // Obtain any formats delivered by out-of-process subscribers.
        Set<Consumer.Format> externalFormats = EvaluationUtilities.getFormatsDeliveredByExternalSubscribers();

        LOGGER.debug( "These formats will be delivered by external subscribers: {}.", externalFormats );

        // Formats delivered by within-process subscribers, in a mutable list
        Set<Consumer.Format> internalFormats = wres.statistics.MessageFactory.getDeclaredFormats( declaration.formats()
                                                                                                             .outputs() );

        internalFormats = new HashSet<>( internalFormats );
        internalFormats.removeAll( externalFormats );

        LOGGER.debug( "These formats will be delivered by internal subscribers: {}.", internalFormats );

        String consumerId = EvaluationEventUtilities.getId();

        // Moving this into the try-with-resources would require a different approach than notifying the evaluation to
        // stop( Exception e ) on encountering an error that is not visible to it. See discussion in #90292.
        EvaluationMessager evaluationMessager = null;

        ConsumerFactory consumerFactory = new StatisticsConsumerFactory( consumerId,
                                                                         new HashSet<>( internalFormats ),
                                                                         netcdfWriters,
                                                                         declaration );

        try ( SharedWriters sharedWriters = EvaluationUtilities.getSharedWriters( declaration,
                                                                                  outputDirectory );
              // Create a subscriber for the format writers that are within-process. The subscriber is built for this
              // evaluation only, and should not serve other evaluations, else there is a risk that short-running
              // subscribers die without managing to serve the evaluations they promised to serve. This complexity
              // disappears when all subscribers are moved to separate, long-running, processes: #89868
              EvaluationSubscriber formatsSubscriber = EvaluationSubscriber.of( consumerFactory,
                                                                                executors.productExecutor(),
                                                                                connections,
                                                                                evaluationId ) )
        {
            // Set the subscriber to cancel
            canceller.setInternalFormatsSubscriber( formatsSubscriber );

            // Start the subscriber
            formatsSubscriber.start();

            // Restrict the subscribers for internally-delivered formats otherwise core clients may steal format
            // writing work from each other. This is expected insofar as all subscribers are par. However, core clients
            // currently run in short-running processes, we want to estimate resources for core clients effectively,
            // and some format writers are stateful (e.g., netcdf), hence this is currently a bad thing. Goal: place
            // all format writers in long-running processes instead. See #88262 and #88267.
            SubscriberApprover subscriberApprover =
                    new SubscriberApprover.Builder().addApprovedSubscriber( internalFormats,
                                                                            consumerId )
                                                    .build();

            // Package the details needed to build the evaluation
            EvaluationDetails evaluationDetails =
                    EvaluationDetailsBuilder.builder()
                                            .systemSettings( systemSettings )
                                            .declaration( declaration )
                                            .evaluationId( evaluationId )
                                            .subscriberApprover( subscriberApprover )
                                            .monitor( monitor )
                                            .databaseServices( databaseServices )
                                            .build();

            // Open an evaluation, to be closed on completion or stopped on exception

            // Look up any needed feature correlations and thresholds, generate a new declaration. These are needed for
            // reading and ingest, as well as subsequent steps, so perform this upfront: #116208
            EvaluationDeclaration declarationWithFeatures = ReaderUtilities.readAndFillFeatures( declaration );

            // Update the small bag-o-state
            evaluationDetails = EvaluationDetailsBuilder.builder( evaluationDetails )
                                                        .declaration( declarationWithFeatures )
                                                        .build();
            // Gridded features cache, if required. See #51232.
            GriddedFeatures.Builder griddedFeaturesBuilder =
                    EvaluationUtilities.getGriddedFeaturesCache( declarationWithFeatures );

            LOGGER.debug( "Beginning ingest of time-series data..." );

            Project project;

            // Is the evaluation in a database? If so, use implementations that support a database
            if ( systemSettings.isUseDatabase() )
            {
                // Build the database caches/ORMs, if required
                DatabaseCaches caches = DatabaseCaches.of( databaseServices.database() );
                // Set the caches
                evaluationDetails = EvaluationDetailsBuilder.builder( evaluationDetails )
                                                            .caches( caches )
                                                            .build();
                DatabaseTimeSeriesIngester databaseIngester =
                        new DatabaseTimeSeriesIngester.Builder().setSystemSettings( evaluationDetails.systemSettings() )
                                                                .setDatabase( databaseServices.database() )
                                                                .setCaches( caches )
                                                                .setIngestExecutor( executors.ingestExecutor() )
                                                                .setLockManager( databaseServices.databaseLockManager() )
                                                                .build();

                List<IngestResult> ingestResults = SourceLoader.load( databaseIngester,
                                                                      evaluationDetails.systemSettings(),
                                                                      declarationWithFeatures,
                                                                      griddedFeaturesBuilder,
                                                                      executors.readingExecutor() );

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

                // Caches are read only from now on, post ingest
                caches.setReadOnly();
            }
            // In-memory evaluation
            else
            {
                // Builder for an in-memory store of time-series
                TimeSeriesStore.Builder timeSeriesStoreBuilder = new TimeSeriesStore.Builder();

                // Ingester that ingests into the in-memory store
                TimeSeriesIngester timeSeriesIngester = InMemoryTimeSeriesIngester.of( timeSeriesStoreBuilder );

                // Load the sources using the ingester and create the ingest results to share
                List<IngestResult> ingestResults = SourceLoader.load( timeSeriesIngester,
                                                                      evaluationDetails.systemSettings(),
                                                                      declarationWithFeatures,
                                                                      griddedFeaturesBuilder,
                                                                      executors.readingExecutor() );

                // The immutable collection of in-memory time-series
                TimeSeriesStore timeSeriesStore = timeSeriesStoreBuilder.build();

                // Set the store
                evaluationDetails = EvaluationDetailsBuilder.builder( evaluationDetails )
                                                            .timeSeriesStore( timeSeriesStore )
                                                            .build();
                project = Projects.getProject( declarationWithFeatures,
                                               timeSeriesStore,
                                               ingestResults );
            }

            // Re-assign the declaration augmented by the ingested data
            declarationWithFeatures = project.getDeclaration();

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

            // Get the features, as described in the ingested time-series data, which may differ in number and details
            // from the declared features. For example, they are filtered for data availability, spatial mask etc. and
            // may include extra descriptive information, such as a geometry or location description.
            Set<FeatureTuple> features = new HashSet<>( project.getFeatures() );
            Set<GeometryTuple> unwrappedFeatures = features.stream()
                                                           .map( FeatureTuple::getGeometryTuple )
                                                           .collect( Collectors.toUnmodifiableSet() );

            // Create the feature groups
            Set<FeatureGroup> featureGroups = EvaluationUtilities.getFeatureGroups( project, features );

            // If summary statistics are required for multi-feature groups, ensure that all the singletons within
            // feature groups are part of the singletons list for evaluation, but do not publish statistics for these
            // singleton features unless they were declared explicitly
            Set<FeatureGroup> doNotPublish = project.getFeatureGroupsForWhichStatisticsShouldNotBePublished();

            // Adjust the declaration to include the fully described features based on the ingested data
            Features dataFeatures = new Features( unwrappedFeatures );
            FeatureGroups dataFeatureGroups = new FeatureGroups( featureGroups.stream()
                                                                              .map( FeatureGroup::getGeometryGroup )
                                                                              // Non-singletons only
                                                                              .filter( g -> g.getGeometryTuplesList()
                                                                                             .size()
                                                                                            > 1 )
                                                                              .collect( Collectors.toSet() ) );
            declarationWithFeaturesAndThresholds =
                    EvaluationDeclarationBuilder.builder( declarationWithFeaturesAndThresholds )
                                                .features( dataFeatures )
                                                .featureGroups( dataFeatureGroups )
                                                .build();

            // Get the atomic metrics and thresholds for processing, each group representing a distinct processing task.
            // Ensure that named features correspond to the features associated with the data rather than declaration,
            // i.e., use the adjusted declaration
            Set<MetricsAndThresholds> metricsAndThresholds =
                    ThresholdSlicer.getMetricsAndThresholdsForProcessing( declarationWithFeaturesAndThresholds );

            // Create any netcdf blobs for writing. See #80267-137.
            Set<FeatureGroup> adjustedFeatureGroups =
                    EvaluationUtilities.adjustFeatureGroupsForSummaryStatistics( featureGroups,
                                                                                 unwrappedFeatures,
                                                                                 declaration.summaryStatistics(),
                                                                                 doNotPublish );

            EvaluationUtilities.createNetcdfBlobs( netcdfWriters,
                                                   adjustedFeatureGroups,
                                                   metricsAndThresholds );

            // Create the evaluation description with any analyzed units and variable names,  post-ingest
            // This is akin to a post-ingest interpolation/augmentation of the declared project. Earlier stages of
            // interpolation include interpolation of missing declaration and service calls to interpolate features and
            // thresholds. This is the latest step in that process of combining the declaration and data
            Evaluation evaluationDescription = MessageFactory.parse( declaration );
            evaluationDescription = EvaluationUtilities.setAnalyzedUnitsAndVariableNames( evaluationDescription,
                                                                                          project );

            // Build the evaluation description for messaging. In the future, there may be a desire to build the
            // evaluation description prior to ingest, in order to message the status of ingest to client applications.
            // In order to build an evaluation description before ingest, those parts of the evaluation description that
            // depend on the data would need to be part of the pool description instead (e.g., the measurement units).
            // Indeed, the timescale is part of the pool description for this reason.
            evaluationMessager = EvaluationMessager.of( evaluationDescription,
                                                        connections,
                                                        Evaluator.CLIENT_ID,
                                                        evaluationDetails.evaluationId(),
                                                        evaluationDetails.subscriberApprover() );

            // Register the messager for cancellation
            canceller.setEvaluationMessager( evaluationMessager );

            // Start the evaluation
            evaluationMessager.start();

            PoolFactory poolFactory = PoolFactory.of( project );
            List<PoolRequest> poolRequests = EvaluationUtilities.getPoolRequests( poolFactory, evaluationDescription );

            int poolCount = poolRequests.size();
            monitor.setPoolCount( poolCount );

            // Report on the completion state of all pools
            // Identify the feature groups for which only summary statistics are calculated - start with the adjusted
            // features and remove all singletons as there are no summary statistics for singletons
            Set<FeatureGroup> summaryStatisticsOnly =
                    EvaluationUtilities.getFeatureGroupsForSummaryStatisticsOnly( adjustedFeatureGroups, declaration );
            PoolReporter poolReporter = new PoolReporter( declarationWithFeaturesAndThresholds,
                                                          summaryStatisticsOnly,
                                                          poolCount,
                                                          true,
                                                          evaluationId );

            // Get a message group tracker to notify the completion of groups that encompass several pools. Currently,
            // this is feature-group shaped, but additional shapes may be desired in future
            PoolGroupTracker groupTracker = PoolGroupTracker.ofFeatureGroupTracker( evaluationMessager, poolRequests );

            // Create the summary statistics calculators to increment with raw statistics
            Map<String, List<SummaryStatisticsCalculator>> summaryStatsCalculators =
                    EvaluationUtilities.getSummaryStatisticsCalculators( declarationWithFeaturesAndThresholds,
                                                                         poolCount );
            Map<String, List<SummaryStatisticsCalculator>> summaryStataCalculatorsForBaseline = Map.of();
            boolean separateMetricsForBaseline = DeclarationUtilities.hasBaseline( declaration )
                                                 && declaration.baseline()
                                                               .separateMetrics();
            if ( separateMetricsForBaseline )
            {
                summaryStataCalculatorsForBaseline =
                        EvaluationUtilities.getSummaryStatisticsCalculators( declarationWithFeaturesAndThresholds,
                                                                             poolCount );
            }

            // Set the project and evaluation, metrics and thresholds and summary statistics
            evaluationDetails = EvaluationDetailsBuilder.builder( evaluationDetails )
                                                        .project( project )
                                                        .evaluation( evaluationMessager )
                                                        .declaration( declarationWithFeaturesAndThresholds )
                                                        .metricsAndThresholds( metricsAndThresholds )
                                                        .summaryStatistics( summaryStatsCalculators )
                                                        .summaryStatisticsForBaseline(
                                                                summaryStataCalculatorsForBaseline )
                                                        .summaryStatisticsOnly( doNotPublish )
                                                        .build();

            // Create and publish the raw evaluation statistics
            PoolDetails poolDetails = new PoolDetails( poolFactory, poolRequests, poolReporter, groupTracker );
            EvaluationUtilities.createAndPublishStatistics( evaluationDetails,
                                                            poolDetails,
                                                            sharedWriters,
                                                            executors );

            // Create and publish any summary statistics derived from the raw statistics
            EvaluationUtilities.createAndPublishSummaryStatistics( summaryStatsCalculators,
                                                                   evaluationMessager );

            EvaluationUtilities.createAndPublishSummaryStatistics( summaryStataCalculatorsForBaseline,
                                                                   evaluationMessager );

            // Report that all publication was completed. At this stage, a message is sent indicating the expected
            // message count for all message types, thereby allowing consumers to know when all messages have arrived.
            evaluationMessager.markPublicationCompleteReportedSuccess();

            // Report on the pools
            poolReporter.report();

            // Wait for all async consumption tasks to complete, including all format writing tasks
            evaluationMessager.await();

            // Since the netcdf consumers are created here, they should be destroyed here. An attempt should be made to
            // close the netcdf writers before the "finally" clause because these writers employ a delayed write, which
            // could still fail exceptionally. Such a failure should stop the evaluation exceptionally. For further
            // context see #81790-21 and the detailed description in EvaluationMessager.await(), which clarifies that
            // awaiting an evaluation to complete does not mean that all consumers have finished their work, only
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
                resources.addAll( sharedWriters.getSampleDataWriters()
                                               .get() );
            }
            if ( sharedWriters.hasSharedBaselineSampleWriters() )
            {
                resources.addAll( sharedWriters.getBaselineSampleDataWriters()
                                               .get() );
            }

            // Messaging failed, possibly in a separate client? Then throw an exception: see #122343
            if ( evaluationMessager.isFailed() )
            {
                throw new WresProcessingException( "Evaluation '"
                                                   + evaluationId
                                                   + "' failed because a messaging client reported an error." );
            }

            return Pair.of( Collections.unmodifiableSet( resources ), projectHash );
        }
        // Allow a user-error to be distinguished separately
        catch ( DeclarationException userError )
        {
            EvaluationUtilities.forceStop( evaluationMessager, userError, evaluationId );

            // Rethrow
            throw userError;
        }
        // Internal error
        catch ( RuntimeException internalError )
        {
            EvaluationUtilities.forceStop( evaluationMessager, internalError, evaluationId );

            // Decorate and rethrow
            throw new WresProcessingException( "Encountered an error while processing evaluation '"
                                               + evaluationId
                                               + "': ",
                                               internalError );
        }
        finally
        {
            // Close the netCDF writers if not closed
            EvaluationUtilities.closeNetcdfWriters( netcdfWriters, evaluationMessager, evaluationId );

            // Clean-up an empty output directory: #67088
            EvaluationUtilities.cleanEmptyOutputDirectory( outputDirectory );

            // Add the paths written by external subscribers
            if ( Objects.nonNull( evaluationMessager ) )
            {
                resources.addAll( evaluationMessager.getPathsWrittenBySubscribers() );
            }

            LOGGER.info( "Wrote the following output: {}", resources );

            // Close the evaluation messager always (even if stopped on exception)
            EvaluationUtilities.closeEvaluationMessager( evaluationMessager, evaluationId );
        }
    }

    /**
     * @return the system settings.
     */
    private SystemSettings getSystemSettings()
    {
        return this.systemSettings;
    }

    /**
     * @return the database.
     */
    private Database getDatabase()
    {
        return this.database;
    }

    /**
     * @return the broker connections.
     */
    private BrokerConnectionFactory getBrokerConnectionFactory()
    {
        return this.brokerConnectionFactory;
    }

    /** Queue monitor. */
    private record QueueMonitor( Database database, Queue<?> poolQueue, Queue<?> thresholdQueue,
                                 Queue<?> metricQueue, Queue<?> productQueue ) implements Runnable
    {
        @Override
        public void run()
        {
            int poolCount = 0;
            int databaseCount = 0;
            int thresholdCount = 0;
            int metricCount = 0;
            int productCount = 0;

            if ( this.poolQueue != null )
            {
                poolCount = this.poolQueue.size();
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

            if ( this.database != null )
            {
                databaseCount = database.getDatabaseQueueTaskCount();
            }

            LOGGER.info( "PoolQ={}, DatabaseQ={}, ThresholdQ={}, MetricQ={}, ProductQ={}",
                         poolCount,
                         databaseCount,
                         thresholdCount,
                         metricCount,
                         productCount );
        }
    }
}
