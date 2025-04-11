package wres.pipeline;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.MetricConstants;
import wres.config.yaml.DeclarationException;
import wres.config.yaml.DeclarationUtilities;
import wres.config.yaml.components.DataType;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.GeneratedBaselines;
import wres.config.yaml.components.ThresholdType;
import wres.datamodel.bootstrap.BootstrapUtilities;
import wres.datamodel.time.TimeWindowSlicer;
import wres.datamodel.types.Ensemble;
import wres.datamodel.MissingValues;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolRequest;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.thresholds.MetricsAndThresholds;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdSlicer;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindowOuter;
import wres.events.EvaluationEventUtilities;
import wres.events.EvaluationMessager;
import wres.reading.netcdf.grid.GriddedFeatures;
import wres.io.retrieving.database.EnsembleSingleValuedRetrieverFactory;
import wres.io.retrieving.memory.EnsembleSingleValuedRetrieverFactoryInMemory;
import wres.statistics.generated.Evaluation;
import wres.writing.csv.pairs.EnsemblePairsWriter;
import wres.metrics.BoxplotSummaryStatisticFunction;
import wres.metrics.DiagramSummaryStatisticFunction;
import wres.metrics.FunctionFactory;
import wres.metrics.ScalarSummaryStatisticFunction;
import wres.metrics.SummaryStatisticsCalculator;
import wres.pipeline.pooling.PoolFactory;
import wres.pipeline.pooling.PoolParameters;
import wres.io.project.Project;
import wres.io.retrieving.RetrieverFactory;
import wres.io.retrieving.database.EnsembleRetrieverFactory;
import wres.io.retrieving.database.SingleValuedRetrieverFactory;
import wres.io.retrieving.memory.EnsembleRetrieverFactoryInMemory;
import wres.io.retrieving.memory.SingleValuedRetrieverFactoryInMemory;
import wres.writing.SharedSampleDataWriters;
import wres.writing.csv.pairs.PairsWriter;
import wres.writing.netcdf.NetcdfOutputWriter;
import wres.pipeline.pooling.PoolProcessor;
import wres.pipeline.pooling.PoolReporter;
import wres.pipeline.statistics.StatisticsProcessor;
import wres.pipeline.statistics.EnsembleStatisticsProcessor;
import wres.pipeline.statistics.SingleValuedStatisticsProcessor;
import wres.statistics.generated.Consumer.Format;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Statistics;
import wres.statistics.generated.SummaryStatistic;
import wres.statistics.generated.Threshold;
import wres.statistics.generated.TimeWindow;
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

    /** A function that estimates the trace count of a pool that contains ensemble traces. */
    private static final ToIntFunction<Pool<TimeSeries<Pair<Double, Ensemble>>>> ENSEMBLE_TRACE_COUNT_ESTIMATOR =
            EvaluationUtilities.getEnsembleTraceCountEstimator();

    /** A function that estimates the trace count of a pool that contains single-valued traces. */
    private static final ToIntFunction<Pool<TimeSeries<Pair<Double, Double>>>> SINGLE_VALUED_TRACE_COUNT_ESTIMATOR =
            EvaluationUtilities.getSingleValuedTraceCountEstimator();

    /** A block size estimator for the stationary bootstrap as applied to single-valued pools.*/
    private static final Function<Pool<TimeSeries<Pair<Double, Double>>>, Pair<Long, Duration>>
            SINGLE_VALUED_BLOCK_SIZE_ESTIMATOR = BootstrapUtilities::getOptimalBlockSizeForStationaryBootstrap;

    /** A block size estimator for the stationary bootstrap as applied to ensemble pools.*/
    private static final Function<Pool<TimeSeries<Pair<Double, Ensemble>>>, Pair<Long, Duration>>
            ENSEMBLE_BLOCK_SIZE_ESTIMATOR = BootstrapUtilities::getOptimalBlockSizeForStationaryBootstrap;

    /** A default region name for summary statistics aggregated across geographic features. */
    private static final String SUMMARY_STATISTICS_ACROSS_FEATURES = "ALL FEATURES";

    /** Re-used string. */
    private static final String CREATED_AN_IN_MEMORY_RETRIEVER_FACTORY =
            "Created an in-memory retriever factory.";

    /** Re-used string. */
    private static final String CREATED_A_RETRIEVER_FACTORY_BACKED_BY_A_PERSISTENT_STORE =
            "Created a retriever factory backed by a persistent store.";

    /** Maximum number of time windows to log. */
    private static final int MAXIMUM_TIME_WINDOWS_TO_LOG = 1000;

    /** Maximum number of geographic features to log. */
    private static final int MAXIMUM_FEATURES_TO_LOG = 10000;

    /**
     * Generates statistics by creating a sequence of pool-shaped tasks, which are then chained together and executed.
     * On completion of each pool-shaped tasks, the statistics associated with that pool are published. Finally, creates
     * any end-of-chain summary statistics and publishes those too.
     *
     * @param evaluationDetails the evaluation details
     * @param poolDetails the pool details
     * @param sharedWriters the shared writers
     * @param executors the executor services
     * @throws NullPointerException if any input is null
     */

    static void createAndPublishStatistics( EvaluationDetails evaluationDetails,
                                            PoolDetails poolDetails,
                                            SharedWriters sharedWriters,
                                            EvaluationExecutors executors )
    {
        Objects.requireNonNull( evaluationDetails );
        Objects.requireNonNull( poolDetails );
        Objects.requireNonNull( sharedWriters );
        Objects.requireNonNull( executors );

        LOGGER.info( "Submitting {} pool tasks for execution, which are awaiting completion. This can take a "
                     + "while...",
                     poolDetails.poolRequests()
                                .size() );

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
        CompletableFuture<Object> poolTaskChain = EvaluationUtilities.getPoolTasks( evaluationDetails,
                                                                                    poolDetails,
                                                                                    sharedWriters,
                                                                                    executors );

        // Wait for the pool chain to complete
        poolTaskChain.join();
    }

    /**
     * Create and publish the summary statistics.
     *
     * @param evaluationDetails the evaluation details
     */
    static void createAndPublishSummaryStatistics( EvaluationDetails evaluationDetails )
    {
        Objects.requireNonNull( evaluationDetails );

        // Main dataset
        EvaluationUtilities.createAndPublishSummaryStatistics( evaluationDetails.summaryStatistics(),
                                                               evaluationDetails.evaluationMessager() );

        // Baseline
        EvaluationUtilities.createAndPublishSummaryStatistics( evaluationDetails.summaryStatisticsForBaseline(),
                                                               evaluationDetails.evaluationMessager() );
    }

    /**
     * Generates a collection of {@link SummaryStatisticsCalculator} from an {@link EvaluationDeclaration}. Currently,
     * supports only {@link wres.statistics.generated.SummaryStatistic.StatisticDimension#FEATURES}.
     * @param declaration the evaluation declaration
     * @param poolCount the number of pools for which raw (non-summary) statistics are required
     * @param clearThresholdValues is true to clear event threshold values from the summary statistics, false otherwise
     * @return the summary statistics calculators
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the dimension is unsupported
     */

    static Map<String, List<SummaryStatisticsCalculator>> getSummaryStatisticsCalculators( EvaluationDeclaration declaration,
                                                                                           long poolCount,
                                                                                           boolean clearThresholdValues )
    {
        Objects.requireNonNull( declaration );

        // No summary statistics?
        if ( declaration.summaryStatistics()
                        .isEmpty() )
        {
            LOGGER.debug( "No summary statistics were declared." );

            return Map.of();
        }

        // Collect the geographic feature dimensions to aggregate, which are the only supported dimensions
        // Note that clearing threshold values is linked to these aggregation dimensions.
        Set<SummaryStatistic.StatisticDimension> dimensions =
                declaration.summaryStatistics()
                           .stream()
                           .map( SummaryStatistic::getDimension )
                           .filter( d -> d == SummaryStatistic.StatisticDimension.FEATURE_GROUP
                                         || d == SummaryStatistic.StatisticDimension.FEATURES )
                           .collect( Collectors.toSet() );

        return EvaluationUtilities.getSummaryStatisticsCalculators( declaration,
                                                                    dimensions,
                                                                    poolCount,
                                                                    clearThresholdValues );
    }

    /**
     * Creates the NetCDF blobs for writing, where needed.
     * @param netcdfWriters the writers
     * @param featureGroups the feature groups
     * @param metricsAndThresholds the metrics and thresholds
     * @throws IOException if the blobs could not be written for any reason
     */

    static void createNetcdfBlobs( List<NetcdfOutputWriter> netcdfWriters,
                                   Set<FeatureGroup> featureGroups,
                                   Set<MetricsAndThresholds> metricsAndThresholds ) throws IOException
    {
        if ( !netcdfWriters.isEmpty() )
        {
            LOGGER.info( "Creating Netcdf blobs for statistics. This can take a while..." );

            for ( NetcdfOutputWriter writer : netcdfWriters )
            {
                writer.createBlobsForWriting( featureGroups,
                                              metricsAndThresholds );
            }

            LOGGER.info( "Finished creating Netcdf blobs, which are now ready to accept statistics." );
        }
    }

    /**
     * Close the NetCDF writers, if required.
     * @param netcdfWriters the NetCDF writers
     * @param evaluation the evaluation messager
     * @param evaluationId the evaluation identifier
     */

    static void closeNetcdfWriters( List<NetcdfOutputWriter> netcdfWriters,
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
                // Forcibly stop the evaluation messager if writing failed
                EvaluationUtilities.forceStop( evaluation, we, evaluationId );
                LOGGER.warn( "Failed to close a netcdf writer.", we );
            }
        }
    }

    /**
     * Deletes an empty output directory when an evaluation fails to produce any output.
     * @param outputDirectory the output directory
     * @throws IOException if the directory could not be deleted for any reason
     */

    static void cleanEmptyOutputDirectory( Path outputDirectory ) throws IOException
    {
        // Clean-up an empty output directory: #67088
        try ( Stream<Path> outputs = Files.list( outputDirectory ) )
        {
            if ( outputs.findAny()
                        .isEmpty() )
            {
                // Will only succeed for an empty directory
                boolean status = Files.deleteIfExists( outputDirectory );

                LOGGER.debug( "Attempted to remove empty output directory {} with success status: {}",
                              outputDirectory,
                              status );
            }
        }
    }

    /**
     * Closes an evaluation messager.
     * @param messager the evaluation messager
     * @param evaluationId the evaluation identifier
     */

    static void closeEvaluationMessager( EvaluationMessager messager,
                                         String evaluationId )
    {
        try
        {
            if ( Objects.nonNull( messager ) )
            {
                LOGGER.info( "Closing the messager for evaluation {}...", evaluationId );
                messager.close();
                LOGGER.info( "The messager for evaluation {} has been closed.", evaluationId );
            }
        }
        catch ( IOException e )
        {
            String message = "Failed to close evaluation " + evaluationId + ".";
            LOGGER.warn( message, e );
        }
    }

    /**
     * Returns an instance of {@link SharedWriters} for shared writing.
     *
     * @param declaration the project declaration
     * @param outputDirectory the output directory for writing
     * @return the shared writer instance
     */

    static SharedWriters getSharedWriters( EvaluationDeclaration declaration,
                                           Path outputDirectory )
    {
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
                                                decimalFormatter );
            // Baseline writer?
            if ( DeclarationUtilities.hasBaseline( declaration ) )
            {
                sharedBaselineSampleWriters = SharedSampleDataWriters.of( Paths.get( outputDirectory.toString(),
                                                                                     PairsWriter.DEFAULT_BASELINE_PAIRS_ZIP_NAME ),
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

    static List<NetcdfOutputWriter> getNetcdfWriters( EvaluationDeclaration declaration,
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

    static Path createTempOutputDirectory( String evaluationId ) throws IOException
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
            tempDir = tempDir + FileSystems.getDefault()
                                           .getSeparator()
                      + jobId;
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

    static Set<Format> getFormatsDeliveredByExternalSubscribers()
    {
        String externalGraphics = System.getProperty( "wres.externalGraphics" );
        String externalNumerics = System.getProperty( "wres.externalNumerics" );

        Set<Format> formats = new HashSet<>();

        // Add external graphics if required
        if ( Objects.nonNull( externalGraphics )
             && "true".equalsIgnoreCase( externalGraphics ) )
        {
            formats.add( Format.PNG );
            formats.add( Format.SVG );
        }

        // Add external numerics if required
        if ( Objects.nonNull( externalNumerics )
             && "true".equalsIgnoreCase( externalNumerics ) )
        {
            formats.add( Format.PROTOBUF );
            formats.add( Format.CSV2 );
        }

        return Collections.unmodifiableSet( formats );
    }

    /**
     * Creates the pool requests from the project.
     *
     * @param poolFactory the pool factory
     * @param evaluation the evaluation description
     * @param evaluationDetails the evaluation details
     * @return the pool requests
     */

    static List<PoolRequest> getPoolRequests( PoolFactory poolFactory,
                                              Evaluation evaluation,
                                              EvaluationDetails evaluationDetails )
    {
        RetrieverFactory<Double, Double, Double> retriever = null;

        // Event detection supports single-valued datasets only
        if ( Objects.nonNull( evaluationDetails.declaration()
                                               .eventDetection() ) )
        {
            retriever = EvaluationUtilities.getSingleValuedRetrieverFactory( evaluationDetails );
        }

        List<PoolRequest> poolRequests = poolFactory.getPoolRequests( evaluation, retriever );

        EvaluationUtilities.logPoolDetails( evaluationDetails, poolRequests );

        return poolRequests;
    }

    /**
     * @param declaration the project declaration
     * @return a gridded feature cache or null if none is required
     */

    static GriddedFeatures.Builder getGriddedFeaturesCache( EvaluationDeclaration declaration )
    {
        GriddedFeatures.Builder griddedFeatures = null;

        if ( Objects.nonNull( declaration.spatialMask() ) )
        {
            griddedFeatures = new GriddedFeatures.Builder( declaration.spatialMask() );
        }

        return griddedFeatures;
    }

    /**
     * Forcibly stops an evaluation messager on encountering an error, if already created.
     * @param evaluationMessager the evaluation messager
     * @param error the error
     * @param evaluationId the evaluation identifier
     */
    static void forceStop( EvaluationMessager evaluationMessager, Exception error, String evaluationId )
    {
        if ( Objects.nonNull( evaluationMessager ) )
        {
            // Stop forcibly
            LOGGER.debug( FORCIBLY_STOPPING_EVALUATION_UPON_ENCOUNTERING_AN_INTERNAL_ERROR, evaluationId );

            evaluationMessager.stop( error );
        }
    }

    /**
     * Appends any feature groups associated with summary statistics (across features) to the input and removes features
     * that should not be published, which are intended for calculating summary statistics only.
     * @param featureGroups the existing feature groups
     * @param features the singleton features
     * @param summaryStatistics the summary statistics declaration
     * @param doNotPublish the feature groups for which statistics should not be published
     * @return the adjusted feature groups
     */
    static Set<FeatureGroup> adjustFeatureGroupsForSummaryStatistics( Set<FeatureGroup> featureGroups,
                                                                      Set<GeometryTuple> features,
                                                                      Set<SummaryStatistic> summaryStatistics,
                                                                      Set<FeatureGroup> doNotPublish )
    {
        // Remove any unwanted feature groups and re-assign to the input variable for further use
        featureGroups = new HashSet<>( featureGroups );
        featureGroups.removeAll( doNotPublish );

        // Summary statistics across geographic features?
        // No, do not add an all-feature group
        if ( summaryStatistics.stream()
                              .noneMatch( n -> n.getDimension() == SummaryStatistic.StatisticDimension.FEATURES ) )
        {
            LOGGER.debug( "No summary statistics across features that require an extra feature group." );
        }
        // Yes, add an all-feature group
        else
        {
            GeometryGroup summaryStatisticsGroup = GeometryGroup.newBuilder()
                                                                .addAllGeometryTuples( features )
                                                                .setRegionName( EvaluationUtilities.SUMMARY_STATISTICS_ACROSS_FEATURES )
                                                                .build();
            FeatureGroup summaryStatisticsFeatureGroup = FeatureGroup.of( summaryStatisticsGroup );
            featureGroups.add( summaryStatisticsFeatureGroup );

            LOGGER.debug( "Added a summary statistics feature group: {}.", summaryStatisticsFeatureGroup );

        }

        return Collections.unmodifiableSet( featureGroups );
    }

    /**
     * Generates the geographic feature groups for which only summary statistics are required and no raw statistics.
     * @param featureGroups the existing feature groups
     * @param declaration the evaluation declaration
     * @return the adjusted feature groups
     */
    static Set<FeatureGroup> getFeatureGroupsForSummaryStatisticsOnly( Set<FeatureGroup> featureGroups,
                                                                       EvaluationDeclaration declaration )
    {
        if ( declaration.summaryStatistics()
                        .isEmpty() )
        {
            return Set.of();
        }

        Set<FeatureGroup> groups = new HashSet<>();

        boolean byGroup =
                declaration.summaryStatistics()
                           .stream()
                           .anyMatch( g -> g.getDimension() == SummaryStatistic.StatisticDimension.FEATURE_GROUP );

        for ( FeatureGroup next : featureGroups )
        {
            if ( !next.isSingleton()
                 && ( byGroup
                      || SUMMARY_STATISTICS_ACROSS_FEATURES.equals( next.getName() ) ) )
            {
                groups.add( next );
            }
        }

        return Collections.unmodifiableSet( groups );
    }

    /**
     * Determines whether there are event thresholds with the same name whose values vary across geographic features.
     * @param metricsAndThresholds the metrics and thresholds
     * @return true if there are event thresholds that vary across features, false if they are fixed
     * @throws NullPointerException if the input is null
     */
    static boolean hasEventThresholdsThatVaryAcrossFeatures( Set<MetricsAndThresholds> metricsAndThresholds )
    {
        Objects.requireNonNull( metricsAndThresholds );

        // Group the thresholds by name and determine whether any groups contain thresholds with different values
        return metricsAndThresholds.stream()
                                   .flatMap( s -> s.thresholds()
                                                   .values()
                                                   .stream()
                                                   .flatMap( Collection::stream ) )
                                   // Group the thresholds by name
                                   .collect( Collectors.groupingBy( t -> t.getThreshold()
                                                                          .getName() ) )
                                   // Are there any named thresholds with different threshold values?
                                   .values()
                                   .stream()
                                   .map( t -> t.stream()
                                               .map( ThresholdOuter::getValues )
                                               .collect( Collectors.toSet() ) )
                                   .anyMatch( c -> c.size() > 1 );
    }

    /**
     * Returns a {@link RetrieverFactory} for single-valued datasets.
     * @param details the evaluation details
     * @return the retriever factory
     */
    static RetrieverFactory<Double, Double, Double> getSingleValuedRetrieverFactory( EvaluationDetails details )
    {
        // Create a retriever factory to support retrieval for this project
        RetrieverFactory<Double, Double, Double> retrieverFactory;
        if ( details.hasInMemoryStore() )
        {
            LOGGER.debug( CREATED_AN_IN_MEMORY_RETRIEVER_FACTORY );
            retrieverFactory = SingleValuedRetrieverFactoryInMemory.of( details.project(),
                                                                        details.timeSeriesStore() );
        }
        else
        {
            LOGGER.debug( CREATED_A_RETRIEVER_FACTORY_BACKED_BY_A_PERSISTENT_STORE );
            retrieverFactory = SingleValuedRetrieverFactory.of( details.project(),
                                                                details.databaseServices()
                                                                       .database(),
                                                                details.caches() );
        }

        return retrieverFactory;
    }

    /**
     * Creates and publishes the summary statistics.
     * @param summaryStatistics the summary statistics calculators, mapped by message group identifier
     * @param messager the evaluation messager
     * @throws NullPointerException if any input is null
     */
    private static void createAndPublishSummaryStatistics( Map<String, List<SummaryStatisticsCalculator>> summaryStatistics,
                                                           EvaluationMessager messager )
    {
        Objects.requireNonNull( summaryStatistics );
        Objects.requireNonNull( messager );

        LOGGER.debug( "Publishing summary statistics from {} summary statistics calculators.",
                      summaryStatistics.size() );

        // Publish the summary statistics per message group
        Set<String> groupIds = new HashSet<>();
        for ( Map.Entry<String, List<SummaryStatisticsCalculator>> next : summaryStatistics.entrySet() )
        {
            // Generate the summary statistics
            String groupId = next.getKey();
            List<SummaryStatisticsCalculator> calculators = next.getValue();
            List<Statistics> nextStatistics = calculators.stream()
                                                         .flatMap( c -> c.get()
                                                                         .stream() )
                                                         .toList();

            nextStatistics.forEach( m -> messager.publish( m, groupId ) );

            groupIds.add( groupId );

            LOGGER.debug( "Published {} summary statistics for group {}", nextStatistics.size(), groupId );
        }

        // Mark the publication complete for all groups
        groupIds.forEach( messager::markGroupPublicationCompleteReportedSuccess );

        LOGGER.debug( "Finished publishing summary statistics." );
    }

    /**
     * Creates one pool task for each pool request and then chains them together, such that all of the pools complete 
     * nominally or one completes exceptionally.
     *
     * @param evaluationDetails the evaluation details
     * @param poolDetails the pool details
     * @param sharedWriters the shared writers
     * @param executors the executor services
     * @return the pool task chain
     */

    private static CompletableFuture<Object> getPoolTasks( EvaluationDetails evaluationDetails,
                                                           PoolDetails poolDetails,
                                                           SharedWriters sharedWriters,
                                                           EvaluationExecutors executors )
    {
        CompletableFuture<Object> poolTasks;

        DataType type = evaluationDetails.project()
                                         .getDeclaration()
                                         .right()
                                         .type();

        // Ensemble pairs
        if ( type == DataType.ENSEMBLE_FORECASTS )
        {
            List<PoolProcessor<Double, Ensemble>> poolProcessors =
                    EvaluationUtilities.getEnsemblePoolProcessors( evaluationDetails,
                                                                   poolDetails,
                                                                   sharedWriters,
                                                                   executors );

            poolTasks = EvaluationUtilities.getPoolTaskChain( poolProcessors,
                                                              executors.poolExecutor(),
                                                              poolDetails.poolReporter() );
        }
        // All other single-valued types
        else
        {
            List<PoolProcessor<Double, Double>> poolProcessors =
                    EvaluationUtilities.getSingleValuedPoolProcessors( evaluationDetails,
                                                                       poolDetails,
                                                                       sharedWriters,
                                                                       executors );

            poolTasks = EvaluationUtilities.getPoolTaskChain( poolProcessors,
                                                              executors.poolExecutor(),
                                                              poolDetails.poolReporter() );
        }

        return poolTasks;
    }

    /**
     * Returns a list of processors for processing single-valued pools, one for each pool request.
     * @param evaluationDetails the evaluation details
     * @param poolDetails the pool details
     * @param sharedWriters the shared writers
     * @param executors the executors
     * @return the single-valued processors
     */

    private static List<PoolProcessor<Double, Double>> getSingleValuedPoolProcessors( EvaluationDetails evaluationDetails,
                                                                                      PoolDetails poolDetails,
                                                                                      SharedWriters sharedWriters,
                                                                                      EvaluationExecutors executors )
    {
        Project project = evaluationDetails.project();

        SystemSettings settings = evaluationDetails.systemSettings();
        PoolParameters poolParameters =
                new PoolParameters.Builder().setFeatureBatchThreshold( settings.getFeatureBatchThreshold() )
                                            .setFeatureBatchSize( settings.getFeatureBatchSize() )
                                            .build();

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
        RetrieverFactory<Double, Double, Double> retrieverFactory =
                EvaluationUtilities.getSingleValuedRetrieverFactory( evaluationDetails );

        // Create the pool suppliers for all pools in this evaluation
        PoolFactory poolFactory = poolDetails.poolFactory();
        List<Pair<PoolRequest, Supplier<Pool<TimeSeries<Pair<Double, Double>>>>>> poolSuppliers =
                poolFactory.getSingleValuedPools( poolDetails.poolRequests(),
                                                  retrieverFactory,
                                                  poolParameters );

        // Stand-up the pair writers
        PairsWriter<Double, Double> pairsWriter = null;
        PairsWriter<Double, Double> basePairsWriter = null;
        if ( sharedWriters.hasSharedSampleWriters() )
        {
            pairsWriter = sharedWriters.getSampleDataWriters()
                                       .getSingleValuedWriter();
        }
        if ( sharedWriters.hasSharedBaselineSampleWriters() )
        {
            basePairsWriter = sharedWriters.getBaselineSampleDataWriters()
                                           .getSingleValuedWriter();
        }

        List<PoolProcessor<Double, Double>> poolProcessors = new ArrayList<>();

        for ( Pair<PoolRequest, Supplier<Pool<TimeSeries<Pair<Double, Double>>>>> next : poolSuppliers )
        {
            PoolRequest poolRequest = next.getKey();

            // Publish statistics?
            boolean publishStatistics =
                    EvaluationUtilities.shouldPublishStatistics( poolRequest,
                                                                 evaluationDetails.summaryStatisticsOnly() );

            Supplier<Pool<TimeSeries<Pair<Double, Double>>>> poolSupplier = next.getValue();

            List<SummaryStatisticsCalculator> calculators = evaluationDetails.summaryStatistics()
                                                                             .values()
                                                                             .stream()
                                                                             .flatMap( List::stream )
                                                                             .toList();

            List<SummaryStatisticsCalculator> baselineCalculators = evaluationDetails.summaryStatisticsForBaseline()
                                                                                     .values()
                                                                                     .stream()
                                                                                     .flatMap( List::stream )
                                                                                     .toList();

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
                            .setEvaluation( evaluationDetails.evaluationMessager() )
                            .setMonitor( evaluationDetails.monitor() )
                            .setTraceCountEstimator( SINGLE_VALUED_TRACE_COUNT_ESTIMATOR )
                            .setSeparateMetricsForBaseline( separateMetrics )
                            .setPoolGroupTracker( poolDetails.poolGroupTracker() )
                            .setSummaryStatisticsCalculators( calculators )
                            .setSummaryStatisticsCalculatorsForBaseline( baselineCalculators )
                            .setPublishStatistics( publishStatistics )
                            .build();

            poolProcessors.add( poolProcessor );
        }

        return Collections.unmodifiableList( poolProcessors );
    }

    /**
     * Returns a list of processors for processing ensemble pools, one for each pool request.
     * @param evaluationDetails the evaluation details
     * @param poolDetails the pool details
     * @param sharedWriters the shared writers
     * @param executors the executors
     * @return the ensemble processors
     */

    private static List<PoolProcessor<Double, Ensemble>> getEnsemblePoolProcessors( EvaluationDetails evaluationDetails,
                                                                                    PoolDetails poolDetails,
                                                                                    SharedWriters sharedWriters,
                                                                                    EvaluationExecutors executors )
    {
        Project project = evaluationDetails.project();
        EvaluationDeclaration declaration = evaluationDetails.declaration();

        SystemSettings settings = evaluationDetails.systemSettings();
        PoolParameters poolParameters =
                new PoolParameters.Builder().setFeatureBatchThreshold( settings.getFeatureBatchThreshold() )
                                            .setFeatureBatchSize( settings.getFeatureBatchSize() )
                                            .build();

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

        PoolFactory poolFactory = poolDetails.poolFactory();

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
                throw new DeclarationException(
                        "Discovered an evaluation with ensemble forecasts and a generated "
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
                LOGGER.debug( CREATED_AN_IN_MEMORY_RETRIEVER_FACTORY );
                retrieverFactory = EnsembleSingleValuedRetrieverFactoryInMemory.of( evaluationDetails.project(),
                                                                                    evaluationDetails.timeSeriesStore() );
            }
            else
            {
                LOGGER.debug( CREATED_A_RETRIEVER_FACTORY_BACKED_BY_A_PERSISTENT_STORE );
                retrieverFactory = EnsembleSingleValuedRetrieverFactory.of( project,
                                                                            evaluationDetails.databaseServices()
                                                                                             .database(),
                                                                            evaluationDetails.caches() );
            }

            // Create the pool suppliers for all pools in this evaluation
            poolSuppliers = poolFactory.getEnsemblePoolsWithGeneratedBaseline( poolDetails.poolRequests(),
                                                                               retrieverFactory,
                                                                               poolParameters );
        }
        else
        {
            RetrieverFactory<Double, Ensemble, Ensemble> retrieverFactory;
            if ( evaluationDetails.hasInMemoryStore() )
            {
                LOGGER.debug( CREATED_AN_IN_MEMORY_RETRIEVER_FACTORY );
                retrieverFactory = EnsembleRetrieverFactoryInMemory.of( evaluationDetails.project(),
                                                                        evaluationDetails.timeSeriesStore() );
            }
            else
            {
                LOGGER.debug( CREATED_A_RETRIEVER_FACTORY_BACKED_BY_A_PERSISTENT_STORE );
                retrieverFactory = EnsembleRetrieverFactory.of( project,
                                                                evaluationDetails.databaseServices()
                                                                                 .database(),
                                                                evaluationDetails.caches() );
            }

            // Create the pool suppliers for all pools in this evaluation
            poolSuppliers = poolFactory.getEnsemblePools( poolDetails.poolRequests(),
                                                          retrieverFactory,
                                                          poolParameters );
        }

        // Stand-up the pair writers
        // The ensemble writers have are instantiated for writing in two parts. First (earlier) the writers are created.
        // Second (here), the writers are primed for writing using the superset of ensemble members across all pools.
        // The second stage accommodates the writing of disparate pools that contain only a subset of ensemble members.
        PairsWriter<Double, Ensemble> pairsWriter = null;
        PairsWriter<Double, Ensemble> basePairsWriter = null;
        if ( sharedWriters.hasSharedSampleWriters() )
        {
            SortedSet<String> labels = project.getEnsembleLabels( DatasetOrientation.RIGHT );
            EnsemblePairsWriter ensembleWriter = sharedWriters.getSampleDataWriters()
                                                              .getEnsembleWriter();
            ensembleWriter.prime( labels );
            pairsWriter = ensembleWriter;
        }
        if ( sharedWriters.hasSharedBaselineSampleWriters() )
        {
            SortedSet<String> labels = project.getEnsembleLabels( DatasetOrientation.BASELINE );
            EnsemblePairsWriter ensembleWriter = sharedWriters.getBaselineSampleDataWriters()
                                                              .getEnsembleWriter();
            ensembleWriter.prime( labels );
            basePairsWriter = ensembleWriter;
        }

        List<PoolProcessor<Double, Ensemble>> poolProcessors = new ArrayList<>();

        for ( Pair<PoolRequest, Supplier<Pool<TimeSeries<Pair<Double, Ensemble>>>>> next : poolSuppliers )
        {
            PoolRequest poolRequest = next.getKey();

            // Publish statistics?
            boolean publishStatistics =
                    EvaluationUtilities.shouldPublishStatistics( poolRequest,
                                                                 evaluationDetails.summaryStatisticsOnly() );

            Supplier<Pool<TimeSeries<Pair<Double, Ensemble>>>> poolSupplier = next.getValue();

            List<SummaryStatisticsCalculator> calculators = evaluationDetails.summaryStatistics()
                                                                             .values()
                                                                             .stream()
                                                                             .flatMap( List::stream )
                                                                             .toList();

            List<SummaryStatisticsCalculator> baselineCalculators = evaluationDetails.summaryStatisticsForBaseline()
                                                                                     .values()
                                                                                     .stream()
                                                                                     .flatMap( List::stream )
                                                                                     .toList();

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
                            .setEvaluation( evaluationDetails.evaluationMessager() )
                            .setMonitor( evaluationDetails.monitor() )
                            .setTraceCountEstimator( ENSEMBLE_TRACE_COUNT_ESTIMATOR )
                            .setSeparateMetricsForBaseline( separateMetrics )
                            .setPoolGroupTracker( poolDetails.poolGroupTracker() )
                            .setSummaryStatisticsCalculators( calculators )
                            .setSummaryStatisticsCalculatorsForBaseline( baselineCalculators )
                            .setPublishStatistics( publishStatistics )
                            .build();

            poolProcessors.add( poolProcessor );
        }

        return Collections.unmodifiableList( poolProcessors );
    }

    /**
     * @param poolRequest the pool request
     * @param summaryStatisticsOnly the singleton features for which raw statistics should not be published
     * @return whether to publish the raw statistics for this pool request
     */
    private static boolean shouldPublishStatistics( PoolRequest poolRequest,
                                                    Set<FeatureGroup> summaryStatisticsOnly )
    {
        // Publish if this is a multi-feature group, else a singleton that is not within the summary statistics only
        // collection
        return !poolRequest.getMetadata()
                           .getFeatureGroup()
                           .isSingleton()
               || !summaryStatisticsOnly.contains( poolRequest.getMetadata()
                                                              .getFeatureGroup() );
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
            // Could be no metrics if sampling uncertainties requested and no metrics support it
            if ( !nextMetrics.metrics()
                             .isEmpty() )
            {
                StatisticsProcessor<Pool<TimeSeries<Pair<Double, Double>>>> nextProcessor =
                        new SingleValuedStatisticsProcessor( nextMetrics,
                                                             slicingExecutor,
                                                             metricExecutor );
                processors.add( nextProcessor );
            }
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
            // Could be no metrics if sampling uncertainties requested and no metrics support it
            if ( !nextMetrics.metrics()
                             .isEmpty() )
            {
                StatisticsProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>> nextProcessor =
                        new EnsembleStatisticsProcessor( nextMetrics,
                                                         slicingExecutor,
                                                         metricExecutor );
                processors.add( nextProcessor );
            }
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
        return project.hasBaseline()
               && project.getDeclaration()
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
            // Estimate the number of traces using the largest of the first ensemble events in each time-series
            List<TimeSeries<Pair<Double, Ensemble>>> series = pool.get();
            int traceCount = series.stream()
                                   .filter( next -> !next.getEvents()
                                                         .isEmpty() )
                                   .mapToInt( next -> next.getEvents()
                                                          .first()
                                                          .getValue()
                                                          .getRight()
                                                          .size() )
                                   .max()
                                   .orElse( 0 );

            return traceCount * series.size();
        };
    }

    /**
     * Joins the filters in the two collections of filters using {@link Predicate#and(Predicate)}. If either collection
     * is empty, returns the opposite collection.
     *
     * @param first the first collection of filters, possibly empty
     * @param second the second collection of filters, possibly empty
     * @return the joined filters
     */

    private static List<Predicate<Statistics>> join( List<Predicate<Statistics>> first,
                                                     List<Predicate<Statistics>> second )
    {
        // First filters only
        if ( second.isEmpty() )
        {
            return first;
        }

        // Second filters only
        if ( first.isEmpty() )
        {
            return second;
        }

        // Combined filters
        List<Predicate<Statistics>> combined = new ArrayList<>();

        for ( Predicate<Statistics> nextFirst : first )
        {
            for ( Predicate<Statistics> nextSecond : second )
            {
                Predicate<Statistics> next = nextFirst.and( nextSecond );
                combined.add( next );
            }
        }

        return Collections.unmodifiableList( combined );
    }

    /**
     * Generates a collection of {@link SummaryStatisticsCalculator}.
     *
     * @param declaration the evaluation declaration
     * @param dimensions the feature dimensions over which to perform aggregation
     * @return the summary statistics calculators
     * @param poolCount the number of pools for which raw (non-summary) statistics are required
     * @param clearThresholdValues is true to clear event threshold values, false otherwise
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the dimension is unsupported
     */

    private static Map<String, List<SummaryStatisticsCalculator>> getSummaryStatisticsCalculators( EvaluationDeclaration declaration,
                                                                                                   Set<SummaryStatistic.StatisticDimension> dimensions,
                                                                                                   long poolCount,
                                                                                                   boolean clearThresholdValues )
    {
        Objects.requireNonNull( declaration );

        boolean separateMetricsForBaseline = DeclarationUtilities.hasBaseline( declaration )
                                             && declaration.baseline()
                                                           .separateMetrics();

        // Summary statistics may be calculated across various dimensions, as declared by a user. Each of these
        // dimensions corresponds to a filter, which must be created. The filters are additive, i.e., they all apply at
        // once

        // The time window filters are built from a comparator that looks for specific attributes of a time window. By
        // default, all attributes are considered, i.e., simple equality. When filtering specific attributes, then
        // refined filters replace the default: see below
        BiPredicate<TimeWindow, TimeWindow> timeWindowComparator = Objects::equals; // Default: compare for equality

        // Get the geographic feature filters and metadata adapters
        List<FeatureGroupFilterAdapter> featureFilters = new ArrayList<>();

        // Summary statistics across all geographic features lumped together
        if ( dimensions.contains( SummaryStatistic.StatisticDimension.FEATURES ) )
        {
            List<FeatureGroupFilterAdapter> filters =
                    EvaluationUtilities.getOneBigFeatureGroupForSummaryStatistics( declaration,
                                                                                   separateMetricsForBaseline );
            featureFilters.addAll( filters );
            LOGGER.debug( "Created {} filters for all geographic features.", filters.size() );
        }

        // Summary statistics across all geographic features in a single feature group
        if ( dimensions.contains( SummaryStatistic.StatisticDimension.FEATURE_GROUP ) )
        {
            List<FeatureGroupFilterAdapter> filters =
                    EvaluationUtilities.getFeatureGroupForSummaryStatistics( declaration,
                                                                             separateMetricsForBaseline );
            featureFilters.addAll( filters );
            LOGGER.debug( "Created {} filters for geographic feature groups.", filters.size() );
        }

        if ( LOGGER.isDebugEnabled() )
        {
            List<FeatureGroup> featureGroups = featureFilters.stream()
                                                             .filter( g -> g.geometryGroup()
                                                                            .getGeometryTuplesCount() > 0 )
                                                             .map( g -> FeatureGroup.of( g.geometryGroup() ) )
                                                             .toList();
            LOGGER.debug( "Creating filters for the following geographic feature groups: {}.", featureGroups );
        }

        // Get the time window filters from the time window comparator and combine with threshold filters
        Set<TimeWindowOuter> timeWindows = TimeWindowSlicer.getTimeWindows( declaration );
        Set<wres.config.yaml.components.Threshold> thresholds = DeclarationUtilities.getInbandThresholds( declaration );
        List<TimeWindowAndThresholdFilterAdapter> timeWindowAndThresholdFilters =
                EvaluationUtilities.getTimeWindowAndThresholdFilters( timeWindows,
                                                                      timeWindowComparator,
                                                                      thresholds,
                                                                      separateMetricsForBaseline,
                                                                      clearThresholdValues );

        return EvaluationUtilities.getSummaryStatisticsCalculators( declaration,
                                                                    featureFilters,
                                                                    timeWindowAndThresholdFilters,
                                                                    timeWindows.size(),
                                                                    poolCount );
    }

    /**
     * Generates a collection of {@link SummaryStatisticsCalculator}.
     *
     * @param declaration the evaluation declaration
     * @param featureFilters the feature filters
     * @param timeWindowAndThresholdFilters the time window and threshold filters
     * @param timeWindowCount the number of time windows
     * @param poolCount the number of pools
     * @return the summary statistics calculators
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the dimension is unsupported
     */

    private static Map<String, List<SummaryStatisticsCalculator>> getSummaryStatisticsCalculators( EvaluationDeclaration declaration,
                                                                                                   List<FeatureGroupFilterAdapter> featureFilters,
                                                                                                   List<TimeWindowAndThresholdFilterAdapter> timeWindowAndThresholdFilters,
                                                                                                   int timeWindowCount,
                                                                                                   long poolCount )
    {
        ChronoUnit timeUnits = declaration.durationFormat();

        LOGGER.debug( "Discovered the following time units for summary statistic generation (where relevant): {}.",
                      timeUnits );

        // Create the calculators
        Set<SummaryStatistic> summaryStatistics = declaration.summaryStatistics();

        LOGGER.debug( "Discovered {} summary statistics to generate.",
                      summaryStatistics.size() );

        Set<ScalarSummaryStatisticFunction> scores = new HashSet<>();
        Set<DiagramSummaryStatisticFunction> diagrams = new HashSet<>();
        Set<BoxplotSummaryStatisticFunction> boxplots = new HashSet<>();

        for ( SummaryStatistic nextStatistic : summaryStatistics )
        {
            SummaryStatistic.StatisticName name = nextStatistic.getStatistic();
            MetricConstants behavioralName = MetricConstants.valueOf( name.name() );

            // Diagram?
            if ( behavioralName.isInGroup( MetricConstants.StatisticType.DIAGRAM ) )
            {
                DiagramSummaryStatisticFunction nextDiagram =
                        FunctionFactory.ofDiagramSummaryStatistic( nextStatistic );
                diagrams.add( nextDiagram );
                LOGGER.debug( "Discovered a diagram summary statistics: {}.", name );
            }
            else if ( behavioralName.isInGroup( MetricConstants.StatisticType.BOXPLOT_PER_POOL ) )
            {
                BoxplotSummaryStatisticFunction nextBoxplot =
                        FunctionFactory.ofBoxplotSummaryStatistic( nextStatistic );
                boxplots.add( nextBoxplot );
                LOGGER.debug( "Discovered a box plot summary statistics: {}.", name );
            }
            else
            {
                // No minimum sample size for summary statistics at present (no declaration hook)
                // Not re-using the minimum sample size on pairs
                ScalarSummaryStatisticFunction nextScalar =
                        FunctionFactory.ofScalarSummaryStatistic( nextStatistic, 0 );
                scores.add( nextScalar );
                LOGGER.debug( "Discovered a scalar summary statistic: {}.", name );
            }
        }

        // Generate one calculator for each combination of filters
        Map<String, List<SummaryStatisticsCalculator>> calculators = new HashMap<>();
        for ( FeatureGroupFilterAdapter nextOuterFilter : featureFilters )
        {
            // Messaging by feature group: get a group id for the next group
            String groupId = EvaluationEventUtilities.getId();

            Predicate<Statistics> featureFilter = nextOuterFilter.filter();
            BinaryOperator<Statistics> featureAdapter = nextOuterFilter.adapter();

            // The feature dimension for which statistics are required
            SummaryStatistic.StatisticDimension dimension = nextOuterFilter.dimension();

            // Filter the statistics by dimension
            Set<ScalarSummaryStatisticFunction> nextScalar = scores.stream()
                                                                   .filter( n -> n.statistic()
                                                                                  .getDimension() == dimension )
                                                                   .collect( Collectors.toUnmodifiableSet() );
            Set<DiagramSummaryStatisticFunction> nextDiagrams = diagrams.stream()
                                                                        .filter( n -> n.statistic()
                                                                                       .getDimension() == dimension )
                                                                        .collect( Collectors.toUnmodifiableSet() );
            Set<BoxplotSummaryStatisticFunction> nextBoxplots = boxplots.stream()
                                                                        .filter( n -> n.statistic()
                                                                                       .getDimension() == dimension )
                                                                        .collect( Collectors.toUnmodifiableSet() );

            List<SummaryStatisticsCalculator> nextCalculators = new ArrayList<>();

            for ( TimeWindowAndThresholdFilterAdapter nextInnerFilter : timeWindowAndThresholdFilters )
            {
                Predicate<Statistics> filter = featureFilter.and( nextInnerFilter.filter() );

                // Create an adapter for the pool number
                long poolNumber = poolCount
                                  + ( ( nextOuterFilter.groupNumber() - 1L ) * timeWindowCount )
                                  + nextInnerFilter.timeWindowNumber();
                BinaryOperator<Statistics> poolNumberAdapter =
                        EvaluationUtilities.getMetadataAdapterForPoolNumber( poolNumber );

                UnaryOperator<Statistics> thresholdAdapter = nextInnerFilter.adapter();
                BinaryOperator<Statistics> metadataAdapter = ( p, q ) ->
                        poolNumberAdapter.apply( featureAdapter.apply( thresholdAdapter.apply( p ), q ), q );

                SummaryStatisticsCalculator calculator = SummaryStatisticsCalculator.of( nextScalar,
                                                                                         nextDiagrams,
                                                                                         nextBoxplots,
                                                                                         filter,
                                                                                         metadataAdapter,
                                                                                         timeUnits );

                nextCalculators.add( calculator );
            }

            calculators.put( groupId, Collections.unmodifiableList( nextCalculators ) );
        }

        LOGGER.debug( "After joining the geometry groups to the time windows and thresholds, produced {} summary "
                      + "statistics calculators.",
                      calculators.size() );

        return Collections.unmodifiableMap( calculators );
    }

    /**
     * Generates a collection of filters for {@link Statistics} based on their {@link TimeWindow}, one for each supplied
     * {@link TimeWindow}.
     *
     * @param timeWindows the time windows
     * @param timeWindowEquality the test for equality of time windows
     * @param separateMetricsForBaseline whether to generate a filter for baseline pools with separate metrics
     * @return the filters
     */
    private static List<Predicate<Statistics>> getTimeWindowFilters( Set<TimeWindowOuter> timeWindows,
                                                                     BiPredicate<TimeWindow, TimeWindow> timeWindowEquality,
                                                                     boolean separateMetricsForBaseline )
    {
        List<Predicate<Statistics>> filters = new ArrayList<>();

        for ( TimeWindowOuter timeWindow : timeWindows )
        {
            // Filter for main pools
            Predicate<Statistics> nextMainFilter = statistics -> statistics.hasPool()
                                                                 && timeWindowEquality.test( statistics.getPool()
                                                                                                       .getTimeWindow(),
                                                                                             timeWindow.getTimeWindow() );
            filters.add( nextMainFilter );

            // Separate metrics for baseline?
            if ( separateMetricsForBaseline )
            {
                Predicate<Statistics> nextBaseFilter = statistics -> !statistics.hasPool()
                                                                     && statistics.hasBaselinePool()
                                                                     && statistics.getBaselinePool()
                                                                                  .getTimeWindow()
                                                                                  .equals( timeWindow.getTimeWindow() );
                filters.add( nextBaseFilter );
            }
        }

        return Collections.unmodifiableList( filters );
    }

    /**
     * Generates a collection of filters for {@link Statistics} based on their {@link Threshold}, one for each supplied
     * combination of event and decision threshold.
     *
     * @param thresholds the thresholds
     * @param separateMetricsForBaseline whether to generate a filter for baseline pools with separate metrics
     * @return the filters
     */
    private static List<Predicate<Statistics>> getThresholdFilters( Set<wres.config.yaml.components.Threshold> thresholds,
                                                                    boolean separateMetricsForBaseline )
    {
        Set<Threshold> eventThresholds = thresholds.stream()
                                                   .filter( t -> t.type() != ThresholdType.PROBABILITY_CLASSIFIER )
                                                   .map( wres.config.yaml.components.Threshold::threshold )
                                                   .collect( Collectors.toSet() );

        List<Predicate<Statistics>> eventThresholdFilters =
                EvaluationUtilities.getThresholdFilters( eventThresholds,
                                                         wres.statistics.generated.Pool::getEventThreshold,
                                                         separateMetricsForBaseline );

        LOGGER.debug( "Discovered {} event thresholds, which produced {} filters.",
                      eventThresholds.size(),
                      eventThresholdFilters.size() );

        Set<Threshold> decisionThresholds = thresholds.stream()
                                                      .filter( t -> t.type() == ThresholdType.PROBABILITY_CLASSIFIER )
                                                      .map( wres.config.yaml.components.Threshold::threshold )
                                                      .collect( Collectors.toSet() );

        List<Predicate<Statistics>> decisionThresholdFilters =
                EvaluationUtilities.getThresholdFilters( decisionThresholds,
                                                         wres.statistics.generated.Pool::getDecisionThreshold,
                                                         separateMetricsForBaseline );

        LOGGER.debug( "Discovered {} decision (probability classifier) thresholds, which produced {} filters.",
                      decisionThresholds.size(),
                      decisionThresholdFilters.size() );

        List<Predicate<Statistics>> joined =
                EvaluationUtilities.join( eventThresholdFilters, decisionThresholdFilters );

        LOGGER.debug( "After joining the event and decision thresholds, produced {} filters.", joined.size() );

        // Join the filters, as needed
        return joined;
    }

    /**
     * Generates a filter for {@link Statistics} from each supplied {@link Threshold} and a threshold getter that
     * returns a {@link Threshold} from a {@link wres.statistics.generated.Pool} at filter time.
     * @param thresholds the thresholds
     * @param thresholdGetter the threshold supplier
     * @param separateMetricsForBaseline whether to generate a separate filter for baseline data
     * @return the filters
     */
    private static List<Predicate<Statistics>> getThresholdFilters( Set<Threshold> thresholds,
                                                                    Function<wres.statistics.generated.Pool, Threshold> thresholdGetter,
                                                                    boolean separateMetricsForBaseline )
    {
        if ( thresholds.isEmpty() )
        {
            LOGGER.debug( "No thresholds to filter." );
            return List.of();
        }

        List<Predicate<Statistics>> filters = new ArrayList<>();

        // Find the unique logical thresholds
        Comparator<Threshold> comparator = ( a, b ) ->
                ThresholdSlicer.getLogicalThresholdComparator()
                               .compare( OneOrTwoThresholds.of( ThresholdOuter.of( a ) ),
                                         OneOrTwoThresholds.of( ThresholdOuter.of( b ) ) );
        Set<Threshold> uniqueThresholds = new TreeSet<>( comparator );
        uniqueThresholds.addAll( thresholds );

        // Iterate the thresholds
        for ( Threshold next : uniqueThresholds )
        {
            Predicate<Statistics> thresholdFilterMain =
                    s -> s.hasPool()
                         // Logically equal thresholds
                         && comparator.compare( next, thresholdGetter.apply( s.getPool() ) )
                            == 0;
            filters.add( thresholdFilterMain );

            // Separate metrics for baseline?
            if ( separateMetricsForBaseline )
            {
                Predicate<Statistics> thresholdFilterBase =
                        s -> !s.hasPool()
                             && s.hasBaselinePool()
                             // Logically equal thresholds
                             && comparator.compare( next, thresholdGetter.apply( s.getBaselinePool() ) )
                                == 0;
                filters.add( thresholdFilterBase );
            }
        }

        return Collections.unmodifiableList( filters );
    }

    /**
     * Creates a metadata adapter for generating summary statistics across geographic features.
     * @param geometryGroup the geometry group
     * @return the metadata adapter
     */

    private static BinaryOperator<Statistics> getMetadataAdapterForFeatureGroup( GeometryGroup geometryGroup )
    {
        return ( existing, latest ) ->
        {
            boolean isBaselinePool = !existing.hasPool()
                                     && existing.hasBaselinePool();

            Statistics.Builder adjusted = existing.toBuilder();

            // Separate baseline pool?
            if ( isBaselinePool )
            {
                adjusted.getBaselinePoolBuilder()
                        .setGeometryGroup( geometryGroup );
            }
            else
            {
                adjusted.getPoolBuilder()
                        .setGeometryGroup( geometryGroup );
            }

            return adjusted.build();
        };
    }

    /**
     * Creates a metadata adapter that removes threshold values if they are unequal across instances.
     *
     * @param clearThresholds is true to clear event threshold values, false otherwise
     * @return the metadata adapter
     */

    private static UnaryOperator<Statistics> getMetadataAdapterForThresholds( boolean clearThresholds )
    {
        return existing ->
        {
            boolean isBaselinePool = !existing.hasPool()
                                     && existing.hasBaselinePool();

            Statistics.Builder adjusted = existing.toBuilder();

            wres.statistics.generated.Pool.Builder existingPool =
                    isBaselinePool ? adjusted.getBaselinePoolBuilder() : adjusted.getPoolBuilder();

            // Clear the threshold values unless they are equal across statistics or represent all data
            if ( existingPool.hasEventThreshold()
                 && !ThresholdOuter.of( existingPool.getEventThreshold() )
                                   .isAllDataThreshold()
                 && clearThresholds )
            {
                // Set to missing rather than clearing: #126545
                Threshold.Builder builder = existingPool.getEventThresholdBuilder()
                                                        .setLeftThresholdValue( MissingValues.DOUBLE );
                if ( existingPool.getEventThreshold()
                                 .getOperator() == Threshold.ThresholdOperator.BETWEEN )
                {
                    builder.setRightThresholdValue( MissingValues.DOUBLE );
                }
            }

            return adjusted.build();
        };
    }

    /**
     * Creates a metadata adapter that alters the pool number to the prescribed number.
     *
     * @param poolNumber the pool number
     * @return the metadata adapter
     */

    private static BinaryOperator<Statistics> getMetadataAdapterForPoolNumber( long poolNumber )
    {
        return ( existing, latest ) ->
        {
            boolean isBaselinePool = !existing.hasPool()
                                     && existing.hasBaselinePool();

            Statistics.Builder adjusted = existing.toBuilder();

            wres.statistics.generated.Pool.Builder existingPool =
                    isBaselinePool ? adjusted.getBaselinePoolBuilder() : adjusted.getPoolBuilder();
            existingPool.setPoolId( poolNumber );

            return adjusted.build();
        };
    }

    /**
     * Generates a filter for {@link Statistics} that ignores all (multi-features) geometry groups in the supplied
     * declaration, retaining all singleton features.
     * @param declaration the declaration
     * @param separateMetricsForBaseline whether to generate a separate filter for baseline data
     * @return the filters
     */

    private static List<FeatureGroupFilterAdapter> getOneBigFeatureGroupForSummaryStatistics( EvaluationDeclaration declaration,
                                                                                              boolean separateMetricsForBaseline )
    {
        Set<GeometryTuple> singletons;
        Set<GeometryGroup> geometryGroups;

        if ( Objects.nonNull( declaration.features() ) )
        {
            singletons = declaration.features()
                                    .geometries();
        }
        else
        {
            singletons = Set.of();
        }

        if ( Objects.nonNull( declaration.featureGroups() ) )
        {
            geometryGroups = declaration.featureGroups()
                                        .geometryGroups();
        }
        else
        {
            geometryGroups = Set.of();
        }

        GeometryGroup oneBigGeometry = GeometryGroup.newBuilder()
                                                    .addAllGeometryTuples( singletons )
                                                    .setRegionName( SUMMARY_STATISTICS_ACROSS_FEATURES )
                                                    .build();

        return EvaluationUtilities.getOneBigFeatureGroupForSummaryStatistics( separateMetricsForBaseline,
                                                                              geometryGroups,
                                                                              oneBigGeometry );
    }

    /**
     * Generates a filter for {@link Statistics} that ignores all (multi-features) geometry groups in the supplied
     * declaration, retaining all singleton features.
     * @param separateMetricsForBaseline whether to generate a separate filter for baseline data
     * @param geometryGroups the geometry groups
     * @param oneBigGeometry the all-feature geometry
     * @return the filters
     */

    private static List<FeatureGroupFilterAdapter> getOneBigFeatureGroupForSummaryStatistics( boolean separateMetricsForBaseline,
                                                                                              Set<GeometryGroup> geometryGroups,
                                                                                              GeometryGroup oneBigGeometry )
    {
        List<FeatureGroupFilterAdapter> filters = new ArrayList<>();

        BinaryOperator<Statistics> adapter = EvaluationUtilities.getMetadataAdapterForFeatureGroup( oneBigGeometry );

        Predicate<Statistics> geometryFilterMain =
                statistics -> statistics.hasPool()
                              && !geometryGroups.contains( statistics.getPool()
                                                                     .getGeometryGroup() );
        FeatureGroupFilterAdapter filter = new FeatureGroupFilterAdapter( oneBigGeometry,
                                                                          geometryFilterMain,
                                                                          adapter,
                                                                          SummaryStatistic.StatisticDimension.FEATURES,
                                                                          1 );
        filters.add( filter );

        // Separate metrics for baseline?
        if ( separateMetricsForBaseline )
        {
            Predicate<Statistics> geometryFilterBase =
                    statistics -> !statistics.hasPool()
                                  && statistics.hasBaselinePool()
                                  && !geometryGroups.contains( statistics.getBaselinePool()
                                                                         .getGeometryGroup() );
            FeatureGroupFilterAdapter baselineFilter = new FeatureGroupFilterAdapter( oneBigGeometry,
                                                                                      geometryFilterBase,
                                                                                      adapter,
                                                                                      SummaryStatistic.StatisticDimension.FEATURES,
                                                                                      1 );
            filters.add( baselineFilter );
        }
        return filters;
    }

    /**
     * Generates a filter for {@link Statistics} from each supplied {@link GeometryGroup}.
     * @param declaration the declaration
     * @param separateMetricsForBaseline whether to generate a separate filter for baseline data
     * @return the filters
     */
    private static List<FeatureGroupFilterAdapter> getFeatureGroupForSummaryStatistics( EvaluationDeclaration declaration,
                                                                                        boolean separateMetricsForBaseline )
    {
        List<FeatureGroupFilterAdapter> filters = new ArrayList<>();

        if ( Objects.nonNull( declaration.featureGroups() ) )
        {
            int groupCount = 1;
            for ( GeometryGroup group : declaration.featureGroups()
                                                   .geometryGroups() )
            {
                FeatureGroupFilterAdapter filter =
                        EvaluationUtilities.getMainFeatureGroupForSummaryStatistics( group, groupCount );
                filters.add( filter );
                groupCount++;

                // Separate metrics for baseline?
                if ( separateMetricsForBaseline )
                {
                    FeatureGroupFilterAdapter
                            baselineFilter =
                            EvaluationUtilities.getBaselineFeatureGroupForSummaryStatistics( group, groupCount );
                    filters.add( baselineFilter );
                    groupCount++;
                }
            }
        }

        return Collections.unmodifiableList( filters );
    }

    /**
     * Generates a feature group filter for the baseline pool from the input.
     * @param group the feature group
     * @param groupNumber the group number
     * @return the filter
     */

    private static FeatureGroupFilterAdapter getMainFeatureGroupForSummaryStatistics( GeometryGroup group,
                                                                                      int groupNumber )
    {
        // Check whether the incoming statistic is a singleton member of the supplied group
        Predicate<Statistics> geometryFilterMain =
                statistics -> statistics.hasPool()
                              && statistics.getPool()
                                           .hasGeometryGroup()
                              && statistics.getPool()
                                           .getGeometryGroup()
                                           .getGeometryTuplesCount() == 1
                              && group.getGeometryTuplesList()
                                      .contains( statistics.getPool()
                                                           .getGeometryGroup()
                                                           .getGeometryTuplesList()
                                                           .get( 0 ) );

        BinaryOperator<Statistics> adapter = EvaluationUtilities.getMetadataAdapterForFeatureGroup( group );

        return new FeatureGroupFilterAdapter( group,
                                              geometryFilterMain,
                                              adapter,
                                              SummaryStatistic.StatisticDimension.FEATURE_GROUP,
                                              groupNumber );
    }

    /**
     * Generates a feature group filter for the baseline pool from the input.
     * @param group the feature group
     * @param groupNumber the group number
     * @return the filter
     */

    private static FeatureGroupFilterAdapter getBaselineFeatureGroupForSummaryStatistics( GeometryGroup group,
                                                                                          int groupNumber )
    {
        // Check whether the incoming statistic is a singleton member of the supplied group
        Predicate<Statistics> geometryFilterBase =
                statistics -> statistics.hasBaselinePool()
                              && statistics.getBaselinePool()
                                           .hasGeometryGroup()
                              && statistics.getBaselinePool()
                                           .getGeometryGroup()
                                           .getGeometryTuplesCount() == 1
                              && group.getGeometryTuplesList()
                                      .contains( statistics.getBaselinePool()
                                                           .getGeometryGroup()
                                                           .getGeometryTuplesList()
                                                           .get( 0 ) );

        BinaryOperator<Statistics> adapter = EvaluationUtilities.getMetadataAdapterForFeatureGroup( group );

        return new FeatureGroupFilterAdapter( group,
                                              geometryFilterBase,
                                              adapter,
                                              SummaryStatistic.StatisticDimension.FEATURE_GROUP,
                                              groupNumber );
    }

    /**
     * Generates summary statistic filters for time windows and thresholds.
     * @param timeWindows the time windows
     * @param timeWindowEquality the test for equality of time windows
     * @param thresholds the thresholds
     * @param separateMetricsForBaseline whether separate metrics are required for a baseline dataset
     * @param clearThresholdValues is true to clear event threshold values, false otherwise
     * @return the filters
     */
    private static List<TimeWindowAndThresholdFilterAdapter> getTimeWindowAndThresholdFilters( Set<TimeWindowOuter> timeWindows,
                                                                                               BiPredicate<TimeWindow, TimeWindow> timeWindowEquality,
                                                                                               Set<wres.config.yaml.components.Threshold> thresholds,
                                                                                               boolean separateMetricsForBaseline,
                                                                                               boolean clearThresholdValues )
    {
        // Get the time window filters
        List<Predicate<Statistics>> timeWindowFilters =
                EvaluationUtilities.getTimeWindowFilters( timeWindows,
                                                          timeWindowEquality,
                                                          separateMetricsForBaseline );

        UnaryOperator<Statistics> adapter =
                EvaluationUtilities.getMetadataAdapterForThresholds( clearThresholdValues );

        LOGGER.debug( "Discovered {} time windows, which produced {} filters.",
                      timeWindows.size(),
                      timeWindowFilters.size() );

        // Get the threshold filters
        List<Predicate<Statistics>> thresholdFilters =
                EvaluationUtilities.getThresholdFilters( thresholds, separateMetricsForBaseline );

        LOGGER.debug( "Discovered {} thresholds, which produced {} filters.",
                      thresholds.size(),
                      thresholdFilters.size() );

        int timeWindowCount = 1;
        List<TimeWindowAndThresholdFilterAdapter> filters = new ArrayList<>();
        for ( Predicate<Statistics> nextFilter : timeWindowFilters )
        {
            List<Predicate<Statistics>> wrappedFilter = List.of( nextFilter );
            List<Predicate<Statistics>> joined = EvaluationUtilities.join( wrappedFilter, thresholdFilters );
            int nextCount = timeWindowCount;
            List<TimeWindowAndThresholdFilterAdapter> nextFilters
                    = joined.stream()
                            .map( n -> new TimeWindowAndThresholdFilterAdapter( n,
                                                                                adapter,
                                                                                nextCount ) )
                            .toList();
            filters.addAll( nextFilters );
            timeWindowCount++;
        }

        LOGGER.debug( "After joining the time windows and thresholds, produced {} filters.",
                      filters.size() );

        return Collections.unmodifiableList( filters );
    }

    /**
     * Log some information about the pools
     * @param evaluationDetails the evaluation details
     * @param poolRequests the pool requests
     */

    private static void logPoolDetails( EvaluationDetails evaluationDetails,
                                        List<PoolRequest> poolRequests )
    {
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

            int timeWindowCount = timeWindows.size();
            int featureCount = features.size();

            // Avoid massive log statements: #129995
            String extraTime = "";
            String extraSpace = "";
            if ( timeWindowCount > MAXIMUM_TIME_WINDOWS_TO_LOG )
            {
                extraTime = "first " + MAXIMUM_TIME_WINDOWS_TO_LOG + " ";
                timeWindows = timeWindows.stream()
                                         .limit( MAXIMUM_TIME_WINDOWS_TO_LOG )
                                         .collect( Collectors.toCollection( TreeSet::new ) );
            }
            if ( featureCount > MAXIMUM_FEATURES_TO_LOG )
            {
                extraSpace = "first " + MAXIMUM_FEATURES_TO_LOG + " ";
                features = features.stream()
                                   .limit( MAXIMUM_FEATURES_TO_LOG )
                                   .collect( Collectors.toCollection( TreeSet::new ) );
            }

            if ( Objects.nonNull( evaluationDetails.declaration()
                                                   .eventDetection() )
                 && featureCount > 1 )
            {
                LOGGER.info( "Created {} pool requests, which include {} feature groups and {} time windows. "
                             + "The {}feature groups are: {}. The time windows are based on event detection and "
                             + "vary by feature group.",
                             poolRequests.size(),
                             featureCount,
                             timeWindowCount,
                             extraSpace,
                             PoolReporter.getPoolItemDescription( features, FeatureGroup::getName ) );
            }
            else
            {
                LOGGER.info( "Created {} pool requests, which include {} feature groups and {} time windows. "
                             + "The {}feature groups are: {}. The {}time windows are: {}.",
                             poolRequests.size(),
                             featureCount,
                             timeWindowCount,
                             extraSpace,
                             PoolReporter.getPoolItemDescription( features, FeatureGroup::getName ),
                             extraTime,
                             PoolReporter.getPoolItemDescription( timeWindows, TimeWindowOuter::toString ) );
            }
        }

        // Log extra details if needed
        EvaluationUtilities.logPoolDetailsExtra( poolRequests );
    }

    /**
     * Log some detailed information about the pools
     * @param poolRequests the pool requests
     */

    private static void logPoolDetailsExtra( List<PoolRequest> poolRequests )
    {
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
    }

    /**
     * A collection of filters/adapters to support summary statistics calculation for a geographic feature group..
     * @param geometryGroup the geometry group
     * @param filter the filter
     * @param adapter the metadata adapter
     * @param dimension the dimension associated with the summary statistic
     * @param groupNumber the feature group number
     */
    private record FeatureGroupFilterAdapter( GeometryGroup geometryGroup,
                                              Predicate<Statistics> filter,
                                              BinaryOperator<Statistics> adapter,
                                              SummaryStatistic.StatisticDimension dimension,
                                              long groupNumber )
    {
    }


    /**
     * A collection of filters/adapters to support summary statistics calculation for time windows and thresholds.
     * @param filter the filter
     * @param adapter the metadata adapter
     * @param timeWindowNumber the time window number
     */
    private record TimeWindowAndThresholdFilterAdapter( Predicate<Statistics> filter,
                                                        UnaryOperator<Statistics> adapter,
                                                        long timeWindowNumber )
    {
    }

    /**
     * Do not construct.
     */

    private EvaluationUtilities()
    {
    }

}