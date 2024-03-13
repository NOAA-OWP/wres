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
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.protobuf.DoubleValue;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.MetricConstants;
import wres.config.yaml.DeclarationException;
import wres.config.yaml.DeclarationInterpolator;
import wres.config.yaml.DeclarationUtilities;
import wres.config.yaml.components.DataType;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.GeneratedBaselines;
import wres.config.yaml.components.ThresholdType;
import wres.datamodel.types.Ensemble;
import wres.datamodel.MissingValues;
import wres.datamodel.bootstrap.BlockSizeEstimator;
import wres.datamodel.bootstrap.InsufficientDataForResamplingException;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolRequest;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.thresholds.MetricsAndThresholds;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdSlicer;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindowOuter;
import wres.events.EvaluationEventUtilities;
import wres.events.EvaluationMessager;
import wres.reading.netcdf.grid.GriddedFeatures;
import wres.io.retrieving.database.EnsembleSingleValuedRetrieverFactory;
import wres.io.retrieving.memory.EnsembleSingleValuedRetrieverFactoryInMemory;
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
import wres.statistics.generated.Evaluation;
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
            SINGLE_VALUED_BLOCK_SIZE_ESTIMATOR = EvaluationUtilities::getOptimalBlockSizeForStationaryBootstrap;

    /** A block size estimator for the stationary bootstrap as applied to ensemble pools.*/
    private static final Function<Pool<TimeSeries<Pair<Double, Ensemble>>>, Pair<Long, Duration>>
            ENSEMBLE_BLOCK_SIZE_ESTIMATOR = EvaluationUtilities::getOptimalBlockSizeForStationaryBootstrap;

    /** A default region name for summary statistics aggregated across geographic features. */
    private static final String SUMMARY_STATISTICS_ACROSS_FEATURES = "ALL FEATURES";

    /** Re-used string. */
    private static final String PERFORMING_RETRIEVAL_WITH_AN_IN_MEMORY_RETRIEVER_FACTORY =
            "Performing retrieval with an in-memory retriever factory.";

    /** Metadata adapter for thresholds. */
    private static final BinaryOperator<Statistics> METADATA_ADAPTER_FOR_THRESHOLDS =
            EvaluationUtilities.getMetadataAdapterForThresholds();

    /** Re-used string. */
    private static final String PERFORMING_RETRIEVAL_WITH_A_RETRIEVER_FACTORY_BACKED_BY_A_PERSISTENT_STORE =
            "Performing retrieval with a retriever factory backed by a persistent store.";

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
     * Creates and publishes the summary statistics.
     * @param summaryStatistics the summary statistics calculators, mapped by message group identifier
     * @param messager the evaluation messager
     * @throws NullPointerException if any input is null
     */
    static void createAndPublishSummaryStatistics( Map<String, List<SummaryStatisticsCalculator>> summaryStatistics,
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
     * Generates a collection of {@link SummaryStatisticsCalculator} from an {@link EvaluationDeclaration}. Currently,
     * supports only {@link wres.statistics.generated.SummaryStatistic.StatisticDimension#FEATURES}.
     * @param declaration the evaluation declaration
     * @param poolCount the number of pools for which raw (non-summary) statistics are required
     * @return the summary statistics calculators
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the dimension is unsupported
     */

    static Map<String, List<SummaryStatisticsCalculator>> getSummaryStatisticsCalculators( EvaluationDeclaration declaration,
                                                                                           long poolCount )
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
        Set<SummaryStatistic.StatisticDimension> dimensions =
                declaration.summaryStatistics()
                           .stream()
                           .map( SummaryStatistic::getDimension )
                           .filter( d -> d == SummaryStatistic.StatisticDimension.FEATURE_GROUP
                                         || d == SummaryStatistic.StatisticDimension.FEATURES )
                           .collect( Collectors.toSet() );

        return EvaluationUtilities.getSummaryStatisticsForFeatures( declaration, dimensions, poolCount );
    }

    /**
     * Interpolates any missing data types and validates the interpolated declaration for internal consistency.
     * @param declaration the declaration with missing data types
     * @param dataTypes the data types detected through ingest
     * @return the interpolated declaration
     * @throws DeclarationException if the declaration is inconsistent with the inferred types
     */

    static EvaluationDeclaration interpolateMissingDataTypes( EvaluationDeclaration declaration,
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
        }

        return declaration;
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
        if ( Objects.nonNull( externalNumerics ) && "true".equalsIgnoreCase( externalNumerics ) )
        {
            formats.add( Format.PROTOBUF );
            formats.add( Format.CSV2 );
        }

        return Collections.unmodifiableSet( formats );
    }

    /**
     * @param project the project
     * @param featuresWithExplicitThresholds features with explicit thresholds (not the implicit "all data" threshold)
     * @return the feature groups
     */

    static Set<FeatureGroup> getFeatureGroups( Project project,
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
                if ( nextGroup.getFeatures()
                              .size() > 1
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

    static Evaluation setAnalyzedUnitsAndVariableNames( Evaluation evaluation,
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

    static List<PoolRequest> getPoolRequests( PoolFactory poolFactory,
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

            LOGGER.info( "Created {} pool requests, which include {} feature groups and {} time windows. "
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
     * @param evaluation the evaluation messager
     * @param error the error
     * @param evaluationId the evaluation identifier
     */
    static void forceStop( EvaluationMessager evaluation, Exception error, String evaluationId )
    {
        if ( Objects.nonNull( evaluation ) )
        {
            // Stop forcibly
            LOGGER.debug( FORCIBLY_STOPPING_EVALUATION_UPON_ENCOUNTERING_AN_INTERNAL_ERROR, evaluationId );

            evaluation.stop( error );
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
                                         .getDeclaredDataset( DatasetOrientation.RIGHT )
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
                            .setEvaluation( evaluationDetails.evaluation() )
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
            poolSuppliers = poolFactory.getEnsemblePoolsWithGeneratedBaseline( poolDetails.poolRequests(),
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
                            .setEvaluation( evaluationDetails.evaluation() )
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
            // Estimate the number of traces using the largest of the first ensemble events in each time-series
            List<TimeSeries<Pair<Double, Ensemble>>> series = pool.get();
            int traceCount = series.stream()
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
     * Estimates the optimal block size for each left-ish time-series in each mini-pool and returns the average of the
     * optimal block sizes across all time-series.
     * @param <R> the type of right-ish time-series data
     * @param pool the pool
     * @return the optimal block size for the stationary bootstrap
     */
    private static <R> Pair<Long, Duration> getOptimalBlockSizeForStationaryBootstrap( Pool<TimeSeries<Pair<Double, R>>> pool )
    {
        List<Pool<TimeSeries<Pair<Double, R>>>> miniPools = pool.getMiniPools();

        List<Pair<Long, Duration>> blockSizes = new ArrayList<>();
        for ( Pool<TimeSeries<Pair<Double, R>>> next : miniPools )
        {
            // Main data with sufficient samples
            if ( !next.get()
                      .isEmpty() )
            {
                Pair<Long, Duration> nextMain =
                        EvaluationUtilities.getOptimalBlockSizesForStationaryBootstrap( next.get() );
                blockSizes.add( nextMain );
            }

            // Baseline data with sufficient samples
            if ( next.hasBaseline()
                 && !next.getBaselineData()
                         .get()
                         .isEmpty() )
            {
                List<TimeSeries<Pair<Double, R>>> baseline = next.getBaselineData()
                                                                 .get();
                Pair<Long, Duration> nextBaseline =
                        EvaluationUtilities.getOptimalBlockSizesForStationaryBootstrap( baseline );
                blockSizes.add( nextBaseline );
            }
        }

        double total = 0;
        Duration totalDuration = Duration.ZERO;
        for ( Pair<Long, Duration> next : blockSizes )
        {
            total += next.getLeft();
            totalDuration = totalDuration.plus( next.getValue() );
        }

        long optimalBlockSize = ( long ) Math.ceil( total / blockSizes.size() );
        Duration averageDuration = totalDuration.dividedBy( blockSizes.size() );

        LOGGER.debug( "Determined an optimal block size of {} timesteps of {} for applying the stationary bootstrap to "
                      + "the pool with metadata: {}. This is an average of the optimal block sizes across all observed "
                      + "time-series within the pool, which included the following block sizes: {}.",
                      optimalBlockSize,
                      averageDuration,
                      pool.getMetadata(),
                      blockSizes );

        return Pair.of( optimalBlockSize, averageDuration );
    }

    /**
     * Estimates the optimal block size for the consolidated left-ish time-series in the input.
     * @param <T> the type of time-series data
     * @param pool the pool
     * @return the optimal block size for the stationary bootstrap
     */
    private static <T> Pair<Long, Duration> getOptimalBlockSizesForStationaryBootstrap( List<TimeSeries<Pair<Double, T>>> pool )
    {
        SortedMap<Instant, Double> consolidated = pool.stream()
                                                      .flatMap( n -> n.getEvents()
                                                                      .stream() )
                                                      .collect( Collectors.toMap( Event::getTime, e -> e.getValue()
                                                                                                        .getLeft(),
                                                                                  ( a, b ) -> a, TreeMap::new ) );

        double[] data = new double[consolidated.size()];
        List<Duration> durations = new ArrayList<>();
        Instant last = null;
        int index = 0;
        for ( Map.Entry<Instant, Double> next : consolidated.entrySet() )
        {
            data[index] = next.getValue();
            Instant time = next.getKey();

            if ( index > 0 )
            {
                Duration between = Duration.between( last, time );
                durations.add( between );
            }

            last = time;
            index++;
        }

        long optimalBlockSize = BlockSizeEstimator.getOptimalBlockSize( data );

        // Find the corresponding timestep, which is the modal timestep
        Duration modalTimestep =
                durations.stream()
                         .collect( Collectors.groupingBy( Function.identity(),
                                                          Collectors.counting() ) )
                         .entrySet()
                         .stream()
                         .max( Map.Entry.comparingByValue() )
                         .map( Map.Entry::getKey )
                         .orElseThrow( () -> new InsufficientDataForResamplingException( "Insufficient data to "
                                                                                         + "calculate the optimal "
                                                                                         + "block size for the "
                                                                                         + "stationary bootstrap." ) );

        return Pair.of( optimalBlockSize, modalTimestep );
    }

    /**
     * Generates a collection of filters for {@link Statistics} based on their {@link TimeWindow}, one for each supplied
     * {@link TimeWindow}.
     *
     * @param timeWindows the time windows
     * @param separateMetricsForBaseline whether to generate a filter for baseline pools with separate metrics
     * @return the filters
     */
    private static List<Predicate<Statistics>> getTimeWindowFilters( Set<TimeWindow> timeWindows,
                                                                     boolean separateMetricsForBaseline )
    {
        List<Predicate<Statistics>> filters = new ArrayList<>();

        for ( TimeWindow timeWindow : timeWindows )
        {
            // Filter for main pools
            Predicate<Statistics> nextMainFilter = statistics -> statistics.hasPool()
                                                                 && statistics.getPool()
                                                                              .getTimeWindow()
                                                                              .equals( timeWindow );
            filters.add( nextMainFilter );

            // Separate metrics for baseline?
            if ( separateMetricsForBaseline )
            {
                Predicate<Statistics> nextBaseFilter = statistics -> !statistics.hasPool()
                                                                     && statistics.hasBaselinePool()
                                                                     && statistics.getBaselinePool()
                                                                                  .getTimeWindow()
                                                                                  .equals( timeWindow );
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
     * Generates a collection of {@link SummaryStatisticsCalculator} from an {@link EvaluationDeclaration} for all
     * geographic features in a single group.
     * @param declaration the evaluation declaration
     * @param dimensions the feature dimensions over which to perform aggregation
     * @return the summary statistics calculators
     * @param poolCount the number of pools for which raw (non-summary) statistics are required
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the dimension is unsupported
     */

    private static Map<String, List<SummaryStatisticsCalculator>> getSummaryStatisticsForFeatures( EvaluationDeclaration declaration,
                                                                                                   Set<SummaryStatistic.StatisticDimension> dimensions,
                                                                                                   long poolCount )
    {
        Objects.requireNonNull( declaration );

        boolean separateMetricsForBaseline = DeclarationUtilities.hasBaseline( declaration )
                                             && declaration.baseline()
                                                           .separateMetrics();

        // Get the time window and threshold filters
        Set<TimeWindow> timeWindows = DeclarationUtilities.getTimeWindows( declaration );
        Set<wres.config.yaml.components.Threshold> thresholds = DeclarationUtilities.getThresholds( declaration );
        List<TimeWindowAndThresholdFilterAdapter> timeWindowAndThresholdFilters =
                EvaluationUtilities.getTimeWindowAndThresholdFilters( timeWindows,
                                                                      thresholds,
                                                                      separateMetricsForBaseline );

        // Get the geographic feature filters and metadata adapters
        List<FeatureGroupFilterAdapter> featureFilters = new ArrayList<>();
        if ( dimensions.contains( SummaryStatistic.StatisticDimension.FEATURES ) )
        {
            List<FeatureGroupFilterAdapter> filters =
                    EvaluationUtilities.getOneBigFeatureGroupForSummaryStatistics( declaration,
                                                                                   separateMetricsForBaseline );
            featureFilters.addAll( filters );
            LOGGER.debug( "Created {} filters for all geographic features.", filters.size() );
        }

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
            LOGGER.debug( "Creating filters for the following geometry groups: {}.", featureGroups );
        }

        // Create the calculators
        Set<SummaryStatistic> summaryStatistics = declaration.summaryStatistics();

        LOGGER.debug( "Discovered {} summary statistics to generate.",
                      summaryStatistics.size() );

        Set<ScalarSummaryStatisticFunction> scalar = new HashSet<>();
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
                scalar.add( nextScalar );
                LOGGER.debug( "Discovered a scalar summary statistic: {}.", name );
            }
        }

        ChronoUnit timeUnits = declaration.durationFormat();

        LOGGER.debug( "Discovered the following time units for summary statistic generation (where relevant): {}.",
                      timeUnits );

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
            Set<ScalarSummaryStatisticFunction> nextScalar = scalar.stream()
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
                                  + ( ( nextOuterFilter.groupNumber() - 1L ) * timeWindows.size() )
                                  + nextInnerFilter.timeWindowNumber();
                BinaryOperator<Statistics> poolNumberAdapter =
                        EvaluationUtilities.getMetadataAdapterForPoolNumber( poolNumber );

                BinaryOperator<Statistics> thresholdAdapter = nextInnerFilter.adapter();
                BinaryOperator<Statistics> metadataAdapter = ( p, q ) ->
                        poolNumberAdapter.apply( featureAdapter.apply( thresholdAdapter.apply( p, q ), q ), q );

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
     * @return the metadata adapter
     */

    private static BinaryOperator<Statistics> getMetadataAdapterForThresholds()
    {
        return ( existing, latest ) ->
        {
            boolean isBaselinePool = !existing.hasPool()
                                     && existing.hasBaselinePool();

            Statistics.Builder adjusted = existing.toBuilder();

            wres.statistics.generated.Pool.Builder existingPool =
                    isBaselinePool ? adjusted.getBaselinePoolBuilder() : adjusted.getPoolBuilder();

            wres.statistics.generated.Pool latestPool =
                    isBaselinePool ? latest.getBaselinePool() : latest.getPool();

            // Clear the threshold values unless they are equal across statistics
            if ( existingPool.hasEventThreshold()
                 && !Objects.equals( existingPool.getEventThreshold()
                                                 .getLeftThresholdValue(),
                                     latestPool.getEventThreshold()
                                               .getLeftThresholdValue() ) )
            {
                // Set to missing rather than clearing: #126545
                Threshold.Builder builder = existingPool.getEventThresholdBuilder()
                                                        .setLeftThresholdValue( DoubleValue.of( MissingValues.DOUBLE ) );
                if ( existingPool.getEventThreshold()
                                 .getOperator() == Threshold.ThresholdOperator.BETWEEN )
                {
                    builder.setRightThresholdValue( DoubleValue.of( MissingValues.DOUBLE ) );
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
     * @param thresholds the thresholds
     * @param separateMetricsForBaseline whether separate metrics are required for a baseline dataset
     * @return the filters
     */
    private static List<TimeWindowAndThresholdFilterAdapter> getTimeWindowAndThresholdFilters( Set<TimeWindow> timeWindows,
                                                                                               Set<wres.config.yaml.components.Threshold> thresholds,
                                                                                               boolean separateMetricsForBaseline )
    {
        // Get the time window filters
        List<Predicate<Statistics>> timeWindowFilters =
                EvaluationUtilities.getTimeWindowFilters( timeWindows,
                                                          separateMetricsForBaseline );

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
                                                                                METADATA_ADAPTER_FOR_THRESHOLDS,
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
                                                        BinaryOperator<Statistics> adapter,
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