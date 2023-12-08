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
import wres.config.yaml.components.ThresholdType;
import wres.datamodel.Ensemble;
import wres.datamodel.bootstrap.BlockSizeEstimator;
import wres.datamodel.bootstrap.InsufficientDataForResamplingException;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolRequest;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.thresholds.MetricsAndThresholds;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindowOuter;
import wres.events.EvaluationMessager;
import wres.io.reading.netcdf.grid.GriddedFeatures;
import wres.io.retrieving.database.EnsembleSingleValuedRetrieverFactory;
import wres.io.retrieving.memory.EnsembleSingleValuedRetrieverFactoryInMemory;
import wres.io.writing.csv.pairs.EnsemblePairsWriter;
import wres.metrics.DiagramStatisticFunction;
import wres.metrics.FunctionFactory;
import wres.metrics.SummaryStatisticFunction;
import wres.metrics.SummaryStatisticsCalculator;
import wres.pipeline.pooling.PoolFactory;
import wres.pipeline.pooling.PoolParameters;
import wres.io.project.Project;
import wres.io.retrieving.RetrieverFactory;
import wres.io.retrieving.database.EnsembleRetrieverFactory;
import wres.io.retrieving.database.SingleValuedRetrieverFactory;
import wres.io.retrieving.memory.EnsembleRetrieverFactoryInMemory;
import wres.io.retrieving.memory.SingleValuedRetrieverFactoryInMemory;
import wres.io.writing.SharedSampleDataWriters;
import wres.io.writing.csv.pairs.PairsWriter;
import wres.io.writing.netcdf.NetcdfOutputWriter;
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

    /** Re-used string. */
    private static final String PERFORMING_RETRIEVAL_WITH_AN_IN_MEMORY_RETRIEVER_FACTORY =
            "Performing retrieval with an in-memory retriever factory.";

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
     * @param summaryStatistics the summary statistics calculators, possibly empty
     * @param sharedWriters the shared writers
     * @param executors the executor services
     * @throws NullPointerException if any input is null
     */

    static void createAndPublishStatistics( EvaluationDetails evaluationDetails,
                                            PoolDetails poolDetails,
                                            List<SummaryStatisticsCalculator> summaryStatistics,
                                            SharedWriters sharedWriters,
                                            EvaluationExecutors executors )
    {
        Objects.requireNonNull( evaluationDetails );
        Objects.requireNonNull( poolDetails );
        Objects.requireNonNull( summaryStatistics );
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
                                                                                    summaryStatistics,
                                                                                    sharedWriters,
                                                                                    executors );

        // Wait for the pool chain to complete
        poolTaskChain.join();
    }

    /**
     * Creates and publishes the summary statistics.
     * @param summaryStatistics the summary statistics
     * @param messager the evaluation messager
     * @throws NullPointerException if any input is null
     */
    static void createAndPublishSummaryStatistics( List<SummaryStatisticsCalculator> summaryStatistics,
                                                   EvaluationMessager messager )
    {
        Objects.requireNonNull( summaryStatistics );
        Objects.requireNonNull( messager );

        LOGGER.debug( "Publishing summary statistics from {} summary statistics calculators.",
                      summaryStatistics.size() );

        for ( SummaryStatisticsCalculator calculator : summaryStatistics )
        {
            // Generate the summary statistics
            List<Statistics> nextStatistics = calculator.get();
            nextStatistics.forEach( messager::publish );
        }

        LOGGER.debug( "Finished publishing summary statistics." );
    }

    /**
     * Generates a collection of {@link SummaryStatisticsCalculator} from an {@link EvaluationDeclaration}. Currently,
     * supports only {@link wres.statistics.generated.SummaryStatistic.StatisticDimension#FEATURES}.
     * @param declaration the evaluation declaration
     * @return the summary statistics calculators
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the dimension is unsupported
     */

    static List<SummaryStatisticsCalculator> getSummaryStatisticsCalculators( EvaluationDeclaration declaration )
    {
        Objects.requireNonNull( declaration );

        // No summary statistics?
        if ( declaration.summaryStatistics()
                        .isEmpty() )
        {
            LOGGER.debug( "No summary statistics were declared." );

            return List.of();
        }

        // Unsupported dimensions?
        Set<SummaryStatistic.StatisticDimension> unsupported =
                declaration.summaryStatistics()
                           .stream()
                           .map( SummaryStatistic::getDimension )
                           .filter( d -> d != SummaryStatistic.StatisticDimension.FEATURES )
                           .collect( Collectors.toSet() );

        if ( !unsupported.isEmpty() )
        {
            throw new IllegalArgumentException( "Unsupported dimension(s) for calculating summary statistics: "
                                                + unsupported
                                                + "." );
        }

        boolean separateMetricsForBaseline = DeclarationUtilities.hasBaseline( declaration )
                                             && declaration.baseline()
                                                           .separateMetrics();

        // Get the time window filters
        Set<TimeWindow> timeWindows = DeclarationUtilities.getTimeWindows( declaration );
        List<Predicate<Statistics>> timeWindowFilters =
                EvaluationUtilities.getTimeWindowFilters( timeWindows,
                                                          separateMetricsForBaseline );

        LOGGER.debug( "Discovered {} time windows, which produced {} filters.",
                      timeWindows.size(),
                      timeWindowFilters.size() );

        // Get the threshold filters
        Set<wres.config.yaml.components.Threshold> thresholds = DeclarationUtilities.getThresholds( declaration );
        List<Predicate<Statistics>> thresholdFilters =
                EvaluationUtilities.getThresholdFilters( thresholds, separateMetricsForBaseline );

        LOGGER.debug( "Discovered {} thresholds, which produced {} filters.",
                      thresholds.size(),
                      thresholds.size() );

        List<Predicate<Statistics>> joined = EvaluationUtilities.join( timeWindowFilters, thresholdFilters );

        LOGGER.debug( "After joining the time windows and thresholds, produced {} filters.",
                      joined.size() );

        // Create the calculators
        Set<SummaryStatistic> summaryStatistics = declaration.summaryStatistics();

        LOGGER.debug( "Discovered {} summary statistics to generate.",
                      summaryStatistics.size() );

        Set<SummaryStatisticFunction> scalar = new HashSet<>();
        Set<DiagramStatisticFunction> diagrams = new HashSet<>();

        for ( SummaryStatistic nextStatistic : summaryStatistics )
        {
            SummaryStatistic.StatisticName name = nextStatistic.getStatistic();
            MetricConstants behavioralName = MetricConstants.valueOf( name.name() );

            // Diagram?
            if ( behavioralName.isInGroup( MetricConstants.StatisticType.DIAGRAM ) )
            {
                DiagramStatisticFunction nextDiagram = FunctionFactory.ofDiagramSummaryStatistic( nextStatistic );
                diagrams.add( nextDiagram );
                LOGGER.debug( "Discovered a summary statistics diagram: {}.", name );
            }
            else
            {
                SummaryStatisticFunction nextScalar = FunctionFactory.ofSummaryStatistic( nextStatistic );
                scalar.add( nextScalar );
                LOGGER.debug( "Discovered a scalar summary statistic: {}.", name );
            }
        }

        ChronoUnit timeUnits = declaration.durationFormat();

        LOGGER.debug( "Discovered the following time units for summary statistic generation (where relevant): {}.",
                      timeUnits );

        // Create a metadata adapter for features, adding new features to an overall group
        BinaryOperator<Statistics> aggregator = ( existing, latest ) ->
        {
            GeometryGroup.Builder adjustedGeo = existing.getPool()
                                                        .getGeometryGroup()
                                                        .toBuilder();
            adjustedGeo.setRegionName( "ALL FEATURES" );
            List<GeometryTuple> newTuples = latest.getPool().getGeometryGroup()
                                                  .getGeometryTuplesList();
            adjustedGeo.addAllGeometryTuples( newTuples );
            Statistics.Builder adjusted = existing.toBuilder();
            adjusted.getPoolBuilder()
                    .setGeometryGroup( adjustedGeo );

            return adjusted.build();
        };

        // Return one calculator for each filter
        return joined.stream()
                     .map( n -> SummaryStatisticsCalculator.of( scalar, diagrams, n, aggregator, timeUnits ) )
                     .toList();
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

            // Validate the declaration in relation to the interpolated data types only
            DeclarationValidator.validateTypes( declaration );
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
     * Creates one pool task for each pool request and then chains them together, such that all of the pools complete 
     * nominally or one completes exceptionally.
     *
     * @param evaluationDetails the evaluation details
     * @param poolDetails the pool details
     * @param summaryStatistics the summary statistics calculators, possibly empty
     * @param sharedWriters the shared writers
     * @param executors the executor services
     * @return the pool task chain
     */

    private static CompletableFuture<Object> getPoolTasks( EvaluationDetails evaluationDetails,
                                                           PoolDetails poolDetails,
                                                           List<SummaryStatisticsCalculator> summaryStatistics,
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
                                                                   summaryStatistics,
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
                                                                       summaryStatistics,
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
     * @param summaryStatistics the summary statistics calculators, possibly empty
     * @param sharedWriters the shared writers
     * @param executors the executors
     * @return the single-valued processors
     */

    private static List<PoolProcessor<Double, Double>> getSingleValuedPoolProcessors( EvaluationDetails evaluationDetails,
                                                                                      PoolDetails poolDetails,
                                                                                      List<SummaryStatisticsCalculator> summaryStatistics,
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
                            .setPoolGroupTracker( poolDetails.poolGroupTracker() )
                            .setSummaryStatisticsCalculators( summaryStatistics )
                            .build();

            poolProcessors.add( poolProcessor );
        }

        return Collections.unmodifiableList( poolProcessors );
    }

    /**
     * Returns a list of processors for processing ensemble pools, one for each pool request.
     * @param evaluationDetails the evaluation details
     * @param poolDetails the pool details
     * @param summaryStatistics the summary statistics calculators, possibly empty
     * @param sharedWriters the shared writers
     * @param executors the executors
     * @return the ensemble processors
     */

    private static List<PoolProcessor<Double, Ensemble>> getEnsemblePoolProcessors( EvaluationDetails evaluationDetails,
                                                                                    PoolDetails poolDetails,
                                                                                    List<SummaryStatisticsCalculator> summaryStatistics,
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
                            .setPoolGroupTracker( poolDetails.poolGroupTracker() )
                            .setSummaryStatisticsCalculators( summaryStatistics )
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
        List<Predicate<Statistics>> filters = new ArrayList<>();
        if ( !thresholds.isEmpty() )
        {
            for ( Threshold next : thresholds )
            {
                Predicate<Statistics> thresholdFilterMain =
                        statistics -> statistics.hasPool()
                                      // Thresholds with equal names or equal thresholds
                                      && ( ( !next.getName()
                                                  .isBlank()
                                             && next.getName()
                                                    .equals( thresholdGetter.apply( statistics.getPool() )
                                                                            .getName() ) )
                                           || next.equals( thresholdGetter.apply( statistics.getPool() ) ) );
                filters.add( thresholdFilterMain );

                // Separate metrics for baseline?
                if ( separateMetricsForBaseline )
                {
                    Predicate<Statistics> thresholdFilterBase =
                            statistics -> !statistics.hasPool()
                                          && statistics.hasBaselinePool()
                                          // Thresholds with equal names or equal thresholds
                                          && ( ( !next.getName()
                                                      .isBlank()
                                                 && next.getName()
                                                        .equals( thresholdGetter.apply( statistics.getBaselinePool() )
                                                                                .getName() ) )
                                               || next.equals( thresholdGetter.apply( statistics.getBaselinePool() ) ) );
                    filters.add( thresholdFilterBase );
                }
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
     * Do not construct.
     */

    private EvaluationUtilities()
    {
    }

}