package wres.io.writing.netcdf;

import java.io.Closeable;
import java.io.IOException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jcip.annotations.GuardedBy;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.Index;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;
import ucar.nc2.Variable;
import ucar.nc2.dt.GridDatatype;
import ucar.nc2.dt.grid.GridDataset;
import ucar.nc2.write.NetcdfFormatWriter;

import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.EnsembleAverageType;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.NetcdfType;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ProjectConfig.Inputs;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.DataUtilities;
import wres.datamodel.MissingValues;
import wres.datamodel.OneOrTwoDoubles;
import wres.config.MetricConstants;
import wres.config.MetricConstants.MetricGroup;
import wres.config.MetricConstants.StatisticType;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.Feature;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter.DoubleScoreComponentOuter;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdSlicer;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.datamodel.thresholds.ThresholdsByMetricAndFeature;
import wres.datamodel.time.TimeWindowOuter;
import wres.datamodel.time.generators.TimeWindowGenerator;
import wres.io.NoDataException;
import wres.io.config.ConfigHelper;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.Pool;
import wres.system.SystemSettings;

/**
 * A writer is instantiated in two stages. First, the writer is built. Second, the blobs are initialized for writing. 
 * In between these two stages, the writer is in an exceptional state with respect to writing statistics. This 
 * requirement stems from the need to build a consumer at evaluation construction time. However, the netcdf writer
 * depends on the thresholds-by-feature, which is part of the internal state of an evaluation. This state is not
 * available at evaluation construction time and must be instantiated post-ingest. Construction of a blob on-the-fly is
 * also not possible as blobs cannot be augmented when using the Java UCAR netcdf library and the first blob of 
 * statistics received by the writer may contain only some of the thresholds-by-feature. If this writer is ever 
 * abstracted to a subscriber in a separate process, this problem will resurface because the internal state of an 
 * evaluation cannot be exposed to such a writer. See #80267-137.
 */

public class NetcdfOutputWriter implements NetcdfWriter<DoubleScoreStatisticOuter>, Closeable
{
    private static final Logger LOGGER = LoggerFactory.getLogger( NetcdfOutputWriter.class );

    private static final String DEFAULT_VECTOR_TEMPLATE = "vector_template.nc";
    private static final String DEFAULT_GRID_TEMPLATE = "lcc_grid_template.nc";
    private static final String FEATURE_TUPLE_VARIABLE_NAME = "lid";
    private static final String FEATURE_GROUP_VARIABLE_NAME = "feature_group_name";
    private static final int VALUE_SAVE_LIMIT = 500;

    // TODO: it is very unlikely that classloading datetime should be used here.
    private static final ZonedDateTime ANALYSIS_TIME = ZonedDateTime.now( ZoneId.of( "UTC" ) );

    private final Object windowLock = new Object();

    private final DestinationConfig destinationConfig;
    private final Path outputDirectory;
    private NetcdfType netcdfConfiguration;

    // TODO: remove when netcdf writing is one stage. Until then, we must return the paths to blobs created rather than
    // statistics written, i.e., more paths are created than necessary
    private Set<Path> pathToBlobs;

    @GuardedBy( "windowLock" )
    private final Map<TimeWindowOuter, TimeWindowWriter> writersMap = new HashMap<>();

    /**
     * Default resolution for writing duration outputs. To change the resolution, change this default.
     */

    private final ChronoUnit durationUnits;

    /**
     * Project declaration.
     */

    private final ProjectConfig projectConfig;

    /**
     * Records whether the writer is ready to write. It is ready when all blobs have been created.
     */

    private final AtomicBoolean isReadyToWrite;

    /**
     * Mapping between each threshold and a standard threshold name for each metric. This is used to help determine the 
     * threshold portion of a variable name to which a statistic corresponds, based on the standard name of a threshold 
     * chosen at blob creation time. There is a separate group for each metric.
     */

    private Map<String, Map<OneOrTwoThresholds, String>> standardThresholdNames = new HashMap<>();

    /**
     * True when using deprecated code, false otherwise. Remove when removing
     * the use of deprecated code.
     * @deprecated
     */
    @Deprecated( since = "5.1", forRemoval = true )
    private final boolean deprecatedVersion;

    /**
     * Returns an instance of the writer. 
     *
     * @param systemSettings The system settings to use.
     * @param projectConfig the project configuration
     * @param destinationDeclaration the destination declaration
     * @param durationUnits the time units for durations
     * @param outputDirectory the directory into which to write
     * @param deprecatedVersion True if using deprecated code, false otherwise
     * @return an instance of the writer
     * @throws NetcdfWriteException if the blobs could not be created for any reason
     */

    public static NetcdfOutputWriter of( SystemSettings systemSettings,
                                         ProjectConfig projectConfig,
                                         DestinationConfig destinationDeclaration,
                                         ChronoUnit durationUnits,
                                         Path outputDirectory,
                                         boolean deprecatedVersion )
    {
        return new NetcdfOutputWriter( systemSettings,
                                       projectConfig,
                                       destinationDeclaration,
                                       durationUnits,
                                       outputDirectory,
                                       deprecatedVersion );
    }

    /**
     * Returns the duration units for writing lead durations.
     *
     * @return the duration units
     */

    ChronoUnit getDurationUnits()
    {
        return this.durationUnits;
    }

    private NetcdfOutputWriter( SystemSettings systemSettings,
                                ProjectConfig projectConfig,
                                DestinationConfig destinationDeclaration,
                                ChronoUnit durationUnits,
                                Path outputDirectory,
                                boolean deprecatedVersion )
    {
        Objects.requireNonNull( systemSettings );
        Objects.requireNonNull( projectConfig, "Specify non-null project config." );
        Objects.requireNonNull( durationUnits, "Specify non-null duration units." );
        Objects.requireNonNull( outputDirectory, "Specify non-null output directory." );
        Objects.requireNonNull( destinationDeclaration );
        LOGGER.debug( "Created NetcdfOutputWriter {}", this );
        this.destinationConfig = destinationDeclaration;
        this.netcdfConfiguration = this.destinationConfig.getNetcdf();
        this.durationUnits = durationUnits;
        this.outputDirectory = outputDirectory;
        this.projectConfig = projectConfig;
        this.isReadyToWrite = new AtomicBoolean();
        this.deprecatedVersion = deprecatedVersion;

        if ( this.netcdfConfiguration == null )
        {
            this.netcdfConfiguration = new NetcdfType( null,
                                                       null,
                                                       null,
                                                       null,
                                                       null );
        }

        Objects.requireNonNull( this.destinationConfig, "The NetcdfOutputWriter wasn't properly initialized." );
    }

    /**
     * Creates the blobs into which outputs will be written.
     *
     * @param featureGroups The super-set of feature groups used in the evaluation.
     * @param thresholdsByMetricAndFeature Thresholds imposed upon input data
     * @throws NetcdfWriteException if the blobs have already been created
     * @throws IOException if the blobs could not be created for any reason
     */

    public void createBlobsForWriting( Set<FeatureGroup> featureGroups,
                                       List<ThresholdsByMetricAndFeature> thresholdsByMetricAndFeature )
            throws IOException
    {
        Objects.requireNonNull( thresholdsByMetricAndFeature );

        if ( this.getIsReadyToWrite().get() )
        {
            throw new NetcdfWriteException( "The netcdf blobs have already been created." );
        }

        // Time windows
        PairConfig pairConfig = this.getProjectConfig()
                                    .getPair();
        Set<TimeWindowOuter> timeWindows = TimeWindowGenerator.getTimeWindowsFromPairConfig( pairConfig );

        // Find the thresholds-by-metric for which blobs should be created

        // Create a map of these with one ThresholdsByMetric for each ensemble average type? Will that create only
        // the variables needed or more than needed? For example, what happens if the mean error is requested for the 
        // ensemble median and not for the ensemble mean - will that produce a variable for the ensemble median only?
        // Use the default averaging type if the evaluation does not contain ensemble forecasts
        boolean hasEnsembles = ConfigHelper.hasEnsembleForecasts( this.getProjectConfig() );
        Function<ThresholdsByMetricAndFeature, EnsembleAverageType> ensembleTypeCalculator = thresholds -> {
            if ( hasEnsembles )
            {
                return thresholds.getEnsembleAverageType();
            }

            return EnsembleAverageType.MEAN;
        };

        Map<EnsembleAverageType, List<ThresholdsByMetricAndFeature>> byType =
                thresholdsByMetricAndFeature.stream()
                                            .collect( Collectors.groupingBy( ensembleTypeCalculator ) );

        Map<EnsembleAverageType, ThresholdsByMetric> thresholds = byType.entrySet()
                                                                        .stream()
                                                                        .collect( Collectors.toUnmodifiableMap( Map.Entry::getKey,
                                                                                                                e -> this.getUniqueThresholdsForScoreMetrics(
                                                                                                                        e.getValue() ) ) );

        // Should be at least one metric with at least one threshold
        if ( thresholds.values()
                       .stream()
                       .allMatch( next -> next.getMetrics()
                                              .isEmpty() ) )
        {
            throw new IOException( "Could not identify any thresholds from which to create blobs." );
        }

        // Units, if declared
        String units = "UNKNOWN";
        if ( Objects.nonNull( pairConfig.getUnit() ) )
        {
            units = pairConfig.getUnit();
        }

        // Desired time scale, if declared
        TimeScaleOuter desiredTimeScale = null;
        if ( Objects.nonNull( pairConfig.getDesiredTimeScale() ) )
        {
            desiredTimeScale = TimeScaleOuter.of( pairConfig.getDesiredTimeScale() );
        }

        Set<Path> allPathsCreated = new HashSet<>();

        // Create blobs from components
        synchronized ( this.windowLock )
        {
            Set<Path> pathsCreated = this.createBlobsAndBlobWriters( this.getProjectConfig()
                                                                         .getInputs(),
                                                                     featureGroups,
                                                                     timeWindows,
                                                                     thresholds,
                                                                     units,
                                                                     desiredTimeScale,
                                                                     this.deprecatedVersion );

            allPathsCreated.addAll( pathsCreated );

            // Flag ready
            this.getIsReadyToWrite()
                .set( true );
        }

        // Expose the paths
        this.pathToBlobs = Collections.unmodifiableSet( allPathsCreated );

        LOGGER.debug( "Created the following netcdf paths for writing: {}.", allPathsCreated );
    }

    /**
     * @return whether the writer is ready to write.
     */

    private AtomicBoolean getIsReadyToWrite()
    {
        return this.isReadyToWrite;
    }

    /**
     * @return the project declaration.
     */

    private ProjectConfig getProjectConfig()
    {
        return this.projectConfig;
    }

    /**
     * @return the paths to blobs created
     */

    private Set<Path> getPathsToBlobsCreated()
    {
        return this.pathToBlobs;
    }

    /**
     * Creates the blobs into which outputs will be written.
     *
     * @param inputs the inputs declaration
     * @param featureGroups The super-set of feature groups used in this evaluation.
     * @param timeWindows the time windows
     * @param thresholds the thresholds
     * @param units the measurement units, if available
     * @param desiredTimeScale the desired time scale, if available
     * @param deprecatedVersion True when using deprecated code, false otherwise
     * @throws IOException if the blobs could not be created for any reason
     * @return the paths written
     */

    private Set<Path> createBlobsAndBlobWriters( Inputs inputs,
                                                 Set<FeatureGroup> featureGroups,
                                                 Set<TimeWindowOuter> timeWindows,
                                                 Map<EnsembleAverageType, ThresholdsByMetric> thresholds,
                                                 String units,
                                                 TimeScaleOuter desiredTimeScale,
                                                 boolean deprecatedVersion )
            throws IOException
    {
        Set<Path> returnMe = new TreeSet<>();

        // Create the standard threshold names
        this.standardThresholdNames =
                this.createStandardThresholdNames( thresholds, Objects.nonNull( inputs.getBaseline() ) );

        LOGGER.debug( "Created this map of standard threshold names: {}", this.standardThresholdNames );

        // One blob and blob writer per time window      
        for ( TimeWindowOuter nextWindow : timeWindows )
        {
            Collection<MetricVariable> variables = this.getMetricVariablesForOneTimeWindow( inputs,
                                                                                            nextWindow,
                                                                                            thresholds,
                                                                                            units,
                                                                                            desiredTimeScale );

            // Create the blob path
            Path targetPath = this.getOutputPathToWriteForOneTimeWindow( this.getOutputDirectory(),
                                                                         this.getProjectConfig().getPair(),
                                                                         this.getDestinationConfig(),
                                                                         this.getScenarioNameForBlobOrNull( inputs ),
                                                                         nextWindow,
                                                                         this.getDurationUnits() );

            String pathActuallyWritten;

            if ( !deprecatedVersion )
            {
                // Create the blob
                pathActuallyWritten =
                        NetcdfOutputFileCreator2.create( this.getProjectConfig(),
                                                         targetPath,
                                                         featureGroups,
                                                         nextWindow,
                                                         NetcdfOutputWriter.ANALYSIS_TIME,
                                                         variables );
            }
            else
            {
                // TODO remove this block, remove if/else
                pathActuallyWritten =
                        NetcdfOutputFileCreator.create( this.getTemplatePath(),
                                                        targetPath,
                                                        this.destinationConfig,
                                                        nextWindow,
                                                        NetcdfOutputWriter.ANALYSIS_TIME,
                                                        variables,
                                                        this.getDurationUnits() );
            }

            returnMe.add( targetPath );

            // Create the blob writer
            TimeWindowWriter writer = new TimeWindowWriter( this,
                                                            pathActuallyWritten,
                                                            nextWindow,
                                                            deprecatedVersion );

            // Add the blob writer to the writer cache
            this.writersMap.put( nextWindow, writer );
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Creates the list of standard threshold names to use across blobs.
     * @param thresholds the thresholds
     * @param hasBaseline whether there are separate metrics for a baseline
     */

    private Map<String, Map<OneOrTwoThresholds, String>>
    createStandardThresholdNames( Map<EnsembleAverageType, ThresholdsByMetric> thresholds, boolean hasBaseline )
    {
        // Create the standard threshold names, sequenced by natural order of the threshold
        SortedSet<OneOrTwoThresholds> union = thresholds.values()
                                                        .stream()
                                                        .map( ThresholdSlicer::getOneOrTwoThresholds )
                                                        .flatMap( next -> next.values().stream() )
                                                        .flatMap( SortedSet::stream )
                                                        .collect( Collectors.toCollection( TreeSet::new ) );

        Map<OneOrTwoThresholds, String> thresholdMap = new HashMap<>();

        int thresholdNumber = 1;
        for ( OneOrTwoThresholds next : union )
        {
            String name = "THRESHOLD_" + thresholdNumber;
            thresholdMap.put( next, name );
            thresholdNumber += 1;
        }

        // Map the thresholds to metric names
        Map<String, Map<OneOrTwoThresholds, String>> returnMe = new TreeMap<>();
        for ( ThresholdsByMetric nextThresholds : thresholds.values() )
        {
            Map<MetricConstants, SortedSet<OneOrTwoThresholds>> nextMetrics =
                    ThresholdSlicer.getOneOrTwoThresholds( nextThresholds );

            Map<String, SortedSet<OneOrTwoThresholds>> decomposed =
                    this.decomposeThresholdsByMetricForBlobCreation( nextMetrics, hasBaseline );

            for ( Map.Entry<String, SortedSet<OneOrTwoThresholds>> nextEntry : decomposed.entrySet() )
            {
                String nextMetric = nextEntry.getKey();
                SortedSet<OneOrTwoThresholds> nextThresholdsForMetric = nextEntry.getValue();
                Map<OneOrTwoThresholds, String> namedThresholds = new HashMap<>();
                nextThresholdsForMetric.forEach( next -> namedThresholds.put( next, thresholdMap.get( next ) ) );

                if ( returnMe.containsKey( nextMetric ) )
                {
                    returnMe.get( nextMetric ).putAll( namedThresholds );
                }
                else
                {
                    returnMe.put( nextMetric, namedThresholds );
                }
            }
        }

        // Render the map unmodifiable
        return Collections.unmodifiableMap( returnMe );
    }

    /**
     * @return the standard threshold names to use across blobs.
     */

    private Map<String, Map<OneOrTwoThresholds, String>> getStandardThresholdNames()
    {
        return this.standardThresholdNames;
    }

    /**
     * Returns a formatted file name for writing outputs to a specific time window using the destination 
     * information and other hints provided.
     *
     * @param outputDirectory the directory into which to write
     * @param pairConfig the pairs declaration
     * @param destinationConfig the destination information
     * @param scenarioName the optional scenario name
     * @param timeWindow the time window
     * @param leadUnits the time units to use for the lead durations
     * @return the file name
     * @throws NoDataException if the output is empty
     * @throws NullPointerException if any of the inputs is null
     * @throws IOException if the path cannot be produced
     */

    private Path getOutputPathToWriteForOneTimeWindow( Path outputDirectory,
                                                       PairConfig pairConfig,
                                                       DestinationConfig destinationConfig,
                                                       String scenarioName,
                                                       TimeWindowOuter timeWindow,
                                                       ChronoUnit leadUnits )
            throws IOException
    {
        Objects.requireNonNull( outputDirectory, "Enter non-null output directory to establish a path for writing." );
        Objects.requireNonNull( destinationConfig, "Enter non-null time window to establish a path for writing." );
        Objects.requireNonNull( timeWindow, "Enter a non-null time window  to establish a path for writing." );
        Objects.requireNonNull( leadUnits,
                                "Enter a non-null time unit for the lead durations to establish a path for writing." );
        Objects.requireNonNull( pairConfig, "Provide non-null pair configuration to establish a path for writing." );

        StringJoiner filename = new StringJoiner( "_" );

        // Add optional scenario identifier
        if ( Objects.nonNull( scenarioName ) )
        {
            filename.add( scenarioName );
        }

        // Add latest reference time identifier (good enough for an ordered sequence of pools, not for arbitrary pools)
        if ( !timeWindow.getLatestReferenceTime().equals( Instant.MAX ) )
        {
            String lastTime = timeWindow.getLatestReferenceTime().toString();
            lastTime = lastTime.replace( "-", "" )
                               .replace( ":", "" )
                               .replace( "Z$", "" );

            filename.add( lastTime );
        }

        // Add latest valid time identifier (good enough for an ordered sequence of pools, not for arbitrary pools)
        // For backwards compatibility of file names, only qualify when valid dates pooling windows are supplied
        if ( Objects.nonNull( pairConfig.getValidDatesPoolingWindow() )
             && !timeWindow.getLatestValidTime().equals( Instant.MAX ) )
        {
            String lastTime = timeWindow.getLatestValidTime().toString();
            lastTime = lastTime.replace( "-", "" )
                               .replace( ":", "" )
                               .replace( "Z$", "" );

            filename.add( lastTime );
        }

        // Format the duration with the default format
        Number numericDuration = DataUtilities.durationToNumericUnits( timeWindow.getLatestLeadDuration(),
                                                                       leadUnits );
        filename.add( numericDuration.toString() );
        filename.add( leadUnits.name().toUpperCase() );

        String extension = "";
        if ( destinationConfig.getType() == DestinationType.NETCDF
             || destinationConfig.getType() == DestinationType.NETCDF_2 )
        {
            extension = ".nc";
        }

        // Sanitize file name
        String safeName = URLEncoder.encode( filename.toString().replace( " ", "_" ) + extension, "UTF-8" );

        return Paths.get( outputDirectory.toString(), safeName );
    }

    /**
     * Returns a scenario name to be used in naming a blob or null.
     *
     * @param inputs the inputs declaration
     * @return an identifier or null
     */

    private String getScenarioNameForBlobOrNull( Inputs inputs )
    {
        String scenarioName = inputs.getRight().getLabel();
        if ( Objects.nonNull( inputs.getBaseline() ) && inputs.getBaseline().isSeparateMetrics() )
        {
            scenarioName = null;
        }

        return scenarioName;
    }

    private String getTemplatePath()
    {
        String templatePath;

        if ( this.getNetcdfConfiguration()
                 .getTemplatePath() == null )
        {
            String defaultTemplate;

            if ( this.isGridded() )
            {
                defaultTemplate = DEFAULT_GRID_TEMPLATE;
            }
            else
            {
                defaultTemplate = DEFAULT_VECTOR_TEMPLATE;
            }

            URL template = NetcdfOutputWriter.class.getClassLoader().getResource( defaultTemplate );
            Objects.requireNonNull( template,
                                    "A default template for netcdf output could not be "
                                    + "found on the class path." );
            templatePath = template.getPath();
        }
        else
        {
            templatePath = this.getDestinationConfig()
                               .getNetcdf()
                               .getTemplatePath();
        }

        return templatePath;
    }

    /**
     * Creates a collection of {@link MetricVariable} for one time window.
     *
     * @param inputs The input configurations
     * @param timeWindow the time windows
     * @param thresholds the thresholds
     * @param units the measurement units, if available
     * @param desiredTimeScale the desired time scale, if available
     * @return the metric variables
     */

    private Collection<MetricVariable> getMetricVariablesForOneTimeWindow( Inputs inputs,
                                                                           TimeWindowOuter timeWindow,
                                                                           Map<EnsembleAverageType, ThresholdsByMetric> thresholds,
                                                                           String units,
                                                                           TimeScaleOuter desiredTimeScale )
    {
        Collection<MetricVariable> merged = new ArrayList<>();

        LOGGER.debug( "Creating metric variables for for time window {} using these thresholds by metric: {}.",
                      timeWindow,
                      thresholds );

        // Iterate through the ensemble average types
        for ( Map.Entry<EnsembleAverageType, ThresholdsByMetric> next : thresholds.entrySet() )
        {
            EnsembleAverageType nextType = next.getKey();
            ThresholdsByMetric nextThresholdsByMetric = next.getValue();

            // Statistics for a separate baseline? If no, there's a single set of variables
            if ( Objects.isNull( inputs.getBaseline() ) || !inputs.getBaseline().isSeparateMetrics() )
            {
                Collection<MetricVariable> variables = this.getMetricVariablesForOneTimeWindow( timeWindow,
                                                                                                nextThresholdsByMetric,
                                                                                                units,
                                                                                                desiredTimeScale,
                                                                                                null,
                                                                                                Objects.nonNull( inputs.getBaseline() ),
                                                                                                nextType );
                merged.addAll( variables );
            }
            else
            {
                // Two sets of variables, one for the right and one for the baseline with separate metrics.
                // For backwards compatibility, only clarify the baseline variable
                Collection<MetricVariable> right = this.getMetricVariablesForOneTimeWindow( timeWindow,
                                                                                            nextThresholdsByMetric,
                                                                                            units,
                                                                                            desiredTimeScale,
                                                                                            null,
                                                                                            Objects.nonNull( inputs.getBaseline() ),
                                                                                            nextType );

                Collection<MetricVariable> baseline = this.getMetricVariablesForOneTimeWindow( timeWindow,
                                                                                               nextThresholdsByMetric,
                                                                                               units,
                                                                                               desiredTimeScale,
                                                                                               LeftOrRightOrBaseline.BASELINE,
                                                                                               Objects.nonNull( inputs.getBaseline() ),
                                                                                               nextType );

                merged.addAll( right );
                merged.addAll( baseline );
            }
        }

        return Collections.unmodifiableCollection( merged );
    }

    /**
     * Creates a collection of {@link MetricVariable} for one time window.
     *
     * @param timeWindow the time windows
     * @param thresholds the thresholds
     * @param units the measurement units, if available
     * @param desiredTimeScale the desired time scale, if available
     * @param context optional context for the variable
     * @param hasBaseline is true if a baseline is declared
     * @param ensembleAverageType the ensemble average type
     * @return the metric variables
     */

    private Collection<MetricVariable> getMetricVariablesForOneTimeWindow( TimeWindowOuter timeWindow,
                                                                           ThresholdsByMetric thresholds,
                                                                           String units,
                                                                           TimeScaleOuter desiredTimeScale,
                                                                           LeftOrRightOrBaseline context,
                                                                           boolean hasBaseline,
                                                                           EnsembleAverageType ensembleAverageType )
    {
        Map<MetricConstants, SortedSet<OneOrTwoThresholds>> thresholdMap =
                ThresholdSlicer.getOneOrTwoThresholds( thresholds );

        Map<String, SortedSet<OneOrTwoThresholds>> decomposed =
                this.decomposeThresholdsByMetricForBlobCreation( thresholdMap, hasBaseline );

        LOGGER.debug( "Discovered these thresholds by metric for blob creation and ensemble average type {}: {}.",
                      ensembleAverageType,
                      decomposed );

        Collection<MetricVariable> returnMe = new ArrayList<>();

        // Context to append?
        String append = "";
        if ( Objects.nonNull( context ) )
        {
            append = "_" + context.name();
        }

        // Ensemble average type to append? For backwards compatibility, do not append the default value: #51670
        if ( Objects.nonNull( ensembleAverageType ) && ensembleAverageType != EnsembleAverageType.MEAN )
        {
            append += "_ENSEMBLE_" + ensembleAverageType.name();
        }

        // One variable for each combination of metric and threshold. 
        // When forming threshold names, thresholds should be mapped to all metrics.
        for ( Map.Entry<String, SortedSet<OneOrTwoThresholds>> nextEntry : decomposed.entrySet() )
        {
            String nextMetric = nextEntry.getKey();
            Set<OneOrTwoThresholds> nextThresholds = nextEntry.getValue();

            Map<OneOrTwoThresholds, String> nextMap = this.getStandardThresholdNames()
                                                          .get( nextMetric );

            for ( OneOrTwoThresholds nextThreshold : nextThresholds )
            {
                String thresholdName = nextMap.get( nextThreshold );
                String variableName = nextMetric + "_" + thresholdName + append;

                MetricVariable nextVariable = new MetricVariable.Builder().setVariableName( variableName )
                                                                          .setTimeWindow( timeWindow )
                                                                          .setMetricName( nextMetric )
                                                                          .setThresholds( nextThreshold )
                                                                          .setUnits( units )
                                                                          .setDesiredTimeScale( desiredTimeScale )
                                                                          .setDurationUnits( this.getDurationUnits() )
                                                                          .setEnsembleAverageType( ensembleAverageType )
                                                                          .build();

                LOGGER.debug( "Created a new metric variable to populate with name {}: {}.",
                              variableName,
                              nextVariable );

                returnMe.add( nextVariable );
            }
        }

        return Collections.unmodifiableCollection( returnMe );
    }

    /**
     * Expands a set of thresholds by metric to include a separate mapping for each component part of a multi-part 
     * metric, because each part requires a separate variable in the netCDF.
     *
     * @param thresholdsByMetric the thresholds-by-metric to expand
     * @param hasBaseline is true if there is a baseline within the pairing
     * @return the expanded thresholds-by-metric
     */

    private Map<String, SortedSet<OneOrTwoThresholds>>
    decomposeThresholdsByMetricForBlobCreation( Map<MetricConstants, SortedSet<OneOrTwoThresholds>> thresholdsByMetric,
                                                boolean hasBaseline )
    {

        Map<String, SortedSet<OneOrTwoThresholds>> returnMe = new TreeMap<>();

        for ( Map.Entry<MetricConstants, SortedSet<OneOrTwoThresholds>> nextEntry : thresholdsByMetric.entrySet() )
        {
            MetricConstants nextMetric = nextEntry.getKey();
            SortedSet<OneOrTwoThresholds> nextThresholds = nextEntry.getValue();

            Set<MetricConstants> components = nextMetric.getAllComponents();

            // Univariate scores are part of both their own group and a decomposition group. We are only interested
            // in the decomposition group here, so filter out components that are also univariate scores
            // #81790
            if ( nextMetric.isInGroup( MetricGroup.UNIVARIATE_STATISTIC ) )
            {
                components = components.stream()
                                       .filter( next -> !next.isInGroup( MetricGroup.UNIVARIATE_STATISTIC ) )
                                       .collect( Collectors.toSet() );

                // Remove the baseline component if there is no baseline: saves an empty variable
                if ( !hasBaseline )
                {
                    components.remove( MetricConstants.BASELINE );
                }
            }

            // Decompose, except for the sample size, which has a large number of associations
            // that are not relevant here
            if ( components.size() > 1 && nextMetric != MetricConstants.SAMPLE_SIZE )
            {
                components.forEach( nextComponent -> returnMe.put( nextMetric.name() + "_" + nextComponent.name(),
                                                                   nextThresholds ) );
            }
            else
            {
                returnMe.put( nextMetric.name(), nextThresholds );
            }
        }

        return Collections.unmodifiableMap( returnMe );
    }

    private boolean isGridded()
    {
        return this.getNetcdfConfiguration().isGridded();
    }

    private NetcdfType getNetcdfConfiguration()
    {
        return this.netcdfConfiguration;
    }

    private DestinationConfig getDestinationConfig()
    {
        return this.destinationConfig;
    }

    private Path getOutputDirectory()
    {
        return this.outputDirectory;
    }

    @Override
    public Set<Path> apply( List<DoubleScoreStatisticOuter> output )
    {
        if ( !this.getIsReadyToWrite().get() )
        {
            throw new NetcdfWriteException( "This netcdf output writer is not ready for writing. The blobs must be "
                                            + "created first. The caller has made an error by asking the writer to "
                                            + "accept statistics before calling createBlobsForWriting." );
        }

        LOGGER.debug( "NetcdfOutputWriter {} accepted output {}.", this, output );

        Map<TimeWindowOuter, List<DoubleScoreStatisticOuter>> outputByTimeWindow =
                output.stream()
                      .collect( Collectors.groupingBy( next -> next.getMetadata().getTimeWindow() ) );

        Set<Path> pathsWritten = new HashSet<>();

        for ( Map.Entry<TimeWindowOuter, List<DoubleScoreStatisticOuter>> entries : outputByTimeWindow.entrySet() )
        {
            TimeWindowOuter timeWindow = entries.getKey();
            List<DoubleScoreStatisticOuter> scores = entries.getValue();

            // All writers have been created by now, as asserted above
            TimeWindowWriter writer = this.writersMap.get( timeWindow );
            try
            {
                writer.write( scores );
            }
            catch ( CoordinateNotFoundException | IOException | InvalidRangeException e )
            {
                throw new NetcdfWriteException( "Encountered an error while writing statistics to Netcdf.", e );
            }
            Path pathWritten = Paths.get( writer.outputPath );
            pathsWritten.add( pathWritten );
        }

        // Add the blobs created too, which is the superset. Ideally, only those blobs that receive statistics would
        // be created, but this cannot be resolved until netcdf writing is one-stage rather than two stage.
        // TODO: eliminate this step when writing is one stage, thereby returning the paths with actual statistics
        Set<Path> pathsToBlobsCreated = this.getPathsToBlobsCreated();
        pathsWritten.addAll( pathsToBlobsCreated );

        return Collections.unmodifiableSet( pathsWritten );
    }

    @Override
    public void close() throws IOException
    {
        LOGGER.debug( "About to wait for writing tasks to finish from {}.", this );

        synchronized ( this.windowLock )
        {
            LOGGER.debug( "About to close writers from {}", this );

            if ( this.writersMap.isEmpty() )
            {
                return;
            }

            // Make an attempt to close each writer before excepting
            List<String> failedToClose = new ArrayList<>();
            IOException lastException = null;
            for ( TimeWindowWriter writer : this.writersMap.values() )
            {
                try
                {
                    LOGGER.debug( "Calling writer.close on {}.", writer );
                    writer.close();
                }
                catch ( IOException ioe )
                {
                    failedToClose.add( writer.toString() );
                    lastException = ioe;
                }
            }

            if ( !failedToClose.isEmpty() )
            {
                throw new IOException( "The following writers could not be closed: " + failedToClose + ".",
                                       lastException );
            }

        }

        LOGGER.debug( "Closed writers from {}.", this );
    }

    /**
     * <p>Returns a {@link ThresholdsByMetric} that contains the union of thresholds across all features and metrics for 
     * which blobs should be created. The goal is to identify the thresholds whose statistics should be recorded in the 
     * same blob versus different blobs when the blob includes multiple features. The attribute of a threshold that 
     * determines whether it is distinct from other thresholds is (in this order):
     *
     * <ol>
     * <li>The label, if the threshold contains a label. This is the column header in a csv or the threshold name 
     * supplied to a threshold service. Otherwise:</li>
     * <li>The threshold probability if the threshold is a probability threshold. Otherwise:</li>
     * <li>The threshold value.</li>
     *
     * <p>See #85491. It is essential that the logic for creating blobs in this method is mirrored by the logic for
     * finding blobs in {@link TimeWindowWriter#getVariableName(MetricConstants, DoubleScoreComponentOuter)}.
     *
     * <p>Removes any metrics that do not produce {@link MetricConstants.StatisticType#DOUBLE_SCORE} because this writer
     * currently only handles scores.
     *
     * @param thresholdsByMetricAndFeature the thresholds to search
     * @return the unique thresholds for which blobs should be created
     */

    private ThresholdsByMetric
    getUniqueThresholdsForScoreMetrics( List<ThresholdsByMetricAndFeature> thresholdsByMetricAndFeature )
    {
        Objects.requireNonNull( thresholdsByMetricAndFeature );

        Comparator<OneOrTwoThresholds> thresholdComparator = ThresholdSlicer.getLogicalThresholdComparator();

        // Create a set of thresholds for each metric in a map.        
        Map<MetricConstants, SortedSet<OneOrTwoThresholds>> thresholdsMap = new EnumMap<>( MetricConstants.class );

        for ( ThresholdsByMetricAndFeature thresholds : thresholdsByMetricAndFeature )
        {
            for ( ThresholdsByMetric next : thresholds.getThresholdsByMetricAndFeature().values() )
            {
                Map<MetricConstants, SortedSet<OneOrTwoThresholds>> nextMapping =
                        ThresholdSlicer.getOneOrTwoThresholds( next );

                for ( Map.Entry<MetricConstants, SortedSet<OneOrTwoThresholds>> nextEntry : nextMapping.entrySet() )
                {
                    MetricConstants nextMetric = nextEntry.getKey();

                    // Only allow scores 
                    if ( nextMetric.isInGroup( StatisticType.DOUBLE_SCORE ) )
                    {
                        // Get the existing mapping or a new sorted set instantiated with the threshold comparator
                        SortedSet<OneOrTwoThresholds> mapped =
                                thresholdsMap.getOrDefault( nextMetric,
                                                            new TreeSet<>( thresholdComparator ) );

                        mapped.addAll( nextEntry.getValue() );

                        thresholdsMap.computeIfAbsent( nextMetric,
                                                       k -> thresholdsMap.put( nextMetric, mapped ) );
                    }
                }
            }
        }

        return new ThresholdsByMetric.Builder().addThresholds( thresholdsMap )
                                               .build();
    }

    /**
     * Writes output for a specific pair of lead times, representing the {@link TimeWindowOuter#getEarliestLeadDuration()} and
     * the {@link TimeWindowOuter#getLatestLeadDuration()}.
     */

    private static class TimeWindowWriter implements Closeable
    {

        private static final String WHILE_ATTEMPTING_TO_WRITE_STATISTICS_TO =
                "While attempting to write statistics to ";
        private static final String FAILED_TO_IDENTIFY_A_COORDINATE_FOR_LOCATION =
                ", failed to identify a coordinate for location ";
        NetcdfOutputWriter outputWriter;
        private boolean useLidForLocationIdentifier;
        private final Map<Object, Integer> vectorCoordinatesMap = new ConcurrentHashMap<>();

        private final List<NetcdfValueKey> valuesToSave = new ArrayList<>();

        private final String outputPath;
        private final TimeWindowOuter timeWindow;
        private final Object writeLock;
        private final boolean isDeprecatedWriter;

        /**
         * A writer to be opened on first write, closed when the {@link NetcdfOutputWriter} that encloses this
         * {@link TimeWindowWriter} is closed.
         */
        private NetcdfFormatWriter writer;

        TimeWindowWriter( NetcdfOutputWriter outputWriter,
                          String outputPath,
                          final TimeWindowOuter timeWindow,
                          boolean isDeprecatedWriter )
        {
            this.outputWriter = outputWriter;
            this.outputPath = outputPath;
            this.timeWindow = timeWindow;
            this.writeLock = new ReentrantLock();
            this.isDeprecatedWriter = isDeprecatedWriter;
        }

        void write( List<DoubleScoreStatisticOuter> scores )
                throws IOException, InvalidRangeException
        {
            //this now needs to somehow get all metadata for all metrics
            // Ensure that the output file exists
            for ( DoubleScoreStatisticOuter score : scores )
            {
                Set<MetricConstants> components = score.getComponents();

                for ( MetricConstants nextComponent : components )
                {
                    this.writeInner( score, nextComponent );
                }
            }
        }

        /**
         * @param score the score to write
         * @param nextComponent the score component to write
         * @throws CoordinateNotFoundException if the geographic feature could not be found
         * @throws InvalidRangeException if the data range could not be reconciled
         * @throws IOException if the write fails for any other reason
         */

        private void writeInner( DoubleScoreStatisticOuter score, MetricConstants nextComponent )
                throws IOException, InvalidRangeException
        {
            // Remove clause when the deprecated netcdf format is removed
            if ( this.isDeprecatedWriter )
            {
                this.writeInnerWithoutGroup( score, nextComponent );
                return;
            }

            DoubleScoreComponentOuter componentScore = score.getComponent( nextComponent );

            String name = this.getVariableName( score.getMetricName(), componentScore );

            // Figure out the location of all values and build the origin in each variable grid
            Pool pool = score.getMetadata()
                             .getPool();
            GeometryGroup group = pool.getGeometryGroup();
            List<GeometryTuple> geometries = group.getGeometryTuplesList();
            String featureGroupName = group.getRegionName();

            // Iterate the features, writing the statistics for each tuple in the group
            for ( GeometryTuple nextGeometry : geometries )
            {
                int[] origin;

                try
                {
                    origin = this.getOrigin( nextGeometry, featureGroupName );

                }
                catch ( CoordinateNotFoundException e )
                {
                    throw new CoordinateNotFoundException( "While trying to write the statistic " + componentScore
                                                           + " to the variable "
                                                           + name
                                                           + " at path "
                                                           + this.outputPath
                                                           + ", failed to identify a required coordinate for one "
                                                           + "or more features.",
                                                           e );
                }

                double actualValue = componentScore.getData()
                                                   .getValue();

                if ( MissingValues.isMissingValue( actualValue ) )
                {
                    actualValue = NetcdfOutputFileCreator2.DOUBLE_FILL_VALUE;
                }

                LOGGER.trace( "Actual value found for {}: {}",
                              componentScore.getMetricName(),
                              actualValue );
                this.saveValues( name, origin, actualValue );
            }
        }

        /**
         * @param score the score to write
         * @param nextComponent the score component to write
         * @throws CoordinateNotFoundException if the geographic feature could not be found
         * @throws InvalidRangeException if the data range could not be reconciled
         * @throws IOException if the write fails for any other reason
         * @deprecated to remove when the deprecated format is removed
         */
        @Deprecated( since = "5.14", forRemoval = true )
        private void writeInnerWithoutGroup( DoubleScoreStatisticOuter score, MetricConstants nextComponent )
                throws IOException, InvalidRangeException
        {
            DoubleScoreComponentOuter componentScore = score.getComponent( nextComponent );

            String name = this.getVariableName( score.getMetricName(), componentScore );

            // Figure out the location of all values and build the origin in each variable grid
            Pool pool = score.getMetadata()
                             .getPool();
            GeometryGroup geoGroup = pool.getGeometryGroup();
            GeometryTuple geometry = geoGroup.getGeometryTuplesList()
                                             .get( 0 );

            int[] origin;

            try
            {
                origin = this.getOrigin( name, geometry );
            }
            catch ( CoordinateNotFoundException e )
            {
                throw new CoordinateNotFoundException( "While trying to write the statistic " + componentScore
                                                       + " to the variable "
                                                       + name
                                                       + " at path "
                                                       + this.outputPath
                                                       + ", failed to identify a required coordinate for one "
                                                       + "or more features.",
                                                       e );
            }

            double actualValue = componentScore.getData()
                                               .getValue();

            LOGGER.trace( "Actual value found for {}: {}",
                          componentScore.getMetricName(),
                          actualValue );
            this.saveValues( name, origin, actualValue );
        }

        private void writeMetricResults() throws IOException, InvalidRangeException
        {
            synchronized ( this.writeLock )
            {
                Array netcdfValue;
                Index ima;

                // Open a writer to write to the path. Must be closed when closing the overall NetcdfOutputWriter instance
                if ( Objects.isNull( this.writer ) )
                {
                    this.writer = NetcdfFormatWriter.openExisting( this.outputPath )
                                                    .build();

                    LOGGER.trace( "Opened an underlying netcdf writer {} for pool {}.", this.writer, this.timeWindow );
                }

                for ( NetcdfValueKey key : this.valuesToSave )
                {
                    int[] shape = new int[key.getOrigin().length];
                    Arrays.fill( shape, 1 );

                    if ( !this.isDeprecatedWriter )
                    {
                        netcdfValue = Array.factory( DataType.DOUBLE, shape );
                        ima = netcdfValue.getIndex();
                        double value = key.getValue();

                        if ( MissingValues.isMissingValue( value ) )
                        {
                            value = NetcdfOutputFileCreator2.DOUBLE_FILL_VALUE;
                        }

                        LOGGER.trace( "Value found for {}: {}", ima, value );
                        netcdfValue.setDouble( ima, value );

                    }
                    else
                    {
                        // TODO remove this block, remove if/else
                        netcdfValue = Array.factory( DataType.FLOAT, shape );

                        ima = netcdfValue.getIndex();
                        netcdfValue.setFloat( ima, ( float ) key.getValue() );
                    }

                    try
                    {
                        this.writer.write( key.getVariableName(), key.getOrigin(), netcdfValue );
                    }
                    catch ( NullPointerException | IOException | InvalidRangeException e )
                    {
                        String exceptionMessage = "While attempting to write data value "
                                                  + key.getValue()
                                                  + " with variable name "
                                                  + key.getVariableName()
                                                  + " to index "
                                                  + Arrays.toString( key.getOrigin() )
                                                  + " within file "
                                                  + this.outputPath
                                                  + ": ";
                        throw new IOException( exceptionMessage, e );
                    }
                }

                this.writer.flush();

                this.valuesToSave.clear();
            }
        }

        /**
         * Attempts to find the standard metric-threshold name of a variable within the netCDF blob that corresponding 
         * to the score metadata.
         *
         * @param metricName the metric name
         * @param score the score whose metric-threshold standard name is required
         * @return the standard name
         * @throws IllegalStateException if the variable name could not be found
         */

        private String getVariableName( MetricConstants metricName,
                                        DoubleScoreComponentOuter score )
        {
            PoolMetadata sampleMetadata = score.getMetadata();

            // Locating the variable relies on the use of a naming convention that matches the naming convention at blob
            // creation time. 
            // TODO: use a common code pathway to generate the name at these two times or, better still, create blobs 
            // on-demand when the first statistic arrives. This is not currently possible with Netcdf 3 and/or the UCAR
            // Java library.

            // Find the metric name
            MetricConstants metricComponentName =
                    MetricConstants.valueOf( score.getData()
                                                  .getMetric()
                                                  .getName()
                                                  .name() );

            String metricNameString = metricName.name();
            if ( metricComponentName != MetricConstants.MAIN )
            {
                metricNameString = metricNameString + "_" + metricComponentName.name();
            }

            String append = "";
            if ( sampleMetadata.getPool().getIsBaselinePool() )
            {
                append = "_" + LeftOrRightOrBaseline.BASELINE.name();
            }

            // Add the ensemble average type where applicable
            wres.statistics.generated.Pool.EnsembleAverageType ensembleAverageType = sampleMetadata.getPool()
                                                                                                   .getEnsembleAverageType();
            if ( ensembleAverageType != wres.statistics.generated.Pool.EnsembleAverageType.MEAN
                 && ensembleAverageType != wres.statistics.generated.Pool.EnsembleAverageType.NONE )
            {
                append += "_ENSEMBLE_" + ensembleAverageType.name();
            }

            // Look for a threshold with a standard name that is like the threshold associated with this score
            LOGGER.debug( "Searching the standard threshold names for metric name {} with qualifier {}.",
                          metricNameString,
                          append );

            Map<OneOrTwoThresholds, String> thresholdMap = this.outputWriter.getStandardThresholdNames()
                                                                            .get( metricNameString );

            // #81594
            if ( Objects.isNull( thresholdMap ) )
            {
                throw new IllegalStateException( "While attempting to write statistics to netcdf for the metric "
                                                 + "variable "
                                                 + metricNameString
                                                 + " and threshold "
                                                 + sampleMetadata.getThresholds()
                                                 + ", failed to locate the variable name in the map of metric "
                                                 + "variables. The map contains the following metric variable names: "
                                                 + this.outputWriter.standardThresholdNames.keySet()
                                                 + ". The sample metadata of the statistic that could not be written "
                                                 + "is: "
                                                 + sampleMetadata );
            }

            for ( Map.Entry<OneOrTwoThresholds, String> nextThreshold : thresholdMap.entrySet() )
            {
                String nextName = nextThreshold.getValue();
                OneOrTwoThresholds thresholdFromArchive = nextThreshold.getKey();
                OneOrTwoThresholds thresholdFromScore = score.getMetadata()
                                                             .getThresholds();

                // Second threshold is always a decision threshold and can be compared directly
                boolean secondThresholdIsEqual =
                        Objects.equals( thresholdFromArchive.second(), thresholdFromScore.second() );

                String name = this.getVariableNameOrNull( thresholdFromScore,
                                                          thresholdFromArchive,
                                                          nextName,
                                                          sampleMetadata,
                                                          metricNameString,
                                                          append,
                                                          secondThresholdIsEqual );

                //Name discovered? return it
                if ( Objects.nonNull( name ) )
                {
                    return name;
                }
            }

            throw new IllegalStateException( "While attempting to write statistics to netcdf for the metric variable "
                                             + metricNameString
                                             + " and threshold "
                                             + sampleMetadata.getThresholds()
                                             + ", discovered the variable name "
                                             + metricNameString
                                             + ", but failed to discover the standard threshold name within the map of "
                                             + "standard names. The map contained the following entries: "
                                             + thresholdMap
                                             + ". The sample metadata of the statistic that could not be written is: "
                                             + sampleMetadata );
        }

        /**
         * Returns a variable name or null from the inputs.
         *
         * @param thresholdFromScore the threshold from the score statistic whose netcdf variable name is required
         * @param thresholdFromArchive a threshold from the archive of thresholds whose names should be searched
         * @param thresholdFromArchiveName the standard name given to the threshold from the archive 
         * @param sampleMetadata the sample metadata
         * @param metricNameString a string name for the metric
         * @param append a string to append to the variable name
         * @param secondThresholdIsEqual
         * @return
         */

        private String getVariableNameOrNull( OneOrTwoThresholds thresholdFromScore,
                                              OneOrTwoThresholds thresholdFromArchive,
                                              String thresholdFromArchiveName,
                                              PoolMetadata sampleMetadata,
                                              String metricNameString,
                                              String append,
                                              boolean secondThresholdIsEqual )
        {
            // First threshold needs to be compared more selectively. First look for a label.
            if ( thresholdFromScore.first().hasLabel() && thresholdFromArchive.first().hasLabel() )
            {

                // Label associated with event threshold is equal, and any decision threshold is equal
                String thresholdWithValuesOne = thresholdFromArchive.first()
                                                                    .getLabel();
                String thresholdWithValuesTwo = sampleMetadata.getThresholds()
                                                              .first()
                                                              .getLabel();

                if ( thresholdWithValuesOne.equals( thresholdWithValuesTwo )
                     && secondThresholdIsEqual )
                {
                    return metricNameString + "_" + thresholdFromArchiveName + append;
                }
            }
            // Next, use the probability identifier for a probability threshold
            else if ( thresholdFromScore.first().hasProbabilities()
                      && thresholdFromArchive.first().hasProbabilities() )
            {
                OneOrTwoDoubles doubles = thresholdFromScore.first().getProbabilities();
                OneOrTwoDoubles otherDoubles = thresholdFromArchive.first().getProbabilities();

                if ( doubles.equals( otherDoubles ) && secondThresholdIsEqual )
                {
                    return metricNameString + "_" + thresholdFromArchiveName + append;
                }
            }
            // Resort to comparing all.
            else if ( thresholdFromScore.first().equals( thresholdFromArchive.first() ) && secondThresholdIsEqual )
            {
                return metricNameString + "_" + thresholdFromArchiveName + append;
            }

            return null;
        }

        private void saveValues( String name, int[] origin, double value )
                throws IOException, InvalidRangeException
        {
            synchronized ( this.writeLock )
            {
                this.valuesToSave.add( new NetcdfValueKey( name, origin, value ) );

                if ( this.valuesToSave.size() > VALUE_SAVE_LIMIT )
                {
                    this.writeMetricResults();
                    LOGGER.trace( "Output {} values to {}", VALUE_SAVE_LIMIT, this.outputPath );
                }
            }
        }

        /**
         * Finds the origin index(es) of the location in the netcdf variables
         * @param feature The location specification detailing where to place a value
         * @param featureGroupName the name of the feature group that contains the feature
         * @return The coordinates for the location within the Netcdf variable describing where to place data
         */
        private int[] getOrigin( GeometryTuple feature, String featureGroupName )
                throws IOException
        {
            int[] origin;
            LOGGER.trace( "Looking for the origin of {} in feature group {}.", feature, featureGroupName );
            Integer vectorIndex = this.getVectorCoordinate( feature,
                                                            featureGroupName );

            if ( vectorIndex == null )
            {
                throw new CoordinateNotFoundException( "An index for the vector coordinate could not "
                                                       + "be evaluated. [value = "
                                                       + NetcdfOutputFileCreator2.getGeometryTupleName( feature )
                                                       + "]. The location was "
                                                       + feature
                                                       + " and the feature group name was "
                                                       + featureGroupName );
            }

            origin = new int[] { vectorIndex };
            LOGGER.trace( "The origin of {} was at {}", feature, origin );
            return origin;
        }


        /**
         * Finds the origin index(es) of the location in the netcdf variables
         * @param name the variable name
         * @param tuple The location specification detailing where to place a value
         * @return The coordinates for the location within the Netcdf variable describing where to place data
         * @deprecated As of 5.1, TODO remove this whole method, keep 1-arg one
         */
        @Deprecated( since = "5.1", forRemoval = true )
        private int[] getOrigin( String name, GeometryTuple tuple ) throws IOException
        {
            int[] origin;
            Geometry location = tuple.getRight();

            LOGGER.trace( "Looking for the origin of {}", location );

            // There must be a more coordinated way to do this without having to keep the file open
            // What if we got the info through the template?
            if ( this.outputWriter.isGridded() )
            {
                if ( Objects.isNull( location.getWkt() ) )
                {
                    throw new CoordinateNotFoundException( "The location '" +
                                                           location
                                                           +
                                                           "' cannot be written to the "
                                                           + "output because the project "
                                                           + "configuration dictates gridded "
                                                           + "output but the location doesn't "
                                                           + "support it." );
                }

                String wkt = location.getWkt();
                Feature.GeoPoint point = Feature.getLonLatFromPointWkt( wkt );

                // contains the the y index and the x index
                origin = new int[2];

                // TODO: Find a different approach to handle grids without a coordinate system
                try ( GridDataset gridDataset = GridDataset.open( this.outputPath ) )
                {
                    GridDatatype variable = gridDataset.findGridDatatype( name );
                    int[] xyIndex = variable.getCoordinateSystem()
                                            .findXYindexFromLatLon( point.y(),
                                                                    point.x(),
                                                                    null );

                    origin[0] = xyIndex[1];
                    origin[1] = xyIndex[0];
                }
            }
            else
            {
                // Only contains the vector id
                Integer vectorIndex = this.getVectorCoordinate( tuple, null );

                if ( vectorIndex == null )
                {

                    throw new CoordinateNotFoundException( "An index for the vector coordinate could not "
                                                           + "be evaluated. [value = "
                                                           + location.getName()
                                                           + "]. The location was "
                                                           + location );
                }

                origin = new int[] { vectorIndex };
            }

            LOGGER.trace( "The origin of {} was at {}", location, origin );
            return origin;
        }


        private Integer getVectorCoordinate( GeometryTuple feature,
                                             String featureGroupName )
                throws IOException
        {
            synchronized ( this.vectorCoordinatesMap )
            {
                // Mapped coordinates already?
                if ( this.vectorCoordinatesMap.isEmpty() )
                {
                    // No, map 'em
                    this.populateCoordinateMap();
                }

                if ( !this.isDeprecatedWriter )
                {
                    String tupleNameInNetcdfFile = NetcdfOutputFileCreator2.getGeometryTupleName( feature );

                    // Qualify the feature tuple name with the group name, as mapped
                    String tupleNameInNetcdfFilePlusGroupName = tupleNameInNetcdfFile + "_" + featureGroupName;

                    this.checkForCoordinateAndThrowExceptionIfNotFound( tupleNameInNetcdfFilePlusGroupName, true );
                    return this.vectorCoordinatesMap.get( tupleNameInNetcdfFilePlusGroupName );
                }
                else
                {
                    // TODO remove this whole block, remove the "if/else"
                    String loc = feature.getRight()
                                        .getName();

                    if ( this.useLidForLocationIdentifier )
                    {
                        this.checkForCoordinateAndThrowExceptionIfNotFound( loc, true );
                        return this.vectorCoordinatesMap.get( loc );
                    }
                    else
                    {
                        this.checkForCoordinateAndThrowExceptionIfNotFound( loc, false );
                        return this.vectorCoordinatesMap.get( Integer.valueOf( loc ) );
                    }
                }
            }
        }

        /**
         * Populates a map of coordinates with those in the netcdf blob.
         * @throws IOException if blob could not be inspected
         */

        private void populateCoordinateMap() throws IOException
        {
            try ( NetcdfFile outputFile = NetcdfFiles.open( this.outputPath ) )
            {
                Variable coordinate = null;

                if ( this.isDeprecatedWriter )
                {
                    // TODO remove this whole block
                    String nameToUse = this.outputWriter.getNetcdfConfiguration()
                                                        .getVectorVariable();

                    LOGGER.debug( "Using {} as the name of the vector variable with the location information.",
                                  nameToUse );

                    coordinate = outputFile.findVariable( nameToUse );

                    if ( coordinate.getDataType() == DataType.CHAR )
                    {
                        this.useLidForLocationIdentifier = true;
                    }
                    else
                    {
                        this.useLidForLocationIdentifier = false;
                        Array values = coordinate.read();

                        for ( int index = 0; index < values.getSize(); ++index )
                        {
                            this.vectorCoordinatesMap.put( values.getObject( index ), index );
                        }
                    }
                }
                else
                {
                    coordinate = outputFile.findVariable( FEATURE_TUPLE_VARIABLE_NAME );
                }

                if ( !this.isDeprecatedWriter )
                {
                    Variable featureGroupVariable = outputFile.findVariable( FEATURE_GROUP_VARIABLE_NAME );

                    // It's probably not necessary to load in everything
                    // We're loading everything in at the moment because we
                    // don't really know what to expect
                    List<Dimension> dimensions =
                            coordinate.getDimensions();
                    List<Dimension> groupDimensions =
                            featureGroupVariable.getDimensions();

                    for ( int wordIndex = 0;
                          wordIndex < dimensions.get( 0 ).getLength();
                          wordIndex++ )
                    {
                        int[] origin = new int[] { wordIndex, 0 };
                        int[] shape = new int[] { 1,
                                dimensions.get( 1 ).getLength() };
                        char[] characters =
                                ( char[] ) coordinate.read( origin,
                                                            shape )
                                                     .get1DJavaArray( DataType.CHAR );
                        String word = String.valueOf( characters ).trim();

                        // Get the corresponding feature group name, same index different shape
                        int[] groupShape = new int[] { 1,
                                groupDimensions.get( 1 ).getLength() };
                        char[] groupCharacters =
                                ( char[] ) featureGroupVariable.read( origin,
                                                                      groupShape )
                                                               .get1DJavaArray( DataType.CHAR );
                        String groupName = String.valueOf( groupCharacters ).trim();

                        String wordWithGroup = word + "_" + groupName;
                        this.vectorCoordinatesMap.put( wordWithGroup, wordIndex );
                    }
                }
                // TODO: remove the switch below for the deprecated writer where the lid is used
                else if ( this.useLidForLocationIdentifier )
                {
                    // It's probably not necessary to load in everything
                    // We're loading everything in at the moment because we
                    // don't really know what to expect
                    List<Dimension> dimensions =
                            coordinate.getDimensions();

                    for ( int wordIndex = 0;
                          wordIndex < dimensions.get( 0 ).getLength();
                          wordIndex++ )
                    {
                        int[] origin = new int[] { wordIndex, 0 };
                        int[] shape = new int[] { 1,
                                dimensions.get( 1 ).getLength() };
                        char[] characters =
                                ( char[] ) coordinate.read( origin,
                                                            shape )
                                                     .get1DJavaArray( DataType.CHAR );
                        String word = String.valueOf( characters ).trim();

                        this.vectorCoordinatesMap.put( word, wordIndex );
                    }
                }
            }
            catch ( InvalidRangeException e )
            {
                throw new IOException( "A coordinate could not be read.", e );
            }
        }

        /**
         * Checks for the presence of a coordinate corresponding to the prescribed location and throws an exception
         * if the coordinate cannot be found.
         *
         * @param location the location to check
         * @param isLocationName is true if the feature is a named location
         * @throws CoordinateNotFoundException if a coordinate could not be found
         */

        private void checkForCoordinateAndThrowExceptionIfNotFound( String location, boolean isLocationName )
        {
            // Location name is the glue
            if ( isLocationName )
            {
                // Exception if not mapped
                if ( !this.vectorCoordinatesMap.containsKey( location ) )
                {
                    throw new CoordinateNotFoundException( WHILE_ATTEMPTING_TO_WRITE_STATISTICS_TO
                                                           + this.outputPath
                                                           + FAILED_TO_IDENTIFY_A_COORDINATE_FOR_LOCATION
                                                           + location
                                                           + "." );
                }
            }
            // Comid is the glue
            else
            {
                Long coordinate;

                try
                {
                    coordinate = Long.parseLong( location );
                }
                catch ( NumberFormatException nfe )
                {
                    throw new CoordinateNotFoundException( WHILE_ATTEMPTING_TO_WRITE_STATISTICS_TO
                                                           + this.outputPath
                                                           + FAILED_TO_IDENTIFY_A_COORDINATE_FOR_LOCATION
                                                           + location
                                                           + " because the NWM feature id was not type long.",
                                                           nfe );
                }

                if ( coordinate > Integer.MAX_VALUE || coordinate < Integer.MIN_VALUE )
                {
                    throw new CoordinateNotFoundException( WHILE_ATTEMPTING_TO_WRITE_STATISTICS_TO
                                                           + this.outputPath
                                                           + FAILED_TO_IDENTIFY_A_COORDINATE_FOR_LOCATION
                                                           + location
                                                           + " because the NWM feature id was out of integer range." );
                }

                if ( !this.vectorCoordinatesMap.containsKey( Integer.valueOf( coordinate.intValue() ) ) )
                {

                    throw new CoordinateNotFoundException( WHILE_ATTEMPTING_TO_WRITE_STATISTICS_TO
                                                           + this.outputPath
                                                           + FAILED_TO_IDENTIFY_A_COORDINATE_FOR_LOCATION
                                                           + location
                                                           + " using the NWM location identifier (comid) "
                                                           + location
                                                           + "." );
                }
            }
        }

        @Override
        public String toString()
        {
            String representation = "TimeWindowWriter";

            if ( Objects.nonNull( this.outputPath ) && !this.outputPath.isBlank() )
            {
                representation = this.outputPath;
            }
            else if ( Objects.nonNull( this.timeWindow ) )
            {
                representation = this.timeWindow.toString();
            }

            return representation;
        }

        @Override
        public void close() throws IOException
        {
            LOGGER.trace( "Closing {}", this );

            try
            {
                if ( !this.valuesToSave.isEmpty() )
                {
                    try
                    {
                        this.writeMetricResults();
                    }
                    catch ( IllegalArgumentException | InvalidRangeException e )
                    {
                        throw new IOException(
                                "Lingering Netcdf results could not be written to disk.",
                                e );
                    }

                    // Compressing the output results in around a 95.33%
                    // decrease in file size. Early tests had files dropping
                    // from 135MB to 6.3MB
                }

            }
            finally
            {
                if ( Objects.nonNull( this.writer ) )
                {
                    LOGGER.trace( "Closing the underlying netcdf writer {} for pool {}.",
                                  this.writer,
                                  this.timeWindow );

                    this.writer.close();
                }
            }
        }

    }
}
