package wres.writing.netcdf;

import java.io.Closeable;
import java.io.IOException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
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

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.math3.util.Precision;
import org.locationtech.jts.geom.Coordinate;
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

import wres.config.yaml.DeclarationUtilities;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.SamplingUncertainty;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.DataUtilities;
import wres.datamodel.MissingValues;
import wres.datamodel.time.TimeWindowSlicer;
import wres.datamodel.types.OneOrTwoDoubles;
import wres.config.MetricConstants;
import wres.config.MetricConstants.MetricGroup;
import wres.config.MetricConstants.StatisticType;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter.DoubleScoreComponentOuter;
import wres.datamodel.thresholds.MetricsAndThresholds;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdSlicer;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.Covariate;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Pool;
import wres.statistics.generated.Pool.EnsembleAverageType;
import wres.statistics.generated.SummaryStatistic;
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
    private static final String CREATED_A_NEW_METRIC_VARIABLE_TO_POPULATE_WITH_NAME =
            "Created a new metric variable to populate with name {}: {}.";
    private final Object windowLock = new Object();
    private final Path outputDirectory;
    private final Outputs.NetcdfFormat netcdfFormat;

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

    private final EvaluationDeclaration declaration;

    /**
     * Records whether the writer is ready to write. It is ready when all blobs have been created.
     */

    private final AtomicBoolean isReadyToWrite;

    /**
     * Mapping between a quantile for sampling uncertainty and its standard name.
     */

    private final Map<Double, String> standardQuantileNamesForSamplingUncertainty;

    /**
     * Mapping between a summary statistic quantile (not for sampling uncertainty) and its standard name.
     */

    private final Map<Double, String> standardQuantileNamesForSummaryStatistics;

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
     * @param declaration the project configuration
     * @param durationUnits the time units for durations
     * @param outputDirectory the directory into which to write
     * @return an instance of the writer
     * @throws NetcdfWriteException if the blobs could not be created for any reason
     */

    public static NetcdfOutputWriter of( SystemSettings systemSettings,
                                         EvaluationDeclaration declaration,
                                         ChronoUnit durationUnits,
                                         Path outputDirectory )
    {
        return new NetcdfOutputWriter( systemSettings,
                                       declaration,
                                       durationUnits,
                                       outputDirectory );
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
                      .collect( Collectors.groupingBy( next -> next.getPoolMetadata().getTimeWindow() ) );

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
     * Returns the duration units for writing lead durations.
     *
     * @return the duration units
     */

    ChronoUnit getDurationUnits()
    {
        return this.durationUnits;
    }

    /**
     * Hidden constructor.
     * @param systemSettings the system settings
     * @param declaration the declaration
     * @param durationUnits the duration units
     * @param outputDirectory the output directory
     * @throws NullPointerException if any input is null
     */

    private NetcdfOutputWriter( SystemSettings systemSettings,
                                EvaluationDeclaration declaration,
                                ChronoUnit durationUnits,
                                Path outputDirectory )
    {
        Objects.requireNonNull( systemSettings );
        Objects.requireNonNull( declaration, "Specify non-null project declaration." );
        Objects.requireNonNull( durationUnits, "Specify non-null duration units." );
        Objects.requireNonNull( outputDirectory, "Specify non-null output directory." );

        Outputs outputs = declaration.formats()
                                     .outputs();

        if ( outputs.hasNetcdf2() == outputs.hasNetcdf() )
        {
            throw new IllegalArgumentException( "To create an output writer, precisely one of the NetCDF or NetCDF2 "
                                                + "formats should be declared." );
        }

        if ( outputs.hasNetcdf() )
        {
            LOGGER.debug( "Creating a writer for the deprecated NetCDF format." );
            this.deprecatedVersion = true;
            this.netcdfFormat = outputs.getNetcdf();

        }
        else
        {
            LOGGER.debug( "Creating a writer for the NetCDF2 format." );
            this.deprecatedVersion = false;
            this.netcdfFormat = null;
        }


        LOGGER.debug( "Created NetcdfOutputWriter {}", this );
        this.durationUnits = durationUnits;
        this.outputDirectory = outputDirectory;
        this.declaration = declaration;
        this.isReadyToWrite = new AtomicBoolean();

        // Set the quantile names for sampling uncertainty
        this.standardQuantileNamesForSamplingUncertainty =
                this.getStandardQuantileNamesForSamplingUncertainty( declaration.sampleUncertainty() );

        this.standardQuantileNamesForSummaryStatistics =
                this.getStandardQuantileNamesForSummaryStatistics( declaration.summaryStatistics() );
    }

    /**
     * Generates the standard quantile names for sampling uncertainty calculation.
     * @param samplingUncertainty the sampling uncertainty declaration
     * @return the standard quantile names, if any
     */
    private Map<Double, String> getStandardQuantileNamesForSamplingUncertainty( SamplingUncertainty samplingUncertainty )
    {
        Map<Double, String> standardQuantileNamesInner = new HashMap<>();
        if ( Objects.nonNull( samplingUncertainty ) )
        {
            SortedSet<Double> quantiles = samplingUncertainty.quantiles();
            int qNumber = 1;
            for ( Double next : quantiles )
            {
                standardQuantileNamesInner.put( next, "Q" + qNumber );
                qNumber++;
            }

            standardQuantileNamesInner = Collections.unmodifiableMap( standardQuantileNamesInner );
            LOGGER.debug( "Created the following standard quantile names by quantile value for sampling uncertainty: "
                          + "{}.",
                          standardQuantileNamesInner );
        }

        return Collections.unmodifiableMap( standardQuantileNamesInner );
    }

    /**
     * Generates the standard quantile names for summary statistics calculation.
     * @param summaryStatistics the smmary statistics declaration
     * @return the standard quantile names, if any
     */
    private Map<Double, String> getStandardQuantileNamesForSummaryStatistics( Set<SummaryStatistic> summaryStatistics )
    {
        Map<Double, String> standardQuantileNamesInner = new HashMap<>();
        int qNumber = 1;
        for ( SummaryStatistic summaryStatistic : summaryStatistics )
        {
            if ( summaryStatistic.getStatistic() == SummaryStatistic.StatisticName.QUANTILE )
            {
                double quantile = summaryStatistic.getProbability();
                standardQuantileNamesInner.put( quantile, "Q" + qNumber );
                qNumber++;
            }
        }

        standardQuantileNamesInner = Collections.unmodifiableMap( standardQuantileNamesInner );
        LOGGER.debug( "Created the following standard quantile names by quantile value for summary statistics: {}.",
                      standardQuantileNamesInner );

        return standardQuantileNamesInner;
    }

    /**
     * Creates the blobs into which outputs will be written.
     *
     * @param featureGroups The super-set of feature groups used in the evaluation.
     * @param metricsAndThresholds Thresholds imposed upon input data
     * @throws NetcdfWriteException if the blobs have already been created
     * @throws IOException if the blobs could not be created for any reason
     */

    public void createBlobsForWriting( Set<FeatureGroup> featureGroups,
                                       Set<MetricsAndThresholds> metricsAndThresholds )
            throws IOException
    {
        Objects.requireNonNull( metricsAndThresholds );

        if ( this.getIsReadyToWrite()
                 .get() )
        {
            throw new NetcdfWriteException( "The netcdf blobs have already been created." );
        }

        // Time windows
        Set<TimeWindowOuter> timeWindows = TimeWindowSlicer.getTimeWindows( this.getDeclaration() );

        // Find the thresholds-by-metric for which blobs should be created

        // Create a map of these with for each ensemble average type? Will that create only the variables needed or
        // more than needed? For example, what happens if the mean error is requested for the ensemble median and not
        // for the ensemble mean - will that produce a variable for the ensemble median only? Use the default averaging
        // type if the evaluation does not contain ensemble forecasts
        boolean hasEnsembles = this.getDeclaration()
                                   .right()
                                   .type() == wres.config.yaml.components.DataType.ENSEMBLE_FORECASTS;
        Function<MetricsAndThresholds, EnsembleAverageType> ensembleTypeCalculator = thresholds -> {
            if ( hasEnsembles )
            {
                return thresholds.ensembleAverageType();
            }

            return EnsembleAverageType.MEAN;
        };

        Map<EnsembleAverageType, List<MetricsAndThresholds>> byType =
                metricsAndThresholds.stream()
                                    .collect( Collectors.groupingBy( ensembleTypeCalculator ) );

        Map<EnsembleAverageType, Map<MetricConstants, SortedSet<OneOrTwoThresholds>>> thresholds =
                byType.entrySet()
                      .stream()
                      .collect( Collectors.toUnmodifiableMap( Map.Entry::getKey,
                                                              e -> this.getUniqueThresholdsForScoreMetrics( e.getValue() ) ) );

        // Should be at least one metric with at least one threshold
        if ( thresholds.values()
                       .stream()
                       .allMatch( Map::isEmpty ) )
        {
            throw new IOException( "Could not identify any metrics for which NetCDF writing is supported. Only score "
                                   + "metrics are supported for NetCDF writing. Please add some scores to your "
                                   + "evaluation or remove the NetCDF writing option and try again." );
        }

        // Units, if declared
        String units = "UNKNOWN";
        if ( Objects.nonNull( this.getDeclaration()
                                  .unit() ) )
        {
            units = this.getDeclaration()
                        .unit();
        }

        // Desired time scale, if declared
        TimeScaleOuter desiredTimeScale = null;
        if ( Objects.nonNull( this.getDeclaration().timeScale() ) )
        {
            desiredTimeScale = TimeScaleOuter.of( this.getDeclaration()
                                                      .timeScale()
                                                      .timeScale() );
        }

        Set<Path> allPathsCreated = new HashSet<>();

        // Create blobs from components
        synchronized ( this.windowLock )
        {
            Set<Path> pathsCreated = this.createBlobsAndBlobWriters( this.getDeclaration(),
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

    private EvaluationDeclaration getDeclaration()
    {
        return this.declaration;
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
     * @param declaration the evaluation declaration
     * @param featureGroups The super-set of feature groups used in this evaluation.
     * @param timeWindows the time windows
     * @param thresholds the thresholds
     * @param units the measurement units, if available
     * @param desiredTimeScale the desired time scale, if available
     * @param deprecatedVersion True when using deprecated code, false otherwise
     * @throws IOException if the blobs could not be created for any reason
     * @return the paths written
     */

    private Set<Path> createBlobsAndBlobWriters( EvaluationDeclaration declaration,
                                                 Set<FeatureGroup> featureGroups,
                                                 Set<TimeWindowOuter> timeWindows,
                                                 Map<EnsembleAverageType, Map<MetricConstants, SortedSet<OneOrTwoThresholds>>> thresholds,
                                                 String units,
                                                 TimeScaleOuter desiredTimeScale,
                                                 boolean deprecatedVersion )
            throws IOException
    {
        Set<Path> returnMe = new TreeSet<>();

        // Create the standard threshold names
        this.standardThresholdNames =
                this.createStandardThresholdNames( thresholds, DeclarationUtilities.hasBaseline( declaration ) );

        LOGGER.debug( "Created this map of standard threshold names: {}", this.standardThresholdNames );

        // One blob and blob writer per time window      
        for ( TimeWindowOuter nextWindow : timeWindows )
        {
            Collection<MetricVariable> variables = this.getMetricVariablesForOneTimeWindow( declaration,
                                                                                            nextWindow,
                                                                                            thresholds,
                                                                                            units,
                                                                                            desiredTimeScale );

            // Create the blob path
            Path targetPath = this.getOutputPathToWriteForOneTimeWindow( this.getOutputDirectory(),
                                                                         this.getDeclaration(),
                                                                         this.getScenarioNameForBlobOrNull( declaration ),
                                                                         nextWindow,
                                                                         this.getDurationUnits() );

            String pathActuallyWritten;

            if ( !deprecatedVersion )
            {
                // Create the blob
                pathActuallyWritten =
                        NetcdfOutputFileCreator2.create( declaration,
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
                                                        nextWindow,
                                                        NetcdfOutputWriter.ANALYSIS_TIME,
                                                        variables );
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
    createStandardThresholdNames( Map<EnsembleAverageType, Map<MetricConstants, SortedSet<OneOrTwoThresholds>>> thresholds,
                                  boolean hasBaseline )
    {
        // Create the standard threshold names, sequenced by natural order of the threshold
        SortedSet<OneOrTwoThresholds> union = thresholds.values()
                                                        .stream()
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
        for ( Map<MetricConstants, SortedSet<OneOrTwoThresholds>> nextMetrics : thresholds.values() )
        {
            Map<MetricNames, SortedSet<OneOrTwoThresholds>> decomposed =
                    this.decomposeThresholdsByMetricForBlobCreation( nextMetrics, hasBaseline );

            for ( Map.Entry<MetricNames, SortedSet<OneOrTwoThresholds>> nextEntry : decomposed.entrySet() )
            {
                String nextMetric = nextEntry.getKey()
                                             .metricName();
                SortedSet<OneOrTwoThresholds> nextThresholdsForMetric = nextEntry.getValue();
                Map<OneOrTwoThresholds, String> namedThresholds = new HashMap<>();
                nextThresholdsForMetric.forEach( next -> namedThresholds.put( next, thresholdMap.get( next ) ) );

                if ( returnMe.containsKey( nextMetric ) )
                {
                    returnMe.get( nextMetric )
                            .putAll( namedThresholds );
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
     * @param declaration the declaration
     * @param scenarioName the optional scenario name
     * @param timeWindow the time window
     * @param leadUnits the time units to use for the lead durations
     * @return the file name
     * @throws NullPointerException if any of the inputs is null
     */

    private Path getOutputPathToWriteForOneTimeWindow( Path outputDirectory,
                                                       EvaluationDeclaration declaration,
                                                       String scenarioName,
                                                       TimeWindowOuter timeWindow,
                                                       ChronoUnit leadUnits )
    {
        Objects.requireNonNull( outputDirectory, "Enter non-null output directory to establish a path for writing." );
        Objects.requireNonNull( timeWindow, "Enter a non-null time window  to establish a path for writing." );
        Objects.requireNonNull( leadUnits,
                                "Enter a non-null time unit for the lead durations to establish a path for writing." );
        Objects.requireNonNull( declaration, "Provide non-null declaration to establish a path for writing." );

        StringJoiner filename = new StringJoiner( "_" );

        // Add optional scenario identifier
        if ( Objects.nonNull( scenarioName ) )
        {
            filename.add( scenarioName );
        }

        // Add the earliest reference time identifier: GitHub #436
        if ( !timeWindow.getEarliestReferenceTime()
                        .equals( Instant.MIN ) )
        {
            String lastTime = timeWindow.getEarliestReferenceTime()
                                        .toString();
            lastTime = lastTime.replace( "-", "" )
                               .replace( ":", "" )
                               .replace( "Z$", "" );

            filename.add( lastTime );
        }

        // Add latest reference time identifier
        if ( !timeWindow.getLatestReferenceTime()
                        .equals( Instant.MAX ) )
        {
            String lastTime = timeWindow.getLatestReferenceTime()
                                        .toString();
            lastTime = lastTime.replace( "-", "" )
                               .replace( ":", "" )
                               .replace( "Z$", "" );

            filename.add( lastTime );
        }

        // Add the earliest valid time identifier: GitHub #436
        if ( !timeWindow.getEarliestValidTime()
                        .equals( Instant.MIN ) )
        {
            String lastTime = timeWindow.getEarliestValidTime()
                                        .toString();
            lastTime = lastTime.replace( "-", "" )
                               .replace( ":", "" )
                               .replace( "Z$", "" );

            filename.add( lastTime );
        }

        // Add latest valid time identifier
        if ( !timeWindow.getLatestValidTime()
                        .equals( Instant.MAX ) )
        {
            String lastTime = timeWindow.getLatestValidTime()
                                        .toString();
            lastTime = lastTime.replace( "-", "" )
                               .replace( ":", "" )
                               .replace( "Z$", "" );

            filename.add( lastTime );
        }

        // Add the earliest lead duration as a qualifier: see GitHub issue #245
        // Format the latest lead duration with the default format
        Number numericDurationLower = DataUtilities.durationToNumericUnits( timeWindow.getEarliestLeadDuration(),
                                                                            leadUnits );

        // Format the latest lead duration with the default format
        Number numericDurationUpper = DataUtilities.durationToNumericUnits( timeWindow.getLatestLeadDuration(),
                                                                            leadUnits );
        String leadDurationQualifier = numericDurationLower
                                       + "-"
                                       + numericDurationUpper;

        filename.add( leadDurationQualifier );
        filename.add( leadUnits.name()
                               .toUpperCase() );

        String extension = "";
        Outputs outputs = declaration.formats()
                                     .outputs();
        if ( outputs.hasNetcdf() || outputs.hasNetcdf2() )
        {
            extension = ".nc";
        }

        // Sanitize file name
        String safeName = URLEncoder.encode( filename.toString().replace( " ", "_" ) + extension,
                                             StandardCharsets.UTF_8 );

        return Paths.get( outputDirectory.toString(), safeName );
    }

    /**
     * Returns a scenario name to be used in naming a blob or null.
     *
     * @param declaration the declaration
     * @return an identifier or null
     */

    private String getScenarioNameForBlobOrNull( EvaluationDeclaration declaration )
    {
        String scenarioName = declaration.right()
                                         .label();
        if ( DeclarationUtilities.hasBaseline( declaration )
             && Boolean.TRUE.equals( declaration.baseline()
                                                .separateMetrics() ) )
        {
            scenarioName = null;
        }

        return scenarioName;
    }

    private String getTemplatePath()
    {
        String templatePath;

        if ( this.getNetcdfFormat()
                 .getTemplatePath()
                 .isBlank() )
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
            templatePath = this.getNetcdfFormat()
                               .getTemplatePath();
        }

        return templatePath;
    }

    /**
     * Creates a collection of {@link MetricVariable} for one time window.
     *
     * @param declaration the declaration
     * @param timeWindow the time windows
     * @param thresholds the thresholds
     * @param units the measurement units, if available
     * @param desiredTimeScale the desired time scale, if available
     * @return the metric variables
     */

    private Collection<MetricVariable> getMetricVariablesForOneTimeWindow( EvaluationDeclaration declaration,
                                                                           TimeWindowOuter timeWindow,
                                                                           Map<EnsembleAverageType, Map<MetricConstants, SortedSet<OneOrTwoThresholds>>> thresholds,
                                                                           String units,
                                                                           TimeScaleOuter desiredTimeScale )
    {
        Collection<MetricVariable> merged = new ArrayList<>();

        LOGGER.debug( "Creating metric variables for for time window {} using these thresholds by metric: {}.",
                      timeWindow,
                      thresholds );

        boolean hasBaseline = DeclarationUtilities.hasBaseline( declaration );

        // Uncover the covariates used for filtering only
        List<Covariate> covariates = MessageFactory.parse( declaration.covariates() );
        covariates = MessageUtilities.getCovariateFilters( covariates );

        // Iterate through the ensemble average types
        for ( Map.Entry<EnsembleAverageType, Map<MetricConstants, SortedSet<OneOrTwoThresholds>>> next : thresholds.entrySet() )
        {
            EnsembleAverageType nextType = next.getKey();
            Map<MetricConstants, SortedSet<OneOrTwoThresholds>> nextThresholdsByMetric = next.getValue();

            // Statistics for a separate baseline? If no, there's a single set of variables
            if ( !hasBaseline || Boolean.TRUE.equals( !declaration.baseline()
                                                                  .separateMetrics() ) )
            {
                MetricVariableParameters parameters = new MetricVariableParameters( timeWindow,
                                                                                    nextThresholdsByMetric,
                                                                                    units,
                                                                                    desiredTimeScale,
                                                                                    null,
                                                                                    hasBaseline,
                                                                                    nextType,
                                                                                    declaration.summaryStatistics(),
                                                                                    covariates );
                Collection<MetricVariable> variables = this.getMetricVariablesForOneTimeWindow( parameters );
                merged.addAll( variables );
            }
            else
            {
                // Two sets of variables, one for the right and one for the baseline with separate metrics.
                // For backwards compatibility, only clarify the baseline variable
                MetricVariableParameters parameters = new MetricVariableParameters( timeWindow,
                                                                                    nextThresholdsByMetric,
                                                                                    units,
                                                                                    desiredTimeScale,
                                                                                    null,
                                                                                    true,
                                                                                    nextType,
                                                                                    declaration.summaryStatistics(),
                                                                                    covariates );
                Collection<MetricVariable> right = this.getMetricVariablesForOneTimeWindow( parameters );

                MetricVariableParameters baselineParameters =
                        new MetricVariableParameters( timeWindow,
                                                      nextThresholdsByMetric,
                                                      units,
                                                      desiredTimeScale,
                                                      DatasetOrientation.BASELINE,
                                                      true,
                                                      nextType,
                                                      declaration.summaryStatistics(),
                                                      covariates );
                Collection<MetricVariable> baseline = this.getMetricVariablesForOneTimeWindow( baselineParameters );

                merged.addAll( right );
                merged.addAll( baseline );
            }
        }

        return Collections.unmodifiableCollection( merged );
    }

    /**
     * Creates a collection of {@link MetricVariable} for one time window.
     *
     * @param parameters the metric variable parameters
     * @return the metric variables
     */

    private Collection<MetricVariable> getMetricVariablesForOneTimeWindow( MetricVariableParameters parameters )
    {
        Map<MetricNames, SortedSet<OneOrTwoThresholds>> decomposed =
                this.decomposeThresholdsByMetricForBlobCreation( parameters.thresholds(),
                                                                 parameters.hasBaseline() );

        LOGGER.debug( "Discovered these thresholds by metric for blob creation and ensemble average type {}: {}.",
                      parameters.ensembleAverageType(),
                      decomposed );

        Collection<MetricVariable> returnMe = new ArrayList<>();

        // Context to append?
        String append = "";
        if ( Objects.nonNull( parameters.context() ) )
        {
            append = "_" + parameters.context()
                                     .name();
        }

        // Ensemble average type to append? For backwards compatibility, do not append the default value: #51670
        if ( Objects.nonNull( parameters.ensembleAverageType() )
             && parameters.ensembleAverageType() != EnsembleAverageType.MEAN )
        {
            append += "_ENSEMBLE_" + parameters.ensembleAverageType()
                                               .name();
        }

        // One variable for each combination of metric and threshold. 
        // When forming threshold names, thresholds should be mapped to all metrics.
        for ( Map.Entry<MetricNames, SortedSet<OneOrTwoThresholds>> nextEntry : decomposed.entrySet() )
        {
            MetricNames nextMetric = nextEntry.getKey();
            Set<OneOrTwoThresholds> nextThresholds = nextEntry.getValue();

            Map<OneOrTwoThresholds, String> nextMap = this.getStandardThresholdNames()
                                                          .get( nextMetric.metricName() );

            for ( OneOrTwoThresholds nextThreshold : nextThresholds )
            {
                String thresholdName = nextMap.get( nextThreshold );
                String variableName = nextMetric.metricName() + "_" + thresholdName + append;

                MetricVariable.Builder nextVariable =
                        new MetricVariable.Builder().setVariableName( variableName )
                                                    .setTimeWindow( parameters.timeWindow() )
                                                    .setMetricName( nextMetric )
                                                    .setThresholds( nextThreshold )
                                                    .setUnits( parameters.units() )
                                                    .setDesiredTimeScale( parameters.desiredTimeScale() )
                                                    .setDurationUnits( this.getDurationUnits() )
                                                    .setEnsembleAverageType( parameters.ensembleAverageType() )
                                                    .setCovariates( parameters.covariates() );

                Collection<MetricVariable> metricVariables =
                        this.getMetricVariableForEachMetricAndSummaryStatistic( nextVariable,
                                                                                parameters.summaryStatistics() );
                returnMe.addAll( metricVariables );
            }
        }

        return Collections.unmodifiableCollection( returnMe );
    }

    /**
     * Creates a {@link MetricVariable} for each summary statistic, else for the raw statistic.
     *
     * @param builder the base variable builder
     * @param summaryStatistics the summary statistics, possibly empty
     * @return the metric variables, one for each quantile or a single nominal value
     */

    private Collection<MetricVariable> getMetricVariableForEachMetricAndSummaryStatistic( MetricVariable.Builder builder,
                                                                                          Set<SummaryStatistic> summaryStatistics )
    {
        Collection<MetricVariable> metricVariables = new ArrayList<>();

        // Add the variable for the raw statistic
        MetricVariable nominal = builder.build();
        metricVariables.add( nominal );

        LOGGER.debug( CREATED_A_NEW_METRIC_VARIABLE_TO_POPULATE_WITH_NAME,
                      nominal.getName(),
                      nominal );

        MetricNames metricNames = nominal.getMetricName();
        MetricConstants metric;

        if ( Objects.nonNull( metricNames.component() ) )
        {
            metric = metricNames.component();
        }
        else
        {
            metric = metricNames.metric();
        }

        // Create a variable for each sample quantile, if this metric supports it
        if ( metric.isSamplingUncertaintyAllowed() )
        {
            Map<Double, String> quantileNames = this.getStandardQuantileNamesForSamplingUncertainty();
            for ( Map.Entry<Double, String> next : quantileNames.entrySet() )
            {
                Double quantile = next.getKey();
                String quantileName = next.getValue();

                String variableName = nominal.getName()
                                      + "_"
                                      + SummaryStatistic.StatisticDimension.RESAMPLED
                                      + "_"
                                      + SummaryStatistic.StatisticName.QUANTILE
                                      + "_"
                                      + quantileName;
                MetricVariable nextVariable = builder.setVariableName( variableName )
                                                     .setSampleQuantile( quantile )
                                                     .build();
                metricVariables.add( nextVariable );

                LOGGER.debug( CREATED_A_NEW_METRIC_VARIABLE_TO_POPULATE_WITH_NAME,
                              variableName,
                              nextVariable );
            }
        }

        // Create a metric variable for each summary statistic

        // Clear the builder for re-use
        if ( !summaryStatistics.isEmpty() )
        {
            builder.setVariableName( null )
                   .setSampleQuantile( null );

            // Only scores allowed in netcdf
            List<SummaryStatistic> filtered =
                    summaryStatistics.stream()
                                     .filter( m -> !MetricConstants.valueOf( m.getStatistic()
                                                                              .name() )
                                                                   .isInGroup( StatisticType.DIAGRAM ) )

                                     .toList();

            for ( SummaryStatistic summaryStatistic : filtered )
            {
                String variableName = nominal.getName()
                                      + "_"
                                      + summaryStatistic.getDimension()
                                      + "_"
                                      + summaryStatistic.getStatistic();
                if ( summaryStatistic.getStatistic() == SummaryStatistic.StatisticName.QUANTILE )
                {
                    Map<Double, String> quantileNames = this.getStandardQuantileNamesForSummaryStatistics();
                    String standardName = quantileNames.get( summaryStatistic.getProbability() );
                    variableName += "_"
                                    + standardName;
                }

                MetricVariable nextVariable = builder.setVariableName( variableName )
                                                     .setSummaryStatistic( summaryStatistic )
                                                     .build();

                LOGGER.debug( CREATED_A_NEW_METRIC_VARIABLE_TO_POPULATE_WITH_NAME,
                              variableName,
                              nextVariable );

                metricVariables.add( nextVariable );
            }
        }

        return Collections.unmodifiableCollection( metricVariables );
    }

    /**
     * Expands a set of thresholds by metric to include a separate mapping for each component part of a multi-part 
     * metric, because each part requires a separate variable in the netCDF.
     *
     * @param thresholdsByMetric the thresholds-by-metric to expand
     * @param hasBaseline is true if there is a baseline within the pairing
     * @return the expanded thresholds-by-metric
     */

    private Map<MetricNames, SortedSet<OneOrTwoThresholds>>
    decomposeThresholdsByMetricForBlobCreation( Map<MetricConstants, SortedSet<OneOrTwoThresholds>> thresholdsByMetric,
                                                boolean hasBaseline )
    {

        Map<MetricNames, SortedSet<OneOrTwoThresholds>> returnMe = new TreeMap<>();

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
            if ( components.size() > 1
                 && nextMetric != MetricConstants.SAMPLE_SIZE )
            {
                components.forEach( nextComponent -> returnMe.put( this.getMetricName( nextMetric, nextComponent ),
                                                                   nextThresholds ) );
            }
            else
            {
                MetricNames names = this.getMetricName( nextMetric, null );
                returnMe.put( names, nextThresholds );
            }
        }

        return Collections.unmodifiableMap( returnMe );
    }

    /**
     * Creates a {@link MetricNames} from the inputs.
     * @param metricName the metric name
     * @param componentName the metric component name
     * @return a name collection
     */
    private MetricNames getMetricName( MetricConstants metricName, MetricConstants componentName )
    {
        // Component present
        if ( Objects.nonNull( componentName ) )
        {
            return new MetricNames( metricName,
                                    componentName,
                                    metricName.name()
                                    + "_"
                                    + componentName.name() );
        }
        // A collected metric. Use the collection name and child name to form the metric name
        else if ( Objects.nonNull( metricName.getCollection() ) )
        {
            return new MetricNames( metricName, null, metricName.getCollection()
                                                                .name()
                                                      + "_"
                                                      + metricName.getChild()
                                                                  .name() );
        }
        // No metric component
        else
        {
            return new MetricNames( metricName, null, metricName.name() );
        }
    }

    private boolean isGridded()
    {
        return this.getNetcdfFormat()
                   .getGridded();
    }

    private Outputs.NetcdfFormat getNetcdfFormat()
    {
        return this.netcdfFormat;
    }

    private Path getOutputDirectory()
    {
        return this.outputDirectory;
    }

    /**
     * <p>Returns a map that contains the union of thresholds across all features and metrics for which blobs should be
     * created. The goal is to identify the thresholds whose statistics should be recorded in the same blob versus
     * different blobs when the blob includes multiple features. The attribute of a threshold that determines whether
     * it is distinct from other thresholds is (in this order):
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
     * <p>Removes any metrics that do not produce {@link MetricConstants.StatisticType#DOUBLE_SCORE} or
     * {@link MetricConstants.StatisticType#DURATION_SCORE} because this writer currently only handles scores.
     *
     * @param metricsAndThresholds the thresholds to search
     * @return the unique thresholds for which blobs should be created
     */

    private Map<MetricConstants, SortedSet<OneOrTwoThresholds>> getUniqueThresholdsForScoreMetrics( List<MetricsAndThresholds> metricsAndThresholds )
    {
        Objects.requireNonNull( metricsAndThresholds );

        Comparator<OneOrTwoThresholds> thresholdComparator = ThresholdSlicer.getLogicalThresholdComparator();

        // Create a set of thresholds for each metric in a map.        
        Map<MetricConstants, SortedSet<OneOrTwoThresholds>> thresholdsMap = new EnumMap<>( MetricConstants.class );

        for ( MetricsAndThresholds thresholds : metricsAndThresholds )
        {
            Set<MetricConstants> metrics = thresholds.metrics();
            Map<FeatureTuple, Set<ThresholdOuter>> thresholdsByFeature = thresholds.thresholds();
            for ( Map.Entry<FeatureTuple, Set<ThresholdOuter>> next : thresholdsByFeature.entrySet() )
            {
                Set<ThresholdOuter> nextThresholds = next.getValue();
                Map<MetricConstants, SortedSet<OneOrTwoThresholds>> composed =
                        ThresholdSlicer.getOneOrTwoThresholds( metrics, nextThresholds );
                for ( Map.Entry<MetricConstants, SortedSet<OneOrTwoThresholds>> nextEntry : composed.entrySet() )
                {
                    MetricConstants nextMetric = nextEntry.getKey();

                    // Only allow scores. Duration scores must be written as double scores at write time
                    if ( nextMetric.isInGroup( StatisticType.DOUBLE_SCORE )
                         || nextMetric.isInGroup( StatisticType.DURATION_SCORE ) )
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

        return Collections.unmodifiableMap( thresholdsMap );
    }

    /**
     * @return the standard quantile names for sampling uncertainty, if any
     */
    private Map<Double, String> getStandardQuantileNamesForSamplingUncertainty()
    {
        return this.standardQuantileNamesForSamplingUncertainty;
    }

    /**
     * @return the standard quantile names for summary statistics that do not involve sampling uncertainty, if any
     */
    private Map<Double, String> getStandardQuantileNamesForSummaryStatistics()
    {
        return this.standardQuantileNamesForSummaryStatistics;
    }

    /**
     * Metadata about a NetCDF value.
     * @param variableName
     * @param origin
     * @param value
     */
    private record NetcdfValueKey( String variableName, int[] origin, double value )
    {
        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || this.getClass() != o.getClass() )
            {
                return false;
            }
            NetcdfValueKey in = ( NetcdfValueKey ) o;
            return Objects.equals( this.variableName(), in.variableName() )
                   && Arrays.equals( this.origin(), in.origin() ) && Precision.equals( this.value(), in.value() );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( this.variableName(), Arrays.hashCode( this.origin() ), this.value() );
        }

        @Override
        public String toString()
        {
            return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                    .append( "variableName", this.variableName() )
                    .append( "origin", this.origin() )
                    .append( "value", this.value() )
                    .toString();
        }
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
        public static final String AND_THRESHOLD = " and threshold ";
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
            // This now needs to somehow get all metadata for all metrics
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
            Pool pool = score.getPoolMetadata()
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

                double actualValue = componentScore.getStatistic()
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
            Pool pool = score.getPoolMetadata()
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

            double actualValue = componentScore.getStatistic()
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
                // Open a writer to write to the path. Must be closed when closing the overall NetcdfOutputWriter instance
                if ( Objects.isNull( this.writer ) )
                {
                    this.writer = NetcdfFormatWriter.openExisting( this.outputPath )
                                                    .build();

                    LOGGER.trace( "Opened an underlying netcdf writer {} for pool {}.", this.writer, this.timeWindow );
                }

                for ( NetcdfValueKey key : this.valuesToSave )
                {
                    Array netcdfValue;
                    Index ima;
                    int[] shape = new int[key.origin().length];
                    Arrays.fill( shape, 1 );

                    if ( !this.isDeprecatedWriter )
                    {
                        netcdfValue = Array.factory( DataType.DOUBLE, shape );
                        ima = netcdfValue.getIndex();
                        double value = key.value();

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
                        netcdfValue.setFloat( ima, ( float ) key.value() );
                    }

                    try
                    {
                        this.writer.write( key.variableName(), key.origin(), netcdfValue );
                    }
                    catch ( NullPointerException | IOException | InvalidRangeException e )
                    {
                        String exceptionMessage = "While attempting to write data value "
                                                  + key.value()
                                                  + " with variable name "
                                                  + key.variableName()
                                                  + " to index "
                                                  + Arrays.toString( key.origin() )
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
            PoolMetadata poolMetadata = score.getPoolMetadata();

            // Locating the variable relies on the use of a naming convention that matches the naming convention at blob
            // creation time. 
            // TODO: use a common code pathway to generate the name at these two times or, better still, create blobs 
            // on-demand when the first statistic arrives. This is not currently possible with Netcdf 3 and/or the UCAR
            // Java library.

            // Find the metric name
            MetricConstants metricComponentName =
                    MetricConstants.valueOf( score.getStatistic()
                                                  .getMetric()
                                                  .getName()
                                                  .name() );

            String metricNameString = metricName.name();
            if ( metricComponentName != MetricConstants.MAIN )
            {
                metricNameString = metricNameString + "_" + metricComponentName.name();
            }

            String append = "";
            if ( poolMetadata.getPool().getIsBaselinePool() )
            {
                append = "_" + DatasetOrientation.BASELINE.name();
            }

            // Add the ensemble average type, where applicable
            wres.statistics.generated.Pool.EnsembleAverageType ensembleAverageType = poolMetadata.getPool()
                                                                                                 .getEnsembleAverageType();
            if ( ensembleAverageType != wres.statistics.generated.Pool.EnsembleAverageType.MEAN
                 && ensembleAverageType != wres.statistics.generated.Pool.EnsembleAverageType.NONE )
            {
                append += "_ENSEMBLE_" + ensembleAverageType.name();
            }

            append += this.getSummaryStatisticNameQualifier( poolMetadata, metricNameString, score );

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
                                                 + AND_THRESHOLD
                                                 + poolMetadata.getThresholds()
                                                 + ", failed to locate the variable name in the map of metric "
                                                 + "variables. The map contains the following metric variable names: "
                                                 + this.outputWriter.standardThresholdNames.keySet()
                                                 + ". The sample metadata of the statistic that could not be written "
                                                 + "is: "
                                                 + poolMetadata );
            }

            for ( Map.Entry<OneOrTwoThresholds, String> nextThreshold : thresholdMap.entrySet() )
            {
                String nextName = nextThreshold.getValue();
                OneOrTwoThresholds thresholdFromArchive = nextThreshold.getKey();
                OneOrTwoThresholds thresholdFromScore = score.getPoolMetadata()
                                                             .getThresholds();

                // Second threshold is always a decision threshold and can be compared directly
                boolean secondThresholdIsEqual =
                        Objects.equals( thresholdFromArchive.second(), thresholdFromScore.second() );

                String name = this.getVariableNameOrNull( thresholdFromScore,
                                                          thresholdFromArchive,
                                                          nextName,
                                                          poolMetadata,
                                                          metricNameString,
                                                          append,
                                                          secondThresholdIsEqual );

                // Name discovered? return it
                if ( Objects.nonNull( name ) )
                {
                    return name;
                }
            }

            throw new IllegalStateException( "While attempting to write statistics to netcdf for the metric variable "
                                             + metricNameString
                                             + AND_THRESHOLD
                                             + poolMetadata.getThresholds()
                                             + ", discovered the variable name "
                                             + metricNameString
                                             + ", but failed to discover the standard threshold name within the map of "
                                             + "standard names. The map contained the following entries: "
                                             + thresholdMap
                                             + ". The sample metadata of the statistic that could not be written is: "
                                             + poolMetadata );
        }

        /**
         * Returns the summary statistic name qualifier when a summary statistic is present.
         *
         * @param poolMetadata the pool metadata
         * @param metricNameString the metric name string
         * @param score the score
         * @return the summary statistic name qualifier
         */

        private String getSummaryStatisticNameQualifier( PoolMetadata poolMetadata,
                                                         String metricNameString,
                                                         DoubleScoreComponentOuter score )
        {
            String qualifier = "";

            // Add the sample quantile using the standard name, where applicable
            if ( score.isSummaryStatistic() )
            {
                SummaryStatistic summaryStatistic = score.getSummaryStatistic();
                qualifier = "_"
                            + summaryStatistic.getDimension()
                            + "_"
                            + summaryStatistic.getStatistic();

                if ( summaryStatistic.getStatistic() == SummaryStatistic.StatisticName.QUANTILE )
                {

                    double quantile = score.getSummaryStatistic()
                                           .getProbability();
                    Map<Double, String> quantileNames;

                    if ( summaryStatistic.getDimension() == SummaryStatistic.StatisticDimension.RESAMPLED )
                    {
                        quantileNames = this.outputWriter.getStandardQuantileNamesForSamplingUncertainty();
                    }
                    else
                    {
                        quantileNames = this.outputWriter.getStandardQuantileNamesForSummaryStatistics();
                    }

                    String quantileName = quantileNames.get( quantile );

                    // Qualifier for the sample quantile not found?
                    if ( Objects.isNull( quantileName ) )
                    {
                        throw new IllegalStateException(
                                "While attempting to write statistics to netcdf for the metric "
                                + "variable "
                                + metricNameString
                                + AND_THRESHOLD
                                + poolMetadata.getThresholds()
                                + ", failed to locate the quantile name corresponding to the "
                                + "quantile: "
                                + quantile
                                + ". The sample metadata of the statistic that could not "
                                + "be written is: "
                                + poolMetadata
                                + ". The available quantile names and associated quantiles are: "
                                + quantileNames
                                + "." );
                    }

                    qualifier += "_" + quantileName;
                }
            }

            return qualifier;
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
         * @param secondThresholdIsEqual whether the second threshold is equal to the first
         * @return the variable name or null
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
            if ( thresholdFromScore.first()
                                   .hasLabel() && thresholdFromArchive.first()
                                                                      .hasLabel() )
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
                String wkt = location.getWkt();
                if ( !wkt.isBlank() )
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

                Coordinate point = DataUtilities.getLonLatFromPointWkt( wkt );

                // contains the the y index and the x index
                origin = new int[2];

                // TODO: Find a different approach to handle grids without a coordinate system
                try ( GridDataset gridDataset = GridDataset.open( this.outputPath ) )
                {
                    GridDatatype variable = gridDataset.findGridDatatype( name );
                    int[] xyIndex = variable.getCoordinateSystem()
                                            .findXYindexFromLatLon( point.getY(),
                                                                    point.getX(),
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
                Variable coordinate;

                if ( this.isDeprecatedWriter )
                {
                    // TODO remove this whole block
                    String nameToUse = this.outputWriter.getNetcdfFormat()
                                                        .getVariableName();

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
                                                           + ". The available names were: "
                                                           + this.vectorCoordinatesMap.keySet() );
                }
            }
            // Comid is the glue
            else
            {
                long coordinate;

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

                if ( !this.vectorCoordinatesMap.containsKey( ( int ) coordinate ) )
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
                        throw new IOException( "Lingering Netcdf results could not be written to disk.", e );
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

    /**
     * Small value class for metric variable creation.
     * @param timeWindow the time windows
     * @param thresholds the thresholds
     * @param units the measurement units, if available
     * @param desiredTimeScale the desired time scale, if available
     * @param context optional context for the variable
     * @param hasBaseline is true if a baseline is declared
     * @param ensembleAverageType the ensemble average type
     * @param summaryStatistics the summary statistics, possibly empty
     * @param covariates the covariates, possibly empty
     * @author James Brown
     */
    private record MetricVariableParameters( TimeWindowOuter timeWindow,
                                             Map<MetricConstants, SortedSet<OneOrTwoThresholds>> thresholds,
                                             String units,
                                             TimeScaleOuter desiredTimeScale,
                                             DatasetOrientation context,
                                             boolean hasBaseline,
                                             EnsembleAverageType ensembleAverageType,
                                             Set<SummaryStatistic> summaryStatistics,
                                             List<Covariate> covariates )
    {
    }
}