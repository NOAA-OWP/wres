package wres.io.writing.netcdf;

import java.io.Closeable;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.Index;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.NetcdfFiles;
import ucar.nc2.Variable;
import ucar.nc2.dt.GridDatatype;
import ucar.nc2.dt.grid.GridDataset;
import wres.config.ProjectConfigs;
import wres.config.generated.*;
import wres.config.generated.ProjectConfig.Inputs;
import wres.datamodel.DatasetIdentifier;
import wres.datamodel.FeatureTuple;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricGroup;
import wres.datamodel.FeatureKey;
import wres.datamodel.MissingValues;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter.DoubleScoreComponentOuter;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.datamodel.time.TimeWindowOuter;
import wres.datamodel.time.generators.TimeWindowGenerator;
import wres.io.concurrency.Executor;
import wres.io.config.ConfigHelper;
import wres.io.writing.WriteException;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryTuple;
import wres.system.SystemSettings;
import wres.util.FutureQueue;
import wres.util.Strings;

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

public class NetcdfOutputWriter implements NetcdfWriter<DoubleScoreStatisticOuter>,
        Closeable
{
    private static final Logger LOGGER = LoggerFactory.getLogger( NetcdfOutputWriter.class );

    private static final String DEFAULT_VECTOR_TEMPLATE = "vector_template.nc";
    private static final String DEFAULT_GRID_TEMPLATE = "lcc_grid_template.nc";
    private static final int VALUE_SAVE_LIMIT = 500;

    // TODO: it is very unlikely that classloading datetime should be used here.
    private static final ZonedDateTime ANALYSIS_TIME = ZonedDateTime.now( ZoneId.of( "UTC" ) );

    private final Executor executor;
    private final Object windowLock = new Object();

    private final DestinationConfig destinationConfig;
    private final Path outputDirectory;
    private NetcdfType netcdfConfiguration;

    // Guarded by windowLock
    private final Map<TimeWindowOuter, TimeWindowWriter> writersMap = new HashMap<>();

    /**
     * Default resolution for writing duration outputs. To change the resolution, change this default.
     */

    private final ChronoUnit durationUnits;

    /**
     * Set of paths that this writer actually wrote to
     * Guarded by windowLock
     */
    private final Set<Path> pathsWrittenTo;

    /**
     * Writing tasks submitted
     * Guarded by windowLock
     */
    private final List<Future<Set<Path>>> writingTasksSubmitted = new ArrayList<>();

    /**
     * Project declaration.
     */

    private final ProjectConfig projectConfig;

    /**
     * Records whether the writer is ready to write. It is ready when all blobs have been created.
     */

    private final AtomicBoolean isReadyToWrite;

    /**
     * Mapping between standard threshold names and representative thresholds for those standard names. This is used
     * to help determine the threshold portion of a variable name to which a statistic corresponds, based on the 
     * standard name of a threshold chosen at blob creation time. There is a separate group for each metric.
     */

    private Map<String, Map<String, OneOrTwoThresholds>> standardThresholdNames = new HashMap<>();

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
     * @param executor The executor to use.
     * @param projectConfig the project configuration
     * @param destinationDeclaration the destination declaration
     * @param durationUnits the time units for durations
     * @param outputDirectory the directory into which to write
     * @param deprecatedVersion True if using deprecated code, false otherwise
     * @return an instance of the writer
     * @throws WriteException if the blobs could not be created for any reason
     */

    public static NetcdfOutputWriter of( SystemSettings systemSettings,
                                         Executor executor,
                                         ProjectConfig projectConfig,
                                         DestinationConfig destinationDeclaration,
                                         ChronoUnit durationUnits,
                                         Path outputDirectory,
                                         boolean deprecatedVersion )
    {
        return new NetcdfOutputWriter( systemSettings,
                                       executor,
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
                                Executor executor,
                                ProjectConfig projectConfig,
                                DestinationConfig destinationDeclaration,
                                ChronoUnit durationUnits,
                                Path outputDirectory,
                                boolean deprecatedVersion )
    {
        Objects.requireNonNull( systemSettings );
        Objects.requireNonNull( executor );
        Objects.requireNonNull( projectConfig, "Specify non-null project config." );
        Objects.requireNonNull( durationUnits, "Specify non-null duration units." );
        Objects.requireNonNull( outputDirectory, "Specify non-null output directory." );
        Objects.requireNonNull( destinationDeclaration );
        LOGGER.debug( "Created NetcdfOutputWriter {}", this );
        this.executor = executor;
        this.destinationConfig = destinationDeclaration;
        this.netcdfConfiguration = this.destinationConfig.getNetcdf();
        this.durationUnits = durationUnits;
        this.outputDirectory = outputDirectory;
        this.projectConfig = projectConfig;
        this.pathsWrittenTo = new TreeSet<>();
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

    private Executor getExecutor()
    {
        return this.executor;
    }

    /**
     * Creates the blobs into which outputs will be written.
     *
     * @param features The super-set of features used in the evaluation.
     * @param thresholds Thresholds imposed upon input data
     * @throws WriteException if the blobs have already been created
     * @throws IOException if the blobs could not be created for any reason
     */

    public void createBlobsForWriting( Set<FeatureTuple> features,
                                       Map<FeatureTuple, ThresholdsByMetric> thresholds )
            throws IOException
    {
        Objects.requireNonNull( thresholds );

        if ( this.getIsReadyToWrite().get() )
        {
            throw new WriteException( "The netcdf blobs have already been created." );
        }

        // Time windows
        PairConfig pairConfig = this.getProjectConfig()
                                    .getPair();
        Set<TimeWindowOuter> timeWindows = TimeWindowGenerator.getTimeWindowsFromPairConfig( pairConfig );

        // Find the thresholds-by-metric for which blobs should be created
        ThresholdsByMetric thresholdsToWrite = this.getUniqueThresholds( thresholds );
        
        // Should be at least one metric with at least one threshold
        if( thresholdsToWrite.hasThresholdsForTheseMetrics().isEmpty() )
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

        // Create blobs from components
        synchronized ( this.windowLock )
        {
            Set<Path> pathsCreated = this.createBlobsAndBlobWriters( this.getProjectConfig()
                                                                         .getInputs(),
                                                                     features,
                                                                     timeWindows,
                                                                     thresholdsToWrite,
                                                                     units,
                                                                     desiredTimeScale,
                                                                     this.deprecatedVersion );
            this.pathsWrittenTo.addAll( pathsCreated );

            // Flag ready
            this.getIsReadyToWrite()
                .set( true );
        }

        LOGGER.debug( "Created the following netcdf paths for writing: {}.", this.getPathsWrittenTo() );
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
     * Creates the blobs into which outputs will be written.
     * 
     * @param inputs the inputs declaration
     * @param features The super-set of features used in this evaluation.
     * @param timeWindows the time windows
     * @param thresholds the thresholds
     * @param units the measurement units, if available
     * @param desiredTimeScale the desired time scale, if available
     * @param deprecatedVersion True when using deprecated code, false otherwise
     * @throws IOException if the blobs could not be created for any reason
     * @return the paths written
     */

    private Set<Path> createBlobsAndBlobWriters( Inputs inputs,
                                                 Set<FeatureTuple> features,
                                                 Set<TimeWindowOuter> timeWindows,
                                                 ThresholdsByMetric thresholds,
                                                 String units,
                                                 TimeScaleOuter desiredTimeScale,
                                                 boolean deprecatedVersion )
            throws IOException
    {
        Set<Path> returnMe = new TreeSet<>();

        // One blob and blob writer per time window      
        for ( TimeWindowOuter nextWindow : timeWindows )
        {

            Collection<MetricVariable> variables = this.getMetricVariablesForOneTimeWindow( inputs,
                                                                                            nextWindow,
                                                                                            thresholds,
                                                                                            units,
                                                                                            desiredTimeScale );

            // Create the blob path
            Path targetPath = ConfigHelper.getOutputPathToWriteForOneTimeWindow( this.getOutputDirectory(),
                                                                                 this.getDestinationConfig(),
                                                                                 this.getIdentifierForBlob( inputs ),
                                                                                 nextWindow,
                                                                                 this.getDurationUnits() );

            String pathActuallyWritten;

            if ( !deprecatedVersion )
            {
                // Create the blob
                pathActuallyWritten =
                        NetcdfOutputFileCreator2.create( this.getProjectConfig(),
                                                         targetPath,
                                                         features,
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
                                                        destinationConfig,
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
     * Returns an identifier to be used in naming a blob.
     * 
     * @param inputs the inputs declaration
     * @return an identifier
     */

    private DatasetIdentifier getIdentifierForBlob( Inputs inputs )
    {
        // Dataset identifier, without a feature/location identifier
        // Use the main variable identifier in case there is a different one for the baseline
        String variableId = ProjectConfigs.getVariableIdFromProjectConfig( inputs, false );
        // Use the scenarioId for the right, unless there is a baseline that requires separate metrics
        // in which case, do not use a scenarioId
        String scenarioId = inputs.getRight().getLabel();
        if ( Objects.nonNull( inputs.getBaseline() ) && inputs.getBaseline().isSeparateMetrics() )
        {
            scenarioId = null;
        }

        return DatasetIdentifier.of( null, variableId, scenarioId );
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
                                                                           ThresholdsByMetric thresholds,
                                                                           String units,
                                                                           TimeScaleOuter desiredTimeScale )
    {
        // Statistics for a separate baseline? If no, there's a single set of variables
        if ( Objects.isNull( inputs.getBaseline() ) || !inputs.getBaseline().isSeparateMetrics() )
        {
            return this.getMetricVariablesForOneTimeWindow( timeWindow,
                                                            thresholds,
                                                            units,
                                                            desiredTimeScale,
                                                            null,
                                                            Objects.nonNull( inputs.getBaseline() ) );
        }

        // Two sets of variables, one for the right and one for the baseline with separate metrics.
        // For backwards compatibility, only clarify the baseline variable
        Collection<MetricVariable> right = this.getMetricVariablesForOneTimeWindow( timeWindow,
                                                                                    thresholds,
                                                                                    units,
                                                                                    desiredTimeScale,
                                                                                    null,
                                                                                    Objects.nonNull( inputs.getBaseline() ) );

        Collection<MetricVariable> baseline = this.getMetricVariablesForOneTimeWindow( timeWindow,
                                                                                       thresholds,
                                                                                       units,
                                                                                       desiredTimeScale,
                                                                                       LeftOrRightOrBaseline.BASELINE,
                                                                                       Objects.nonNull( inputs.getBaseline() ) );

        Collection<MetricVariable> merged = new ArrayList<>( right );
        merged.addAll( baseline );

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
     * @return the metric variables
     */

    private Collection<MetricVariable> getMetricVariablesForOneTimeWindow( TimeWindowOuter timeWindow,
                                                                           ThresholdsByMetric thresholds,
                                                                           String units,
                                                                           TimeScaleOuter desiredTimeScale,
                                                                           LeftOrRightOrBaseline context,
                                                                           boolean hasBaseline )
    {

        Map<MetricConstants, SortedSet<OneOrTwoThresholds>> thresholdMap = thresholds.getOneOrTwoThresholds();

        Map<String, SortedSet<OneOrTwoThresholds>> decomposed =
                this.decomposeThresholdsByMetricForBlobCreation( thresholdMap, hasBaseline );

        Collection<MetricVariable> returnMe = new ArrayList<>();

        // Context to append?
        String append = "";
        if ( Objects.nonNull( context ) )
        {
            append = "_" + context.name();
        }

        // One variable for each combination of metric and threshold
        for ( Map.Entry<String, SortedSet<OneOrTwoThresholds>> nextEntry : decomposed.entrySet() )
        {
            String nextMetric = nextEntry.getKey();
            Set<OneOrTwoThresholds> nextThresholds = nextEntry.getValue();
            int thresholdNumber = 1;

            Map<String, OneOrTwoThresholds> nextMap = this.standardThresholdNames.get( nextMetric );
            if ( Objects.isNull( nextMap ) )
            {
                nextMap = new HashMap<>();
                this.standardThresholdNames.put( nextMetric, nextMap );
            }

            for ( OneOrTwoThresholds nextThreshold : nextThresholds )
            {
                String thresholdName = "THRESHOLD_" + thresholdNumber;

                // Add to the cache of standard threshold names
                nextMap.put( thresholdName, nextThreshold );

                String variableName = nextMetric + "_" + thresholdName + append;

                MetricVariable nextVariable = new MetricVariable( variableName,
                                                                  timeWindow,
                                                                  nextMetric,
                                                                  nextThreshold,
                                                                  units,
                                                                  desiredTimeScale,
                                                                  this.getDurationUnits() );
                returnMe.add( nextVariable );
                thresholdNumber++;
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
            throw new WriteException( "This netcdf output writer is not ready for writing. The blobs must be "
                                      + "created first. The caller has made an error by asking the writer to accept statistics "
                                      + "before calling createBlobsForWriting." );
        }

        LOGGER.debug( "NetcdfOutputWriter {} accepted output {}.", this, output );

        Map<TimeWindowOuter, List<DoubleScoreStatisticOuter>> outputByTimeWindow = wres.util.Collections.group(
                                                                                                                output,
                                                                                                                score -> score.getMetadata()
                                                                                                                              .getTimeWindow() );

        for ( Map.Entry<TimeWindowOuter, List<DoubleScoreStatisticOuter>> entries : outputByTimeWindow.entrySet() )
        {
            TimeWindowOuter timeWindow = entries.getKey();
            List<DoubleScoreStatisticOuter> scores = entries.getValue();

            synchronized ( this.windowLock )
            {
                Callable<Set<Path>> writerTask = new Callable<Set<Path>>()
                {
                    @Override
                    public Set<Path> call() throws IOException, InvalidRangeException, CoordinateNotFoundException
                    {
                        Set<Path> pathsWritten = new HashSet<>( 1 );
                        NetcdfOutputWriter.TimeWindowWriter writer = writersMap.get( this.window );
                        writer.write( this.output );
                        Path pathWritten = Paths.get( writer.outputPath );
                        pathsWritten.add( pathWritten );
                        return Collections.unmodifiableSet( pathsWritten );
                    }

                    Callable<Set<Path>> initialize( final TimeWindowOuter window,
                                                    final List<DoubleScoreStatisticOuter> scores )
                    {
                        this.output = scores;
                        this.window = window;
                        return this;
                    }

                    private List<DoubleScoreStatisticOuter> output;
                    private TimeWindowOuter window;
                }.initialize( timeWindow, scores );

                LOGGER.debug( "Submitting a task to write to a netcdf file." );
                Executor writerExecutor = this.getExecutor();
                Future<Set<Path>> taskFuture = writerExecutor.submit( writerTask );
                this.writingTasksSubmitted.add( taskFuture );
            }
        }

        return this.getPathsWrittenTo();
    }

    @Override
    public void close()
    {

        LOGGER.debug( "About to wait for writing tasks to finish from {}", this );

        synchronized ( this.windowLock )
        {
            try
            {
                // Complete outstanding tasks
                for ( Future<Set<Path>> writingTaskResult : this.writingTasksSubmitted )
                {
                    writingTaskResult.get();
                }
            }
            catch ( InterruptedException ie )
            {
                LOGGER.warn( "Interrupted while completing a netcdf writing task.", ie );

                Thread.currentThread().interrupt();
            }
            catch ( ExecutionException ee )
            {
                String message = "Failed to complete a netcdf writing task for " + this.destinationConfig;

                throw new WriteException( message, ee );
            }

            LOGGER.debug( "About to close writers from {}", this );

            if ( this.writersMap.isEmpty() )
            {
                return;
            }

            ExecutorService closeExecutor = null;

            try
            {
                closeExecutor = Executors.newFixedThreadPool( this.writersMap.size() );

                FutureQueue<Object> closeQueue = new FutureQueue<>( 3000, TimeUnit.MILLISECONDS );

                try
                {
                    for ( TimeWindowWriter writer : this.writersMap.values() )
                    {
                        Callable<Object> closeTask = new Callable<Object>()
                        {
                            @Override
                            public Object call() throws IOException
                            {
                                try
                                {
                                    LOGGER.debug( "Calling writer.close on {}", writer );
                                    writer.close();
                                }
                                catch ( IOException ioe )
                                {
                                    throw new IOException( "The writer for " + writer.toString()
                                                           + " could not be closed.",
                                                           ioe );
                                }
                                return null;
                            }

                            Callable<Object> initialize( TimeWindowWriter writer )
                            {
                                this.writer = writer;
                                return this;
                            }

                            private TimeWindowWriter writer;
                        }.initialize( writer );
                        closeQueue.add( closeExecutor.submit( closeTask ) );
                    }

                    closeQueue.loop();
                }
                catch ( ExecutionException e )
                {
                    throw new WriteException(
                                              "A netCDF output could not be written",
                                              e );
                }
            }
            finally
            {
                if ( closeExecutor != null && !closeExecutor.isShutdown() )
                {
                    closeExecutor.shutdown();
                }
            }
        }

        LOGGER.debug( "Closed writers from {}", this );

    }

    /**
     * Return a snapshot of the paths written to (so far)
     */

    private Set<Path> getPathsWrittenTo()
    {
        LOGGER.debug( "getPathsWrittenTo from NetcdfOutputWriter {}: {}",
                      this,
                      this.pathsWrittenTo );
        return Collections.unmodifiableSet( this.pathsWrittenTo );
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
     * <p> See #85491. It is essential that the logic for creating blobs in this method is mirrored by the logic for
     * finding blobs in {@link TimeWindowWriter#getVariableName(MetricConstants, DoubleScoreComponentOuter)}.
     * 
     * @param thresholds the thresholds to search
     * @return the unique thresholds for which blobs should be created
     */

    private ThresholdsByMetric getUniqueThresholds( Map<FeatureTuple, ThresholdsByMetric> thresholds )
    {
        Objects.requireNonNull( thresholds );

        // The metrics are constant across all features because metrics cannot be declared per feature. However, the
        // thresholds can vary across features. 

        // First, create a comparator that compares two threshold tuples.
        Comparator<OneOrTwoThresholds> thresholdComparator = ( OneOrTwoThresholds one, OneOrTwoThresholds another ) -> {

            // Compare the second/decision threshold first, which is compared on all content if it exists.
            int compare = Objects.compare( one.second(),
                                           another.second(),
                                           Comparator.nullsFirst( Comparator.naturalOrder() ) );

            if ( compare != 0 )
            {
                return compare;
            }

            // Compare the first threshold by label if both have a label. Thresholds with a label have the same meaning
            // across features, so statistics should be stored accordingly.
            if ( one.first().hasLabel() && another.first().hasLabel() )
            {
                return Objects.compare( one.first().getLabel(),
                                        another.first().getLabel(),
                                        Comparator.naturalOrder() );
            }

            // Compare by probability threshold if both are probability thresholds. Thresholds that are probability 
            // thresholds have the same meaning across features, so should be stored accordingly.
            if ( one.first().hasProbabilities() && another.first().hasProbabilities() )
            {
                return Objects.compare( one.first().getProbabilities(),
                                        another.first().getProbabilities(),
                                        Comparator.naturalOrder() );
            }

            // Resort to a full comparison.           
            return Objects.compare( one,
                                    another,
                                    Comparator.nullsFirst( Comparator.naturalOrder() ) );
        };

        // Create a set of thresholds for each metric in a map.        
        Map<MetricConstants, SortedSet<OneOrTwoThresholds>> thresholdsMap = new EnumMap<>( MetricConstants.class );

        for ( ThresholdsByMetric next : thresholds.values() )
        {
            Map<MetricConstants, SortedSet<OneOrTwoThresholds>> nextMapping = next.getOneOrTwoThresholds();

            for ( Map.Entry<MetricConstants, SortedSet<OneOrTwoThresholds>> nextEntry : nextMapping.entrySet() )
            {
                MetricConstants nextMetric = nextEntry.getKey();

                // Get the existing mapping or a new sorted set instantiated with the threshold comparator
                SortedSet<OneOrTwoThresholds> mapped =
                        thresholdsMap.getOrDefault( nextMetric,
                                                    new TreeSet<>( thresholdComparator ) );

                mapped.addAll( nextEntry.getValue() );

                // Add it to the map if not already there
                if ( !thresholdsMap.containsKey( nextMetric ) )
                {
                    thresholdsMap.put( nextMetric, mapped );
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
        private final ReentrantLock writeLock;
        private final boolean isDeprecatedWriter;

        /**
         * A writer to be opened on first write, closed when the {@link NetcdfOutputWriter} that encloses this
         * {@link TimeWindowWriter} is closed.
         */
        private NetcdfFileWriter writer;

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
                throws IOException, InvalidRangeException, CoordinateNotFoundException
        {
            //this now needs to somehow get all metadata for all metrics
            // Ensure that the output file exists
            for ( DoubleScoreStatisticOuter score : scores )
            {
                Set<MetricConstants> components = score.getComponents();

                for ( MetricConstants nextComponent : components )
                {
                    DoubleScoreComponentOuter componentScore = score.getComponent( nextComponent );

                    String name = this.getVariableName( score.getMetricName(), componentScore );

                    // Figure out the location of all values and build the origin in each variable grid
                    GeometryTuple location = score.getMetadata()
                                                  .getPool()
                                                  .getGeometryTuplesList()
                                                  .get( 0 );

                    int[] origin;

                    try
                    {
                        if ( !this.isDeprecatedWriter )
                        {
                            origin = this.getOrigin( location );
                        }
                        else
                        {
                            // TODO remove this block, remove if/else
                            origin = this.getOrigin( name, location );
                        }
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

                    if ( !isDeprecatedWriter )
                    {
                        if ( actualValue == MissingValues.DOUBLE
                             || ( Double.isNaN( MissingValues.DOUBLE )
                                  && Double.isNaN( actualValue ) ) )
                        {
                            actualValue = NetcdfOutputFileCreator2.DOUBLE_FILL_VALUE;
                        }
                    }

                    LOGGER.trace( "Actual value found for {}: {}",
                                  componentScore.getMetricName(),
                                  actualValue );
                    this.saveValues( name, origin, actualValue );
                }
            }
        }

        private void writeMetricResults() throws IOException, InvalidRangeException
        {
            this.writeLock.lock();

            Array netcdfValue;
            Index ima;

            // Open a writer to write to the path. Must be closed when closing the overall NetcdfOutputWriter instance
            if ( Objects.isNull( this.writer ) )
            {
                this.writer = NetcdfFileWriter.openExisting( this.outputPath );

                LOGGER.trace( "Opened an underlying netcdf writer {} for pool {}.", this.writer, this.timeWindow );
            }

            try
            {
                for ( NetcdfValueKey key : this.valuesToSave )
                {
                    int[] shape = new int[key.getOrigin().length];
                    Arrays.fill( shape, 1 );

                    if ( !this.isDeprecatedWriter )
                    {
                        netcdfValue = Array.factory( DataType.DOUBLE, shape );
                        ima = netcdfValue.getIndex();
                        double value = key.getValue();

                        if ( value == MissingValues.DOUBLE
                             || ( Double.isNaN( MissingValues.DOUBLE )
                                  && Double.isNaN( value ) ) )
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
                        netcdfValue.setFloat( ima, (float) key.getValue() );
                    }

                    try
                    {
                        writer.write( key.getVariableName(), key.getOrigin(), netcdfValue );
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

                writer.flush();

                this.valuesToSave.clear();
            }
            finally
            {
                if ( this.writeLock.isHeldByCurrentThread() )
                {
                    this.writeLock.unlock();
                }
            }
        }

        /**
         * Attempts to find the standard metric-threshold name of a variable within the netCDF blob that corresponding to 
         * the score metadata. Attempts to use the threshold name to locate the threshold. Otherwise, uses the natural 
         * order of the thresholds.
         * 
         * @param metricName the metric name
         * @param score the score whose metric-threshold standard name is required
         * @return the standard name
         */

        private String getVariableName( MetricConstants metricName,
                                        DoubleScoreComponentOuter score )
        {
            SampleMetadata sampleMetadata = score.getMetadata();

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
            if ( LeftOrRightOrBaseline.BASELINE.equals( sampleMetadata.getIdentifier().getLeftOrRightOrBaseline() ) )
            {
                append = "_" + LeftOrRightOrBaseline.BASELINE.name();
            }

            // Look for a threshold with a standard name that is like the threshold associated with this score
            // Only use this technique when the thresholds are named
            LOGGER.debug( "Searching the standard threshold names for metric name {}.", metricNameString );

            Map<String, OneOrTwoThresholds> metricMap =
                    this.outputWriter.standardThresholdNames.get( metricNameString );

            for ( Map.Entry<String, OneOrTwoThresholds> nextThreshold : metricMap.entrySet() )
            {
                String nextName = nextThreshold.getKey();
                OneOrTwoThresholds thresholdFromArchive = nextThreshold.getValue();
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

            // Couldn't find a variable name, which is not allowed
            throw new IllegalArgumentException( "Could not find the name of the thresholds variable corresponding to "
                                                + "the threshold "
                                                + sampleMetadata.getThresholds()
                                                + " and metric "
                                                + metricNameString
                                                + " associated with "
                                                + sampleMetadata
                                                + "." );
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
                                              SampleMetadata sampleMetadata,
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
            this.writeLock.lock();

            try
            {
                this.valuesToSave.add( new NetcdfValueKey( name, origin, value ) );

                if ( this.valuesToSave.size() > VALUE_SAVE_LIMIT )
                {
                    this.writeMetricResults();
                    LOGGER.trace( "Output {} values to {}", VALUE_SAVE_LIMIT, this.outputPath );
                }
            }
            finally
            {
                if ( this.writeLock.isHeldByCurrentThread() )
                {
                    this.writeLock.unlock();
                }
            }
        }

        /**
         * Finds the origin index(es) of the location in the netcdf variables
         * @param location The location specification detailing where to place a value
         * @return The coordinates for the location within the Netcdf variable describing where to place data
         */
        private int[] getOrigin( GeometryTuple location ) throws IOException, CoordinateNotFoundException
        {
            int[] origin;
            LOGGER.trace( "Looking for the origin of {}", location );
            Integer vectorIndex = this.getVectorCoordinate( location,
                                                            "lid" );

            if ( vectorIndex == null )
            {
                throw new CoordinateNotFoundException( "An index for the vector coordinate could not "
                                                       + "be evaluated. [value = "
                                                       + NetcdfOutputFileCreator2.getGeometryTupleName( location )
                                                       + "]. The location was "
                                                       + location );
            }

            origin = new int[] { vectorIndex };
            LOGGER.trace( "The origin of {} was at {}", location, origin );
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
        private int[] getOrigin( String name, GeometryTuple tuple ) throws IOException, CoordinateNotFoundException
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
                FeatureKey.GeoPoint point = FeatureKey.getLonLatFromPointWkt( wkt );

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
                Integer vectorIndex = this.getVectorCoordinate(
                                                                tuple,
                                                                this.outputWriter.getNetcdfConfiguration()
                                                                                 .getVectorVariable() );

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


        private Integer getVectorCoordinate( GeometryTuple location, String vectorVariableName )
                throws IOException, CoordinateNotFoundException
        {
            synchronized ( vectorCoordinatesMap )
            {
                if ( vectorCoordinatesMap.size() == 0 )
                {
                    try ( NetcdfFile outputFile = NetcdfFiles.open( this.outputPath ) )
                    {
                        Variable coordinate = outputFile.findVariable( vectorVariableName );

                        if ( this.isDeprecatedWriter )
                        {
                            // TODO remove this whole block

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
                                    vectorCoordinatesMap.put( values.getObject( index ), index );
                                }
                            }
                        }

                        if ( !this.isDeprecatedWriter || this.useLidForLocationIdentifier )
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
                                        (char[]) coordinate.read( origin,
                                                                  shape )
                                                           .get1DJavaArray( DataType.CHAR );
                                String word =
                                        String.valueOf( characters ).trim();
                                vectorCoordinatesMap.put( word, wordIndex );
                            }
                        }
                    }
                    catch ( InvalidRangeException e )
                    {
                        throw new IOException( "A coordinate could not be read.", e );
                    }
                }

                if ( !this.isDeprecatedWriter )
                {
                    String tupleNameInNetcdfFile = NetcdfOutputFileCreator2.getGeometryTupleName( location );
                    this.checkForCoordinateAndThrowExceptionIfNotFound( tupleNameInNetcdfFile, true );
                    return vectorCoordinatesMap.get( tupleNameInNetcdfFile );
                }
                else
                {
                    // TODO remove this whole block, remove the "if/else"
                    String loc = location.getRight()
                                         .getName();

                    if ( this.useLidForLocationIdentifier )
                    {
                        this.checkForCoordinateAndThrowExceptionIfNotFound( loc, true );

                        return vectorCoordinatesMap.get( loc );
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
         * Checks for the presence of a coordinate corresponding to the prescribed location and throws an exception
         * if the coordinate cannot be found.
         * 
         * @param location the location to check
         * @param isLocationName is true if the feature is a named location
         * @throws CoordinateNotFoundException if a coordinate could not be found
         */

        private void checkForCoordinateAndThrowExceptionIfNotFound( String location, boolean isLocationName )
                throws CoordinateNotFoundException
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

            if ( Strings.hasValue( this.outputPath ) )
            {
                representation = this.outputPath;
            }
            else if ( this.timeWindow != null )
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
                    catch ( InvalidRangeException e )
                    {
                        throw new IOException(
                                               "Lingering NetCDF results could not be written to disk.",
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
