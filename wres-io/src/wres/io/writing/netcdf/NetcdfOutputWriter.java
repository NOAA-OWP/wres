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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
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
import wres.config.FeaturePlus;
import wres.config.generated.*;
import wres.config.generated.ProjectConfig.Inputs;
import wres.datamodel.DatasetIdentifier;
import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.scale.TimeScale;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.datamodel.thresholds.ThresholdsGenerator;
import wres.datamodel.time.TimeWindow;
import wres.datamodel.time.generators.TimeWindowGenerator;
import wres.io.concurrency.Executor;
import wres.io.config.ConfigHelper;
import wres.io.retrieval.UnitMapper;
import wres.io.writing.WriteException;
import wres.system.SystemSettings;
import wres.util.FutureQueue;
import wres.util.Strings;

public class NetcdfOutputWriter implements NetcdfWriter<DoubleScoreStatistic>,
                                           Supplier<Set<Path>>,
                                           Closeable
{   
    private static final Logger LOGGER = LoggerFactory.getLogger( NetcdfOutputWriter.class );

    private static final String DEFAULT_VECTOR_TEMPLATE = "vector_template.nc";
    private static final String DEFAULT_GRID_TEMPLATE = "lcc_grid_template.nc";
    private static final int VALUE_SAVE_LIMIT = 500;
    private static final ZonedDateTime ANALYSIS_TIME = ZonedDateTime.now( ZoneId.of( "UTC" ) );

    private final SystemSettings systemSettings;
    private final Executor executor;
    private final Object windowLock = new Object();

    private final DestinationConfig destinationConfig;
    private final Path outputDirectory;
    private NetcdfType netcdfConfiguration;
    
    // Guarded by windowLock
    private final Map<TimeWindow, TimeWindowWriter> writersMap = new HashMap<>();

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
     * Mapping between standard threshold names and representative thresholds for those standard names. This is used
     * to help determine the threshold portion of a variable name to which a statistic corresponds, based on the 
     * standard name of a threshold chosen at blob creation time. There is a separate group for each metric.
     */
    
    private Map<String,Map<String,OneOrTwoThresholds>> standardThresholdNames = new HashMap<>();
    
    /**
     * Returns an instance of the writer. 
     *
     * @param systemSettings The system settings to use.
     * @param executor The executor to use.
     * @param projectConfig the project configuration
     * @param durationUnits the time units for durations
     * @param outputDirectory the directory into which to write
     * @param thresholds Thresholds that will be imposed on input data
     * @return an instance of the writer
     * @throws IOException if the blobs could not be created for any reason
     */

    public static NetcdfOutputWriter of( SystemSettings systemSettings,
                                         Executor executor,
                                         ProjectConfig projectConfig,
                                         ChronoUnit durationUnits,
                                         Path outputDirectory,
                                         Map<Feature, ThresholdsByMetric> thresholds
    ) throws IOException
    {
        return new NetcdfOutputWriter(
                systemSettings,
                executor,
                projectConfig,
                durationUnits,
                outputDirectory,
                thresholds
        );
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
                                ChronoUnit durationUnits,
                                Path outputDirectory,
                                Map<Feature, ThresholdsByMetric> thresholds ) throws IOException
    {
        Objects.requireNonNull( systemSettings );
        Objects.requireNonNull( executor );
        Objects.requireNonNull( projectConfig, "Specify non-null project config." );
        Objects.requireNonNull( durationUnits, "Specify non-null duration units." );
        Objects.requireNonNull( outputDirectory, "Specify non-null output directory." );

        LOGGER.debug( "Created NetcdfOutputWriter {}", this );
        this.systemSettings = systemSettings;
        this.executor = executor;
        this.destinationConfig = ConfigHelper.getDestinationsOfType( projectConfig, DestinationType.NETCDF ).get( 0 );
        this.netcdfConfiguration = this.destinationConfig.getNetcdf();
        this.durationUnits = durationUnits;
        this.outputDirectory = outputDirectory;

        if ( this.netcdfConfiguration == null )
        {
            this.netcdfConfiguration = new NetcdfType( null,
                                                       null,
                                                       null,
                                                       null,
                                                       null );
        }

        // Create the blobs into which statistics will be written and a writer per blob
        this.pathsWrittenTo = this.createBlobsAndBlobWriters( projectConfig, thresholds );
        
        Objects.requireNonNull( this.destinationConfig, "The NetcdfOutputWriter wasn't properly initialized." );
    }

    private Executor getExecutor()
    {
        return this.executor;
    }

    /**
     * Creates the blobs into which outputs will be written.
     *
     * @param projectConfig The project configuration.
     * @param thresholds Thresholds imposed upon input data
     * @throws IOException if the blobs could not be created for any reason
     * @return the paths written
     */

    private Set<Path> createBlobsAndBlobWriters(
            ProjectConfig projectConfig,
            Map<Feature, ThresholdsByMetric> thresholds
    ) throws IOException
    {
        // Time windows
        PairConfig pairConfig = projectConfig.getPair();
        Set<TimeWindow> timeWindows = TimeWindowGenerator.getTimeWindowsFromPairConfig( pairConfig );

        Optional<ThresholdsByMetric> possibleThresholds = thresholds.values().stream().findFirst();

        ThresholdsByMetric thresholdsToLoad = possibleThresholds.orElseGet(
                () -> ThresholdsGenerator.getThresholdsFromConfig(projectConfig)
        );

        // Units, if declared
        String units = "UNKNOWN";
        if ( Objects.nonNull( pairConfig.getUnit() ) )
        {
            units = pairConfig.getUnit();
        }
        
        // Desired time scale, if declared
        TimeScale desiredTimeScale = null;
        if ( Objects.nonNull( pairConfig.getDesiredTimeScale() ) )
        {
            desiredTimeScale = TimeScale.of( pairConfig.getDesiredTimeScale() );
        }
        
        // Create blobs from components
        return this.createBlobsAndBlobWriters(
                projectConfig.getInputs(),
                timeWindows,
                thresholdsToLoad,
                units,
                desiredTimeScale
        );
    }
    
    /**
     * Creates the blobs into which outputs will be written.
     * 
     * @param inputs the inputs declaration
     * @param timeWindows the time windows
     * @param thresholds the thresholds
     * @param units the measurement units, if available
     * @param desiredTimeScale the desired time scale, if available
     * @throws IOException if the blobs could not be created for any reason
     * @return the paths written
     */

    private Set<Path> createBlobsAndBlobWriters( Inputs inputs,
                                                 Set<TimeWindow> timeWindows,
                                                 ThresholdsByMetric thresholds,
                                                 String units,
                                                 TimeScale desiredTimeScale )
            throws IOException
    {
        Set<Path> returnMe = new TreeSet<>();

        // One blob and blob writer per time window      
        for ( TimeWindow nextWindow : timeWindows )
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

            // Create the blob
            String pathActuallyWritten = NetcdfOutputFileCreator.create( this.getTemplatePath(),
                                                                         targetPath,
                                                                         this.getDestinationConfig(),
                                                                         nextWindow,
                                                                         NetcdfOutputWriter.ANALYSIS_TIME,
                                                                         variables,
                                                                         this.getDurationUnits() );

            returnMe.add( targetPath );

            // Create the blob writer
            TimeWindowWriter writer = new TimeWindowWriter( this,
                                                            pathActuallyWritten,
                                                            nextWindow );

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
        String variableId = ConfigHelper.getVariableIdFromProjectConfig( inputs, false );
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
                                                                           TimeWindow timeWindow,
                                                                           ThresholdsByMetric thresholds,
                                                                           String units,
                                                                           TimeScale desiredTimeScale )
    {
        // Statistics for a separate baseline? If no, there's a single set of variables
        if ( Objects.isNull( inputs.getBaseline() ) || !inputs.getBaseline().isSeparateMetrics() )
        {
            return this.getMetricVariablesForOneTimeWindow( timeWindow, thresholds, units, desiredTimeScale, null );
        }

        // Two sets of variables, one for the right and one for the baseline with separate metrics
        // FOr backwards compatibility, only clarify the baseline variable
        Collection<MetricVariable> right = this.getMetricVariablesForOneTimeWindow( timeWindow,
                                                                                    thresholds,
                                                                                    units,
                                                                                    desiredTimeScale,
                                                                                    null );

        Collection<MetricVariable> baseline = this.getMetricVariablesForOneTimeWindow( timeWindow,
                                                                                       thresholds,
                                                                                       units,
                                                                                       desiredTimeScale,
                                                                                       LeftOrRightOrBaseline.BASELINE );

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
     * @return the metric variables
     */

    private Collection<MetricVariable> getMetricVariablesForOneTimeWindow( TimeWindow timeWindow,
                                                                           ThresholdsByMetric thresholds,
                                                                           String units,
                                                                           TimeScale desiredTimeScale,
                                                                           LeftOrRightOrBaseline context )
    {

        Map<MetricConstants, SortedSet<OneOrTwoThresholds>> thresholdMap = thresholds.getOneOrTwoThresholds();

        Map<String, SortedSet<OneOrTwoThresholds>> decomposed =
                this.decomposeThresholdsByMetricForBlobCreation( thresholdMap );

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
     * @return the expanded thresholds-by-metric
     */

    private Map<String, SortedSet<OneOrTwoThresholds>>
            decomposeThresholdsByMetricForBlobCreation( Map<MetricConstants, SortedSet<OneOrTwoThresholds>> thresholdsByMetric )
    {

        Map<String, SortedSet<OneOrTwoThresholds>> returnMe = new TreeMap<>();

        for ( Map.Entry<MetricConstants, SortedSet<OneOrTwoThresholds>> nextEntry : thresholdsByMetric.entrySet() )
        {
            MetricConstants nextMetric = nextEntry.getKey();
            SortedSet<OneOrTwoThresholds> nextThresholds = nextEntry.getValue();

            Set<MetricConstants> components = nextMetric.getAllComponents();

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
    public void accept( List<DoubleScoreStatistic> output )
    {
        LOGGER.debug( "NetcdfOutputWriter {} accepted output {}.", this, output );

        Map<TimeWindow, List<DoubleScoreStatistic>> outputByTimeWindow = wres.util.Collections.group(
                output,
                score -> score.getMetadata().getSampleMetadata().getTimeWindow()
        );

        for ( Map.Entry<TimeWindow, List<DoubleScoreStatistic>> entries : outputByTimeWindow.entrySet() )
        {
            TimeWindow timeWindow = entries.getKey();
            List<DoubleScoreStatistic> scores = entries.getValue();

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

                    Callable<Set<Path>> initialize( final TimeWindow window,
                                                    final List<DoubleScoreStatistic> scores )
                    {
                        this.output = scores;
                        this.window = window;
                        return this;
                    }

                    private List<DoubleScoreStatistic> output;
                    private TimeWindow window;
                }.initialize( timeWindow, scores );

                LOGGER.debug( "Submitting a task to write to a netcdf file." );
                Executor writerExecutor = this.getExecutor();
                Future<Set<Path>> taskFuture = writerExecutor.submit( writerTask );
                this.writingTasksSubmitted.add( taskFuture );
            }
        }
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

    @Override
    public Set<Path> get()
    {
        return this.getPathsWrittenTo();
    }

    /**
     * Return a snapshot of the paths written to (so far)
     */

    private Set<Path> getPathsWrittenTo()
    {
        LOGGER.debug( "getPathsWrittenTo from NetcdfOutputWriter {}: {}",
                      this, this.pathsWrittenTo );
        return Collections.unmodifiableSet( this.pathsWrittenTo );
    }


    /**
     * Writes output for a specific pair of lead times, representing the {@link TimeWindow#getEarliestLeadDuration()} and
     * the {@link TimeWindow#getLatestLeadDuration()}.
     */
    
    private static class TimeWindowWriter implements Closeable
    {

        private static final String WHILE_ATTEMPTING_TO_WRITE_STATISTICS_TO = "While attempting to write statistics to ";
        private static final String FAILED_TO_IDENTIFY_A_COORDINATE_FOR_LOCATION = ", failed to identify a coordinate for location ";
        NetcdfOutputWriter outputWriter;
        private boolean useLidForLocationIdentifier;
        private final Map<Object, Integer> vectorCoordinatesMap = new ConcurrentHashMap<>();

        private final List<NetcdfValueKey> valuesToSave = new ArrayList<>();

        private final String outputPath;
        private final TimeWindow timeWindow;
        private final ReentrantLock writeLock;

        /**
         * A writer to be opened on first write, closed when the {@link NetcdfOutputWriter} that encloses this
         * {@link TimeWindowWriter} is closed.
         */
        private NetcdfFileWriter writer;
        
        TimeWindowWriter( NetcdfOutputWriter outputWriter,
                          String outputPath,
                          final TimeWindow timeWindow )
        {
            this.outputWriter = outputWriter;
            this.outputPath = outputPath;
            this.timeWindow = timeWindow;
            this.writeLock = new ReentrantLock();
        }

        void write( List<DoubleScoreStatistic> scores )
                throws IOException, InvalidRangeException, CoordinateNotFoundException
        {
            //this now needs to somehow get all metadata for all metrics
            // Ensure that the output file exists
            for ( DoubleScoreStatistic score : scores )
            {
                Set<MetricConstants> components = score.getComponents();

                for ( MetricConstants nextComponent : components )
                {
                    DoubleScoreStatistic componentScore = score.getComponent( nextComponent );
                    
                    String name = this.getVariableName( componentScore, scores );
                    
                    // Figure out the location of all values and build the origin in each variable grid
                    Location location = score.getMetadata()
                                             .getSampleMetadata()
                                             .getIdentifier()
                                             .getGeospatialID();

                    int[] origin;

                    try
                    {
                        origin = this.getOrigin( name, location );
                    }
                    catch ( CoordinateNotFoundException e )
                    {
                        throw new CoordinateNotFoundException( "While trying to write the statistic " + componentScore
                                                               + "to the variable "
                                                               + name
                                                               + " at path "
                                                               + this.outputPath
                                                               + ", failed to identify a required coordinate for one "
                                                               + "or more features.",
                                                               e );
                    }

                    Double actualValue = componentScore.getData();
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
            if( Objects.isNull( this.writer ) )
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
                    netcdfValue = Array.factory( DataType.FLOAT, shape );

                    ima = netcdfValue.getIndex();
                    netcdfValue.setFloat( ima, (float) key.getValue() );

                    String exceptionMessage = "While attempting to write data value "
                                              + key.getValue()
                                              + " with variable name "
                                              + key.getVariableName()
                                              + " to index "
                                              + Arrays.toString( key.getOrigin() )
                                              + " within file "
                                              + this.outputPath
                                              + ": ";
                    
                    try
                    {
                        writer.write( key.getVariableName(), key.getOrigin(), netcdfValue );
                    }
                    catch ( NullPointerException | IOException | InvalidRangeException e )
                    {
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
         * @param score the score whose metric-threshold standard name is required
         * @return the standard name
         */
        
        private String getVariableName( DoubleScoreStatistic score, List<DoubleScoreStatistic> scores )
        {
            StatisticMetadata statisticMetadata = score.getMetadata();
            SampleMetadata sampleMetadata = statisticMetadata.getSampleMetadata();

            // Find the metric name
            MetricConstants metricId = statisticMetadata.getMetricID();
            MetricConstants metricComponentId = statisticMetadata.getMetricComponentID();
            String metricName = metricId.name();
            if ( metricComponentId != MetricConstants.MAIN )
            {
                metricName = metricName + "_" + metricComponentId.name();
            }
            
            String append = "";
            if( LeftOrRightOrBaseline.BASELINE.equals( sampleMetadata.getIdentifier().getLeftOrRightOrBaseline() ) )
            {
                append = "_" + LeftOrRightOrBaseline.BASELINE.name();
            }

            // Look for a threshold with a standard name that is like the threshold associated with this score
            // Only use this technique when the thresholds are named
            Map<String, OneOrTwoThresholds> metricMap = this.outputWriter.standardThresholdNames.get( metricName );

            for ( Map.Entry<String, OneOrTwoThresholds> nextThreshold : metricMap.entrySet() )
            {
                String nextName = nextThreshold.getKey();
                OneOrTwoThresholds threshold = nextThreshold.getValue();
                if ( threshold.first().hasLabel() )
                {

                    // Label associated with event threshold is equal, and any decision threshold is equal
                    String thresholdWithValuesOne = threshold.first()
                                                             .getLabel();
                    String thresholdWithValuesTwo = sampleMetadata.getThresholds()
                                                                  .first()
                                                                  .getLabel();

                    // Decision threshold equal?
                    boolean hasSecond = threshold.hasTwo();
                    boolean secondEqual = hasSecond &&
                                          threshold.second().equals( sampleMetadata.getThresholds().second() );

                    if ( thresholdWithValuesOne.equals( thresholdWithValuesTwo )
                         && ( !hasSecond || secondEqual ) )
                    {
                        return metricName + "_" + nextName + append;
                    }
                }
            }

            // Couldn't find a similar threshold, so look in the context instead
            // Filter the scores by identifier, then return the threshold name based on order
            SortedSet<OneOrTwoThresholds> statistics = scores.stream()
                                                          .filter( a -> a.getMetadata().getMetricID() == metricId )
                                                          .map( a -> a.getMetadata().getSampleMetadata().getThresholds() )
                                                          .collect( Collectors.toCollection( TreeSet::new ) );

            // Find the elements strictly less than the element of interest, then add one
            int thresholdNumber = statistics.headSet( sampleMetadata.getThresholds() )
                                            .size()
                                  + 1;

            if ( thresholdNumber < 1 )
            {
                throw new IllegalArgumentException( "Could not find the name of the thresholds variable corresponding to "
                                                    + "the threshold "
                                                    + sampleMetadata.getThresholds()
                                                    + " and metric "
                                                    + metricName
                                                    + " associated with "
                                                    + sampleMetadata
                                                    + ". Looked in this list of thresholds: "
                                                    + statistics );
            }

            return metricName + "_THRESHOLD_" + thresholdNumber + append;
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
        private int[] getOrigin( String name, Location location ) throws IOException, CoordinateNotFoundException
        {
            int[] origin;

            LOGGER.trace( "Looking for the origin of {}", location );

            // There must be a more coordinated way to do this without having to keep the file open
            // What if we got the info through the template?
            if ( this.outputWriter.isGridded() )
            {
                if ( !location.hasCoordinates() )
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

                // contains the the y index and the x index
                origin = new int[2];

                // TODO: Find a different approach to handle grids without a coordinate system
                try ( GridDataset gridDataset = GridDataset.open( this.outputPath ) )
                {
                    GridDatatype variable = gridDataset.findGridDatatype( name );
                    int[] xyIndex = variable.getCoordinateSystem()
                                            .findXYindexFromLatLon( location.getLatitude(),
                                                                    location.getLongitude(),
                                                                    null );

                    origin[0] = xyIndex[1];
                    origin[1] = xyIndex[0];
                }
            }
            else
            {
                // Only contains the vector id
                Integer vectorIndex = this.getVectorCoordinate(
                        location,
                        this.outputWriter.getNetcdfConfiguration().getVectorVariable()
                );

                if (vectorIndex == null)
                {

                    throw new CoordinateNotFoundException( "An index for the vector coordinate could not "
                                                           + "be evaluated. [value = "
                                                           + location.getVectorIdentifier()
                                                           + "]. The location was " + location );
                }

                origin = new int[] { vectorIndex };
            }

            LOGGER.trace( "The origin of {} was at {}", location, origin );
            return origin;
        }

        private Integer getVectorCoordinate( Location location, String vectorVariableName )
                throws IOException, CoordinateNotFoundException
        {
            synchronized ( vectorCoordinatesMap )
            {
                if (vectorCoordinatesMap.size() == 0)
                {
                    try( NetcdfFile outputFile = NetcdfFiles.open(this.outputPath)) {
                        Variable coordinate = outputFile.findVariable(vectorVariableName);
                        Array values = coordinate.read();

                        // It's probably not necessary to load in everything
                        // We're loading everything in at the moment because we
                        // don't really know what to expect
                        if (coordinate.getDataType() == DataType.CHAR)
                        {
                            this.useLidForLocationIdentifier = true;

                            List<Dimension> dimensions = coordinate.getDimensions();

                            for (int wordIndex = 0; wordIndex < dimensions.get(0).getLength(); wordIndex++)
                            {
                                int[] origin = new int[]{wordIndex, 0};
                                int[] shape = new int[]{1, dimensions.get(1).getLength()};

                                char[] characters = (char[])coordinate.read(origin, shape).get1DJavaArray(DataType.CHAR);
                                String word = String.valueOf(characters).trim();

                                vectorCoordinatesMap.put(word, wordIndex);
                            }
                        }
                        else
                        {
                            this.useLidForLocationIdentifier = false;

                            for ( int index = 0; index < values.getSize(); ++index )
                            {
                                vectorCoordinatesMap.put( values.getObject( index ), index );
                            }
                        }
                    } catch (InvalidRangeException e) {
                        throw new IOException("A coordinate could not be read.", e);
                    }
                }

                if ( this.useLidForLocationIdentifier )
                {
                    this.checkForCoordinateAndThrowExceptionIfNotFound( location, true );

                    return vectorCoordinatesMap.get( location.getLocationName() );
                }
                else
                {
                    this.checkForCoordinateAndThrowExceptionIfNotFound( location, false );

                    return this.vectorCoordinatesMap.get( location.getVectorIdentifier().intValue() );
                }
            }
            
        }

        /**
         * Checks for the presence of a coordinate corresponding to the prescribed location and throws an exception
         * if the coordinate cannot be found.
         * 
         * @param location the location to check
         * @param isLocationName is true to use the location name as the identifier, false for the NWM identifier
         * @throws CoordinateNotFoundException if a coordinate could not be found
         */

        private void checkForCoordinateAndThrowExceptionIfNotFound( Location location, boolean isLocationName )
                throws CoordinateNotFoundException
        {

            // Location name is the glue
            if ( isLocationName )
            {
                if ( !this.vectorCoordinatesMap.containsKey( location.getLocationName() ) )
                {
                    throw new CoordinateNotFoundException( WHILE_ATTEMPTING_TO_WRITE_STATISTICS_TO
                                                           + this.outputPath
                                                           + FAILED_TO_IDENTIFY_A_COORDINATE_FOR_LOCATION
                                                           + location
                                                           + " using the location name "
                                                           + location.getLocationName()
                                                           + "." );
                }
            }
            // Comid is the glue
            else
            {
                Long coordinate = location.getVectorIdentifier();

                if ( Objects.isNull( coordinate ) )
                {
                    throw new CoordinateNotFoundException( WHILE_ATTEMPTING_TO_WRITE_STATISTICS_TO
                                                           + this.outputPath
                                                           + FAILED_TO_IDENTIFY_A_COORDINATE_FOR_LOCATION
                                                           + location
                                                           + " because the NWM location identifier (comid) was absent." );
                }

                if ( !this.vectorCoordinatesMap.containsKey( coordinate.intValue() ) )
                {

                    throw new CoordinateNotFoundException( WHILE_ATTEMPTING_TO_WRITE_STATISTICS_TO
                                                           + this.outputPath
                                                           + FAILED_TO_IDENTIFY_A_COORDINATE_FOR_LOCATION
                                                           + location
                                                           + " using the NWM location identifier (comid) "
                                                           + location.getVectorIdentifier().intValue()
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
                    LOGGER.trace( "Closing the underlying netcdf writer {} for pool {}.", this.writer, this.timeWindow );
                    
                    this.writer.close();
                }
            }
        }

    }
}
