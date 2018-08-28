package wres.io.writing.netcdf;

import java.io.Closeable;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.Index;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;
import ucar.nc2.dt.GridDatatype;
import ucar.nc2.dt.grid.GridDataset;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.NetcdfType;
import wres.config.generated.ProjectConfig;
import wres.datamodel.metadata.Location;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.ListOfStatistics;
import wres.io.concurrency.Executor;
import wres.io.config.ConfigHelper;
import wres.io.writing.WriteException;
import wres.util.Strings;

public class NetcdfOutputWriter implements NetcdfWriter<DoubleScoreStatistic>,
                                           Supplier<Set<Path>>,
                                           Closeable
{
    private static final Logger LOGGER = LoggerFactory.getLogger( NetcdfOutputWriter.class );

    private static final String DEFAULT_VECTOR_TEMPLATE = "legend_and_nhdplusv2_template.nc";
    private static final String DEFAULT_GRID_TEMPLATE = "lcc_grid_template.nc";

    private static final Object WINDOW_LOCK = new Object();
    private static final Map<TimeWindow, TimeWindowWriter> WRITERS = new ConcurrentHashMap<>();

    private static final Map<Object, Integer> VECTOR_COORDINATES = new ConcurrentHashMap<>();
    private static final int VALUE_SAVE_LIMIT = 500;

    private DestinationConfig destinationConfig;
    private NetcdfType netcdfConfiguration;

    /**
     * Set of paths that this writer actually wrote to
     */
    private final Set<Path> pathsWrittenTo = new ConcurrentSkipListSet<>();

    /**
     * Writing tasks submitted
     */
    private final List<Future<Set<Path>>> writingTasksSubmitted = new CopyOnWriteArrayList<>();

    public static NetcdfOutputWriter of( final ProjectConfig projectConfig )
    {
        return new NetcdfOutputWriter( projectConfig );
    }

    private NetcdfOutputWriter( final ProjectConfig projectConfig )
    {
        LOGGER.debug( "Created NetcdfOutputWriter {}", this );
        this.destinationConfig = ConfigHelper.getDestinationsOfType( projectConfig, DestinationType.NETCDF ).get( 0 );
        this.netcdfConfiguration = this.destinationConfig.getNetcdf();

        if ( this.netcdfConfiguration == null )
        {
            this.netcdfConfiguration = new NetcdfType(null,
                                                      null,
                                                      null,
                                                      null,
                                                      null );
        }

        Objects.requireNonNull( this.destinationConfig, "The NetcdfOutputWriter wasn't properly initialized." );
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

    @Override
    public void accept( ListOfStatistics<DoubleScoreStatistic> output )
    {
        LOGGER.debug( "NetcdfOutputWriter {} accepted output {}.", this, output );

        Map<TimeWindow, List<DoubleScoreStatistic>> outputByTimeWindow = wres.util.Collections.group(
                output,
                score -> score.getMetadata().getSampleMetadata().getTimeWindow()
        );

        for (TimeWindow window : outputByTimeWindow.keySet())
        {
            List<DoubleScoreStatistic> scores = outputByTimeWindow.get( window );

            synchronized ( NetcdfOutputWriter.WINDOW_LOCK )
            {
                if ( !NetcdfOutputWriter.WRITERS.containsKey( window ) )
                {
                    Collection<MetricVariable> variables = MetricVariable.getAll( scores );
                    NetcdfOutputWriter.WRITERS.put( window,
                                      new TimeWindowWriter( this,
                                                            window,
                                                            variables ) );
                }

                Callable<Set<Path>> writerTask = new Callable<Set<Path>>()
                {
                    @Override
                    public Set<Path> call() throws Exception
                    {
                        Set<Path> pathsWrittenTo = new HashSet<>( 1 );

                        try
                        {
                            NetcdfOutputWriter.TimeWindowWriter writer = WRITERS.get( this.window );
                            writer.write( this.output );
                            Path pathWritten = Paths.get( writer.outputPath );
                            pathsWrittenTo.add( pathWritten );
                            return Collections.unmodifiableSet( pathsWrittenTo );
                        }
                        catch ( Exception e )
                        {
                            LOGGER.error( "Writing to output failed.", e );
                            LOGGER.error( Strings.getStackTrace( e ) );
                            throw e;
                        }
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
                }.initialize( window, scores );

                LOGGER.debug( "Submitting a task to write to a netcdf file." );
                Future<Set<Path>> taskFuture = Executor.submit( writerTask );
                this.writingTasksSubmitted.add( taskFuture );
            }
        }
    }

    @Override
    public void close()
    {
        LOGGER.debug( "Closing writers..." );
        synchronized ( NetcdfOutputWriter.WINDOW_LOCK )
        {
            if ( NetcdfOutputWriter.WRITERS.isEmpty() )
            {
                return;
            }

            ExecutorService closeExecutor = null;

            try
            {
                closeExecutor = Executors.newFixedThreadPool( NetcdfOutputWriter.WRITERS.size() );

                Queue<Future<?>> closeTasks = new LinkedList<>();

                for ( TimeWindowWriter writer : NetcdfOutputWriter.WRITERS.values() )
                {
                    Callable<?> closeTask = new Callable<Object>()
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
                                throw new IOException(
                                        "The writer for " + writer.toString()
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

                    closeTasks.add( closeExecutor.submit( closeTask ) );
                }

                Future<?> task = closeTasks.poll();

                while ( task != null )
                {
                    try
                    {
                        if ( closeTasks.isEmpty() )
                        {
                            task.get();
                        }
                        else
                        {
                            task.get( 3000, TimeUnit.MILLISECONDS );
                        }
                        LOGGER.trace( "Close Task Complete." );
                    }
                    catch ( InterruptedException e )
                    {
                        LOGGER.warn( "Output writing has been interrupted.",
                                     e );
                        Thread.currentThread().interrupt();
                    }
                    catch ( ExecutionException e )
                    {
                        throw new WriteException(
                                "A netCDF output could not be written",
                                e );
                    }
                    catch ( TimeoutException e )
                    {
                        // Since it took so long to close this writer, try to move on to the next one and try
                        // to clear the queue
                        LOGGER.trace( "Task took too long; moving on." );
                        closeTasks.add( task );
                    }

                    task = closeTasks.poll();
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

        LOGGER.debug( "Closing NetcdfOutputWriter {}", this );

        try
        {
            // Figure out which paths were written to. These should all be
            // complete by this point, right?
            for ( Future<Set<Path>> writingTaskResult : this.writingTasksSubmitted )
            {
                Set<Path> oneSetOfPaths = writingTaskResult.get();
                LOGGER.debug( "Some paths written to by {}: {}", this, oneSetOfPaths );
                this.pathsWrittenTo.addAll( oneSetOfPaths );
            }
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted while getting paths from netcdf writers.", ie );
            Thread.currentThread().interrupt();
        }
        catch ( ExecutionException ee )
        {
            String message = "Failed to get a path from netcdf writer for " + this.destinationConfig;
            throw new RuntimeException( message, ee );
        }
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
     * Writes output for a specific pair of lead times, representing the {@link TimeWindow#getEarliestLeadTime()} and
     * the {@link TimeWindow#getLatestLeadTime()}.
     */
    
    private static class TimeWindowWriter implements Closeable
    {
        private final ZonedDateTime ANALYSIS_TIME = ZonedDateTime.now( ZoneId.of( "UTC" ) );

        NetcdfOutputWriter outputWriter;

        TimeWindowWriter( NetcdfOutputWriter outputWriter,
                          final TimeWindow timeWindow,
                          final Collection<MetricVariable> metricVariables )
        {
            this.outputWriter = outputWriter;
            this.timeWindow = timeWindow;
            this.metricVariables = metricVariables;
            this.creationLock = new ReentrantLock();
            this.writeLock = new ReentrantLock();
        }

        void write( Collection<DoubleScoreStatistic> output )
                throws IOException, InvalidRangeException
        {
            //this now needs to somehow get all metadata for all metrics
            // Ensure that the output file exists
            this.buildWriter( output, this.metricVariables );

            for (DoubleScoreStatistic score : output)
            {
                String name = MetricVariable.getName( score );
                // Figure out the location of all values and build the origin in each variable grid
                Location location = score.getMetadata().getSampleMetadata().getIdentifier().getGeospatialID();

                int[] origin = this.getOrigin( name, location );
                Double actualValue = score.getData();

                this.saveValues( name, origin, actualValue );
            }
        }

        private void buildWriter(final Collection<DoubleScoreStatistic> output, final Collection<MetricVariable> variables )
                throws IOException
        {
            this.creationLock.lock();

            try
            {
                if ( !Strings.hasValue( this.outputPath ) )
                {
                    this.outputPath = NetcdfOutputFileCreator.create( getTemplatePath( this.outputWriter ),
                                                                      this.outputWriter.getDestinationConfig(),
                                                                      this.timeWindow,
                                                                      this.ANALYSIS_TIME,
                                                                      variables,
                                                                      output );
                }
            }
            // Exception is being caught because the threads calling metric
            // writing are currently eating runtime errors.
            catch ( Exception e )
            {
                LOGGER.error( "The output writer could not be created", e );
                throw new IOException( "The output writer could not be created.", e );
            }
            finally
            {
                this.creationLock.unlock();
            }
        }

        private static String getTemplatePath( NetcdfOutputWriter outputWriter )
        {
            String templatePath;

            if ( outputWriter.getNetcdfConfiguration()
                             .getTemplatePath() == null )
            {
                String defaultTemplate;

                if ( outputWriter.isGridded() )
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
                templatePath = outputWriter.getDestinationConfig()
                                           .getNetcdf()
                                           .getTemplatePath();
            }

            return templatePath;
        }


        private void writeMetricResults() throws IOException, InvalidRangeException
        {
            this.writeLock.lock();

            Array netcdfValue;
            Index ima;

            try ( NetcdfFileWriter writer = NetcdfFileWriter.openExisting( this.outputPath ) )
            {
                for ( NetcdfValueKey key : this.valuesToSave )
                {
                    int[] shape = new int[key.getOrigin().length];
                    Arrays.fill( shape, 1 );
                    netcdfValue = Array.factory( DataType.FLOAT, shape );

                    ima = netcdfValue.getIndex();
                    netcdfValue.setFloat( ima, (float) key.getValue() );

                    try
                    {
                        writer.write( key.getVariableName(), key.getOrigin(), netcdfValue );
                    }
                    catch ( Exception e )
                    {
                        LOGGER.trace( "Error encountered while writing Netcdf data" );
                        throw e;
                    }
                }

                writer.flush();

                this.valuesToSave.clear();
            }
            catch ( Exception e )
            {
                LOGGER.error( "Error Occurred when writing directly to the Netcdf output" );
                LOGGER.error( Strings.getStackTrace( e ) );
                throw e;
            }
            finally
            {
                if ( this.writeLock.isHeldByCurrentThread() )
                {
                    this.writeLock.unlock();
                }
            }
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
         * @return
         */
        private int[] getOrigin( String name, Location location )
                throws IOException, InvalidRangeException
        {
            int[] origin;

            LOGGER.trace( "Looking for the origin of {}", location );

            // There must be a more coordinated way to do this without having to keep the file open
            // What if we got the info through the template?
            if ( this.outputWriter.isGridded() )
            {
                if ( !location.hasCoordinates() )
                {
                    throw new WriteException( "The location '" +
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
                                                                location.getVectorIdentifier().intValue(),
                                                                this.outputWriter.getNetcdfConfiguration()
                                                                                 .getVectorVariable() );
                Objects.requireNonNull(
                                        vectorIndex,
                                        "An index for the vector coordinate could not "
                                                     + "be evaluated. [value = "
                                                     + location.getVectorIdentifier()
                                                     + "]" );

                origin = new int[] { vectorIndex };
            }

            LOGGER.trace( "The origin of {} was at {}", location, origin );
            return origin;
        }

        private Integer getVectorCoordinate( Integer value, String vectorVariableName )
                throws IOException
        {
            synchronized ( VECTOR_COORDINATES )
            {
                if ( VECTOR_COORDINATES.size() == 0 )
                {
                    try ( NetcdfFile outputFile = NetcdfFile.open( this.outputPath ) )
                    {
                        Variable coordinate = outputFile.findVariable( vectorVariableName );

                        Array values = coordinate.read();

                        // It's probably not necessary to load in everything
                        // We're loading everything in at the moment because we
                        // don't really know what to expect
                        for ( int index = 0; index < values.getSize(); ++index )
                        {
                            VECTOR_COORDINATES.put( values.getObject( index ), index );
                        }
                    }
                }

                return VECTOR_COORDINATES.get( value );
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

        private final List<NetcdfValueKey> valuesToSave = new ArrayList<>();

        private String outputPath = null;
        private final TimeWindow timeWindow;
        private final Collection<MetricVariable> metricVariables;
        private final ReentrantLock creationLock;
        private final ReentrantLock writeLock;

        @Override
        public void close() throws IOException
        {
            LOGGER.trace( "Closing {}", this );
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

                //this.compressOutput( this.outputPath );
            }
        }

        // TODO: Evaluate compression options
        /*private void compressOutput(String uncompressedFileName)
                throws IOException
        {
            String compressedFilename = uncompressedFileName + ".gz";
        
            byte[] buffer = new byte[1024];
        
            LOGGER.debug("Compressing {}...", uncompressedFileName);
        
            try (
                    FileInputStream fileInput = new FileInputStream(uncompressedFileName);
                    GZIPOutputStream gzipOutputStream = new GZIPOutputStream( new FileOutputStream( compressedFilename ) )
            )
            {
        
                int bytesRead;
        
                while ((bytesRead = fileInput.read(buffer)) > 0)
                {
                    gzipOutputStream.write( buffer, 0, bytesRead );
                }
        
                gzipOutputStream.finish();
        
            }
        
            LOGGER.debug("Finished compressing {}", compressedFilename);
        
        }*/
    }
}
