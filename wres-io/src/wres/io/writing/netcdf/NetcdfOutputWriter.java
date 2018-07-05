package wres.io.writing.netcdf;

import java.io.Closeable;
import java.io.IOException;
import java.net.URL;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.tuple.Pair;
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

import wres.config.ProjectConfigPlus;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.NetcdfType;
import wres.config.generated.ProjectConfig;
import wres.datamodel.Location;
import wres.datamodel.MetricConstants;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.MapKey;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMultiMapByTimeAndThreshold;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.io.concurrency.Executor;
import wres.io.config.ConfigHelper;
import wres.io.writing.WriteException;
import wres.util.Strings;

public class NetcdfOutputWriter implements NetcdfWriter<DoubleScoreOutput>
{
    private static final Logger
            LOGGER = LoggerFactory.getLogger( NetcdfOutputWriter.class );

    private static final String DEFAULT_VECTOR_TEMPLATE = "legend_and_nhdplusv2_template.nc";
    private static final String DEFAULT_GRID_TEMPLATE = "lcc_grid_template.nc";

    private static final Object WINDOW_LOCK = new Object();
    private static final Map<TimeWindow, TimeWindowWriter> WRITERS = new ConcurrentHashMap<>(  );
    private static final Map<Object, Integer> VECTOR_COORDINATES = new ConcurrentHashMap<>(  );
    private static final int VALUE_SAVE_LIMIT = 500;

    private static final ZonedDateTime ANALYSIS_TIME = ZonedDateTime.now( ZoneId.of("UTC") );

    private static DestinationConfig destinationConfig;
    private static NetcdfType netcdfConfiguration;

    private NetcdfOutputWriter(){}

    public static NetcdfOutputWriter of( final ProjectConfigPlus projectConfig)
    {
        NetcdfOutputWriter.initialize( projectConfig.getProjectConfig() );
        return new NetcdfOutputWriter();
    }

    private static synchronized void initialize(final ProjectConfig projectConfig)
    {
        if (NetcdfOutputWriter.destinationConfig == null)
        {
            NetcdfOutputWriter.destinationConfig =
                    Collections.unmodifiableList(
                            ConfigHelper.getDestinationsOfType( projectConfig,
                                                                DestinationType.NETCDF )
                    ).get( 0 );

            NetcdfOutputWriter.netcdfConfiguration = NetcdfOutputWriter.destinationConfig.getNetcdf();

            if (NetcdfOutputWriter.netcdfConfiguration == null)
            {
                NetcdfOutputWriter.netcdfConfiguration = new NetcdfType(
                        null,
                        null,
                        null,
                        null,
                        null
                );
            }
        }
    }

    private static boolean isGridded() throws IOException
    {
        return NetcdfOutputWriter.getNetcdfConfiguration().isGridded();
    }

    private static NetcdfType getNetcdfConfiguration()
    {
        Objects.requireNonNull(NetcdfOutputWriter.destinationConfig,
                               "The NetcdfOutputWriter wasn't properly initialized.");
        return NetcdfOutputWriter.netcdfConfiguration;
    }

    private static DestinationConfig getDestinationConfig()
    {
        Objects.requireNonNull(NetcdfOutputWriter.destinationConfig,
                               "The NetcdfOutputWriter wasn't properly initialized.");
        return NetcdfOutputWriter.destinationConfig;
    }

    public static void write(final TimeWindow window, final MetricOutputMultiMapByTimeAndThreshold<DoubleScoreOutput> output)
    {
        synchronized ( NetcdfOutputWriter.WINDOW_LOCK )
        {
            if (!NetcdfOutputWriter.WRITERS.containsKey( window ))
            {
                NetcdfOutputWriter.WRITERS.put(window, new TimeWindowWriter(window));
            }

            Callable<Object> writerTask = new Callable<Object>() {
                @Override
                public Object call() throws Exception
                {
                    try
                    {
                        NetcdfOutputWriter.WRITERS.get( window ).write( output );
                    }
                    catch (Exception e)
                    {
                        LOGGER.error("Writing to output failed.", e);
                        LOGGER.error(Strings.getStackTrace( e ));
                        throw e;
                    }
                    return null;
                }

                Callable<Object> initialize(final TimeWindow window, final MetricOutputMultiMapByTimeAndThreshold<DoubleScoreOutput> output)
                {
                    this.window = window;
                    this.output = output;
                    return this;
                }

                private TimeWindow window;
                private MetricOutputMultiMapByTimeAndThreshold<DoubleScoreOutput> output;
            }.initialize( window, output );

            LOGGER.debug("Submitting a task to write to a netcdf file.");
            Executor.submit( writerTask );
        }
    }

    @Override
    public void accept( MetricOutputMultiMapByTimeAndThreshold<DoubleScoreOutput> output )
    {
        for ( Map.Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<DoubleScoreOutput>> m : output.entrySet() )
        {
            for ( TimeWindow window : m.getValue().setOfTimeWindowKeyByLeadTime() )
            {
                NetcdfOutputWriter.write( window, output );
            }
        }
    }

    public static void close()
    {
        LOGGER.trace("Closing writers...");
        synchronized ( NetcdfOutputWriter.WINDOW_LOCK )
        {
            if (NetcdfOutputWriter.WRITERS.isEmpty())
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
                        public Object call() throws Exception
                        {
                            try
                            {
                                writer.close();
                            }
                            catch ( Exception e )
                            {
                                throw new Exception( "The writer for " + writer.toString() + " could not be closed.",
                                                     e );
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
                        LOGGER.error( "Output writing has been interrupted." );
                        Thread.currentThread().interrupt();
                    }
                    catch ( ExecutionException e )
                    {
                        // Not halting because other output needs to be written and some files may still be valid
                        LOGGER.error( "Output could not be written.", e );
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
                if (closeExecutor != null && !closeExecutor.isShutdown())
                {
                    closeExecutor.shutdown();
                }
            }
        }
    }

    private static class TimeWindowWriter implements Closeable
    {
        TimeWindowWriter( final TimeWindow window )
        {
            this.window = window;
            this.creationLock = new ReentrantLock(  );
            this.writeLock = new ReentrantLock(  );
        }

        void write(MetricOutputMultiMapByTimeAndThreshold<DoubleScoreOutput> output)
                throws IOException, InvalidRangeException
        {
            // Ensure that the output file exists
            this.buildWriter( output );

            // We want to write data for each encountered metric
            for ( Map.Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<DoubleScoreOutput>> scoreOutput : output.entrySet() )
            {
                // Determine the name of the metric
                String metricName = scoreOutput.getKey().getKey().toString();

                // Now that we know the name of the metric, we now want the
                // collection of all values recorded for the metric at all
                // thresholds
                MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> metricOutputMap =
                        scoreOutput.getValue().filterByLeadTime( this.window );

                // Figure out the location of all values and build the origin in each variable grid
                Location location = metricOutputMap.getMetadata().getIdentifier().getGeospatialID();

                // Iterate through each threshold
                for (Map.Entry<Pair<TimeWindow, OneOrTwoThresholds>, DoubleScoreOutput> score : metricOutputMap.entrySet())
                {
                    // Determine the name of the Netcdf Variable by combining
                    // the name of the metric and its threshold
                    String name = NetcdfOutputFileCreator.getMetricVariableName( metricName, score.getKey().getRight() );
                    int[] origin = this.getOrigin( name, location );
                    Double actualValue = score.getValue().getData();

                    this.saveValues( name, origin, actualValue );
                }
            }
        }

        private void buildWriter(MetricOutputMultiMapByTimeAndThreshold<DoubleScoreOutput> output)
                throws IOException
        {
            this.creationLock.lock();

            try
            {
                if ( !Strings.hasValue( this.outputPath ) )
                {
                    this.outputPath = NetcdfOutputFileCreator.create(
                            getTemplatePath(),
                            getDestinationConfig(),
                            this.window,
                            NetcdfOutputWriter.ANALYSIS_TIME,
                            output
                    );
                }
            }
            // Exception is being caught because the threads calling metric
            // writing are currently eating runtime errors.
            catch( Exception e )
            {
                LOGGER.error( "The output writer could not be created", e );
                throw new IOException( "The output writer could not be created.", e );
            }
            finally
            {
                this.creationLock.unlock();
            }
        }

        private static String getTemplatePath() throws IOException
        {
            String templatePath;

            if (NetcdfOutputWriter.getNetcdfConfiguration().getTemplatePath() == null)
            {
                String defaultTemplate;

                if (NetcdfOutputWriter.isGridded())
                {
                    defaultTemplate = DEFAULT_GRID_TEMPLATE;
                }
                else
                {
                    defaultTemplate = DEFAULT_VECTOR_TEMPLATE;
                }

                URL template = NetcdfOutputWriter.class.getClassLoader().getResource( defaultTemplate );
                Objects.requireNonNull( template, "A default template for netcdf output could not be "
                                                  + "found on the class path." );
                templatePath = template.getPath();
            }
            else
            {
                templatePath = NetcdfOutputWriter.getDestinationConfig().getNetcdf().getTemplatePath();
            }

            return templatePath;
        }


        private void writeMetricResults() throws IOException, InvalidRangeException
        {
            this.writeLock.lock();

            Array netcdfValue;
            Index ima;

            try (NetcdfFileWriter writer = NetcdfFileWriter.openExisting( this.outputPath ))
            {
                for (NetcdfValueKey key : this.valuesToSave)
                {
                    int[] shape = new int[key.getOrigin().length];
                    Arrays.fill(shape, 1);
                    netcdfValue = Array.factory( DataType.FLOAT, shape );

                    ima = netcdfValue.getIndex();
                    netcdfValue.setFloat( ima, (float)key.getValue() );

                    try
                    {
                        writer.write( key.getVariableName(), key.getOrigin(), netcdfValue );
                    }
                    catch(Exception e)
                    {
                        LOGGER.trace("Error encountered while writing Netcdf data");
                        throw e;
                    }
                }

                writer.flush();

                this.valuesToSave.clear();
            }
            catch(Exception e)
            {
                LOGGER.error("Error Occurred when writing directly to the Netcdf output");
                LOGGER.error(Strings.getStackTrace( e ));
                throw e;
            }
            finally
            {
                if (this.writeLock.isHeldByCurrentThread())
                {
                    this.writeLock.unlock();
                }
            }
        }

        private void saveValues(String name, int[] origin, double value)
                throws IOException, InvalidRangeException
        {
            this.writeLock.lock();

            try
            {
                this.valuesToSave.add( new NetcdfValueKey( name, origin, value ) );

                if ( this.valuesToSave.size() > VALUE_SAVE_LIMIT)
                {
                    this.writeMetricResults();
                    LOGGER.trace("Output {} values to {}", VALUE_SAVE_LIMIT, this.outputPath);
                }
            }
            finally
            {
                if (this.writeLock.isHeldByCurrentThread())
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
        private int[] getOrigin(String name, Location location)
                throws IOException, InvalidRangeException
        {
            int[] origin = null;

            LOGGER.trace("Looking for the origin of {}", location);

            // There must be a more coordinated way to do this without having to keep the file open
            // What if we got the info through the template?
            if (isGridded())
            {
                if (!location.hasCoordinates())
                {
                    throw new WriteException( "The location '" +
                                              location +
                                              "' cannot be written to the "
                                              + "output because the project "
                                              + "configuration dictates gridded "
                                              + "output but the location doesn't "
                                              + "support it." );
                }

                // contains the the y index and the x index
                origin = new int[2];

                // TODO: Find a different approach to handle grids without a coordinate system
                try (GridDataset gridDataset = GridDataset.open( this.outputPath ))
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
                        NetcdfOutputWriter.getNetcdfConfiguration().getVectorVariable()
                );
                Objects.requireNonNull(
                        vectorIndex,
                        "An index for the vector coordinate could not "
                        + "be evaluated. [value = " + location.getVectorIdentifier() + "]");

                origin = new int[]{vectorIndex};
            }

            LOGGER.trace("The origin of {} was at {}", location, origin);
            return origin;
        }

        private Integer getVectorCoordinate( Integer value, String vectorVariableName)
                throws IOException
        {
            synchronized ( VECTOR_COORDINATES )
            {
                if ( VECTOR_COORDINATES.size() == 0)
                {
                    try (NetcdfFile outputFile = NetcdfFile.open( this.outputPath ))
                    {
                        Variable coordinate = outputFile.findVariable( vectorVariableName );

                        Array values = coordinate.read();

                        // It's probably not necessary to load in everything
                        // We're loading everything in at the moment because we
                        // don't really know what to expect
                        for (int index = 0; index < values.getSize(); ++index)
                        {
                            VECTOR_COORDINATES.put( values.getObject( index ), index);
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

            if ( Strings.hasValue( this.outputPath ))
            {
                representation = this.outputPath;
            }
            else if (this.window != null)
            {
                representation = this.window.toString();
            }

            return representation;
        }

        private final List<NetcdfValueKey> valuesToSave = new ArrayList<>(  );

        private String outputPath = null;
        private final TimeWindow window;
        private final ReentrantLock creationLock;
        private final ReentrantLock writeLock;

        @Override
        public void close() throws IOException
        {
            LOGGER.trace("Closing {}", this);
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
