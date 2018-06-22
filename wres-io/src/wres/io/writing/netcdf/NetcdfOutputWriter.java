package wres.io.writing.netcdf;

import java.io.Closeable;
import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.Array;
import ucar.ma2.ArrayDouble;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;
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

    private static final Object WINDOW_LOCK = new Object();
    private static final Map<TimeWindow, TimeWindowWriter> WRITERS = new ConcurrentHashMap<>(  );
    private static final Map<Object, Integer> VECTOR_COORDINATES = new ConcurrentHashMap<>(  );
    private static final int VALUE_SAVE_LIMIT = 500;

    private static ZonedDateTime ANALYSIS_TIME = ZonedDateTime.now( ZoneId.of("UTC") );

    private static List<DestinationConfig> destinationConfig;

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
                    );
        }
    }

    private static NetcdfType getNetcdfConfig() throws IOException
    {
        return NetcdfOutputWriter.getDestinationConfig().getNetcdf();
    }

    private static boolean isGridded() throws IOException
    {
        return Strings.hasValue(NetcdfOutputWriter.getNetcdfConfig().getGridXVariable()) &&
               Strings.hasValue( NetcdfOutputWriter.getNetcdfConfig().getGridYVariable() );
    }

    // TODO: We are currently only supporting one Netcdf destination tag
    private static DestinationConfig getDestinationConfig() throws IOException
    {
        Objects.requireNonNull(NetcdfOutputWriter.destinationConfig,
                               "The NetcdfOutputWriter was never initialized.");

        if (!NetcdfOutputWriter.destinationConfig.isEmpty())
        {
            return NetcdfOutputWriter.destinationConfig.get( 0 );
        }
        else
        {
            throw new IOException( "No Netcdf destination was configured, yet "
                                   + "the application is still trying to output "
                                   + "Netcdf data." );
        }
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
                    NetcdfOutputWriter.WRITERS.get( window ).write( output );
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
        synchronized ( NetcdfOutputWriter.WINDOW_LOCK )
        {
            for (TimeWindowWriter writer : NetcdfOutputWriter.WRITERS.values())
            {
                try
                {
                    writer.close();
                }
                catch ( IOException e )
                {
                    LOGGER.error( "The NetCDF writer for '{}' could not be closed.", writer );
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
                if (this.outputWriter == null)
                {
                    this.outputWriter = NetcdfOutputFileCreator.create(
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
            return NetcdfOutputWriter.getDestinationConfig().getNetcdf().getTemplatePath();
        }


        private void writeMetricResults()
                throws IOException, InvalidRangeException
        {
            this.writeLock.lock();

            try
            {
                // TODO: This should be responsible for opening and closing the writer; it shouldn't be left open
                for (NetcdfValueKey key : this.valuesToSave)
                {
                    ArrayDouble.D1 netcdfValue = new ArrayDouble.D1( 1 );
                    netcdfValue.set( 0, key.getValue() );

                    this.outputWriter.write( key.getVariableName(), key.getOrigin(), netcdfValue );
                }

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
                    LOGGER.debug("Output {} values to {}", VALUE_SAVE_LIMIT, this.outputWriter.getNetcdfFile().getLocation());
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
            int[] origin;

            LOGGER.debug("Looking for the origin of {}", location);

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

                // contains the time index, the y index, and the x index
                origin = new int[3];
                origin[0] = 0;

                try (
                    NetcdfDataset dataset = new NetcdfDataset( this.outputWriter.getNetcdfFile() );
                    GridDataset gridDataset = new GridDataset( dataset ))
                {
                    GridDatatype variable =
                            gridDataset.findGridDatatype( name );
                    int[] xyIndex = variable.getCoordinateSystem()
                                            .findXYindexFromLatLon( location.getLatitude(),
                                                                    location.getLongitude(),
                                                                    null );
                    xyIndex = variable.getCoordinateSystem()
                                            .findXYindexFromCoord( location.getLatitude(),
                                                                    location.getLongitude(),
                                                                    null );

                    origin[1] = xyIndex[1];
                    origin[2] = xyIndex[0];
                }
            }
            else
            {
                // Only contains the vector id
                origin = new int[1];

                Integer vectorIndex = this.getVectorCoordinate(
                        location.getVectorIdentifier().intValue(),
                        getNetcdfConfig().getVectorVariable(),
                        this.outputWriter
                );

                Objects.requireNonNull(
                        vectorIndex,
                        "An index for the vector coordinate could not "
                        + "be evaluated. [value = " + location.getVectorIdentifier() + "]");

                origin[0] = vectorIndex;
            }

            LOGGER.debug("The origin of {} was at {}", location, origin);
            return origin;
        }

        private Integer getVectorCoordinate( Integer value, String vectorVariableName, NetcdfFileWriter writer)
                throws IOException
        {
            synchronized ( VECTOR_COORDINATES )
            {
                if ( VECTOR_COORDINATES.size() == 0)
                {
                    Variable coordinate = writer.findVariable( vectorVariableName );

                    Array values = coordinate.read();

                    // It's probably not necessary to load in everything
                    // We're loading everything in at the moment because we
                    // don't really know what to expect
                    for (int index = 0; index < values.getSize(); ++index)
                    {
                        VECTOR_COORDINATES.put( values.getObject( index ), index);
                    }
                }

                return VECTOR_COORDINATES.get( value );
            }
        }

        @Override
        public String toString()
        {
            String representation = "TimeWindowWriter";

            if (this.outputWriter != null && this.outputWriter.getNetcdfFile() != null)
            {
                representation = this.outputWriter.getNetcdfFile().getLocation();
            }
            else if (this.window != null)
            {
                representation = this.window.toString();
            }

            return representation;
        }

        private final List<NetcdfValueKey> valuesToSave = new ArrayList<>(  );

        private NetcdfFileWriter outputWriter = null;
        private final TimeWindow window;
        private final ReentrantLock creationLock;
        private final ReentrantLock writeLock;

        @Override
        public void close() throws IOException
        {
            LOGGER.debug("Closing {}", this);
            if ( this.outputWriter != null )
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

                //String uncompressedFilename = this.outputWriter.getNetcdfFile().getLocation();
                this.outputWriter.close();
                //this.compressOutput( uncompressedFilename );
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
