package wres.io.writing.netcdf;

import java.io.Closeable;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.Array;
import ucar.ma2.ArrayDouble;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;

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
import wres.util.NetCDF;
import wres.util.Strings;

public class NetcdfOutputWriter implements NetcdfWriter<DoubleScoreOutput>
{
    private static final Logger
            LOGGER = LoggerFactory.getLogger( NetcdfOutputWriter.class );

    private static final Object WINDOW_LOCK = new Object();
    private static final Map<TimeWindow, TimeWindowWriter> WRITERS = new ConcurrentHashMap<>(  );
    private static final Map<Object, Integer> vectorCoordinates = new ConcurrentHashMap<>(  );

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
                int[] origin = this.getOrigin( location );

                // Iterate through each threshold
                for (Map.Entry<Pair<TimeWindow, OneOrTwoThresholds>, DoubleScoreOutput> score : metricOutputMap.entrySet())
                {
                    // Determine the name of the Netcdf Variable by combining
                    // the name of the metric and its threshold
                    String name = NetcdfOutputFileCreator.getMetricVariableName( metricName, score.getKey().getRight() );
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

                // TODO: Make the upper bound a constant or setting
                if ( this.valuesToSave.size() > 500)
                {
                    this.writeMetricResults();
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
        private int[] getOrigin(Location location)
                throws IOException, InvalidRangeException
        {
            int[] origin;

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

                Variable yVariable = this.outputWriter.findVariable( getNetcdfConfig().getGridYVariable() );
                Variable xVariable = this.outputWriter.findVariable( getNetcdfConfig().getGridXVariable() );

                Integer yIndex = NetCDF.getCoordinateIndex(yVariable, location.getLatitude());
                Integer xIndex = NetCDF.getCoordinateIndex( xVariable, location.getLongitude() );

                Objects.requireNonNull( yIndex, "An index for the Y coordinate could not be evaluated." );
                Objects.requireNonNull( xIndex, "An index for the X coordinate could not be evaluated." );

                origin[1] = yIndex;
                origin[2] = xIndex;
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

            return origin;
        }

        private Integer getVectorCoordinate( Integer value, String vectorVariableName, NetcdfFileWriter writer)
                throws IOException
        {
            synchronized ( vectorCoordinates )
            {
                if (vectorCoordinates.size() == 0)
                {
                    Variable coordinate = writer.findVariable( vectorVariableName );

                    Array values = coordinate.read();

                    for (int index = 0; index < values.getSize(); ++index)
                    {
                        vectorCoordinates.put(values.getObject( index ), index);
                    }
                }

                return vectorCoordinates.get( value );
            }
        }

        @Override
        public String toString()
        {
            return this.outputWriter.getNetcdfFile().getLocation();
        }

        private final List<NetcdfValueKey> valuesToSave = new ArrayList<>(  );

        private NetcdfFileWriter outputWriter = null;
        private final TimeWindow window;
        private final ReentrantLock creationLock;
        private final ReentrantLock writeLock;

        @Override
        public void close() throws IOException
        {
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
