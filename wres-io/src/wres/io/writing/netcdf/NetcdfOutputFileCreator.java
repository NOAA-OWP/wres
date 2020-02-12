package wres.io.writing.netcdf;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ucar.ma2.ArrayInt;
import ucar.ma2.DataType;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFileWriter;
import wres.config.generated.DestinationConfig;
import wres.datamodel.time.TimeWindow;

class NetcdfOutputFileCreator
{
    private NetcdfOutputFileCreator(){}

    private static final Logger
            LOGGER = LoggerFactory.getLogger( NetcdfOutputFileCreator.class);
    private static final Object CREATION_LOCK = new Object();

    static String create( final String templatePath,
                          Path targetPath,
                          final DestinationConfig destinationConfig,
                          final TimeWindow window,
                          final ZonedDateTime analysisTime,
                          final Collection<MetricVariable> metricVariables,
                          final ChronoUnit durationUnits )
            throws IOException
    {
        // We're locking because each created output will be using the same
        // template. We're only reading from the template, though, so it may be
        // worth experimenting with no locking
        synchronized ( NetcdfOutputFileCreator.CREATION_LOCK )
        {
            if ( targetPath.toFile().exists() )
            {
                LOGGER.warn("The file '{}' will be overwritten.", targetPath);
                Files.deleteIfExists( targetPath );
            }

            LOGGER.debug( "Opening a copier to create a new file at {} based off of {}. The file will contain {} "
                          + "variables.",
                          targetPath,
                          templatePath,
                          metricVariables.size() );

            String location;

            try ( NetCDFCopier copier = new NetCDFCopier( templatePath, targetPath.toString(), analysisTime) )
            {
                for ( MetricVariable metricVariable : metricVariables )
                {
                    copier.addVariable( metricVariable.getName(),
                                        DataType.FLOAT,
                                        copier.getMetricDimensionNames(),
                                        metricVariable.getAttributes() );
                }

                // The writer isn't manually closed or held within a try-with-resources because the
                // copier that created it also closes it when it closes. This is done to ensure that the writer
                // is correctly flushed if not done prior
                NetcdfFileWriter writer = copier.write();

                ArrayInt.D1 duration = new ArrayInt.D1( 1, false );
                duration.set( 0,
                              ( int ) window.getLatestLeadDuration().toMinutes() );

                try
                {
                    writer.write( "time", duration );
                }
                catch ( InvalidRangeException e )
                {
                    throw new IOException("The lead time could not be written to the output.", e );
                }

                ArrayInt.D1 analysisMinutes = new ArrayInt.D1( 1, false );
                analysisMinutes.set( 0,
                                     ( int ) Duration.between( Instant.ofEpochSecond(
                                             0 ), analysisTime.toInstant() )
                                                     .toMinutes() );

                try
                {
                    writer.write( "analysis_time", analysisMinutes );
                }
                catch ( InvalidRangeException e )
                {
                    throw new IOException("The analysis time could not be written to the output.", e );
                }

                writer.flush();

                location = writer.getNetcdfFile().getLocation();
            }

            return location;

        }
    }
}
