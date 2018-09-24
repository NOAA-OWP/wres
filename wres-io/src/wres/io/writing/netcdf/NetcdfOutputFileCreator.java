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
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.io.config.ConfigHelper;

class NetcdfOutputFileCreator
{
    private NetcdfOutputFileCreator(){}

    private static final Logger
            LOGGER = LoggerFactory.getLogger( NetcdfOutputFileCreator.class);
    private static final Object CREATION_LOCK = new Object();

    static String create( final String templatePath,
                                    final DestinationConfig destinationConfig,
                                    final TimeWindow window,
                                    final ZonedDateTime analysisTime,
                                    final Collection<MetricVariable> metricVariables,
                                    final Collection<DoubleScoreStatistic> output,
                                    final ChronoUnit durationUnits )
            throws IOException
    {
        // We're locking because each created output will be using the same
        // template. We're only reading from the template, though, so it may be
        // worth experimenting with no locking
        synchronized ( NetcdfOutputFileCreator.CREATION_LOCK )
        {
            Path targetPath = ConfigHelper.getOutputPathToWriteForOneTimeWindow( destinationConfig,
                                                                                 window,
                                                                                 output,
                                                                                 durationUnits );
            if ( targetPath.toFile().exists())
            {
                LOGGER.warn("The file '{}' will be overwritten.", targetPath);
                Files.deleteIfExists( targetPath );
            }

            LOGGER.debug("Opening a copier to create a new file at {} based off of {}.", targetPath, templatePath);

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

                NetcdfFileWriter writer = copier.write();

                ArrayInt.D1 duration = new ArrayInt.D1( 1, false );
                duration.set( 0,
                              ( int ) window.getLatestLeadTime().toMinutes() );

                try
                {
                    writer.write( "time", duration );
                }
                catch ( InvalidRangeException e )
                {
                    throw new IOException(
                            "The lead time could not be written to the output." );
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
                    throw new IOException(
                            "The analysis time could not be written to the output." );
                }

                writer.flush();

                location = writer.getNetcdfFile().getLocation();
            }

            return location;

        }
    }

    static String getMetricVariableName( final String metricName,
                                                 final OneOrTwoThresholds thresholds)
    {
        // We start with the raw name of the metric
        String name = metricName.replace(" ", "_");

        // If the calling metric is associated with a threshold, combine
        // threshold information with it
        if (thresholds != null && thresholds.first() != null && thresholds.first().hasProbabilities())
        {
            // We first get the probability in raw percentage, i.e., 0.95 becomes 95
            Double probability = thresholds.first().getProbabilities().first() * 100.0;

            // We now want to indicate that it is a probability and attach
            // the integer representation. If the probability ended up
            // being 37.25, we want the name "_Pr_37" not "_Pr_37.25"
            name += "_Pr_" + probability.toString().replaceAll( "\\.", "" );
        }

        return name;
    }
}
