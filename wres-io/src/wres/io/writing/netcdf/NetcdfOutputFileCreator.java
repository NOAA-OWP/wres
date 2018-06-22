package wres.io.writing.netcdf;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.StringJoiner;
import java.util.TreeMap;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.ArrayInt;
import ucar.ma2.DataType;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFileWriter;

import wres.config.generated.DestinationConfig;
import wres.datamodel.MetricConstants;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.MapKey;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMultiMapByTimeAndThreshold;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.io.utilities.NoDataException;

class NetcdfOutputFileCreator
{
    private NetcdfOutputFileCreator(){}

    private static final Logger
            LOGGER = LoggerFactory.getLogger( NetcdfOutputFileCreator.class);
    private static final Object CREATION_LOCK = new Object();

    static NetcdfFileWriter create( final String templatePath,
                                    final DestinationConfig destinationConfig,
                                    final TimeWindow window,
                                    final ZonedDateTime analysisTime,
                                    final MetricOutputMultiMapByTimeAndThreshold<DoubleScoreOutput> output)
            throws IOException
    {
        // We're locking because each created output will be using the same
        // template. We're only reading from the template, though, so it may be
        // worth experimenting with no locking
        synchronized ( NetcdfOutputFileCreator.CREATION_LOCK )
        {
            Path targetPath = getFileName( destinationConfig, window, output );
            if ( targetPath.toFile().exists())
            {
                LOGGER.warn("The file '{}' will be overwritten.", targetPath);
                Files.deleteIfExists( targetPath );
            }

            LOGGER.debug("Opening a copier to create a new file at {} based off of {}.", targetPath.toString(), templatePath);

            // This is essentially leaving the underlying copier open. We need
            // to close it, but we can't currently do so without closing the
            // writer. We need the copier to return the name of the target,
            // not the actual writer. We can then open a new one later.
            NetCDFCopier copier = new NetCDFCopier( templatePath, targetPath.toString(), analysisTime);

            // Iterate through each metric
            for ( Map.Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<DoubleScoreOutput>> metrics : output
                    .entrySet() )
            {
                MetricConstants metricConstants = metrics.getKey().getKey();

                MetricOutputMapByTimeAndThreshold<DoubleScoreOutput>
                        values = metrics.getValue();

                // Now iterate through each threshold for said metric
                for ( Map.Entry<Pair<TimeWindow, OneOrTwoThresholds>, DoubleScoreOutput> outputWindow : values.entrySet() )
                {
                    // TODO: Use getOutputDimension(), not getInputDimension()
                    // Extra handling is required in cases where there is no output dimension
                    NetcdfOutputFileCreator.addVariable(
                            copier,
                            metricConstants,
                            values.getMetadata()
                                  .getDimension()
                                  .getDimension(),
                            outputWindow
                    );
                }
            }

            NetcdfFileWriter writer = copier.write();

            ArrayInt.D1 duration = new ArrayInt.D1( 1, false );
            duration.set( 0, (int)window.getLatestLeadTime().toMinutes() );

            try
            {
                writer.write( "time", duration );
            }
            catch ( InvalidRangeException e )
            {
                throw new IOException( "The lead time could not be written to the output." );
            }

            ArrayInt.D1 analysisMinutes = new ArrayInt.D1(1, false);
            analysisMinutes.set( 0, (int)Duration.between( Instant.ofEpochSecond( 0 ), analysisTime.toInstant() ).toMinutes());

            try
            {
                writer.write( "analysis_time", analysisMinutes );
            }
            catch ( InvalidRangeException e )
            {
                throw new IOException( "The analysis time could not be written to the output." );
            }

            return writer;

        }
    }

    private static Path getFileName(final DestinationConfig destinationConfig,
                                      final TimeWindow window,
                                      final MetricOutputMultiMapByTimeAndThreshold<DoubleScoreOutput> output)
            throws NoDataException
    {
        StringJoiner filename = new StringJoiner("_", "wres.", ".nc");
        filename.add( NetcdfOutputFileCreator.getVariableName( output ) );
        filename.add("lead");
        filename.add(window.getLatestLeadTime().toString());

        if (!window.getLatestTime().equals( Instant.MAX ))
        {
            String lastTime = window.getLatestTime().toString();

            // TODO: Format the last time in the style of "20180505T2046"
            // instead of "2018-05-05 20:46:00.000-0000"
            lastTime = lastTime.replaceAll( "-", "" ).replaceAll( ":", "" );

            filename.add(lastTime);
        }

        return Paths.get( destinationConfig.getPath(), filename.toString());
    }

    private static String getVariableName( final MetricOutputMultiMapByTimeAndThreshold<DoubleScoreOutput> output )
            throws NoDataException
    {
        // We want to write data for each encountered metric
        for ( Map.Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<DoubleScoreOutput>> scoreOutput : output.entrySet() )
        {
            if (scoreOutput.getValue().getMetadata() != null)
            {
                return scoreOutput.getValue().getMetadata().getIdentifier().getVariableID();
            }
        }

        throw new NoDataException( "There wasn't any metadata attached to output for " + output.toString() );
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

    private static void addVariable(final NetCDFCopier copier,
                             final MetricConstants metric,
                             final String measurementUnit,
                             final Map.Entry<Pair<TimeWindow, OneOrTwoThresholds>, DoubleScoreOutput> output)
            throws IOException
    {
        String name = NetcdfOutputFileCreator.getMetricVariableName( metric.toString(), output.getKey().getRight() );

        String longName = metric.toString() + " " + output.getKey().getValue().toString();
        String firstCondition = output.getKey().getValue().first().getCondition().name();
        String secondCondition = "None";

        if (output.getKey().getValue().second() != null)
        {
            secondCondition = output.getKey().getValue().second().getCondition().name();
        }

        Double firstBound = output.getKey().getValue().first().getValues().first();
        Double secondBound = null;

        if (output.getKey().getValue().second() != null)
        {
            secondBound = output.getKey().getValue().second().getValues().first();
        }

        String firstDataType = output.getKey().getValue().first().getDataType().name();
        String secondDataType = "None";

        if (output.getKey().getValue().second() != null)
        {
            secondDataType = output.getKey().getValue().second().getDataType().name();
        }

        // We only add timing information until we get a lead variable in
        String earliestTime = "ALL";
        String latestTime = "ALL";
        int earliestLead = (int)output.getKey().getKey().getEarliestLeadTimeInHours();
        int latestLead = (int)output.getKey().getKey().getLatestLeadTimeInHours();

        if (!output.getKey().getKey().getEarliestTime().equals( Instant.MIN ))
        {
            earliestTime = output.getKey().getKey().getEarliestTime().toString();
        }

        if (!output.getKey().getKey().getLatestTime().equals( Instant.MAX ))
        {
            latestTime = output.getKey().getKey().getLatestTime().toString();
        }

        Map<String, Object> attributes = new TreeMap<>(  );

        attributes.put("latest_time", latestTime);
        attributes.put("earliest_time", earliestTime);
        attributes.put( "earliest_lead", earliestLead );
        attributes.put("latest_lead", latestLead);
        attributes.put("first_data_type", firstDataType);
        attributes.put("second_data_type", secondDataType);
        attributes.put("first_bound", firstBound);
        attributes.put("second_bound", secondBound);
        attributes.put("unit", measurementUnit);
        attributes.put("long_name", longName);
        attributes.put("first_condition", firstCondition);
        attributes.put("second_condition", secondCondition);

        copier.addVariable( name, DataType.DOUBLE, copier.getMetricDimensionNames(), attributes );

    }
}
