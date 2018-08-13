package wres.io.writing.netcdf;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.SortedSet;
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
import wres.datamodel.Slicer;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.ListOfMetricOutput;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.io.utilities.NoDataException;

class NetcdfOutputFileCreator
{
    private NetcdfOutputFileCreator(){}

    private static final Logger
            LOGGER = LoggerFactory.getLogger( NetcdfOutputFileCreator.class);
    private static final Object CREATION_LOCK = new Object();

    static String create( final String templatePath,
                                    final DestinationConfig destinationConfig,
                                    final Duration earliestLeadTime,
                                    final Duration latestLeadTime,
                                    final ZonedDateTime analysisTime,
                                    final ListOfMetricOutput<DoubleScoreOutput> output)
            throws IOException
    {
        // We're locking because each created output will be using the same
        // template. We're only reading from the template, though, so it may be
        // worth experimenting with no locking
        synchronized ( NetcdfOutputFileCreator.CREATION_LOCK )
        {
            Path targetPath = getFileName( destinationConfig, earliestLeadTime, latestLeadTime, output );
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
            SortedSet<MetricConstants> metrics = Slicer.discover( output, next -> next.getMetadata().getMetricID() );
            
            for ( MetricConstants nextMetric : metrics )
            {
                // Obtain the values for the next metric
                ListOfMetricOutput<DoubleScoreOutput> values = Slicer.filter( output, nextMetric );

                // Obtain the unique pairs of thresholds and time windows available in the output
                // Discover the available pairs of time windows and thresholds
                SortedSet<Pair<TimeWindow, OneOrTwoThresholds>> pairsOfTimesAndThresholds =
                        Slicer.discover( output,
                                         next -> Pair.of( next.getMetadata().getTimeWindow(),
                                                          next.getMetadata().getThresholds() ) );
                
                // Find the metadata for the first element, which is sufficient here
                MetricOutputMetadata meta = values.getData().get( 0 ).getMetadata();
                
                // Now iterate through each threshold for said metric
                for ( Pair<TimeWindow, OneOrTwoThresholds> nextPair : pairsOfTimesAndThresholds )
                {
                    // TODO: Use getOutputDimension(), not getInputDimension()
                    // Extra handling is required in cases where there is no output dimension
                    NetcdfOutputFileCreator.addVariable( copier,
                                                         nextMetric,
                                                         meta.getMeasurementUnit().getUnit(),
                                                         nextPair.getLeft(),
                                                         nextPair.getRight() );
                }
            }

            NetcdfFileWriter writer = copier.write();

            ArrayInt.D1 duration = new ArrayInt.D1( 1, false );
            duration.set( 0, (int)latestLeadTime.toMinutes() );

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

            writer.flush();

            String location = writer.getNetcdfFile().getLocation();
            writer.close();

            return location;

        }
    }

    private static Path getFileName(final DestinationConfig destinationConfig,
                                      final Duration earliestLeadTime,
                                      final Duration latestLeadTime,
                                      final ListOfMetricOutput<DoubleScoreOutput> output)
            throws NoDataException
    {
        if( output.getData().isEmpty() )
        {
            throw new NoDataException("No data available to write.");
        }
        
        StringJoiner filename = new StringJoiner("_", "wres.", ".nc");
        filename.add( output.getData().get( 0 ).getMetadata().getIdentifier().getVariableID() );
        filename.add("lead");
        filename.add(latestLeadTime.toString());

        if (!latestLeadTime.equals( Instant.MAX ))
        {
            String lastTime = latestLeadTime.toString();

            // TODO: Format the last time in the style of "20180505T2046"
            // instead of "2018-05-05 20:46:00.000-0000"
            lastTime = lastTime.replaceAll( "-", "" ).replaceAll( ":", "" );

            filename.add(lastTime);
        }

        return Paths.get( destinationConfig.getPath(), filename.toString());
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

    //Map.Entry<Pair<TimeWindow, OneOrTwoThresholds>, DoubleScoreOutput> output
    
    private static void addVariable(final NetCDFCopier copier,
                             final MetricConstants metric,
                             final String measurementUnit,
                             final TimeWindow timeWindow,
                             final OneOrTwoThresholds threshold )
            throws IOException
    {
        String name = NetcdfOutputFileCreator.getMetricVariableName( metric.toString(), threshold );

        String longName = metric.toString() + " " + threshold;
        String firstCondition = threshold.first().getCondition().name();
        String secondCondition = "None";

        Double firstBound = threshold.first().getValues().first();
        Double secondBound = null;
        
        String firstDataType = threshold.first().getDataType().name();
        String secondDataType = "None";
        
        // Two thresholds available
        if (threshold.hasTwo())
        {
            secondCondition = threshold.second().getCondition().name();
            secondBound = threshold.second().getValues().first();
            secondDataType = threshold.second().getDataType().name();
        }
        
        // We only add timing information until we get a lead variable in
        String earliestTime = "ALL";
        String latestTime = "ALL";
        int earliestLead = (int)timeWindow.getEarliestLeadTimeInHours();
        int latestLead = (int)timeWindow.getLatestLeadTimeInHours();

        if (!timeWindow.getEarliestTime().equals( Instant.MIN ))
        {
            earliestTime = timeWindow.getEarliestTime().toString();
        }

        if (!timeWindow.getLatestTime().equals( Instant.MAX ))
        {
            latestTime = timeWindow.getLatestTime().toString();
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


        copier.addVariable( name, DataType.FLOAT, copier.getMetricDimensionNames(), attributes );

    }
}
