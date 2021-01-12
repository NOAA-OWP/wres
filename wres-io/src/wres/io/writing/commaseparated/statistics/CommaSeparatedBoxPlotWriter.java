package wres.io.writing.commaseparated.statistics;

import java.io.IOException;
import java.nio.file.Path;
import java.text.Format;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.function.Function;
import java.util.stream.Collectors;

import wres.config.ProjectConfigException;
import wres.config.ProjectConfigs;
import wres.config.generated.DestinationConfig;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.StatisticType;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.Slicer;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.time.TimeWindowOuter;
import wres.io.config.ConfigHelper;
import wres.io.writing.commaseparated.CommaSeparatedUtilities;
import wres.statistics.generated.BoxplotMetric.LinkedValueType;
import wres.statistics.generated.BoxplotMetric.QuantileValueType;
import wres.statistics.generated.BoxplotStatistic.Box;

/**
 * Helps write box plots comprising {@link BoxplotStatisticOuter} to a file of Comma Separated Values (CSV).
 * 
 * @author james.brown@hydrosolved.com
 */

public class CommaSeparatedBoxPlotWriter extends CommaSeparatedStatisticsWriter
        implements Function<List<BoxplotStatisticOuter>, Set<Path>>
{

    /**
     * Returns an instance of a writer.
     * 
     * @param projectConfig the project configuration
     * @param durationUnits the time units for durations
     * @param outputDirectory the directory into which to write
     * @return a writer
     * @throws NullPointerException if either input is null 
     * @throws ProjectConfigException if the project configuration is not valid for writing
     */

    public static CommaSeparatedBoxPlotWriter of( ProjectConfig projectConfig,
                                                  ChronoUnit durationUnits,
                                                  Path outputDirectory )
    {
        return new CommaSeparatedBoxPlotWriter( projectConfig, durationUnits, outputDirectory );
    }

    /**
     * Writes all output for one box plot type.
     *
     * @param output the box plot output
     * @throws NullPointerException if the input is null
     * @throws CommaSeparatedWriteException if the output cannot be written
     */

    @Override
    public Set<Path> apply( final List<BoxplotStatisticOuter> output )
    {
        Objects.requireNonNull( output, "Specify non-null input data when writing box plot outputs." );

        Set<Path> paths = new HashSet<>();

        // Write output
        // In principle, each destination could have a different formatter, so 
        // the output must be generated separately for each destination
        List<DestinationConfig> numericalDestinations =
                ProjectConfigs.getNumericalDestinations( super.getProjectConfig() );

        for ( DestinationConfig destinationConfig : numericalDestinations )
        {
            // Formatter
            Format formatter = ConfigHelper.getDecimalFormatter( destinationConfig );

            // Write the output
            try
            {
                // Group the statistics by the LRB context in which they appear. There will be one path written
                // for each group (e.g., one path for each window with LeftOrRightOrBaseline.RIGHT data and one for 
                // each window with LeftOrRightOrBaseline.BASELINE data): #48287
                Map<LeftOrRightOrBaseline, List<BoxplotStatisticOuter>> groups =
                        Slicer.getStatisticsGroupedByContext( output );

                for ( List<BoxplotStatisticOuter> nextGroup : groups.values() )
                {
                    Set<Path> innerPathsWrittenTo =
                            CommaSeparatedBoxPlotWriter.writeOneBoxPlotOutputType( super.getOutputDirectory(),
                                                                                   nextGroup,
                                                                                   formatter,
                                                                                   super.getDurationUnits() );
                    paths.addAll( innerPathsWrittenTo );
                }
            }
            catch ( IOException e )
            {
                throw new CommaSeparatedWriteException( "While writing comma separated output: ", e );
            }
        }

        return Collections.unmodifiableSet( paths );
    }

    /**
     * Writes all output for one box plot type.
     *
     * @param outputDirectory the directory into which to write
     * @param output the box plot output to iterate through
     * @param formatter optional formatter, can be null
     * @param durationUnits the time units for durations
     * @throws IOException if the output cannot be written 
     */

    private static Set<Path> writeOneBoxPlotOutputType( Path outputDirectory,
                                                        List<BoxplotStatisticOuter> output,
                                                        Format formatter,
                                                        ChronoUnit durationUnits )
            throws IOException
    {
        Set<Path> pathsWrittenTo = new HashSet<>();

        // Distinguish between pooled outputs and outputs per pair
        // TODO: this will not be necessary once all pools are written to a single destination

        // Iterate through types per pair
        List<BoxplotStatisticOuter> perPair = Slicer.filter( output,
                                                             next -> next.getMetricName()
                                                                         .isInGroup( StatisticType.BOXPLOT_PER_PAIR ) );

        SortedSet<MetricConstants> metricsPerPair = Slicer.discover( perPair, BoxplotStatisticOuter::getMetricName );

        for ( MetricConstants next : metricsPerPair )
        {
            Set<Path> innerPathsWrittenTo =
                    CommaSeparatedBoxPlotWriter.writeOneBoxPlotOutputTypePerTimeWindow( outputDirectory,
                                                                                        Slicer.filter( output, next ),
                                                                                        formatter,
                                                                                        durationUnits );
            pathsWrittenTo.addAll( innerPathsWrittenTo );
        }

        // Iterate through the pool types
        List<BoxplotStatisticOuter> perPool = output.stream()
                                                    .filter( next -> next.getMetricName()
                                                                         .isInGroup( StatisticType.BOXPLOT_PER_POOL ) )
                                                    .collect( Collectors.toList() );

        SortedSet<MetricConstants> metricsPerPool = Slicer.discover( perPool, BoxplotStatisticOuter::getMetricName );
        for ( MetricConstants next : metricsPerPool )
        {
            Set<Path> innerPathsWrittenTo =
                    CommaSeparatedBoxPlotWriter.writeOneBoxPlotOutputTypePerMetric( outputDirectory,
                                                                                    Slicer.filter( output, next ),
                                                                                    formatter,
                                                                                    durationUnits );
            pathsWrittenTo.addAll( innerPathsWrittenTo );
        }

        return Collections.unmodifiableSet( pathsWrittenTo );
    }

    /**
     * Writes one box plot for all thresholds at each time window in the input.
     *
     * @param outputDirectory the directory into which to write  
     * @param output the box plot output
     * @param formatter optional formatter, can be null
     * @param durationUnits the time units for durations
     * @throws IOException if the output cannot be written
     * @return set of paths actually written to
     */

    private static Set<Path> writeOneBoxPlotOutputTypePerTimeWindow( Path outputDirectory,
                                                                     List<BoxplotStatisticOuter> output,
                                                                     Format formatter,
                                                                     ChronoUnit durationUnits )
            throws IOException
    {
        Set<Path> pathsWrittenTo = new HashSet<>( 1 );

        // Loop across time windows
        SortedSet<TimeWindowOuter> timeWindows =
                Slicer.discover( output, meta -> meta.getMetadata().getTimeWindow() );
        for ( TimeWindowOuter nextWindow : timeWindows )
        {
            List<BoxplotStatisticOuter> next =
                    Slicer.filter( output, data -> data.getMetadata().getTimeWindow().equals( nextWindow ) );

            MetricConstants metricName = next.get( 0 ).getMetricName();
            SampleMetadata meta = next.get( 0 ).getMetadata();

            StringJoiner headerRow =
                    CommaSeparatedUtilities.getTimeWindowHeaderFromSampleMetadata( meta,
                                                                                   durationUnits );
            List<RowCompareByLeft> rows =
                    CommaSeparatedBoxPlotWriter.getRowsForOneBoxPlot( next, formatter, durationUnits );

            // Add the header row
            rows.add( RowCompareByLeft.of( HEADER_INDEX,
                                           CommaSeparatedBoxPlotWriter.getBoxPlotHeader( next, headerRow ) ) );
            // Write the output
            Path outputPath =
                    DataFactory.getPathFromSampleMetadata( outputDirectory,
                                                           meta,
                                                           nextWindow,
                                                           durationUnits,
                                                           metricName,
                                                           null );

            Path finishedPath = CommaSeparatedStatisticsWriter.writeTabularOutputToFile( rows, outputPath );
            // If writeTabularOutputToFile did not throw an exception, assume
            // it succeeded in writing to the file, track outputs now.
            pathsWrittenTo.add( finishedPath );
        }

        return Collections.unmodifiableSet( pathsWrittenTo );
    }

    /**
     * Writes all box plot output into a single destination for one metric.
     *
     * @param outputDirectory the directory into which to write  
     * @param output the box plot output
     * @param formatter optional formatter, can be null
     * @param durationUnits the time units for durations
     * @throws IOException if the output cannot be written
     * @return set of paths actually written to
     */

    private static Set<Path> writeOneBoxPlotOutputTypePerMetric( Path outputDirectory,
                                                                 List<BoxplotStatisticOuter> output,
                                                                 Format formatter,
                                                                 ChronoUnit durationUnits )
            throws IOException
    {
        Set<Path> pathsWrittenTo = new HashSet<>( 1 );

        if ( !output.isEmpty() )
        {
            MetricConstants metricName = output.get( 0 ).getMetricName();
            SampleMetadata meta = output.get( 0 ).getMetadata();

            StringJoiner headerRow =
                    CommaSeparatedUtilities.getTimeWindowHeaderFromSampleMetadata( meta,
                                                                                   durationUnits );
            List<RowCompareByLeft> rows =
                    CommaSeparatedBoxPlotWriter.getRowsForOneBoxPlot( output, formatter, durationUnits );

            // Add the header row
            rows.add( RowCompareByLeft.of( HEADER_INDEX,
                                           CommaSeparatedBoxPlotWriter.getBoxPlotHeader( output, headerRow ) ) );
            // Write the output
            Path outputPath = DataFactory.getPathFromSampleMetadata( outputDirectory,
                                                                     meta,
                                                                     metricName,
                                                                     null );

            Path finishedPath = CommaSeparatedStatisticsWriter.writeTabularOutputToFile( rows, outputPath );
            // If writeTabularOutputToFile did not throw an exception, assume
            // it succeeded in writing to the file, track outputs now.
            pathsWrittenTo.add( finishedPath );
        }

        return Collections.unmodifiableSet( pathsWrittenTo );
    }

    /**
     * Returns the results for one box plot output.
     *
     * @param output the box plot output
     * @param formatter optional formatter, can be null
     * @param durationUnits the time units for durations
     * @return the rows to write
     */

    private static List<RowCompareByLeft>
            getRowsForOneBoxPlot( List<BoxplotStatisticOuter> output,
                                  Format formatter,
                                  ChronoUnit durationUnits )
    {
        List<RowCompareByLeft> returnMe = new ArrayList<>();

        // Discover the time windows and thresholds to loop
        SortedSet<OneOrTwoThresholds> thresholds =
                Slicer.discover( output, meta -> meta.getMetadata().getThresholds() );
        SortedSet<TimeWindowOuter> timeWindows =
                Slicer.discover( output, meta -> meta.getMetadata().getTimeWindow() );

        SampleMetadata metadata = CommaSeparatedStatisticsWriter.getSampleMetadataFromListOfStatistics( output );

        // Loop across the thresholds
        for ( OneOrTwoThresholds t : thresholds )
        {
            // Loop across time windows
            for ( TimeWindowOuter timeWindow : timeWindows )
            {
                BoxplotStatisticOuter nextValues = Slicer.filter( output,
                                                                  next -> next.getMetadata()
                                                                              .getThresholds()
                                                                              .equals( t )
                                                                          && next.getMetadata()
                                                                                 .getTimeWindow()
                                                                                 .equals( timeWindow ) )
                                                         .get( 0 );

                boolean hasLinkedValue = nextValues.getData().getMetric().getLinkedValueType() != LinkedValueType.NONE;

                // Add each box
                for ( Box nextBox : nextValues.getData().getStatisticsList() )
                {
                    List<Double> data = new ArrayList<>();

                    // Add linked value if available
                    if ( hasLinkedValue )
                    {
                        data.add( nextBox.getLinkedValue() );
                    }

                    data.addAll( nextBox.getQuantilesList() );
                    CommaSeparatedStatisticsWriter.addRowToInput( returnMe,
                                                                  SampleMetadata.of( metadata, timeWindow ),
                                                                  data,
                                                                  formatter,
                                                                  false,
                                                                  durationUnits );
                }
            }
        }

        return returnMe;
    }

    /**
     * Helper that mutates the header for box plots based on the input.
     * 
     * @param output the box plot output
     * @param headerRow the header row
     * @return the mutated header
     */

    private static StringJoiner getBoxPlotHeader( List<BoxplotStatisticOuter> output,
                                                  StringJoiner headerRow )
    {
        // Append to header
        StringJoiner returnMe = new StringJoiner( "," );

        returnMe.add( "FEATURE DESCRIPTION" );

        returnMe.merge( headerRow );

        // Discover the first item and use this to help
        BoxplotStatisticOuter nextValues = output.get( 0 );
        SortedSet<OneOrTwoThresholds> thresholds =
                Slicer.discover( output, next -> next.getMetadata().getThresholds() );

        LinkedValueType linkedValueType = nextValues.getData().getMetric().getLinkedValueType();
        QuantileValueType quantileValueType = nextValues.getData().getMetric().getQuantileValueType();
        List<Double> headerProbabilities = nextValues.getData().getMetric().getQuantilesList();
        String lType = linkedValueType.toString().replace( "_", " " );
        String qType = quantileValueType.toString().replace( "_", " " );

        if ( nextValues.getData().getStatisticsCount() != 0 )
        {
            for ( OneOrTwoThresholds nextThreshold : thresholds )
            {

                // Probabilities and types are fixed for all boxes in the collection
                if ( linkedValueType != LinkedValueType.NONE )
                {
                    returnMe.add( HEADER_DELIMITER + lType
                                  + HEADER_DELIMITER
                                  + nextThreshold );
                }

                for ( double nextProb : headerProbabilities )
                {
                    returnMe.add( HEADER_DELIMITER + qType
                                  + HEADER_DELIMITER
                                  + nextThreshold
                                  + HEADER_DELIMITER
                                  + "QUANTILE Pr="
                                  + nextProb );
                }
            }
        }

        return returnMe;
    }

    /**
     * Hidden constructor.
     * 
     * @param projectConfig the project configuration
     * @param durationUnits the duration units
     * @param outputDirectory the directory into which to write
     * @throws NullPointerException if either input is null 
     * @throws ProjectConfigException if the project configuration is not valid for writing 
     */

    private CommaSeparatedBoxPlotWriter( ProjectConfig projectConfig,
                                         ChronoUnit durationUnits,
                                         Path outputDirectory )
    {
        super( projectConfig, durationUnits, outputDirectory );
    }

}
