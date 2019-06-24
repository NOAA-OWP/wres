package wres.io.writing.commaseparated.statistics;

import java.io.IOException;
import java.nio.file.Path;
import java.text.Format;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import wres.config.ProjectConfigException;
import wres.config.generated.DestinationConfig;
import wres.config.generated.ProjectConfig;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.StatisticGroup;
import wres.datamodel.Slicer;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.statistics.BoxPlotStatistic;
import wres.datamodel.statistics.BoxPlotStatistics;
import wres.datamodel.statistics.ListOfStatistics;
import wres.datamodel.statistics.StatisticMetadata;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.time.TimeWindow;
import wres.io.config.ConfigHelper;
import wres.io.writing.commaseparated.CommaSeparatedUtilities;

/**
 * Helps write box plots comprising {@link BoxPlotStatistics} to a file of Comma Separated Values (CSV).
 * 
 * @author james.brown@hydrosolved.com
 */

public class CommaSeparatedBoxPlotWriter extends CommaSeparatedStatisticsWriter
        implements Consumer<ListOfStatistics<BoxPlotStatistics>>, Supplier<Set<Path>>
{

    /**
     * Set of paths that this writer actually wrote to
     */
    private final Set<Path> pathsWrittenTo = new HashSet<>();

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
    public void accept( final ListOfStatistics<BoxPlotStatistics> output )
    {
        Set<Path> pathsWrittenTo = new HashSet<>( 1 );

        Objects.requireNonNull( output, "Specify non-null input data when writing box plot outputs." );

        // Write output
        // In principle, each destination could have a different formatter, so 
        // the output must be generated separately for each destination
        List<DestinationConfig> numericalDestinations =
                ConfigHelper.getNumericalDestinations( super.getProjectConfig() );

        for ( DestinationConfig destinationConfig : numericalDestinations )
        {
            // Formatter
            Format formatter = ConfigHelper.getDecimalFormatter( destinationConfig );

            // Write the output
            try
            {
                Set<Path> innerPathsWrittenTo =
                        CommaSeparatedBoxPlotWriter.writeOneBoxPlotOutputType( super.getOutputDirectory(),
                                                                               destinationConfig,
                                                                               output,
                                                                               formatter,
                                                                               super.getDurationUnits() );
                pathsWrittenTo.addAll( innerPathsWrittenTo );
            }
            catch ( IOException e )
            {
                throw new CommaSeparatedWriteException( "While writing comma separated output: ", e );
            }
        }

        this.pathsWrittenTo.addAll( pathsWrittenTo );
    }

    /**
     * Return a snapshot of the paths written to (so far)
     * 
     * @return the paths written so far.
     */

    @Override
    public Set<Path> get()
    {
        return this.getPathsWrittenTo();
    }

    /**
     * Writes all output for one box plot type.
     *
     * @param outputDirectory the directory into which to write
     * @param destinationConfig the destination configuration    
     * @param output the box plot output to iterate through
     * @param formatter optional formatter, can be null
     * @param durationUnits the time units for durations
     * @throws IOException if the output cannot be written 
     */

    private static Set<Path> writeOneBoxPlotOutputType( Path outputDirectory,
                                                        DestinationConfig destinationConfig,
                                                        ListOfStatistics<BoxPlotStatistics> output,
                                                        Format formatter,
                                                        ChronoUnit durationUnits )
            throws IOException
    {
        Set<Path> pathsWrittenTo = new HashSet<>( 1 );

        // Distinguish between pooled outputs and outputs per pair
        // TODO: this will not be necessary once all pools are written to a single destination

        // Iterate through types per pair
        ListOfStatistics<BoxPlotStatistics> perPair =
                Slicer.filter( output, meta -> meta.getMetricID().isInGroup( StatisticGroup.BOXPLOT_PER_PAIR ) );

        SortedSet<MetricConstants> metricsPerPair =
                Slicer.discover( perPair, meta -> meta.getMetadata().getMetricID() );
        for ( MetricConstants next : metricsPerPair )
        {
            Set<Path> innerPathsWrittenTo =
                    CommaSeparatedBoxPlotWriter.writeOneBoxPlotOutputTypePerTimeWindow( outputDirectory,
                                                                                        destinationConfig,
                                                                                        Slicer.filter( output, next ),
                                                                                        formatter,
                                                                                        durationUnits );
            pathsWrittenTo.addAll( innerPathsWrittenTo );
        }

        // Iterate through the pool types
        ListOfStatistics<BoxPlotStatistics> perPool =
                Slicer.filter( output, meta -> meta.getMetricID().isInGroup( StatisticGroup.BOXPLOT_PER_POOL ) );

        SortedSet<MetricConstants> metricsPerPool =
                Slicer.discover( perPool, meta -> meta.getMetadata().getMetricID() );
        for ( MetricConstants next : metricsPerPool )
        {
            Set<Path> innerPathsWrittenTo =
                    CommaSeparatedBoxPlotWriter.writeOneBoxPlotOutputTypePerMetric( outputDirectory,
                                                                                    destinationConfig,
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
     * @param destinationConfig the destination configuration    
     * @param output the box plot output
     * @param formatter optional formatter, can be null
     * @param durationUnits the time units for durations
     * @throws IOException if the output cannot be written
     * @return set of paths actually written to
     */

    private static Set<Path> writeOneBoxPlotOutputTypePerTimeWindow( Path outputDirectory,
                                                                     DestinationConfig destinationConfig,
                                                                     ListOfStatistics<BoxPlotStatistics> output,
                                                                     Format formatter,
                                                                     ChronoUnit durationUnits )
            throws IOException
    {
        Set<Path> pathsWrittenTo = new HashSet<>( 1 );

        // Loop across time windows
        SortedSet<TimeWindow> timeWindows =
                Slicer.discover( output, meta -> meta.getMetadata().getSampleMetadata().getTimeWindow() );
        for ( TimeWindow nextWindow : timeWindows )
        {
            ListOfStatistics<BoxPlotStatistics> next =
                    Slicer.filter( output, data -> data.getSampleMetadata().getTimeWindow().equals( nextWindow ) );

            StatisticMetadata meta = next.getData().get( 0 ).getMetadata();

            StringJoiner headerRow =
                    CommaSeparatedUtilities.getPartialTimeWindowHeaderFromSampleMetadata( meta.getSampleMetadata(),
                                                                                          durationUnits );
            List<RowCompareByLeft> rows =
                    CommaSeparatedBoxPlotWriter.getRowsForOneBoxPlot( next, formatter, durationUnits );

            // Add the header row
            rows.add( RowCompareByLeft.of( HEADER_INDEX,
                                           CommaSeparatedBoxPlotWriter.getBoxPlotHeader( next, headerRow ) ) );
            // Write the output
            Path outputPath = ConfigHelper.getOutputPathToWrite( outputDirectory,
                                                                 destinationConfig,
                                                                 meta,
                                                                 nextWindow,
                                                                 durationUnits );

            CommaSeparatedStatisticsWriter.writeTabularOutputToFile( rows, outputPath );
            // If writeTabularOutputToFile did not throw an exception, assume
            // it succeeded in writing to the file, track outputs now.
            pathsWrittenTo.add( outputPath );
        }

        return Collections.unmodifiableSet( pathsWrittenTo );
    }

    /**
     * Writes all box plot output into a single destination for one metric.
     *
     * @param outputDirectory the directory into which to write
     * @param destinationConfig the destination configuration    
     * @param output the box plot output
     * @param formatter optional formatter, can be null
     * @param durationUnits the time units for durations
     * @throws IOException if the output cannot be written
     * @return set of paths actually written to
     */

    private static Set<Path> writeOneBoxPlotOutputTypePerMetric( Path outputDirectory,
                                                                 DestinationConfig destinationConfig,
                                                                 ListOfStatistics<BoxPlotStatistics> output,
                                                                 Format formatter,
                                                                 ChronoUnit durationUnits )
            throws IOException
    {
        Set<Path> pathsWrittenTo = new HashSet<>( 1 );

        if( !output.getData().isEmpty() )
        {
            StatisticMetadata meta = output.getData().get( 0 ).getMetadata();

            StringJoiner headerRow =
                    CommaSeparatedUtilities.getPartialTimeWindowHeaderFromSampleMetadata( meta.getSampleMetadata(),
                                                                                          durationUnits );
            List<RowCompareByLeft> rows =
                    CommaSeparatedBoxPlotWriter.getRowsForOneBoxPlot( output, formatter, durationUnits );

            // Add the header row
            rows.add( RowCompareByLeft.of( HEADER_INDEX,
                                           CommaSeparatedBoxPlotWriter.getBoxPlotHeader( output, headerRow ) ) );
            // Write the output
            Path outputPath = ConfigHelper.getOutputPathToWrite( outputDirectory,
                                                                 destinationConfig,
                                                                 meta );

            CommaSeparatedStatisticsWriter.writeTabularOutputToFile( rows, outputPath );
            // If writeTabularOutputToFile did not throw an exception, assume
            // it succeeded in writing to the file, track outputs now.
            pathsWrittenTo.add( outputPath );
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
            getRowsForOneBoxPlot( ListOfStatistics<BoxPlotStatistics> output,
                                  Format formatter,
                                  ChronoUnit durationUnits )
    {
        List<RowCompareByLeft> returnMe = new ArrayList<>();

        // Discover the time windows and thresholds to loop
        SortedSet<OneOrTwoThresholds> thresholds =
                Slicer.discover( output, meta -> meta.getMetadata().getSampleMetadata().getThresholds() );
        SortedSet<TimeWindow> timeWindows =
                Slicer.discover( output, meta -> meta.getMetadata().getSampleMetadata().getTimeWindow() );
        // Loop across the thresholds
        for ( OneOrTwoThresholds t : thresholds )
        {
            // Loop across time windows
            for ( TimeWindow timeWindow : timeWindows )
            {
                BoxPlotStatistics nextValues = Slicer.filter( output,
                                                              next -> next.getSampleMetadata()
                                                                          .getThresholds()
                                                                          .equals( t )
                                                                      && next.getSampleMetadata()
                                                                             .getTimeWindow()
                                                                             .equals( timeWindow ) )
                                                     .getData()
                                                     .get( 0 );
                // Add each box
                for ( BoxPlotStatistic nextBox : nextValues )
                {
                    List<Double> data = new ArrayList<>();
                    
                    // Add linked value if available
                    if( nextBox.hasLinkedValue() )
                    {
                        data.add( nextBox.getLinkedValue() );
                    }
                    
                    data.addAll( Arrays.stream( nextBox.getData().getDoubles() )
                                       .boxed()
                                       .collect( Collectors.toList() ) );
                    CommaSeparatedStatisticsWriter.addRowToInput( returnMe,
                                                                  timeWindow,
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

    private static StringJoiner getBoxPlotHeader( ListOfStatistics<BoxPlotStatistics> output,
                                                  StringJoiner headerRow )
    {
        // Append to header
        StringJoiner returnMe = new StringJoiner( "," );
        returnMe.merge( headerRow );

        // Discover the first item and use this to help
        BoxPlotStatistics nextValues = output.getData().get( 0 );
        SortedSet<OneOrTwoThresholds> thresholds =
                Slicer.discover( output, next -> next.getMetadata().getSampleMetadata().getThresholds() );

        if ( !nextValues.getData().isEmpty() )
        {
            for ( OneOrTwoThresholds nextThreshold : thresholds )
            {
                BoxPlotStatistic statistic = nextValues.getData().get( 0 );

                // Probabilities and types are fixed for all boxes in the collection
                if ( nextValues.getData().get( 0 ).hasLinkedValue() )
                {
                    returnMe.add( HEADER_DELIMITER + statistic.getLinkedValueType()
                                  + HEADER_DELIMITER
                                  + nextThreshold );
                }

                VectorOfDoubles headerProbabilities = statistic.getProbabilities();

                for ( double nextProb : headerProbabilities.getDoubles() )
                {
                    returnMe.add( HEADER_DELIMITER + statistic.getValueType()
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
     * Return a snapshot of the paths written to (so far)
     */

    private Set<Path> getPathsWrittenTo()
    {
        return Collections.unmodifiableSet( this.pathsWrittenTo );
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
