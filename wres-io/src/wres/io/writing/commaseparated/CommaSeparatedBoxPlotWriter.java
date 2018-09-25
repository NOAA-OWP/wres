package wres.io.writing.commaseparated;

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
import wres.datamodel.Slicer;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.metadata.StatisticMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.sampledata.pairs.EnsemblePair;
import wres.datamodel.statistics.BoxPlotStatistic;
import wres.datamodel.statistics.ListOfStatistics;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.io.config.ConfigHelper;

/**
 * Helps write box plots comprising {@link BoxPlotStatistic} to a file of Comma Separated Values (CSV).
 * 
 * @author james.brown@hydrosolved.com
 */

public class CommaSeparatedBoxPlotWriter extends CommaSeparatedWriter
        implements Consumer<ListOfStatistics<BoxPlotStatistic>>, Supplier<Set<Path>>
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
     * @return a writer
     * @throws NullPointerException if either input is null 
     * @throws ProjectConfigException if the project configuration is not valid for writing
     */

    public static CommaSeparatedBoxPlotWriter of( final ProjectConfig projectConfig, final ChronoUnit durationUnits )
    {
        return new CommaSeparatedBoxPlotWriter( projectConfig, durationUnits );
    }

    /**
     * Writes all output for one box plot type.
     *
     * @param output the box plot output
     * @throws NullPointerException if the input is null
     * @throws CommaSeparatedWriteException if the output cannot be written
     */

    @Override
    public void accept( final ListOfStatistics<BoxPlotStatistic> output )
    {
        Set<Path> pathsWrittenTo = new HashSet<>( 1 );

        Objects.requireNonNull( output, "Specify non-null input data when writing box plot outputs." );

        // Write output
        // In principle, each destination could have a different formatter, so 
        // the output must be generated separately for each destination
        List<DestinationConfig> numericalDestinations =
                ConfigHelper.getNumericalDestinations( this.getProjectConfig() );
        for ( DestinationConfig destinationConfig : numericalDestinations )
        {
            // Formatter
            Format formatter = ConfigHelper.getDecimalFormatter( destinationConfig );

            // Write the output
            try
            {
                Set<Path> innerPathsWrittenTo =
                        CommaSeparatedBoxPlotWriter.writeOneBoxPlotOutputType( destinationConfig,
                                                                               output,
                                                                               formatter,
                                                                               this.getDurationUnits() );
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
     * @param destinationConfig the destination configuration    
     * @param output the box plot output to iterate through
     * @param formatter optional formatter, can be null
     * @param durationUnits the time units for durations
     * @throws IOException if the output cannot be written 
     */

    private static Set<Path> writeOneBoxPlotOutputType( DestinationConfig destinationConfig,
                                                        ListOfStatistics<BoxPlotStatistic> output,
                                                        Format formatter,
                                                        ChronoUnit durationUnits )
            throws IOException
    {
        Set<Path> pathsWrittenTo = new HashSet<>( 1 );

        // Iterate through types
        SortedSet<MetricConstants> metrics = Slicer.discover( output, meta -> meta.getMetadata().getMetricID() );
        for ( MetricConstants next : metrics )
        {
            Set<Path> innerPathsWrittenTo =
                    CommaSeparatedBoxPlotWriter.writeOneBoxPlotOutputTypePerTimeWindow( destinationConfig,
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
     * @param destinationConfig the destination configuration    
     * @param output the box plot output
     * @param formatter optional formatter, can be null
     * @param durationUnits the time units for durations
     * @throws IOException if the output cannot be written
     * @return set of paths actually written to
     */

    private static Set<Path> writeOneBoxPlotOutputTypePerTimeWindow( DestinationConfig destinationConfig,
                                                                     ListOfStatistics<BoxPlotStatistic> output,
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
            ListOfStatistics<BoxPlotStatistic> next =
                    Slicer.filter( output, data -> data.getSampleMetadata().getTimeWindow().equals( nextWindow ) );

            StatisticMetadata meta = next.getData().get( 0 ).getMetadata();

            StringJoiner headerRow =
                    CommaSeparatedWriter.getDefaultHeaderFromSampleMetadata( meta.getSampleMetadata(), durationUnits );
            List<RowCompareByLeft> rows =
                    CommaSeparatedBoxPlotWriter.getRowsForOneBoxPlot( next, formatter, durationUnits );

            // Add the header row
            rows.add( RowCompareByLeft.of( HEADER_INDEX,
                                           CommaSeparatedBoxPlotWriter.getBoxPlotHeader( next, headerRow ) ) );
            // Write the output
            Path outputPath = ConfigHelper.getOutputPathToWrite( destinationConfig,
                                                                 meta,
                                                                 nextWindow,
                                                                 durationUnits );

            CommaSeparatedWriter.writeTabularOutputToFile( rows, outputPath );
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
            getRowsForOneBoxPlot( ListOfStatistics<BoxPlotStatistic> output,
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
                BoxPlotStatistic nextValues = Slicer.filter( output,
                                                             next -> next.getSampleMetadata()
                                                                         .getThresholds()
                                                                         .equals( t )
                                                                     && next.getSampleMetadata()
                                                                            .getTimeWindow()
                                                                            .equals( timeWindow ) )
                                                    .getData()
                                                    .get( 0 );
                // Add each box
                for ( EnsemblePair nextBox : nextValues )
                {
                    List<Double> data = new ArrayList<>();
                    data.add( nextBox.getLeft() );
                    data.addAll( Arrays.stream( nextBox.getRight() ).boxed().collect( Collectors.toList() ) );
                    CommaSeparatedWriter.addRowToInput( returnMe,
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

    private static StringJoiner getBoxPlotHeader( ListOfStatistics<BoxPlotStatistic> output,
                                                  StringJoiner headerRow )
    {
        // Append to header
        StringJoiner returnMe = new StringJoiner( "," );
        returnMe.merge( headerRow );
        // Discover the first item and use this to help
        BoxPlotStatistic nextValues = output.getData().get( 0 );
        SortedSet<OneOrTwoThresholds> thresholds =
                Slicer.discover( output, next -> next.getMetadata().getSampleMetadata().getThresholds() );
        for ( OneOrTwoThresholds nextThreshold : thresholds )
        {
            returnMe.add( HEADER_DELIMITER + nextValues.getDomainAxisDimension() + HEADER_DELIMITER + nextThreshold );
            VectorOfDoubles headerProbabilities = nextValues.getProbabilities();
            for ( double nextProb : headerProbabilities.getDoubles() )
            {
                returnMe.add( HEADER_DELIMITER + nextValues.getRangeAxisDimension()
                              + HEADER_DELIMITER
                              + nextThreshold
                              + HEADER_DELIMITER
                              + "QUANTILE Pr="
                              + nextProb );
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
     * @throws NullPointerException if either input is null 
     * @throws ProjectConfigException if the project configuration is not valid for writing 
     */

    private CommaSeparatedBoxPlotWriter( ProjectConfig projectConfig, ChronoUnit durationUnits )
    {
        super( projectConfig, durationUnits );
    }

}
