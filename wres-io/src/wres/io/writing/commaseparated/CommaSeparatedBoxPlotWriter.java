package wres.io.writing.commaseparated;

import java.io.IOException;
import java.nio.file.Path;
import java.text.Format;
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
import java.util.stream.Collectors;

import wres.config.ProjectConfigException;
import wres.config.generated.DestinationConfig;
import wres.config.generated.ProjectConfig;
import wres.datamodel.MetricConstants;
import wres.datamodel.Slicer;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.inputs.pairs.EnsemblePair;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.BoxPlotOutput;
import wres.datamodel.outputs.ListOfMetricOutput;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.io.config.ConfigHelper;

/**
 * Helps write box plots comprising {@link BoxPlotOutput} to a file of Comma Separated Values (CSV).
 * 
 * @author james.brown@hydrosolved.com
 */

public class CommaSeparatedBoxPlotWriter extends CommaSeparatedWriter
        implements Consumer<ListOfMetricOutput<BoxPlotOutput>>
{

    /**
     * Returns an instance of a writer.
     * 
     * @param projectConfig the project configuration
     * @return a writer
     * @throws NullPointerException if the input is null 
     * @throws ProjectConfigException if the project configuration is not valid for writing
     */

    public static CommaSeparatedBoxPlotWriter of( final ProjectConfig projectConfig )
    {
        return new CommaSeparatedBoxPlotWriter( projectConfig );
    }

    /**
     * Writes all output for one box plot type.
     *
     * @param output the box plot output
     * @throws NullPointerException if the input is null
     * @throws CommaSeparatedWriteException if the output cannot be written
     */

    @Override
    public void accept( final ListOfMetricOutput<BoxPlotOutput> output )
    {
        Objects.requireNonNull( output, "Specify non-null input data when writing box plot outputs." );

        // Write output
        // In principle, each destination could have a different formatter, so 
        // the output must be generated separately for each destination
        List<DestinationConfig> numericalDestinations = ConfigHelper.getNumericalDestinations( projectConfig );
        for ( DestinationConfig destinationConfig : numericalDestinations )
        {
            // Formatter
            Format formatter = ConfigHelper.getDecimalFormatter( destinationConfig );

            // Write the output
            try
            {
                CommaSeparatedBoxPlotWriter.writeOneBoxPlotOutputType( destinationConfig, output, formatter );
            }
            catch ( IOException e )
            {
                throw new CommaSeparatedWriteException( "While writing comma separated output: ", e );
            }
        }

    }

    /**
     * Writes all output for one box plot type.
     *
     * @param destinationConfig the destination configuration    
     * @param output the box plot output to iterate through
     * @param formatter optional formatter, can be null
     * @throws IOException if the output cannot be written 
     */

    private static void writeOneBoxPlotOutputType( DestinationConfig destinationConfig,
                                                   ListOfMetricOutput<BoxPlotOutput> output,
                                                   Format formatter )
            throws IOException
    {
        // Iterate through types
        SortedSet<MetricConstants> metrics = Slicer.discover( output, meta -> meta.getMetadata().getMetricID() );
        for ( MetricConstants next : metrics )
        {
            CommaSeparatedBoxPlotWriter.writeOneBoxPlotOutputTypePerTimeWindow( destinationConfig,
                                                                                Slicer.filter( output, next ),
                                                                                formatter );
        }
    }

    /**
     * Writes one box plot for all thresholds at each time window in the input.
     * 
     * @param destinationConfig the destination configuration    
     * @param output the box plot output
     * @param formatter optional formatter, can be null
     * @throws IOException if the output cannot be written
     * @return set of paths actually written to
     */

    private static Set<Path> writeOneBoxPlotOutputTypePerTimeWindow( DestinationConfig destinationConfig,
                                                                     ListOfMetricOutput<BoxPlotOutput> output,
                                                                     Format formatter )
            throws IOException
    {
        Set<Path> pathsWrittenTo = new HashSet<>( 1 );

        // Loop across time windows
        SortedSet<TimeWindow> timeWindows = Slicer.discover( output, meta -> meta.getMetadata().getTimeWindow() );
        for ( TimeWindow nextWindow : timeWindows )
        {
            ListOfMetricOutput<BoxPlotOutput> next =
                    Slicer.filter( output, data -> data.getTimeWindow().equals( nextWindow ) );
            
            MetricOutputMetadata meta = next.getData().get( 0 ).getMetadata();
            
            StringJoiner headerRow = new StringJoiner( "," );
            headerRow.merge( HEADER_DEFAULT );
            List<RowCompareByLeft> rows = CommaSeparatedBoxPlotWriter.getRowsForOneBoxPlot( next, formatter );

            // Add the header row
            rows.add( RowCompareByLeft.of( HEADER_INDEX,
                                           CommaSeparatedBoxPlotWriter.getBoxPlotHeader( next, headerRow ) ) );
            // Write the output
            Path outputPath = ConfigHelper.getOutputPathToWrite( destinationConfig, meta, nextWindow );

            Set<Path> innerPathsWrittenTo = CommaSeparatedWriter.writeTabularOutputToFile( rows, outputPath );
            pathsWrittenTo.addAll( innerPathsWrittenTo );
        }

        return Collections.unmodifiableSet( pathsWrittenTo );
    }

    /**
     * Returns the results for one box plot output.
     *
     * @param output the box plot output
     * @param formatter optional formatter, can be null
     * @return the rows to write
     */

    private static List<RowCompareByLeft>
            getRowsForOneBoxPlot( ListOfMetricOutput<BoxPlotOutput> output,
                                  Format formatter )
    {
        List<RowCompareByLeft> returnMe = new ArrayList<>();

        // Discover the time windows and thresholds to loop
        SortedSet<OneOrTwoThresholds> thresholds =
                Slicer.discover( output, meta -> meta.getMetadata().getThresholds() );
        SortedSet<TimeWindow> timeWindows = Slicer.discover( output, meta -> meta.getMetadata().getTimeWindow() );
        // Loop across the thresholds
        for ( OneOrTwoThresholds t : thresholds )
        {
            // Loop across time windows
            for ( TimeWindow timeWindow : timeWindows )
            {
                BoxPlotOutput nextValues = Slicer.filter( output,
                                                          next -> next.getThresholds().equals( t )
                                                                  && next.getTimeWindow().equals( timeWindow ) )
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
                                                        false );
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

    private static StringJoiner getBoxPlotHeader( ListOfMetricOutput<BoxPlotOutput> output,
                                                  StringJoiner headerRow )
    {
        // Append to header
        StringJoiner returnMe = new StringJoiner( "," );
        returnMe.merge( headerRow );
        // Discover the first item and use this to help
        BoxPlotOutput nextValues = output.getData().get( 0 );
        SortedSet<OneOrTwoThresholds> thresholds =
                Slicer.discover( output, next -> next.getMetadata().getThresholds() );
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
     * Hidden constructor.
     * 
     * @param projectConfig the project configuration
     * @throws ProjectConfigException if the project configuration is not valid for writing 
     */

    private CommaSeparatedBoxPlotWriter( ProjectConfig projectConfig )
    {
        super( projectConfig );
    }

}
