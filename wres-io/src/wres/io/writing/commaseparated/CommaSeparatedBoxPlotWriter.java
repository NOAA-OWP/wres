package wres.io.writing.commaseparated;

import java.io.IOException;
import java.nio.file.Path;
import java.text.Format;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;

import wres.config.ProjectConfigException;
import wres.config.generated.DestinationConfig;
import wres.config.generated.ProjectConfig;
import wres.datamodel.MetricConstants;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.BoxPlotOutput;
import wres.datamodel.outputs.MapKey;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMultiMapByTimeAndThreshold;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.io.config.ConfigHelper;

/**
 * Helps write box plots comprising {@link BoxPlotOutput} to a file of Comma Separated Values (CSV).
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 1.0
 */

public class CommaSeparatedBoxPlotWriter extends CommaSeparatedWriter
        implements Consumer<MetricOutputMultiMapByTimeAndThreshold<BoxPlotOutput>>
{

    /**
     * Returns an instance of a writer.
     * 
     * @param projectConfig the project configuration
     * @return a writer
     * @throws NullPointerException if the input is null 
     * @throws ProjectConfigException if the project configuration is not valid for writing
     */

    public static CommaSeparatedBoxPlotWriter of( final ProjectConfig projectConfig ) throws ProjectConfigException
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
    public void accept( final MetricOutputMultiMapByTimeAndThreshold<BoxPlotOutput> output )
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
                                                   MetricOutputMultiMapByTimeAndThreshold<BoxPlotOutput> output,
                                                   Format formatter )
            throws IOException
    {
        // Loop across the box plot output
        for ( Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<BoxPlotOutput>> m : output.entrySet() )
        {
            // Write the output
            CommaSeparatedBoxPlotWriter.writeOneBoxPlotOutputTypePerTimeWindow( destinationConfig,
                                                                                m.getValue(),
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
     */

    private static void writeOneBoxPlotOutputTypePerTimeWindow( DestinationConfig destinationConfig,
                                                                MetricOutputMapByTimeAndThreshold<BoxPlotOutput> output,
                                                                Format formatter )
            throws IOException
    {
        // Loop across time windows
        for ( TimeWindow timeWindow : output.setOfTimeWindowKey() )
        {
            MetricOutputMetadata meta = output.getMetadata();
            MetricOutputMapByTimeAndThreshold<BoxPlotOutput> next = output.filterByTime( timeWindow );
            StringJoiner headerRow = new StringJoiner( "," );
            headerRow.merge( HEADER_DEFAULT );
            List<RowCompareByLeft> rows = CommaSeparatedBoxPlotWriter.getRowsForOneBoxPlot( next, formatter );

            // Add the header row
            rows.add( RowCompareByLeft.of( HEADER_INDEX,
                                           CommaSeparatedBoxPlotWriter.getBoxPlotHeader( next, headerRow ) ) );
            // Write the output
            Path outputPath = ConfigHelper.getOutputPathToWrite( destinationConfig, meta, timeWindow );

            CommaSeparatedWriter.writeTabularOutputToFile( rows, outputPath );
        }
    }

    /**
     * Returns the results for one box plot output.
     *
     * @param output the box plot output
     * @param formatter optional formatter, can be null
     * @return the rows to write
     */

    private static List<RowCompareByLeft>
            getRowsForOneBoxPlot( MetricOutputMapByTimeAndThreshold<BoxPlotOutput> output,
                                  Format formatter )
    {
        List<RowCompareByLeft> returnMe = new ArrayList<>();

        // Add the rows
        // Loop across the thresholds
        for ( OneOrTwoThresholds t : output.setOfThresholdKey() )
        {
            // Loop across time windows
            for ( TimeWindow timeWindow : output.setOfTimeWindowKey() )
            {
                Pair<TimeWindow, OneOrTwoThresholds> key = Pair.of( timeWindow, t );
                if ( output.containsKey( key ) )
                {
                    BoxPlotOutput nextValues = output.get( key );
                    // Add each box
                    for ( PairOfDoubleAndVectorOfDoubles nextBox : nextValues )
                    {
                        List<Double> data = new ArrayList<>();
                        data.add( nextBox.getItemOne() );
                        data.addAll( Arrays.stream( nextBox.getItemTwo() ).boxed().collect( Collectors.toList() ) );
                        CommaSeparatedWriter.addRowToInput( returnMe,
                                                            timeWindow,
                                                            data,
                                                            formatter,
                                                            false );
                    }
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

    private static StringJoiner getBoxPlotHeader( MetricOutputMapByTimeAndThreshold<BoxPlotOutput> output,
                                                  StringJoiner headerRow )
    {
        // Append to header
        StringJoiner returnMe = new StringJoiner( "," );
        returnMe.merge( headerRow );
        BoxPlotOutput nextValues = output.getValue( 0 );
        for ( OneOrTwoThresholds nextThreshold : output.setOfThresholdKey() )
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

    private CommaSeparatedBoxPlotWriter( ProjectConfig projectConfig ) throws ProjectConfigException
    {
        super( projectConfig );
    }

}
