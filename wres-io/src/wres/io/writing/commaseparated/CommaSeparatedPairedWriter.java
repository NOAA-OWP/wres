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

import org.apache.commons.lang3.tuple.Pair;

import wres.config.ProjectConfigException;
import wres.config.generated.DestinationConfig;
import wres.config.generated.ProjectConfig;
import wres.datamodel.MetricConstants;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.MapKey;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMultiMapByTimeAndThreshold;
import wres.datamodel.outputs.PairedOutput;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.io.config.ConfigHelper;

/**
 * Helps write paired output comprising {@link PairedOutput} to a file of Comma Separated Values (CSV).
 * 
 * @param <S> the left side of the paired output type
 * @param <T> the right side if the paired output type
 * @author james.brown@hydrosolved.com
 */

public class CommaSeparatedPairedWriter<S, T> extends CommaSeparatedWriter
        implements Consumer<MetricOutputMultiMapByTimeAndThreshold<PairedOutput<S, T>>>
{

    /**
     * Returns an instance of a writer.
     * 
     * @param <S> the left side of the paired output type
     * @param <T> the right side if the paired output type
     * @param projectConfig the project configuration
     * @return a writer
     * @throws NullPointerException if the input is null 
     * @throws ProjectConfigException if the project configuration is not valid for writing
     */

    public static <S, T> CommaSeparatedPairedWriter<S, T> of( final ProjectConfig projectConfig )
    {
        return new CommaSeparatedPairedWriter<>( projectConfig );
    }

    /**
     * Writes all output for one box plot type.
     *
     * @param output the box plot output
     * @throws NullPointerException if the input is null
     * @throws CommaSeparatedWriteException if the output cannot be written
     */

    @Override
    public void accept( final MetricOutputMultiMapByTimeAndThreshold<PairedOutput<S, T>> output )
    {
        Objects.requireNonNull( output, "Specify non-null input data when writing box plot outputs." );

        // Write output
        // In principle, each destination could have a different formatter, so 
        // the output must be generated separately for each destination
        List<DestinationConfig> numericalDestinations = ConfigHelper.getNumericalDestinations( projectConfig );
        for ( DestinationConfig destinationConfig : numericalDestinations )
        {

            // Formatter
            Format formatter = null;

            // Write per time-window
            try
            {
                CommaSeparatedPairedWriter.writeOnePairedOutputType( destinationConfig, output, formatter );
            }
            catch ( IOException e )
            {
                throw new CommaSeparatedWriteException( "While writing comma separated output: ", e );
            }

        }

    }

    /**
     * Writes all output for one paired type.
     *
     * @param <S> the left side of the paired output type
     * @param <T> the right side if the paired output type
     * @param destinationConfig the destination configuration    
     * @param output the paired output to iterate through
     * @param formatter optional formatter, can be null
     * @throws IOException if the output cannot be written
     */

    private static <S, T> void writeOnePairedOutputType( DestinationConfig destinationConfig,
                                                         MetricOutputMultiMapByTimeAndThreshold<PairedOutput<S, T>> output,
                                                         Format formatter )
            throws IOException
    {
        // Loop across paired output
        for ( Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<PairedOutput<S, T>>> m : output.entrySet() )
        {
            StringJoiner headerRow = new StringJoiner( "," );
            headerRow.merge( HEADER_DEFAULT );
            List<RowCompareByLeft> rows =
                    CommaSeparatedPairedWriter.getRowsForOnePairedOutput( m.getKey().getKey(),
                                                                          m.getValue(),
                                                                          headerRow,
                                                                          formatter );

            // Add the header row
            rows.add( RowCompareByLeft.of( HEADER_INDEX, headerRow ) );

            // Write the output
            MetricOutputMetadata meta = m.getValue().getMetadata();
            Path outputPath = ConfigHelper.getOutputPathToWrite( destinationConfig, meta );

            CommaSeparatedWriter.writeTabularOutputToFile( rows, outputPath );
        }
    }


    /**
     * Returns the results for one paired output.
     *
     * @param <S> the left side of the paired output type
     * @param <T> the right side if the paired output type
     * @param metricName the score name
     * @param output the paired output
     * @param headerRow the header row
     * @param formatter optional formatter, can be null
     * @return the rows to write
     */

    private static <S, T> List<RowCompareByLeft>
            getRowsForOnePairedOutput( MetricConstants metricName,
                                       MetricOutputMapByTimeAndThreshold<PairedOutput<S, T>> output,
                                       StringJoiner headerRow,
                                       Format formatter )
    {
        String outerName = metricName.toString() + HEADER_DELIMITER;
        List<RowCompareByLeft> returnMe = new ArrayList<>();

        // Add the rows
        // Loop across the thresholds
        for ( OneOrTwoThresholds t : output.setOfThresholdKey() )
        {
            // Append to header
            headerRow.add( outerName + "BASIS TIME" + HEADER_DELIMITER + t );
            headerRow.add( outerName + "DURATION" + HEADER_DELIMITER + t );
            // Loop across time windows
            for ( TimeWindow timeWindow : output.setOfTimeWindowKey() )
            {
                Pair<TimeWindow, OneOrTwoThresholds> key = Pair.of( timeWindow, t );
                if ( output.containsKey( key ) )
                {
                    List<Pair<S, T>> nextValues = output.get( key ).getData();
                    for ( Pair<S, T> nextPair : nextValues )
                    {
                        CommaSeparatedWriter.addRowToInput( returnMe,
                                                            timeWindow,
                                                            Arrays.asList( nextPair.getLeft(), nextPair.getRight() ),
                                                            formatter,
                                                            true,
                                                            nextPair.getLeft().toString() );
                    }
                }
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

    private CommaSeparatedPairedWriter( ProjectConfig projectConfig )
    {
        super( projectConfig );
    }

}
