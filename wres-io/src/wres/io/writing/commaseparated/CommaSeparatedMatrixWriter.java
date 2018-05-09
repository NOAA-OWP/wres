package wres.io.writing.commaseparated;

import java.io.IOException;
import java.nio.file.Path;
import java.text.Format;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import org.apache.commons.lang3.tuple.Pair;

import wres.config.ProjectConfigException;
import wres.config.generated.DestinationConfig;
import wres.config.generated.OutputTypeSelection;
import wres.config.generated.ProjectConfig;
import wres.datamodel.MetricConstants;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.MapKey;
import wres.datamodel.outputs.MatrixOutput;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMultiMapByTimeAndThreshold;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.io.config.ConfigHelper;

/**
 * Helps write contingency tables comprising {@link MatrixOutput} to a file of Comma Separated Values (CSV).
 * 
 * @author james.brown@hydrosolved.com
 */

public class CommaSeparatedMatrixWriter extends CommaSeparatedWriter
        implements Consumer<MetricOutputMultiMapByTimeAndThreshold<MatrixOutput>>
{

    /**
     * Returns an instance of a writer.
     * 
     * @param projectConfig the project configuration
     * @return a writer
     * @throws NullPointerException if the input is null 
     * @throws ProjectConfigException if the project configuration is not valid for writing
     */

    public static CommaSeparatedMatrixWriter of( final ProjectConfig projectConfig ) throws ProjectConfigException
    {
        return new CommaSeparatedMatrixWriter( projectConfig );
    }

    /**
     * Writes all output for one matrix type.
     *
     * @param output the matrix output
     * @throws NullPointerException if the input is null
     * @throws CommaSeparatedWriteException if the output cannot be written
     */

    @Override
    public void accept( final MetricOutputMultiMapByTimeAndThreshold<MatrixOutput> output )
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

            // Default, per time-window
            try
            {
                CommaSeparatedMatrixWriter.writeOneMatrixOutputType( projectConfig,
                                                                     destinationConfig,
                                                                     output,
                                                                     formatter );
            }
            catch ( IOException e )
            {
                throw new CommaSeparatedWriteException( "While writing comma separated output: ", e );
            }

        }
    }

    /**
     * Writes all output for one matrix type.
     *
     * @param projectConfig the project configuration
     * @param destinationConfig the destination configuration    
     * @param output the matrix output
     * @param formatter optional formatter, can be null
     * @throws IOException if the output cannot be written
     */

    private static void writeOneMatrixOutputType( ProjectConfig projectConfig,
                                                  DestinationConfig destinationConfig,
                                                  MetricOutputMultiMapByTimeAndThreshold<MatrixOutput> output,
                                                  Format formatter )
            throws IOException
    {
        // Obtain the output type configuration with any override for ALL_VALID metrics
        OutputTypeSelection diagramType = ConfigHelper.getOutputTypeSelection( projectConfig, destinationConfig );

        // Loop across diagrams
        for ( Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<MatrixOutput>> m : output.entrySet() )
        {
            
            StringJoiner headerRow = new StringJoiner( "," );
            headerRow.merge( HEADER_DEFAULT );

            // Default, per time-window
            if ( diagramType == OutputTypeSelection.DEFAULT || diagramType == OutputTypeSelection.LEAD_THRESHOLD )
            {
                CommaSeparatedMatrixWriter.writeOneMatrixOutputTypePerTimeWindow( destinationConfig,
                                                                                  m.getValue(),
                                                                                  headerRow,
                                                                                  formatter );
            }
            // Per threshold
            else if ( diagramType == OutputTypeSelection.THRESHOLD_LEAD )
            {
                CommaSeparatedMatrixWriter.writeOneMatrixOutputTypePerThreshold( destinationConfig,
                                                                                 m.getValue(),
                                                                                 headerRow,
                                                                                 formatter );
            }
        }
    }

    /**
     * Writes one matrix output for all thresholds at each time window in the input.
     * 
     * @param destinationConfig the destination configuration    
     * @param output the matrix output
     * @param headerRow the header row
     * @param formatter optional formatter, can be null
     * @throws IOException if the output cannot be written
     */

    private static void writeOneMatrixOutputTypePerTimeWindow( DestinationConfig destinationConfig,
                                                               MetricOutputMapByTimeAndThreshold<MatrixOutput> output,
                                                               StringJoiner headerRow,
                                                               Format formatter )
            throws IOException
    {
        // Loop across time windows
        for ( TimeWindow timeWindow : output.setOfTimeWindowKey() )
        {
            MetricOutputMetadata meta = output.getMetadata();
            MetricOutputMapByTimeAndThreshold<MatrixOutput> next = output.filterByTime( timeWindow );
            List<RowCompareByLeft> rows = CommaSeparatedMatrixWriter.getRowsForOneMatrixOutput( next, formatter );

            // Add the header row
            rows.add( RowCompareByLeft.of( HEADER_INDEX,
                                           CommaSeparatedMatrixWriter.getMatrixOutputHeader( next, headerRow ) ) );

            // Write the output
            Path outputPath = ConfigHelper.getOutputPathToWrite( destinationConfig, meta, timeWindow );

            CommaSeparatedWriter.writeTabularOutputToFile( rows, outputPath );
        }
    }

    /**
     * Writes one matrix output for all time windows at each threshold in the input.
     * 
     * @param destinationConfig the destination configuration    
     * @param output the matrix output
     * @param headerRow the header row
     * @param formatter optional formatter, can be null
     * @throws IOException if the output cannot be written 
     */

    private static void writeOneMatrixOutputTypePerThreshold( DestinationConfig destinationConfig,
                                                              MetricOutputMapByTimeAndThreshold<MatrixOutput> output,
                                                              StringJoiner headerRow,
                                                              Format formatter )
            throws IOException
    {
        // Loop across thresholds
        for ( OneOrTwoThresholds threshold : output.setOfThresholdKey() )
        {
            MetricOutputMetadata meta = output.getMetadata();
            MetricOutputMapByTimeAndThreshold<MatrixOutput> next = output.filterByThreshold( threshold );
            List<RowCompareByLeft> rows = CommaSeparatedMatrixWriter.getRowsForOneMatrixOutput( next, formatter );

            // Add the header row
            rows.add( RowCompareByLeft.of( HEADER_INDEX,
                                           CommaSeparatedMatrixWriter.getMatrixOutputHeader( next, headerRow ) ) );

            // Write the output
            Path outputPath = ConfigHelper.getOutputPathToWrite( destinationConfig, meta, threshold );

            CommaSeparatedWriter.writeTabularOutputToFile( rows, outputPath );
        }
    }

    /**
     * Helper that mutates the header for matrix based on the input.
     * 
     * @param output the matrix output
     * @param headerRow the header row
     * @return the mutated header
     */

    private static StringJoiner getMatrixOutputHeader( MetricOutputMapByTimeAndThreshold<MatrixOutput> output,
                                                       StringJoiner headerRow )
    {
        // Append to header
        StringJoiner returnMe = new StringJoiner( "," );
        returnMe.merge( headerRow );
        String metricName = output.getMetadata().getMetricID().toString();
        MatrixOutput data = output.getValue( 0 );
        List<String> dimensions = new ArrayList<>();
        // Add names
        if ( data.hasComponentNames() )
        {
            data.getComponentNames().forEach( a -> dimensions.add( a.toString() ) );
        }
        // Add simple [row,column] indices
        else
        {
            IntStream.range( 0, data.getData().rows() )
                     .forEach( rowIndex -> IntStream.range( 0, data.getData().columns() )
                                                    .forEach( colIndex -> dimensions.add( "[" + rowIndex
                                                                                          + ","
                                                                                          + colIndex
                                                                                          + "]" ) ) );
        }

        //Add the metric name, dimension, and threshold for each column-vector
        for ( OneOrTwoThresholds nextThreshold : output.setOfThresholdKey() )
        {
            for ( String nextDimension : dimensions )
            {
                returnMe.add( HEADER_DELIMITER + metricName
                              + HEADER_DELIMITER
                              + nextDimension
                              + HEADER_DELIMITER
                              + nextThreshold.toString() );
            }
        }

        return returnMe;
    }

    /**
     * Returns the results for one matrix output.
     *
     * @param output the matrix output
     * @param formatter optional formatter, can be null
     * @return the rows to write
     */

    private static List<RowCompareByLeft>
            getRowsForOneMatrixOutput( MetricOutputMapByTimeAndThreshold<MatrixOutput> output,
                                       Format formatter )
    {
        List<RowCompareByLeft> returnMe = new ArrayList<>();

        // Add the rows
        // Loop across time windows
        for ( TimeWindow timeWindow : output.setOfTimeWindowKey() )
        {
            // Loop across the thresholds, merging results when multiple thresholds occur
            List<Double> merge = new ArrayList<>();
            for ( OneOrTwoThresholds threshold : output.setOfThresholdKey() )
            {
                Pair<TimeWindow, OneOrTwoThresholds> key = Pair.of( timeWindow, threshold );
                if ( output.containsKey( key ) )
                {
                    MatrixOutput next = output.get( key );

                    // Add the row
                    next.iterator().forEachRemaining( merge::add );
                }
            }
            // Add the merged row
            CommaSeparatedWriter.addRowToInput( returnMe,
                                                timeWindow,
                                                merge,
                                                formatter,
                                                false );
        }

        return returnMe;
    }

    /**
     * Hidden constructor.
     * 
     * @param projectConfig the project configuration
     * @throws ProjectConfigException if the project configuration is not valid for writing 
     */

    private CommaSeparatedMatrixWriter( ProjectConfig projectConfig ) throws ProjectConfigException
    {
        super( projectConfig );
    }

}
