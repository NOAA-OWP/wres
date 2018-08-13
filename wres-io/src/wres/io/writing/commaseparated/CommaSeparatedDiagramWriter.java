package wres.io.writing.commaseparated;

import java.io.IOException;
import java.nio.file.Path;
import java.text.Format;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.function.Consumer;

import wres.config.ProjectConfigException;
import wres.config.generated.DestinationConfig;
import wres.config.generated.OutputTypeSelection;
import wres.config.generated.ProjectConfig;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.Slicer;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.ListOfMetricOutput;
import wres.datamodel.outputs.MultiVectorOutput;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.io.config.ConfigHelper;

/**
 * Helps write box plots comprising {@link MultiVectorOutput} to a file of Comma Separated Values (CSV).
 * 
 * @author james.brown@hydrosolved.com
 */

public class CommaSeparatedDiagramWriter extends CommaSeparatedWriter
        implements Consumer<ListOfMetricOutput<MultiVectorOutput>>
{

    /**
     * Returns an instance of a writer.
     * 
     * @param projectConfig the project configuration
     * @return a writer
     * @throws NullPointerException if the input is null 
     * @throws ProjectConfigException if the project configuration is not valid for writing
     */

    public static CommaSeparatedDiagramWriter of( final ProjectConfig projectConfig )
    {
        return new CommaSeparatedDiagramWriter( projectConfig );
    }

    /**
     * Writes all output for one diagram type.
     *
     * @param output the diagram output
     * @throws NullPointerException if the input is null
     * @throws CommaSeparatedWriteException if the output cannot be written
     */

    @Override
    public void accept( final ListOfMetricOutput<MultiVectorOutput> output )
    {
        Objects.requireNonNull( output, "Specify non-null input data when writing diagram outputs." );

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
                CommaSeparatedDiagramWriter.writeOneDiagramOutputType( projectConfig,
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
     * Writes all output for one diagram type.
     *
     * @param projectConfig the project configuration
     * @param destinationConfig the destination configuration    
     * @param output the diagram output
     * @param formatter optional formatter, can be null
     * @throws IOException if the output cannot be written
     */

    private static void writeOneDiagramOutputType( ProjectConfig projectConfig,
                                                   DestinationConfig destinationConfig,
                                                   ListOfMetricOutput<MultiVectorOutput> output,
                                                   Format formatter )
            throws IOException
    {
        // Obtain the output type configuration
        OutputTypeSelection diagramType = ConfigHelper.getOutputTypeSelection( projectConfig, destinationConfig );

        // Loop across diagrams
        SortedSet<MetricConstants> metrics = Slicer.discover( output, next -> next.getMetadata().getMetricID() );
        for ( MetricConstants m : metrics )
        {

            StringJoiner headerRow = new StringJoiner( "," );
            headerRow.merge( HEADER_DEFAULT );

            // Default, per time-window
            if ( diagramType == OutputTypeSelection.DEFAULT || diagramType == OutputTypeSelection.LEAD_THRESHOLD )
            {
                CommaSeparatedDiagramWriter.writeOneDiagramOutputTypePerTimeWindow( destinationConfig,
                                                                                    Slicer.filter( output, m ),
                                                                                    headerRow,
                                                                                    formatter );
            }
            // Per threshold
            else if ( diagramType == OutputTypeSelection.THRESHOLD_LEAD )
            {
                CommaSeparatedDiagramWriter.writeOneDiagramOutputTypePerThreshold( destinationConfig,
                                                                                   Slicer.filter( output, m ),
                                                                                   headerRow,
                                                                                   formatter );
            }
        }
    }

    /**
     * Writes one diagram for all thresholds at each time window in the input.
     * 
     * @param destinationConfig the destination configuration    
     * @param output the diagram output
     * @param headerRow the header row
     * @param formatter optional formatter, can be null
     * @throws IOException if the output cannot be written
     */

    private static void writeOneDiagramOutputTypePerTimeWindow( DestinationConfig destinationConfig,
                                                                ListOfMetricOutput<MultiVectorOutput> output,
                                                                StringJoiner headerRow,
                                                                Format formatter )
            throws IOException
    {
        // Loop across time windows
        SortedSet<TimeWindow> timeWindows = Slicer.discover( output, next -> next.getMetadata().getTimeWindow() );
        for ( TimeWindow timeWindow : timeWindows )
        {
            ListOfMetricOutput<MultiVectorOutput> next =
                    Slicer.filter( output, data -> data.getTimeWindow().equals( timeWindow ) );
            
            MetricOutputMetadata meta = next.getData().get( 0 ).getMetadata();
            
            List<RowCompareByLeft> rows = getRowsForOneDiagram( next, formatter );

            // Add the header row
            rows.add( RowCompareByLeft.of( HEADER_INDEX, getDiagramHeader( next, headerRow ) ) );

            // Write the output
            Path outputPath = ConfigHelper.getOutputPathToWrite( destinationConfig, meta, timeWindow );

            CommaSeparatedWriter.writeTabularOutputToFile( rows, outputPath );
        }
    }

    /**
     * Writes one diagram for all time windows at each threshold in the input.
     * 
     * @param destinationConfig the destination configuration    
     * @param output the diagram output
     * @param headerRow the header row
     * @param formatter optional formatter, can be null
     * @throws IOException if the output cannot be written
     */

    private static void writeOneDiagramOutputTypePerThreshold( DestinationConfig destinationConfig,
                                                               ListOfMetricOutput<MultiVectorOutput> output,
                                                               StringJoiner headerRow,
                                                               Format formatter )
            throws IOException
    {
        // Loop across thresholds
        SortedSet<OneOrTwoThresholds> thresholds =
                Slicer.discover( output, meta -> meta.getMetadata().getThresholds() );
        for ( OneOrTwoThresholds threshold : thresholds )
        {
            
            ListOfMetricOutput<MultiVectorOutput> next =
                    Slicer.filter( output, data -> data.getThresholds().equals( threshold ) );
            
            MetricOutputMetadata meta = next.getData().get( 0 ).getMetadata();
            
            List<RowCompareByLeft> rows = CommaSeparatedDiagramWriter.getRowsForOneDiagram( next, formatter );

            // Add the header row
            rows.add( RowCompareByLeft.of( HEADER_INDEX,
                                           CommaSeparatedDiagramWriter.getDiagramHeader( next, headerRow ) ) );

            // Write the output
            Path outputPath = ConfigHelper.getOutputPathToWrite( destinationConfig, meta, threshold );

            CommaSeparatedWriter.writeTabularOutputToFile( rows, outputPath );
        }
    }

    /**
     * Returns the results for one diagram output.
     *
     * @param output the diagram output
     * @param formatter optional formatter, can be null
     * @return the rows to write
     */

    private static List<RowCompareByLeft>
            getRowsForOneDiagram( ListOfMetricOutput<MultiVectorOutput> output,
                                  Format formatter )
    {
        List<RowCompareByLeft> returnMe = new ArrayList<>();

        // Discover the time windows and thresholds to loop
        SortedSet<OneOrTwoThresholds> thresholds =
                Slicer.discover( output, meta -> meta.getMetadata().getThresholds() );
        SortedSet<TimeWindow> timeWindows = Slicer.discover( output, meta -> meta.getMetadata().getTimeWindow() );
        // Loop across time windows
        for ( TimeWindow timeWindow : timeWindows )
        {
            // Loop across the thresholds, merging results when multiple thresholds occur
            Map<Integer, List<Double>> merge = new TreeMap<>();
            for ( OneOrTwoThresholds threshold : thresholds )
            {
                // One output per time window and threshold
                MultiVectorOutput nextOutput = Slicer.filter( output,
                                                              data -> data.getThresholds().equals( threshold )
                                                                      && data.getTimeWindow().equals( timeWindow ) )
                                                     .getData()
                                                     .get( 0 );
                CommaSeparatedDiagramWriter.addRowsForOneDiagramAtOneTimeWindowAndThreshold( nextOutput, merge );
            }
            // Add the merged rows
            for ( List<Double> next : merge.values() )
            {
                CommaSeparatedWriter.addRowToInput( returnMe,
                                                    timeWindow,
                                                    next,
                                                    formatter,
                                                    false );
            }
        }

        return returnMe;
    }

    /**
     * Adds rows to the input map of merged rows for a specific {@link TimeWindow} and {@link OneOrTwoThresholds}.
     *
     * @param output the diagram output
     * @param key the key for which rows are required
     * @param merge the merged rows to mutate
     */

    private static void
            addRowsForOneDiagramAtOneTimeWindowAndThreshold( MultiVectorOutput output,
                                                             Map<Integer, List<Double>> merge )
    {

        // Check that the output exists
        if ( Objects.nonNull( output ) )
        {

            // For safety, find the largest vector and use Double.NaN in place for vectors of differing size
            // In practice, all should be the same length
            int maxRows = output.getData()
                                .values()
                                .stream()
                                .max( Comparator.comparingInt( VectorOfDoubles::size ) )
                                .get()
                                .size();

            // Loop until the maximum row 
            for ( int rowIndex = 0; rowIndex < maxRows; rowIndex++ )
            {
                // Add the row
                List<Double> row = CommaSeparatedDiagramWriter.getOneRowForOneDiagram( output, rowIndex );
                if ( merge.containsKey( rowIndex ) )
                {
                    merge.get( rowIndex ).addAll( row );
                }
                else
                {
                    merge.put( rowIndex, row );
                }
            }
        }
    }

    /**
     * Returns the row from the diagram from the input data at a specified threshold and row index.
     * 
     * @param next the data
     * @param row the row index to generate
     * @return the row data
     */

    private static List<Double> getOneRowForOneDiagram( MultiVectorOutput next, int row )
    {
        List<Double> valuesToAdd = new ArrayList<>();
        for ( Entry<MetricDimension, VectorOfDoubles> nextColumn : next.getData().entrySet() )
        {
            // Populate the values
            Double addMe = Double.NaN;
            if ( row < nextColumn.getValue().size() )
            {
                addMe = nextColumn.getValue().getDoubles()[row];
            }
            valuesToAdd.add( addMe );
        }
        return valuesToAdd;
    }

    /**
     * Helper that mutates the header for diagrams based on the input.
     * 
     * @param output the diagram output
     * @param headerRow the header row
     * @return the mutated header
     */

    private static StringJoiner getDiagramHeader( ListOfMetricOutput<MultiVectorOutput> output,
                                                  StringJoiner headerRow )
    {
        // Append to header
        StringJoiner returnMe = new StringJoiner( "," );
        returnMe.merge( headerRow );
        // Discover first item to help
        MultiVectorOutput data = output.getData().get( 0 );
        String metricName = data.getMetadata().getMetricID().toString();
        
        Set<MetricDimension> dimensions = data.getData().keySet();
        //Add the metric name, dimension, and threshold for each column-vector
        SortedSet<OneOrTwoThresholds> thresholds =
                Slicer.discover( output, meta -> meta.getMetadata().getThresholds() );
        for ( OneOrTwoThresholds nextThreshold : thresholds )
        {
            for ( MetricDimension nextDimension : dimensions )
            {
                returnMe.add( HEADER_DELIMITER + metricName
                              + HEADER_DELIMITER
                              + nextDimension.toString()
                              + HEADER_DELIMITER
                              + nextThreshold.toString() );
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

    private CommaSeparatedDiagramWriter( ProjectConfig projectConfig )
    {
        super( projectConfig );
    }

}
