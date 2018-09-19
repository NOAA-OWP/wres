package wres.io.writing.commaseparated;

import java.io.IOException;
import java.nio.file.Path;
import java.text.Format;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

import wres.config.ProjectConfigException;
import wres.config.generated.DestinationConfig;
import wres.config.generated.OutputTypeSelection;
import wres.config.generated.ProjectConfig;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.Slicer;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.metadata.StatisticMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.statistics.ListOfStatistics;
import wres.datamodel.statistics.MultiVectorStatistic;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.io.config.ConfigHelper;

/**
 * Helps write box plots comprising {@link MultiVectorStatistic} to a file of Comma Separated Values (CSV).
 * 
 * @author james.brown@hydrosolved.com
 */

public class CommaSeparatedDiagramWriter extends CommaSeparatedWriter
        implements Consumer<ListOfStatistics<MultiVectorStatistic>>, Supplier<Set<Path>>
{
    /**
     * Set of paths that this writer actually wrote to
     */
    private final Set<Path> pathsWrittenTo = new HashSet<>();

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
    public void accept( final ListOfStatistics<MultiVectorStatistic> output )
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
                Set<Path> innerPathsWrittenTo =
                        CommaSeparatedDiagramWriter.writeOneDiagramOutputType( projectConfig,
                                                                               destinationConfig,
                                                                               output,
                                                                               formatter );
                this.pathsWrittenTo.addAll( innerPathsWrittenTo );
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

    private static Set<Path> writeOneDiagramOutputType( ProjectConfig projectConfig,
                                                        DestinationConfig destinationConfig,
                                                        ListOfStatistics<MultiVectorStatistic> output,
                                                        Format formatter )
            throws IOException
    {
        Set<Path> pathsWrittenTo = new HashSet<>( 1 );

        // Obtain the output type configuration
        OutputTypeSelection diagramType = ConfigHelper.getOutputTypeSelection( projectConfig, destinationConfig );

        // Loop across diagrams
        SortedSet<MetricConstants> metrics = Slicer.discover( output, next -> next.getMetadata().getMetricID() );
        for ( MetricConstants m : metrics )
        {
            StringJoiner headerRow = CommaSeparatedWriter.getDefaultHeaderFromSampleMetadata( output.getData()
                                                                                                    .get( 0 )
                                                                                                    .getMetadata()
                                                                                                    .getSampleMetadata() );

            Set<Path> innerPathsWrittenTo = Collections.emptySet();

            // Default, per time-window
            if ( diagramType == OutputTypeSelection.DEFAULT || diagramType == OutputTypeSelection.LEAD_THRESHOLD )
            {
                innerPathsWrittenTo =
                        CommaSeparatedDiagramWriter.writeOneDiagramOutputTypePerTimeWindow( destinationConfig,
                                                                                            Slicer.filter( output, m ),
                                                                                            headerRow,
                                                                                            formatter );
            }
            // Per threshold
            else if ( diagramType == OutputTypeSelection.THRESHOLD_LEAD )
            {
                innerPathsWrittenTo =
                        CommaSeparatedDiagramWriter.writeOneDiagramOutputTypePerThreshold( destinationConfig,
                                                                                           Slicer.filter( output, m ),
                                                                                           headerRow,
                                                                                           formatter );
            }

            pathsWrittenTo.addAll( innerPathsWrittenTo );

        }

        return Collections.unmodifiableSet( pathsWrittenTo );
    }


    /**
     * Writes one diagram for all thresholds at each time window in the input.
     * 
     * @param destinationConfig the destination configuration    
     * @param output the diagram output
     * @param headerRow the header row
     * @param formatter optional formatter, can be null
     * @throws IOException if the output cannot be written
     * @return set of paths actually written to
     */

    private static Set<Path> writeOneDiagramOutputTypePerTimeWindow( DestinationConfig destinationConfig,
                                                                     ListOfStatistics<MultiVectorStatistic> output,
                                                                     StringJoiner headerRow,
                                                                     Format formatter )
            throws IOException
    {
        Set<Path> pathsWrittenTo = new HashSet<>( 1 );

        // Loop across time windows
        SortedSet<TimeWindow> timeWindows =
                Slicer.discover( output, next -> next.getMetadata().getSampleMetadata().getTimeWindow() );
        for ( TimeWindow timeWindow : timeWindows )
        {
            ListOfStatistics<MultiVectorStatistic> next =
                    Slicer.filter( output, data -> data.getSampleMetadata().getTimeWindow().equals( timeWindow ) );

            StatisticMetadata meta = next.getData().get( 0 ).getMetadata();

            List<RowCompareByLeft> rows = getRowsForOneDiagram( next, formatter );

            // Add the header row
            rows.add( RowCompareByLeft.of( HEADER_INDEX, getDiagramHeader( next, headerRow ) ) );

            // Write the output
            Path outputPath = ConfigHelper.getOutputPathToWrite( destinationConfig,
                                                                 meta,
                                                                 timeWindow,
                                                                 CommaSeparatedWriter.DEFAULT_DURATION_UNITS );

            CommaSeparatedWriter.writeTabularOutputToFile( rows, outputPath );

            // If writeTabularOutputToFile did not throw an exception, assume
            // it succeeded in writing to the file, track outputs now (add must
            // be called after the above call).
            pathsWrittenTo.add( outputPath );
        }

        return Collections.unmodifiableSet( pathsWrittenTo );
    }


    /**
     * Writes one diagram for all time windows at each threshold in the input.
     * 
     * @param destinationConfig the destination configuration    
     * @param output the diagram output
     * @param headerRow the header row
     * @param formatter optional formatter, can be null
     * @throws IOException if the output cannot be written
     * @return set of paths actually written to
     */

    private static Set<Path> writeOneDiagramOutputTypePerThreshold( DestinationConfig destinationConfig,
                                                                    ListOfStatistics<MultiVectorStatistic> output,
                                                                    StringJoiner headerRow,
                                                                    Format formatter )
            throws IOException
    {
        Set<Path> pathsWrittenTo = new HashSet<>( 1 );

        // Loop across thresholds
        SortedSet<OneOrTwoThresholds> thresholds =
                Slicer.discover( output, meta -> meta.getMetadata().getSampleMetadata().getThresholds() );
        for ( OneOrTwoThresholds threshold : thresholds )
        {

            ListOfStatistics<MultiVectorStatistic> next =
                    Slicer.filter( output, data -> data.getSampleMetadata().getThresholds().equals( threshold ) );

            StatisticMetadata meta = next.getData().get( 0 ).getMetadata();

            List<RowCompareByLeft> rows = CommaSeparatedDiagramWriter.getRowsForOneDiagram( next, formatter );

            // Add the header row
            rows.add( RowCompareByLeft.of( HEADER_INDEX,
                                           CommaSeparatedDiagramWriter.getDiagramHeader( next, headerRow ) ) );

            // Write the output
            Path outputPath = ConfigHelper.getOutputPathToWrite( destinationConfig, meta, threshold );

            CommaSeparatedWriter.writeTabularOutputToFile( rows, outputPath );

            // If writeTabularOutputToFile did not throw an exception, assume
            // it succeeded in writing to the file, track outputs now (add must
            // be called after the above call).
            pathsWrittenTo.add( outputPath );
        }

        return Collections.unmodifiableSet( pathsWrittenTo );
    }

    /**
     * Returns the results for one diagram output.
     *
     * @param output the diagram output
     * @param formatter optional formatter, can be null
     * @return the rows to write
     */

    private static List<RowCompareByLeft>
            getRowsForOneDiagram( ListOfStatistics<MultiVectorStatistic> output,
                                  Format formatter )
    {
        List<RowCompareByLeft> returnMe = new ArrayList<>();

        // Discover the time windows and thresholds to loop
        SortedSet<OneOrTwoThresholds> thresholds =
                Slicer.discover( output, meta -> meta.getMetadata().getSampleMetadata().getThresholds() );
        SortedSet<TimeWindow> timeWindows =
                Slicer.discover( output, meta -> meta.getMetadata().getSampleMetadata().getTimeWindow() );
        // Loop across time windows
        for ( TimeWindow timeWindow : timeWindows )
        {
            // Loop across the thresholds, merging results when multiple thresholds occur
            Map<Integer, List<Double>> merge = new TreeMap<>();
            for ( OneOrTwoThresholds threshold : thresholds )
            {
                // One output per time window and threshold
                MultiVectorStatistic nextOutput = Slicer.filter( output,
                                                                 data -> data.getSampleMetadata()
                                                                             .getThresholds()
                                                                             .equals( threshold )
                                                                         && data.getSampleMetadata()
                                                                                .getTimeWindow()
                                                                                .equals( timeWindow ) )
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
            addRowsForOneDiagramAtOneTimeWindowAndThreshold( MultiVectorStatistic output,
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

    private static List<Double> getOneRowForOneDiagram( MultiVectorStatistic next, int row )
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

    private static StringJoiner getDiagramHeader( ListOfStatistics<MultiVectorStatistic> output,
                                                  StringJoiner headerRow )
    {
        // Append to header
        StringJoiner returnMe = new StringJoiner( "," );
        returnMe.merge( headerRow );
        // Discover first item to help
        MultiVectorStatistic data = output.getData().get( 0 );
        String metricName = data.getMetadata().getMetricID().toString();

        Set<MetricDimension> dimensions = data.getData().keySet();
        //Add the metric name, dimension, and threshold for each column-vector
        SortedSet<OneOrTwoThresholds> thresholds =
                Slicer.discover( output, meta -> meta.getMetadata().getSampleMetadata().getThresholds() );
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
     * Return a snapshot of the paths written to (so far)
     */

    @Override
    public Set<Path> get()
    {
        return this.getPathsWrittenTo();
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
     * @throws ProjectConfigException if the project configuration is not valid for writing 
     */

    private CommaSeparatedDiagramWriter( ProjectConfig projectConfig )
    {
        super( projectConfig );
    }

}
