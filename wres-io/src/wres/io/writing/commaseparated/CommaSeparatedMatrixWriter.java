package wres.io.writing.commaseparated;

import java.io.IOException;
import java.nio.file.Path;
import java.text.Format;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import wres.config.ProjectConfigException;
import wres.config.generated.DestinationConfig;
import wres.config.generated.OutputTypeSelection;
import wres.config.generated.ProjectConfig;
import wres.datamodel.MetricConstants;
import wres.datamodel.Slicer;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.statistics.ListOfMetricOutput;
import wres.datamodel.statistics.MatrixOutput;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.io.config.ConfigHelper;

/**
 * Helps write contingency tables comprising {@link MatrixOutput} to a file of Comma Separated Values (CSV).
 * 
 * @author james.brown@hydrosolved.com
 */

public class CommaSeparatedMatrixWriter extends CommaSeparatedWriter
        implements Consumer<ListOfMetricOutput<MatrixOutput>>, Supplier<Set<Path>>
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

    public static CommaSeparatedMatrixWriter of( final ProjectConfig projectConfig )
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
    public void accept( final ListOfMetricOutput<MatrixOutput> output )
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
                Set<Path> innerPathsWrittenTo =
                        CommaSeparatedMatrixWriter.writeOneMatrixOutputType( projectConfig,
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
     * Writes all output for one matrix type.
     *
     * @param projectConfig the project configuration
     * @param destinationConfig the destination configuration    
     * @param output the matrix output
     * @param formatter optional formatter, can be null
     * @throws IOException if the output cannot be written
     */

    private static Set<Path> writeOneMatrixOutputType( ProjectConfig projectConfig,
                                                       DestinationConfig destinationConfig,
                                                       ListOfMetricOutput<MatrixOutput> output,
                                                       Format formatter )
            throws IOException
    {
        Set<Path> pathsWrittenTo = new HashSet<>( 1 );

        // Obtain the output type configuration with any override for ALL_VALID metrics
        OutputTypeSelection diagramType = ConfigHelper.getOutputTypeSelection( projectConfig, destinationConfig );

        // Loop across metrics
        SortedSet<MetricConstants> metrics = Slicer.discover( output, next -> next.getMetadata().getMetricID() );
        for ( MetricConstants m : metrics )
        {

            StringJoiner headerRow = new StringJoiner( "," );
            headerRow.merge( HEADER_DEFAULT );

            Set<Path> innerPathsWrittenTo = Collections.emptySet();

            // Default, per time-window
            if ( diagramType == OutputTypeSelection.DEFAULT || diagramType == OutputTypeSelection.LEAD_THRESHOLD )
            {
                innerPathsWrittenTo =
                        CommaSeparatedMatrixWriter.writeOneMatrixOutputTypePerTimeWindow( destinationConfig,
                                                                                          Slicer.filter( output, m ),
                                                                                          headerRow,
                                                                                          formatter );
            }
            // Per threshold
            else if ( diagramType == OutputTypeSelection.THRESHOLD_LEAD )
            {
                innerPathsWrittenTo =
                        CommaSeparatedMatrixWriter.writeOneMatrixOutputTypePerThreshold( destinationConfig,
                                                                                         Slicer.filter( output, m ),
                                                                                         headerRow,
                                                                                         formatter );
            }

            pathsWrittenTo.addAll( innerPathsWrittenTo );
        }

        return Collections.unmodifiableSet( pathsWrittenTo );
    }

    /**
     * Writes one matrix output for all thresholds at each time window in the input.
     * 
     * @param destinationConfig the destination configuration    
     * @param output the matrix output
     * @param headerRow the header row
     * @param formatter optional formatter, can be null
     * @throws IOException if the output cannot be written
     * @return set of paths actually written to
     */

    private static Set<Path> writeOneMatrixOutputTypePerTimeWindow( DestinationConfig destinationConfig,
                                                                    ListOfMetricOutput<MatrixOutput> output,
                                                                    StringJoiner headerRow,
                                                                    Format formatter )
            throws IOException
    {
        Set<Path> pathsWrittenTo = new HashSet<>( 1 );

        // Loop across time windows
        SortedSet<TimeWindow> timeWindows = Slicer.discover( output, next -> next.getMetadata().getTimeWindow() );
        for ( TimeWindow timeWindow : timeWindows )
        {
            ListOfMetricOutput<MatrixOutput> next =
                    Slicer.filter( output, data -> data.getTimeWindow().equals( timeWindow ) );
            
            MetricOutputMetadata meta = next.getData().get( 0 ).getMetadata();
            
            List<RowCompareByLeft> rows = CommaSeparatedMatrixWriter.getRowsForOneMatrixOutput( next, formatter );

            // Add the header row
            rows.add( RowCompareByLeft.of( HEADER_INDEX,
                                           CommaSeparatedMatrixWriter.getMatrixOutputHeader( next, headerRow ) ) );

            // Write the output
            Path outputPath = ConfigHelper.getOutputPathToWrite( destinationConfig, meta, timeWindow );

            CommaSeparatedWriter.writeTabularOutputToFile( rows, outputPath );

            // If writeTabularOutputToFile did not throw an exception, assume
            // it succeeded in writing to the file, track outputs now (add must
            // be called after the above call).
            pathsWrittenTo.add( outputPath );
        }

        return Collections.unmodifiableSet( pathsWrittenTo );
    }

    /**
     * Writes one matrix output for all time windows at each threshold in the input.
     * 
     * @param destinationConfig the destination configuration    
     * @param output the matrix output
     * @param headerRow the header row
     * @param formatter optional formatter, can be null
     * @throws IOException if the output cannot be written
     * @return set of paths actually written to
     */

    private static Set<Path> writeOneMatrixOutputTypePerThreshold( DestinationConfig destinationConfig,
                                                                   ListOfMetricOutput<MatrixOutput> output,
                                                                   StringJoiner headerRow,
                                                                   Format formatter )
            throws IOException
    {
        Set<Path> pathsWrittenTo = new HashSet<>( 1 );

        // Loop across thresholds
        SortedSet<OneOrTwoThresholds> thresholds =
                Slicer.discover( output, meta -> meta.getMetadata().getThresholds() );
        for ( OneOrTwoThresholds threshold : thresholds )
        {
            ListOfMetricOutput<MatrixOutput> next =
                    Slicer.filter( output, data -> data.getThresholds().equals( threshold ) );
            
            MetricOutputMetadata meta = next.getData().get( 0 ).getMetadata();
            
            List<RowCompareByLeft> rows = CommaSeparatedMatrixWriter.getRowsForOneMatrixOutput( next, formatter );

            // Add the header row
            rows.add( RowCompareByLeft.of( HEADER_INDEX,
                                           CommaSeparatedMatrixWriter.getMatrixOutputHeader( next, headerRow ) ) );

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
     * Helper that mutates the header for matrix based on the input.
     * 
     * @param output the matrix output
     * @param headerRow the header row
     * @return the mutated header
     */

    private static StringJoiner getMatrixOutputHeader( ListOfMetricOutput<MatrixOutput> output,
                                                       StringJoiner headerRow )
    {
        // Append to header
        StringJoiner returnMe = new StringJoiner( "," );
        returnMe.merge( headerRow );
        MatrixOutput data = output.getData().get( 0 );
        String metricName = data.getMetadata().getMetricID().toString();
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
        SortedSet<OneOrTwoThresholds> thresholds =
                Slicer.discover( output, next -> next.getMetadata().getThresholds() );
        for ( OneOrTwoThresholds nextThreshold : thresholds )
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
            getRowsForOneMatrixOutput( ListOfMetricOutput<MatrixOutput> output,
                                       Format formatter )
    {
        List<RowCompareByLeft> returnMe = new ArrayList<>();

        // Add the rows
        // Loop across time windows
        SortedSet<OneOrTwoThresholds> thresholds =
                Slicer.discover( output, meta -> meta.getMetadata().getThresholds() );
        SortedSet<TimeWindow> timeWindows = Slicer.discover( output, meta -> meta.getMetadata().getTimeWindow() );
        for ( TimeWindow timeWindow : timeWindows )
        {
            // Loop across the thresholds, merging results when multiple thresholds occur
            List<Double> merge = new ArrayList<>();
            for ( OneOrTwoThresholds threshold : thresholds )
            {
                // One output per time window and threshold
                MatrixOutput nextOutput = Slicer.filter( output,
                                                         data -> data.getThresholds().equals( threshold )
                                                                 && data.getTimeWindow().equals( timeWindow ) )
                                                .getData()
                                                .get( 0 );

                // Add the row
                nextOutput.iterator().forEachRemaining( merge::add );
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

    private CommaSeparatedMatrixWriter( ProjectConfig projectConfig )
    {
        super( projectConfig );
    }

}
