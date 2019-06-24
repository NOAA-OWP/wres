package wres.io.writing.commaseparated.statistics;

import java.io.IOException;
import java.nio.file.Path;
import java.text.Format;
import java.time.temporal.ChronoUnit;
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
import wres.datamodel.statistics.ListOfStatistics;
import wres.datamodel.statistics.MatrixStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.time.TimeWindow;
import wres.io.config.ConfigHelper;
import wres.io.writing.commaseparated.CommaSeparatedUtilities;

/**
 * Helps write contingency tables comprising {@link MatrixStatistic} to a file of Comma Separated Values (CSV).
 * 
 * @author james.brown@hydrosolved.com
 */

public class CommaSeparatedMatrixWriter extends CommaSeparatedStatisticsWriter
        implements Consumer<ListOfStatistics<MatrixStatistic>>, Supplier<Set<Path>>
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

    public static CommaSeparatedMatrixWriter of( ProjectConfig projectConfig,
                                                 ChronoUnit durationUnits,
                                                 Path outputDirectory )
    {
        return new CommaSeparatedMatrixWriter( projectConfig, durationUnits, outputDirectory );
    }

    /**
     * Writes all output for one matrix type.
     *
     * @param output the matrix output
     * @throws NullPointerException if the input is null
     * @throws CommaSeparatedWriteException if the output cannot be written
     */

    @Override
    public void accept( final ListOfStatistics<MatrixStatistic> output )
    {
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

            // Default, per time-window
            try
            {
                Set<Path> innerPathsWrittenTo =
                        CommaSeparatedMatrixWriter.writeOneMatrixOutputType( super.getOutputDirectory(),
                                                                             super.getProjectConfig(),
                                                                             destinationConfig,
                                                                             output,
                                                                             formatter,
                                                                             super.getDurationUnits() );
                this.pathsWrittenTo.addAll( innerPathsWrittenTo );
            }
            catch ( IOException e )
            {
                throw new CommaSeparatedWriteException( "While writing comma separated output: ", e );
            }
        }
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
     * Writes all output for one matrix type.
     *
     * @param outputDirectory the directory into which to write
     * @param projectConfig the project configuration
     * @param destinationConfig the destination configuration    
     * @param output the matrix output
     * @param formatter optional formatter, can be null
     * @param durationUnits the time units for durations
     * @throws IOException if the output cannot be written
     */

    private static Set<Path> writeOneMatrixOutputType( Path outputDirectory,
                                                       ProjectConfig projectConfig,
                                                       DestinationConfig destinationConfig,
                                                       ListOfStatistics<MatrixStatistic> output,
                                                       Format formatter,
                                                       ChronoUnit durationUnits )
            throws IOException
    {
        Set<Path> pathsWrittenTo = new HashSet<>( 1 );

        // Obtain the output type configuration with any override for ALL_VALID metrics
        OutputTypeSelection diagramType = ConfigHelper.getOutputTypeSelection( projectConfig, destinationConfig );

        // Loop across metrics
        SortedSet<MetricConstants> metrics = Slicer.discover( output, next -> next.getMetadata().getMetricID() );
        for ( MetricConstants m : metrics )
        {

            StringJoiner headerRow = CommaSeparatedUtilities.getPartialTimeWindowHeaderFromSampleMetadata( output.getData()
                                                                                                          .get( 0 )
                                                                                                          .getMetadata()
                                                                                                          .getSampleMetadata(),
                                                                                                    durationUnits );

            Set<Path> innerPathsWrittenTo = Collections.emptySet();

            // Default, per time-window
            if ( diagramType == OutputTypeSelection.DEFAULT || diagramType == OutputTypeSelection.LEAD_THRESHOLD )
            {
                innerPathsWrittenTo =
                        CommaSeparatedMatrixWriter.writeOneMatrixOutputTypePerTimeWindow( outputDirectory,
                                                                                          destinationConfig,
                                                                                          Slicer.filter( output, m ),
                                                                                          headerRow,
                                                                                          formatter,
                                                                                          durationUnits );
            }
            // Per threshold
            else if ( diagramType == OutputTypeSelection.THRESHOLD_LEAD )
            {
                innerPathsWrittenTo =
                        CommaSeparatedMatrixWriter.writeOneMatrixOutputTypePerThreshold( outputDirectory,
                                                                                         destinationConfig,
                                                                                         Slicer.filter( output, m ),
                                                                                         headerRow,
                                                                                         formatter,
                                                                                         durationUnits );
            }

            pathsWrittenTo.addAll( innerPathsWrittenTo );
        }

        return Collections.unmodifiableSet( pathsWrittenTo );
    }

    /**
     * Writes one matrix output for all thresholds at each time window in the input.
     *
     * @param outputDirectory the directory into which to write
     * @param destinationConfig the destination configuration    
     * @param output the matrix output
     * @param headerRow the header row
     * @param formatter optional formatter, can be null
     * @param durationUnits the time units for durations
     * @throws IOException if the output cannot be written
     * @return set of paths actually written to
     */

    private static Set<Path> writeOneMatrixOutputTypePerTimeWindow( Path outputDirectory,
                                                                    DestinationConfig destinationConfig,
                                                                    ListOfStatistics<MatrixStatistic> output,
                                                                    StringJoiner headerRow,
                                                                    Format formatter,
                                                                    ChronoUnit durationUnits )
            throws IOException
    {
        Set<Path> pathsWrittenTo = new HashSet<>( 1 );

        // Loop across time windows
        SortedSet<TimeWindow> timeWindows =
                Slicer.discover( output, next -> next.getMetadata().getSampleMetadata().getTimeWindow() );
        for ( TimeWindow timeWindow : timeWindows )
        {
            ListOfStatistics<MatrixStatistic> next =
                    Slicer.filter( output, data -> data.getSampleMetadata().getTimeWindow().equals( timeWindow ) );

            StatisticMetadata meta = next.getData().get( 0 ).getMetadata();

            List<RowCompareByLeft> rows =
                    CommaSeparatedMatrixWriter.getRowsForOneMatrixOutput( next, formatter, durationUnits );

            // Add the header row
            rows.add( RowCompareByLeft.of( HEADER_INDEX,
                                           CommaSeparatedMatrixWriter.getMatrixOutputHeader( next, headerRow ) ) );

            // Write the output
            Path outputPath = ConfigHelper.getOutputPathToWrite( outputDirectory,
                                                                 destinationConfig,
                                                                 meta,
                                                                 timeWindow,
                                                                 durationUnits );

            CommaSeparatedStatisticsWriter.writeTabularOutputToFile( rows, outputPath );

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
     * @param outputDirectory the directory into which to write
     * @param destinationConfig the destination configuration    
     * @param output the matrix output
     * @param headerRow the header row
     * @param formatter optional formatter, can be null
     * @param durationUnits the time units for durations
     * @throws IOException if the output cannot be written
     * @return set of paths actually written to
     */

    private static Set<Path> writeOneMatrixOutputTypePerThreshold( Path outputDirectory,
                                                                   DestinationConfig destinationConfig,
                                                                   ListOfStatistics<MatrixStatistic> output,
                                                                   StringJoiner headerRow,
                                                                   Format formatter,
                                                                   ChronoUnit durationUnits )
            throws IOException
    {
        Set<Path> pathsWrittenTo = new HashSet<>( 1 );

        // Loop across thresholds
        SortedSet<OneOrTwoThresholds> thresholds =
                Slicer.discover( output, meta -> meta.getMetadata().getSampleMetadata().getThresholds() );
        for ( OneOrTwoThresholds threshold : thresholds )
        {
            ListOfStatistics<MatrixStatistic> next =
                    Slicer.filter( output, data -> data.getSampleMetadata().getThresholds().equals( threshold ) );

            StatisticMetadata meta = next.getData().get( 0 ).getMetadata();

            List<RowCompareByLeft> rows =
                    CommaSeparatedMatrixWriter.getRowsForOneMatrixOutput( next, formatter, durationUnits );

            // Add the header row
            rows.add( RowCompareByLeft.of( HEADER_INDEX,
                                           CommaSeparatedMatrixWriter.getMatrixOutputHeader( next, headerRow ) ) );

            // Write the output
            Path outputPath = ConfigHelper.getOutputPathToWrite( outputDirectory,
                                                                 destinationConfig,
                                                                 meta,
                                                                 threshold );

            CommaSeparatedStatisticsWriter.writeTabularOutputToFile( rows, outputPath );

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

    private static StringJoiner getMatrixOutputHeader( ListOfStatistics<MatrixStatistic> output,
                                                       StringJoiner headerRow )
    {
        // Append to header
        StringJoiner returnMe = new StringJoiner( "," );
        returnMe.merge( headerRow );
        MatrixStatistic data = output.getData().get( 0 );
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
                Slicer.discover( output, next -> next.getMetadata().getSampleMetadata().getThresholds() );
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
     * @param durationUnits the time units for durations
     * @return the rows to write
     */

    private static List<RowCompareByLeft>
            getRowsForOneMatrixOutput( ListOfStatistics<MatrixStatistic> output,
                                       Format formatter,
                                       ChronoUnit durationUnits )
    {
        List<RowCompareByLeft> returnMe = new ArrayList<>();

        // Add the rows
        // Loop across time windows
        SortedSet<OneOrTwoThresholds> thresholds =
                Slicer.discover( output, meta -> meta.getMetadata().getSampleMetadata().getThresholds() );
        SortedSet<TimeWindow> timeWindows =
                Slicer.discover( output, meta -> meta.getMetadata().getSampleMetadata().getTimeWindow() );
        for ( TimeWindow timeWindow : timeWindows )
        {
            // Loop across the thresholds, merging results when multiple thresholds occur
            List<Double> merge = new ArrayList<>();
            for ( OneOrTwoThresholds threshold : thresholds )
            {
                // One output per time window and threshold
                MatrixStatistic nextOutput = Slicer.filter( output,
                                                            data -> data.getSampleMetadata()
                                                                        .getThresholds()
                                                                        .equals( threshold )
                                                                    && data.getSampleMetadata()
                                                                           .getTimeWindow()
                                                                           .equals( timeWindow ) )
                                                   .getData()
                                                   .get( 0 );

                // Add the row
                nextOutput.iterator().forEachRemaining( merge::add );
            }
            // Add the merged row
            CommaSeparatedStatisticsWriter.addRowToInput( returnMe,
                                                timeWindow,
                                                merge,
                                                formatter,
                                                false,
                                                durationUnits );
        }

        return returnMe;
    }

    /**
     * Return a snapshot of the paths written to (so far)
     * 
     * @return the paths written so far.
     */

    private Set<Path> getPathsWrittenTo()
    {
        return Collections.unmodifiableSet( this.pathsWrittenTo );
    }

    /**
     * Hidden constructor.
     * 
     * @param projectConfig the project configuration
     * @param durationUnits the time units for durations
     * @param outputDirectory the directory into which to write
     * @throws NullPointerException if either input is null 
     * @throws ProjectConfigException if the project configuration is not valid for writing 
     */

    private CommaSeparatedMatrixWriter( ProjectConfig projectConfig,
                                        ChronoUnit durationUnits,
                                        Path outputDirectory )
    {
        super( projectConfig, durationUnits, outputDirectory );
    }

}
