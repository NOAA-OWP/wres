package wres.io.writing.commaseparated.statistics;

import java.io.IOException;
import java.nio.file.Path;
import java.text.Format;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.ProjectConfigs;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.OutputTypeSelection;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.Slicer;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.time.TimeWindowOuter;
import wres.io.config.ConfigHelper;
import wres.io.writing.commaseparated.CommaSeparatedUtilities;
import wres.statistics.generated.DiagramStatistic.DiagramStatisticComponent;

/**
 * Helps write box plots comprising {@link DiagramStatisticOuter} to a file of Comma Separated Values (CSV).
 * 
 * @author james.brown@hydrosolved.com
 */

public class CommaSeparatedDiagramWriter extends CommaSeparatedStatisticsWriter
        implements Function<List<DiagramStatisticOuter>,Set<Path>>
{
    
    private static final Logger LOGGER = LoggerFactory.getLogger( CommaSeparatedDiagramWriter.class );

    /**
     * Returns an instance of a writer.
     * 
     * @param projectConfig the project configuration
     * @param durationUnits the time units for durations
     * @param outputDirectory the directory into which to write
     * @return a writer
     * @throws NullPointerException if the input is null 
     * @throws ProjectConfigException if the project configuration is not valid for writing
     */

    public static CommaSeparatedDiagramWriter of( ProjectConfig projectConfig,
                                                  ChronoUnit durationUnits,
                                                  Path outputDirectory )
    {
        return new CommaSeparatedDiagramWriter( projectConfig, durationUnits, outputDirectory );
    }

    /**
     * Writes all output for one diagram type.
     *
     * @param output the diagram output
     * @throws NullPointerException if either input is null 
     * @throws CommaSeparatedWriteException if the output cannot be written
     */

    @Override
    public Set<Path> apply( final List<DiagramStatisticOuter> output )
    {
        Objects.requireNonNull( output, "Specify non-null input data when writing diagram outputs." );

        Set<Path> paths = new HashSet<>();
        
        // Write output
        // In principle, each destination could have a different formatter, so 
        // the output must be generated separately for each destination
        // #89191
        List<DestinationConfig> numericalDestinations = ProjectConfigs.getDestinationsOfType( super.getProjectConfig(),
                                                                                              DestinationType.NUMERIC,
                                                                                              DestinationType.CSV );
        
        LOGGER.debug( "Writer {} received {} diagram statistics to write to the destination types {}.",
                      this,
                      output.size(),
                      numericalDestinations );
        
        for ( DestinationConfig destinationConfig : numericalDestinations )
        {

            // Formatter
            Format formatter = ConfigHelper.getDecimalFormatter( destinationConfig );

            // Default, per time-window
            try
            {
                // Group the statistics by the LRB context in which they appear. There will be one path written
                // for each group (e.g., one path for each window with LeftOrRightOrBaseline.RIGHT data and one for 
                // each window with LeftOrRightOrBaseline.BASELINE data): #48287
                Map<LeftOrRightOrBaseline, List<DiagramStatisticOuter>> groups =
                        Slicer.getStatisticsGroupedByContext( output );

                for ( List<DiagramStatisticOuter> nextGroup : groups.values() )
                {
                    Set<Path> innerPathsWrittenTo =
                            CommaSeparatedDiagramWriter.writeOneDiagramOutputType( super.getOutputDirectory(),
                                                                                   super.getProjectConfig(),
                                                                                   destinationConfig,
                                                                                   nextGroup,
                                                                                   formatter,
                                                                                   super.getDurationUnits() );
                    paths.addAll( innerPathsWrittenTo );
                }
            }
            catch ( IOException e )
            {
                throw new CommaSeparatedWriteException( "While writing comma separated output: ", e );
            }
        }

        return Collections.unmodifiableSet( paths );      
    }

    /**
     * Writes all output for one diagram type.
     *
     * @param outputDirectory the directory into which to write
     * @param projectConfig the project configuration
     * @param destinationConfig the destination configuration    
     * @param output the diagram output
     * @param formatter optional formatter, can be null
     * @param durationUnits the time units for durations
     * @throws IOException if the output cannot be written
     */

    private static Set<Path> writeOneDiagramOutputType( Path outputDirectory,
                                                        ProjectConfig projectConfig,
                                                        DestinationConfig destinationConfig,
                                                        List<DiagramStatisticOuter> output,
                                                        Format formatter,
                                                        ChronoUnit durationUnits )
            throws IOException
    {
        Set<Path> pathsWrittenTo = new HashSet<>( 1 );

        // Obtain the output type configuration
        OutputTypeSelection diagramType = ConfigHelper.getOutputTypeSelection( projectConfig, destinationConfig );

        // Loop across diagrams
        SortedSet<MetricConstants> metrics = Slicer.discover( output, DiagramStatisticOuter::getMetricName );
        for ( MetricConstants m : metrics )
        {
            StringJoiner headerRow =
                    CommaSeparatedUtilities.getTimeWindowHeaderFromSampleMetadata( output.get( 0 )
                                                                                         .getMetadata(),
                                                                                   durationUnits );

            Set<Path> innerPathsWrittenTo = Collections.emptySet();

            // Default, per time-window
            if ( diagramType == OutputTypeSelection.DEFAULT || diagramType == OutputTypeSelection.LEAD_THRESHOLD )
            {
                innerPathsWrittenTo =
                        CommaSeparatedDiagramWriter.writeOneDiagramOutputTypePerTimeWindow( outputDirectory,
                                                                                            Slicer.filter( output, m ),
                                                                                            headerRow,
                                                                                            formatter,
                                                                                            durationUnits );
            }
            // Per threshold
            else if ( diagramType == OutputTypeSelection.THRESHOLD_LEAD )
            {
                innerPathsWrittenTo =
                        CommaSeparatedDiagramWriter.writeOneDiagramOutputTypePerThreshold( outputDirectory,
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
     * Writes one diagram for all thresholds at each time window in the input.
     *
     * @param outputDirectory the directory into which to write 
     * @param output the diagram output
     * @param headerRow the header row
     * @param formatter optional formatter, can be null
     * @param durationUnits the time units for durations
     * @throws IOException if the output cannot be written
     * @return set of paths actually written to
     */

    private static Set<Path> writeOneDiagramOutputTypePerTimeWindow( Path outputDirectory,
                                                                     List<DiagramStatisticOuter> output,
                                                                     StringJoiner headerRow,
                                                                     Format formatter,
                                                                     ChronoUnit durationUnits )
            throws IOException
    {
        Set<Path> pathsWrittenTo = new HashSet<>( 1 );

        // Loop across time windows
        SortedSet<TimeWindowOuter> timeWindows =
                Slicer.discover( output, next -> next.getMetadata().getTimeWindow() );
        for ( TimeWindowOuter timeWindow : timeWindows )
        {
            List<DiagramStatisticOuter> next =
                    Slicer.filter( output, data -> data.getMetadata().getTimeWindow().equals( timeWindow ) );

            MetricConstants metricName = next.get( 0 ).getMetricName();
            PoolMetadata meta = next.get( 0 ).getMetadata();

            List<RowCompareByLeft> rows = getRowsForOneDiagram( next, formatter, durationUnits );

            // Add the header row
            rows.add( RowCompareByLeft.of( HEADER_INDEX,
                                           CommaSeparatedDiagramWriter.getDiagramHeader( next,
                                                                                         headerRow ) ) );

            // Write the output
            Path outputPath = DataFactory.getPathFromSampleMetadata( outputDirectory,
                                                                     meta,
                                                                     timeWindow,
                                                                     durationUnits,
                                                                     metricName,
                                                                     null );

            Path finishedPath = CommaSeparatedStatisticsWriter.writeTabularOutputToFile( rows, outputPath );

            // If writeTabularOutputToFile did not throw an exception, assume
            // it succeeded in writing to the file, track outputs now (add must
            // be called after the above call).
            pathsWrittenTo.add( finishedPath );
        }

        return Collections.unmodifiableSet( pathsWrittenTo );
    }


    /**
     * Writes one diagram for all time windows at each threshold in the input.
     *
     * @param outputDirectory the directory into which to write 
     * @param output the diagram output
     * @param headerRow the header row
     * @param formatter optional formatter, can be null
     * @param durationUnits the time units for durations
     * @throws IOException if the output cannot be written
     * @return set of paths actually written to
     */

    private static Set<Path> writeOneDiagramOutputTypePerThreshold( Path outputDirectory,
                                                                    List<DiagramStatisticOuter> output,
                                                                    StringJoiner headerRow,
                                                                    Format formatter,
                                                                    ChronoUnit durationUnits )
            throws IOException
    {
        Set<Path> pathsWrittenTo = new HashSet<>( 1 );

        // Loop across thresholds
        SortedSet<OneOrTwoThresholds> thresholds =
                Slicer.discover( output, meta -> meta.getMetadata().getThresholds() );
        for ( OneOrTwoThresholds threshold : thresholds )
        {

            List<DiagramStatisticOuter> next =
                    Slicer.filter( output, data -> data.getMetadata().getThresholds().equals( threshold ) );

            MetricConstants metricName = next.get( 0 ).getMetricName();
            PoolMetadata meta = next.get( 0 ).getMetadata();

            List<RowCompareByLeft> rows =
                    CommaSeparatedDiagramWriter.getRowsForOneDiagram( next, formatter, durationUnits );

            // Add the header row
            rows.add( RowCompareByLeft.of( HEADER_INDEX,
                                           CommaSeparatedDiagramWriter.getDiagramHeader( next, headerRow ) ) );

            // Write the output
            Path outputPath = DataFactory.getPathFromSampleMetadata( outputDirectory,
                                                                     meta,
                                                                     threshold,
                                                                     metricName,
                                                                     null );

            Path finishedPath = CommaSeparatedStatisticsWriter.writeTabularOutputToFile( rows, outputPath );

            // If writeTabularOutputToFile did not throw an exception, assume
            // it succeeded in writing to the file, track outputs now (add must
            // be called after the above call).
            pathsWrittenTo.add( finishedPath );
        }

        return Collections.unmodifiableSet( pathsWrittenTo );
    }

    /**
     * Returns the results for one diagram output.
     *
     * @param output the diagram output
     * @param formatter optional formatter, can be null
     * @param durationUnits the time units for durations
     * @return the rows to write
     */

    private static List<RowCompareByLeft>
            getRowsForOneDiagram( List<DiagramStatisticOuter> output,
                                  Format formatter,
                                  ChronoUnit durationUnits )
    {
        List<RowCompareByLeft> returnMe = new ArrayList<>();

        // Discover the time windows and thresholds to loop
        SortedSet<OneOrTwoThresholds> thresholds =
                Slicer.discover( output, meta -> meta.getMetadata().getThresholds() );
        SortedSet<TimeWindowOuter> timeWindows =
                Slicer.discover( output, meta -> meta.getMetadata().getTimeWindow() );

        PoolMetadata metadata = CommaSeparatedStatisticsWriter.getSampleMetadataFromListOfStatistics( output );

        // Loop across time windows
        for ( TimeWindowOuter timeWindow : timeWindows )
        {
            // Loop across the thresholds, merging results when multiple thresholds occur
            Map<Integer, List<Double>> merge = new TreeMap<>();
            for ( OneOrTwoThresholds threshold : thresholds )
            {
                // One output per time window and threshold
                DiagramStatisticOuter nextOutput = Slicer.filter( output,
                                                                  data -> data.getMetadata()
                                                                              .getThresholds()
                                                                              .equals( threshold )
                                                                          && data.getMetadata()
                                                                                 .getTimeWindow()
                                                                                 .equals( timeWindow ) )
                                                         .get( 0 );
                CommaSeparatedDiagramWriter.addRowsForOneDiagramAtOneTimeWindowAndThreshold( nextOutput, merge );
            }
            // Add the merged rows
            for ( List<Double> next : merge.values() )
            {
                CommaSeparatedStatisticsWriter.addRowToInput( returnMe,
                                                              PoolMetadata.of( metadata, timeWindow ),
                                                              next,
                                                              formatter,
                                                              false,
                                                              durationUnits );
            }
        }

        return returnMe;
    }

    /**
     * Adds rows to the input map of merged rows for a specific {@link TimeWindowOuter} and {@link OneOrTwoThresholds}.
     *
     * @param output the diagram output
     * @param key the key for which rows are required
     * @param merge the merged rows to mutate
     */

    private static void
            addRowsForOneDiagramAtOneTimeWindowAndThreshold( DiagramStatisticOuter output,
                                                             Map<Integer, List<Double>> merge )
    {

        // Check that the output exists
        if ( Objects.nonNull( output ) )
        {

            // For safety, find the largest vector and use Double.NaN in place for vectors of differing size
            // In practice, all should be the same length
            int maxRows = output.getData()
                                .getStatisticsList()
                                .stream()
                                .mapToInt( DiagramStatisticComponent::getValuesCount )
                                .max()
                                .orElse( 0 );

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
     * Returns the row from the diagram at a specified  row index.
     * 
     * @param next the data
     * @param row the row index to generate
     * @return the row data
     */

    private static List<Double> getOneRowForOneDiagram( DiagramStatisticOuter next, int row )
    {
        List<Double> valuesToAdd = new ArrayList<>();

        SortedSet<MetricDimension> dimensions = next.getComponentNames();
        for ( MetricDimension nextDimension : dimensions )
        {
            VectorOfDoubles doubles = next.getComponent( nextDimension );

            // Populate the values
            Double addMe = Double.NaN;
            if ( row < doubles.size() )
            {
                addMe = doubles.getDoubles()[row];
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

    private static StringJoiner getDiagramHeader( List<DiagramStatisticOuter> output,
                                                  StringJoiner headerRow )
    {
        // Append to header
        StringJoiner returnMe = new StringJoiner( "," );

        returnMe.add( "FEATURE DESCRIPTION" );

        returnMe.merge( headerRow );
        // Discover first item to help
        DiagramStatisticOuter data = output.get( 0 );
        String metricName = data.getMetricName().toString();

        SortedSet<MetricDimension> dimensions = data.getComponentNames();
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
     * @param durationUnits the time units for durations
     * @param outputDirectory the directory into which to write
     * @throws NullPointerException if either input is null 
     * @throws ProjectConfigException if the project configuration is not valid for writing 
     */

    private CommaSeparatedDiagramWriter( ProjectConfig projectConfig,
                                         ChronoUnit durationUnits,
                                         Path outputDirectory )
    {
        super( projectConfig, durationUnits, outputDirectory );
    }

}
