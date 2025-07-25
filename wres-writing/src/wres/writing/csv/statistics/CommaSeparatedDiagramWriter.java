package wres.writing.csv.statistics;

import java.nio.file.Path;
import java.text.Format;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.datamodel.DataUtilities;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.Slicer;
import wres.config.MetricConstants;
import wres.config.MetricConstants.MetricDimension;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.time.TimeWindowOuter;
import wres.writing.csv.CommaSeparatedUtilities;
import wres.statistics.generated.DiagramStatistic.DiagramStatisticComponent;
import wres.statistics.generated.Pool.EnsembleAverageType;

/**
 * Helps write box plots comprising {@link DiagramStatisticOuter} to a file of Comma Separated Values (CSV).
 *
 * @author James Brown
 * @deprecated since v5.8. Use the {@link CsvStatisticsWriter} instead.
 */

@Deprecated( since = "5.8", forRemoval = true )
public class CommaSeparatedDiagramWriter extends CommaSeparatedStatisticsWriter
        implements Function<List<DiagramStatisticOuter>, Set<Path>>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( CommaSeparatedDiagramWriter.class );

    /**
     * Returns an instance of a writer.
     *
     * @param declaration the project declaration
     * @param outputDirectory the directory into which to write
     * @return a writer
     * @throws NullPointerException if the input is null
     */

    public static CommaSeparatedDiagramWriter of( EvaluationDeclaration declaration,
                                                  Path outputDirectory )
    {
        return new CommaSeparatedDiagramWriter( declaration, outputDirectory );
    }

    /**
     * Writes all output for one diagram type.
     *
     * @param statistics the diagram output
     * @throws NullPointerException if either input is null 
     * @throws CommaSeparatedWriteException if the output cannot be written
     */

    @Override
    public Set<Path> apply( List<DiagramStatisticOuter> statistics )
    {
        Objects.requireNonNull( statistics, "Specify non-null input data when writing diagram outputs." );

        EvaluationDeclaration declaration = super.getDeclaration();
        if ( !declaration.validDatePools()
                         .isEmpty()
             || !declaration.referenceDatePools()
                            .isEmpty() )
        {
            LOGGER.warn( "The legacy CSV format does not support diagram metrics alongside pooling window declaration. "
                         + "As such, {} diagram statistics will not be written to the legacy CSV format. Please "
                         + "consider declaring the CSV2 format instead, which supports all metrics.",
                         statistics.size() );

            return Collections.emptySet();
        }

        Set<Path> paths = new HashSet<>();

        LOGGER.debug( "Writer {} received {} diagram statistics to write to CSV.",
                      this,
                      statistics.size() );

        // Remove statistics that represent quantiles of a sampling distribution
        statistics = CommaSeparatedStatisticsWriter.filter( statistics );

        // Formatter
        Format formatter = declaration.decimalFormat();

        // Default, per time-window
        // Group the statistics by the LRB context in which they appear. There will be one path written
        // for each group (e.g., one path for each window with LeftOrRightOrBaseline.RIGHT data and one for
        // each window with LeftOrRightOrBaseline.BASELINE data): #48287
        Map<DatasetOrientation, List<DiagramStatisticOuter>> groups =
                Slicer.getGroupedStatistics( statistics );

        for ( List<DiagramStatisticOuter> nextGroup : groups.values() )
        {
            // Slice by ensemble averaging type
            List<List<DiagramStatisticOuter>> sliced =
                    CommaSeparatedDiagramWriter.getSlicedStatistics( nextGroup );
            for ( List<DiagramStatisticOuter> nextSlice : sliced )
            {
                Set<Path> innerPathsWrittenTo =
                        CommaSeparatedDiagramWriter.writeOneDiagramOutputType( super.getOutputDirectory(),
                                                                               declaration,
                                                                               nextSlice,
                                                                               formatter,
                                                                               super.getDurationUnits() );
                paths.addAll( innerPathsWrittenTo );
            }
        }

        return Collections.unmodifiableSet( paths );
    }

    /**
     * Writes all output for one diagram type.
     *
     * @param outputDirectory the directory into which to write
     * @param declaration the declaration
     * @param output the diagram output
     * @param formatter optional formatter, can be null
     * @param durationUnits the time units for durations
     */

    private static Set<Path> writeOneDiagramOutputType( Path outputDirectory,
                                                        EvaluationDeclaration declaration,
                                                        List<DiagramStatisticOuter> output,
                                                        Format formatter,
                                                        ChronoUnit durationUnits )
    {
        Set<Path> pathsWrittenTo = new HashSet<>( 1 );

        // Loop across diagrams
        SortedSet<MetricConstants> metrics = Slicer.discover( output, DiagramStatisticOuter::getMetricName );
        for ( MetricConstants m : metrics )
        {
            StringJoiner headerRow =
                    CommaSeparatedUtilities.getTimeWindowHeaderFromSampleMetadata( output.get( 0 )
                                                                                         .getPoolMetadata(),
                                                                                   durationUnits );

            Set<Path> innerPathsWrittenTo;

            // Default, per time-window
            innerPathsWrittenTo =
                    CommaSeparatedDiagramWriter.writeOneDiagramOutputTypePerTimeWindow( outputDirectory,
                                                                                        Slicer.filter( output, m ),
                                                                                        headerRow,
                                                                                        formatter,
                                                                                        durationUnits,
                                                                                        declaration );

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
     * @param declaration the declaration
     * @return set of paths actually written to
     */

    private static Set<Path> writeOneDiagramOutputTypePerTimeWindow( Path outputDirectory,
                                                                     List<DiagramStatisticOuter> output,
                                                                     StringJoiner headerRow,
                                                                     Format formatter,
                                                                     ChronoUnit durationUnits,
                                                                     EvaluationDeclaration declaration )
    {
        Set<Path> pathsWrittenTo = new HashSet<>( 1 );

        // Loop across time windows
        SortedSet<TimeWindowOuter> timeWindows =
                Slicer.discover( output, next -> next.getPoolMetadata().getTimeWindow() );
        for ( TimeWindowOuter timeWindow : timeWindows )
        {
            List<DiagramStatisticOuter> next =
                    Slicer.filter( output, data -> data.getPoolMetadata()
                                                       .getTimeWindow()
                                                       .equals( timeWindow ) );

            MetricConstants metricName = next.get( 0 ).getMetricName();
            PoolMetadata meta = next.get( 0 ).getPoolMetadata();

            List<RowCompareByLeft> rows =
                    CommaSeparatedDiagramWriter.getRowsForOneDiagram( next, formatter, durationUnits );

            // Add the header row
            rows.add( RowCompareByLeft.of( HEADER_INDEX,
                                           CommaSeparatedDiagramWriter.getDiagramHeader( next,
                                                                                         headerRow ) ) );

            // Write the output
            String append = CommaSeparatedDiagramWriter.getPathQualifier( timeWindow,
                                                                          declaration,
                                                                          durationUnits,
                                                                          output );

            Path outputPath = DataUtilities.getPathFromPoolMetadata( outputDirectory,
                                                                     meta,
                                                                     append,
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
                Slicer.discover( output, meta -> meta.getPoolMetadata().getThresholds() );
        SortedSet<TimeWindowOuter> timeWindows =
                Slicer.discover( output, meta -> meta.getPoolMetadata().getTimeWindow() );

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
                                                                  data -> data.getPoolMetadata()
                                                                              .getThresholds()
                                                                              .equals( threshold )
                                                                          && data.getPoolMetadata()
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
            int maxRows = output.getStatistic()
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
        SortedSet<String> qualifiers = next.getStatistic()
                                           .getStatisticsList()
                                           .stream()
                                           .map( DiagramStatisticComponent::getName )
                                           .collect( Collectors.toCollection( TreeSet::new ) );

        for ( String qualifier : qualifiers )
        {
            for ( MetricDimension nextDimension : dimensions )
            {
                List<Double> doubles = CommaSeparatedDiagramWriter.getComponent( next, nextDimension, qualifier );

                // Populate the values
                double addMe = Double.NaN;
                if ( row < doubles.size() )
                {
                    addMe = doubles.get( row );
                }

                valuesToAdd.add( addMe );
            }
        }
        return valuesToAdd;
    }

    /**
     * Returns a prescribed vector from the map or null if no mapping exists.
     *
     * @param diagram the diagram
     * @param name the component name
     * @param qualifier the component qualifier
     * @return a vector or null
     */

    private static List<Double> getComponent( DiagramStatisticOuter diagram,
                                              MetricDimension name,
                                              String qualifier )
    {
        DiagramStatisticComponent component = diagram.getComponent( name, qualifier );
        return component.getValuesList();
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

        SortedSet<String> qualifiers = data.getStatistic()
                                           .getStatisticsList()
                                           .stream()
                                           .map( DiagramStatisticComponent::getName )
                                           .collect( Collectors.toCollection( TreeSet::new ) );

        //Add the metric name, dimension, and threshold for each column-vector
        SortedSet<OneOrTwoThresholds> thresholds =
                Slicer.discover( output, meta -> meta.getPoolMetadata().getThresholds() );
        for ( OneOrTwoThresholds nextThreshold : thresholds )
        {
            for ( String qualifier : qualifiers )
            {
                for ( MetricDimension nextDimension : dimensions )
                {
                    String q = qualifier;
                    if ( !q.isEmpty() )
                    {
                        q = HEADER_DELIMITER + q;
                    }

                    returnMe.add( HEADER_DELIMITER + metricName
                                  + HEADER_DELIMITER
                                  + nextDimension.toString()
                                  + q
                                  + HEADER_DELIMITER
                                  + nextThreshold.toString() );
                }
            }
        }

        return returnMe;
    }

    /**
     * Slices the statistics for individual graphics. Returns as many sliced lists of statistics as graphics to create.
     *
     * @param statistics the statistics to slice
     * @return the sliced statistics to write
     */

    private static List<List<DiagramStatisticOuter>> getSlicedStatistics( List<DiagramStatisticOuter> statistics )
    {
        List<List<DiagramStatisticOuter>> sliced = new ArrayList<>();

        // Slice by ensemble averaging function and then by secondary threshold
        for ( EnsembleAverageType type : EnsembleAverageType.values() )
        {
            List<DiagramStatisticOuter> innerSlice = Slicer.filter( statistics,
                                                                    value -> type == value.getPoolMetadata()
                                                                                          .getPoolDescription()
                                                                                          .getEnsembleAverageType() );
            // Slice by secondary threshold
            if ( !innerSlice.isEmpty() )
            {
                sliced.add( innerSlice );
            }
        }

        return Collections.unmodifiableList( sliced );
    }

    /**
     * Generates a path qualifier based on the statistics provided.
     * @param timeWindow the time window
     * @param declaration the declaration
     * @param leadUnits the lead duration units
     * @param statistics the statistics
     * @return a path qualifier or null if non is required
     */

    private static String getPathQualifier( TimeWindowOuter timeWindow,
                                            EvaluationDeclaration declaration,
                                            ChronoUnit leadUnits,
                                            List<DiagramStatisticOuter> statistics )
    {
        // Pooling windows
        if ( !declaration.validDatePools()
                         .isEmpty()
             || Objects.nonNull( declaration.eventDetection() )
             || !declaration.timePools()
                            .isEmpty() )
        {
            return DataUtilities.toStringSafe( timeWindow, leadUnits );
        }

        // Qualify all windows with the latest lead duration, unless it is the maximum value
        String windowQualifier = "";

        // Needs to be fully qualified, but this would change the file names, which is arguably a breaking change
        // See GitHub ticket #540
        if ( !timeWindow.getLatestLeadDuration()
                        .equals( TimeWindowOuter.DURATION_MAX ) )
        {
            windowQualifier = DataUtilities.toStringSafe( timeWindow.getLatestLeadDuration(), leadUnits )
                              + "_"
                              + leadUnits.name()
                                         .toUpperCase();
        }

        return windowQualifier
               + CommaSeparatedDiagramWriter.getEnsembleAverageQualifierString( statistics );
    }

    /**
     * Creates qualifier string for the ensemble average type where needed.
     * @param statistics the statistics to inspect
     * @return the qualifier string
     */

    private static String getEnsembleAverageQualifierString( List<DiagramStatisticOuter> statistics )
    {
        String append = "";

        // Non-default averaging types that should be qualified?
        // #51670
        SortedSet<EnsembleAverageType> types =
                Slicer.discover( statistics,
                                 next -> next.getPoolMetadata().getPoolDescription().getEnsembleAverageType() );

        Optional<EnsembleAverageType> type =
                types.stream()
                     .filter( next -> next != EnsembleAverageType.MEAN && next != EnsembleAverageType.NONE
                                      && next != EnsembleAverageType.UNRECOGNIZED )
                     .findFirst();

        if ( type.isPresent() )
        {
            append = "_ENSEMBLE_" + type.get()
                                        .name();
        }

        return append;
    }

    /**
     * Hidden constructor.
     *
     * @param declaration the project configuration
     * @param outputDirectory the directory into which to write
     * @throws NullPointerException if either input is null 
     */

    private CommaSeparatedDiagramWriter( EvaluationDeclaration declaration,
                                         Path outputDirectory )
    {
        super( declaration, outputDirectory );
    }
}
