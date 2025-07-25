package wres.writing.csv.statistics;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.datamodel.DataUtilities;
import wres.datamodel.Slicer;
import wres.config.MetricConstants;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.writing.csv.CommaSeparatedUtilities;

/**
 * Helps write paired output comprising {@link DurationDiagramStatisticOuter} to a file of Comma Separated Values (CSV).
 * 
 * @author James Brown
 * @deprecated since v5.8. Use the {@link CsvStatisticsWriter} instead.
 */

@Deprecated( since = "5.8", forRemoval = true )
public class CommaSeparatedDurationDiagramWriter extends CommaSeparatedStatisticsWriter
        implements Function<List<DurationDiagramStatisticOuter>, Set<Path>>
{

    private static final Logger LOGGER = LoggerFactory.getLogger( CommaSeparatedDurationDiagramWriter.class );

    /**
     * Returns an instance of a writer.
     * 
     * @param declaration the project declaration
     * @param outputDirectory the directory into which to write
     * @return a writer
     * @throws NullPointerException if either input is null
     */

    public static CommaSeparatedDurationDiagramWriter of( EvaluationDeclaration declaration,
                                                          Path outputDirectory )
    {
        return new CommaSeparatedDurationDiagramWriter( declaration, outputDirectory );
    }

    /**
     * Writes all output for one box plot type.
     *
     * @param statistics the box plot output
     * @throws NullPointerException if the input is null
     * @throws CommaSeparatedWriteException if the output cannot be written
     */

    @Override
    public Set<Path> apply( List<DurationDiagramStatisticOuter> statistics )
    {
        Objects.requireNonNull( statistics, "Specify non-null input data when writing box plot outputs." );

        Set<Path> paths = new HashSet<>();

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Writer {} received {} duration diagram statistics to write to CSV.",
                          this,
                          statistics.size() );
        }

        // Remove statistics that represent quantiles of a sampling distribution
        statistics = CommaSeparatedStatisticsWriter.filter( statistics );

        // Write per time-window
        try
        {
            // Group the statistics by the LRB context in which they appear. There will be one path written
            // for each group (e.g., one path for each window with DatasetOrientation.RIGHT data and one for
            // each window with DatasetOrientation.BASELINE data): #48287
            Map<DatasetOrientation, List<DurationDiagramStatisticOuter>> groups =
                    Slicer.getGroupedStatistics( statistics );

            for ( List<DurationDiagramStatisticOuter> nextGroup : groups.values() )
            {
                Set<Path> innerPathsWrittenTo =
                        CommaSeparatedDurationDiagramWriter.writeOnePairedOutputType( super.getOutputDirectory(),
                                                                                      nextGroup,
                                                                                      super.getDurationUnits() );

                paths.addAll( innerPathsWrittenTo );
            }
        }
        catch ( IOException e )
        {
            throw new CommaSeparatedWriteException( "While writing comma separated output: ", e );
        }

        return Collections.unmodifiableSet( paths );
    }

    /**
     * Writes all output for one paired type.
     *
     * @param outputDirectory the directory into which to write  
     * @param output the paired output to iterate through
     * @param durationUnits the time units for durations
     * @throws IOException if the output cannot be written
     * @return set of paths actually written to
     */

    private static Set<Path> writeOnePairedOutputType( Path outputDirectory,
                                                       List<DurationDiagramStatisticOuter> output,
                                                       ChronoUnit durationUnits )
            throws IOException
    {
        Set<Path> pathsWrittenTo = new HashSet<>( 1 );

        // Loop across metrics
        SortedSet<MetricConstants> metrics = Slicer.discover( output, DurationDiagramStatisticOuter::getMetricName );
        for ( MetricConstants m : metrics )
        {
            StringJoiner headerRow = new StringJoiner( "," );

            headerRow.add( "FEATURE DESCRIPTION" );

            StringJoiner timeWindowHeader =
                    CommaSeparatedUtilities.getTimeWindowHeaderFromSampleMetadata( output.get( 0 )
                                                                                         .getPoolMetadata(),
                                                                                   durationUnits );
            headerRow.merge( timeWindowHeader );

            List<DurationDiagramStatisticOuter> nextOutput = Slicer.filter( output, m );

            List<RowCompareByLeft> rows =
                    CommaSeparatedDurationDiagramWriter.getRowsForOnePairedOutput( m,
                                                                                   nextOutput,
                                                                                   headerRow,
                                                                                   durationUnits );

            // Add the header row
            rows.add( RowCompareByLeft.of( HEADER_INDEX, headerRow ) );

            // Write the output
            PoolMetadata meta = nextOutput.get( 0 ).getPoolMetadata();
            MetricConstants metricName = nextOutput.get( 0 ).getMetricName();

            Path outputPath = DataUtilities.getPathFromPoolMetadata( outputDirectory,
                                                                     meta,
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
     * Returns the results for one paired output.
     *
     * @param metricName the score name
     * @param output the paired output
     * @param headerRow the header row
     * @param durationUnits the time units for durations
     * @return the rows to write
     */

    private static List<RowCompareByLeft>
            getRowsForOnePairedOutput( MetricConstants metricName,
                                       List<DurationDiagramStatisticOuter> output,
                                       StringJoiner headerRow,
                                       ChronoUnit durationUnits )
    {
        String outerName = metricName.toString() + HEADER_DELIMITER;
        List<RowCompareByLeft> returnMe = new ArrayList<>();

        // Discover the time windows and thresholds
        SortedSet<OneOrTwoThresholds> thresholds =
                Slicer.discover( output, meta -> meta.getPoolMetadata().getThresholds() );

        PoolMetadata metadata = CommaSeparatedStatisticsWriter.getSampleMetadataFromListOfStatistics( output );

        // Add the rows
        // Loop across the thresholds
        for ( OneOrTwoThresholds t : thresholds )
        {
            // Append to header
            headerRow.add( outerName + "BASIS TIME" + HEADER_DELIMITER + t );
            headerRow.add( outerName + "DURATION" + HEADER_DELIMITER + t );

            // Slice by threshold
            List<DurationDiagramStatisticOuter> sliced = Slicer.filter( output,
                                                                        data -> data.getPoolMetadata()
                                                                                    .getThresholds()
                                                                                    .equals( t ) );

            // Loop across the outputs
            for ( DurationDiagramStatisticOuter next : sliced )
            {
                // Loop across the pairs
                for ( Pair<Instant, Duration> nextPair : next.getPairs() )
                {
                    CommaSeparatedStatisticsWriter.addRowToInput( returnMe,
                                                                  PoolMetadata.of( metadata,
                                                                                     next.getPoolMetadata()
                                                                                         .getTimeWindow() ),
                                                                  Arrays.asList( nextPair.getLeft(),
                                                                                 nextPair.getRight() ),
                                                                  null,
                                                                  // Append if there are multiple thresholds
                                                                  // otherwise, create a new row
                                                                  thresholds.size() > 1,
                                                                  durationUnits,
                                                                  nextPair.getLeft().toString() );
                }
            }
        }

        return returnMe;
    }

    /**
     * Hidden constructor.
     * 
     * @param declaration the project configuration
     * @param outputDirectory the directory into which to write
     * @throws NullPointerException if either input is null
     */

    private CommaSeparatedDurationDiagramWriter( EvaluationDeclaration declaration,
                                                 Path outputDirectory )
    {
        super( declaration, outputDirectory );
    }

}
