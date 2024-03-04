package wres.writing.csv.statistics;

import java.io.IOException;
import java.nio.file.Path;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.datamodel.DataUtilities;
import wres.datamodel.Slicer;
import wres.config.MetricConstants;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.ScoreStatistic;
import wres.datamodel.statistics.ScoreStatistic.ScoreComponent;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.time.TimeWindowOuter;
import wres.writing.csv.CommaSeparatedUtilities;
import wres.statistics.generated.Pool.EnsembleAverageType;

/**
 * Helps write scores comprising {@link ScoreStatistic} to a file of Comma Separated Values (CSV).
 *
 * @param <S>  the score component type
 * @param <T> the score type
 * @author James Brown
 * @deprecated since v5.8. Use the {@link CsvStatisticsWriter} instead.
 */

@Deprecated( since = "5.8", forRemoval = true )
public class CommaSeparatedScoreWriter<S extends ScoreComponent<?>, T extends ScoreStatistic<?, S>>
        extends CommaSeparatedStatisticsWriter
        implements Function<List<T>, Set<Path>>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( CommaSeparatedScoreWriter.class );

    /**
     * Mapper that provides a string representation of the score.
     */

    private final Function<S, String> mapper;

    /**
     * Returns an instance of a writer.
     *
     * @param <S>  the score component type
     * @param <T> the score type
     * @param declaration the project declaration
     * @param outputDirectory the directory into which to write
     * @param mapper a mapper function that provides a string representation of the score
     * @return a writer
     * @throws NullPointerException if either input is null
     */

    public static <S extends ScoreComponent<?>, T extends ScoreStatistic<?, S>> CommaSeparatedScoreWriter<S, T>
    of( EvaluationDeclaration declaration,
        Path outputDirectory,
        Function<S, String> mapper )
    {
        return new CommaSeparatedScoreWriter<>( declaration, outputDirectory, mapper );
    }

    /**
     * Writes all output for one score type.
     *
     * @param statistics the score output
     * @throws NullPointerException if the input is null
     * @throws CommaSeparatedWriteException if the output cannot be written
     */

    @Override
    public Set<Path> apply( List<T> statistics )
    {
        Objects.requireNonNull( statistics );

        LOGGER.debug( "Writer {} received {} score statistics to write to CSV.",
                      this,
                      statistics.size() );

        // Remove statistics that represent quantiles of a sampling distribution
        statistics = CommaSeparatedStatisticsWriter.filter( statistics );

        Set<Path> paths = new HashSet<>();

        // Write per time-window
        try
        {
            // Group the statistics by the LRB context in which they appear. There will be one path written
            // for each group (e.g., one path for each window with DatasetOrientation.RIGHT data and one for
            // each window with DatasetOrientation.BASELINE data): #48287
            Map<DatasetOrientation, List<T>> groups =
                    Slicer.getGroupedStatistics( statistics );

            for ( List<T> nextGroup : groups.values() )
            {
                Set<Path> innerPathsWrittenTo =
                        CommaSeparatedScoreWriter.writeOneScoreOutputType( super.getOutputDirectory(),
                                                                           nextGroup,
                                                                           this.getDurationUnits(),
                                                                           this.mapper );
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
     * Writes all output for one score type.
     *
     * @param <S>  the score component type
     * @param <T> the score type
     * @param outputDirectory the directory into which to write
     * @param destinationConfig the destination configuration    
     * @param statistics the score output to iterate through
     * @param durationUnits the time units for durations
     * @param mapper a mapper function that provides a string representation of the score
     * @throws IOException if the output cannot be written
     * @return set of paths actually written to
     */

    private static <S extends ScoreComponent<?>, T extends ScoreStatistic<?, S>> Set<Path>
    writeOneScoreOutputType( Path outputDirectory,
                             List<T> statistics,
                             ChronoUnit durationUnits,
                             Function<S, String> mapper )
            throws IOException
    {
        Set<Path> pathsWrittenTo = new HashSet<>( 1 );

        // Loop across metrics
        SortedSet<MetricConstants> metrics = Slicer.discover( statistics, T::getMetricName );
        for ( MetricConstants m : metrics )
        {
            List<T> nextMetric = Slicer.filter( statistics, m );

            // Get the sliced statistics
            List<List<T>> allOutputs = CommaSeparatedScoreWriter.getSlicedStatistics( nextMetric );

            // Process each output
            for ( List<T> nextOutput : allOutputs )
            {
                if ( !nextOutput.isEmpty() )
                {
                    StringJoiner headerRow = new StringJoiner( "," );

                    headerRow.add( "FEATURE DESCRIPTION" );

                    StringJoiner timeWindowHeader =
                            CommaSeparatedUtilities.getTimeWindowHeaderFromSampleMetadata( statistics.get( 0 )
                                                                                                     .getPoolMetadata(),
                                                                                           durationUnits );

                    headerRow.merge( timeWindowHeader );

                    List<RowCompareByLeft> rows =
                            CommaSeparatedScoreWriter.getRowsForOneScore( m,
                                                                          nextOutput,
                                                                          headerRow,
                                                                          durationUnits,
                                                                          mapper );

                    // Add the header row
                    rows.add( RowCompareByLeft.of( HEADER_INDEX, headerRow ) );

                    // Write the output
                    String append = CommaSeparatedScoreWriter.getPathQualifier( nextOutput );
                    PoolMetadata meta = nextOutput.get( 0 ).getPoolMetadata();
                    Path outputPath = DataUtilities.getPathFromPoolMetadata( outputDirectory,
                                                                             meta,
                                                                             append,
                                                                             m,
                                                                             null );

                    Path finishedPath = CommaSeparatedStatisticsWriter.writeTabularOutputToFile( rows, outputPath );

                    // If writeTabularOutputToFile did not throw an exception, assume
                    // it succeeded in writing to the file, track outputs now (add must
                    // be called after the above call).
                    pathsWrittenTo.add( finishedPath );
                }
            }
        }

        return Collections.unmodifiableSet( pathsWrittenTo );
    }

    /**
     * Returns the results for one score output.
     *
     * @param <S>  the score component type
     * @param <T> the score type
     * @param scoreName the score name
     * @param output the score output
     * @param headerRow the header row
     * @param durationUnits the time units for durations
     * @param mapper a mapper function that provides a string representation of the score
     * @return the rows to write
     */

    private static <S extends ScoreComponent<?>, T extends ScoreStatistic<?, S>> List<RowCompareByLeft>
    getRowsForOneScore( MetricConstants scoreName,
                        List<T> output,
                        StringJoiner headerRow,
                        ChronoUnit durationUnits,
                        Function<S, String> mapper )
    {
        // Slice score by components
        Map<MetricConstants, List<S>> helper = Slicer.filterByMetricComponent( output );

        String outerName = scoreName.toString();
        List<RowCompareByLeft> returnMe = new ArrayList<>();
        // Loop across components
        for ( Entry<MetricConstants, List<S>> e : helper.entrySet() )
        {
            // Add the component name unless there is only one component named "MAIN"
            String name = outerName;
            if ( helper.size() > 1 || !helper.containsKey( MetricConstants.MAIN ) )
            {
                name = name + HEADER_DELIMITER + e.getKey().toString();
            }
            CommaSeparatedScoreWriter.addRowsForOneScoreComponent( name,
                                                                   e.getValue(),
                                                                   headerRow,
                                                                   returnMe,
                                                                   durationUnits,
                                                                   mapper );
        }
        return returnMe;
    }

    /**
     * Mutates the input header and rows, adding results for one score component.
     *
     * @param <S> the score component type
     * @param name the column name
     * @param component the score component results
     * @param headerRow the header row
     * @param rows the data rows
     * @param durationUnits the time units for durations
     * @param mapper a mapper function that provides a string representation of the score
     */

    private static <S extends ScoreComponent<?>> void
    addRowsForOneScoreComponent( String name,
                                 List<S> component,
                                 StringJoiner headerRow,
                                 List<RowCompareByLeft> rows,
                                 ChronoUnit durationUnits,
                                 Function<S, String> mapper )
    {

        // Discover the time windows and thresholds
        SortedSet<OneOrTwoThresholds> thresholds =
                Slicer.discover( component, meta -> meta.getPoolMetadata().getThresholds() );
        SortedSet<TimeWindowOuter> timeWindows =
                Slicer.discover( component, meta -> meta.getPoolMetadata().getTimeWindow() );

        PoolMetadata metadata = CommaSeparatedStatisticsWriter.getSampleMetadataFromListOfStatistics( component );

        // Loop across the thresholds
        for ( OneOrTwoThresholds t : thresholds )
        {
            String column = name + HEADER_DELIMITER + t;

            headerRow.add( column );
            // Loop across time windows
            for ( TimeWindowOuter timeWindow : timeWindows )
            {
                // Find the next score
                List<S> nextScore = Slicer.filter( component,
                                                   next -> next.getPoolMetadata()
                                                               .getThresholds()
                                                               .equals( t )
                                                           && next.getPoolMetadata()
                                                                  .getTimeWindow()
                                                                  .equals( timeWindow ) );
                if ( !nextScore.isEmpty() )
                {
                    S score = nextScore.get( 0 );
                    String stringScore = mapper.apply( score );

                    CommaSeparatedStatisticsWriter.addRowToInput( rows,
                                                                  nextScore.get( 0 )
                                                                           .getPoolMetadata(),
                                                                  Collections.singletonList( stringScore ),
                                                                  null,
                                                                  true,
                                                                  durationUnits );
                }
                // Write no data in place: see #48387
                else
                {
                    CommaSeparatedStatisticsWriter.addRowToInput( rows,
                                                                  PoolMetadata.of( metadata, timeWindow ),
                                                                  Collections.singletonList( null ),
                                                                  null,
                                                                  true,
                                                                  durationUnits );
                }
            }
        }
    }

    /**
     * Slices the statistics for individual graphics. Returns as many sliced lists of statistics as graphics to create.
     *
     * @param destinationConfig the destination configuration
     * @param statistics the statistics to slice
     * @return the sliced statistics to write
     */

    private static <S extends ScoreComponent<?>, T extends ScoreStatistic<?, S>> List<List<T>>
    getSlicedStatistics( List<T> statistics )
    {
        List<List<T>> sliced = new ArrayList<>();

        SortedSet<ThresholdOuter> secondThreshold =
                Slicer.discover( statistics, next -> next.getPoolMetadata().getThresholds().second() );

        // Slice by ensemble averaging function
        for ( EnsembleAverageType type : EnsembleAverageType.values() )
        {
            List<T> innerSlice = Slicer.filter( statistics,
                                                value -> type == value.getPoolMetadata()
                                                                      .getPool()
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
     * @param destinationConfig the destination configuration
     * @param statistics the statistics
     * @return a path qualifier or null if non is required
     */

    private static <S extends ScoreComponent<?>, T extends ScoreStatistic<?, S>> String
    getPathQualifier( List<T> statistics )
    {
        String append = null;

        // Secondary threshold? If yes, only one, as this was sliced above
        SortedSet<ThresholdOuter> second =
                Slicer.discover( statistics,
                                 next -> next.getPoolMetadata()
                                             .getThresholds()
                                             .second() );

        // Non-default averaging types that should be qualified?
        // #51670
        SortedSet<EnsembleAverageType> types =
                Slicer.discover( statistics,
                                 next -> next.getPoolMetadata().getPool().getEnsembleAverageType() );

        Optional<EnsembleAverageType> type =
                types.stream()
                     .filter( next -> next != EnsembleAverageType.MEAN && next != EnsembleAverageType.NONE
                                      && next != EnsembleAverageType.UNRECOGNIZED )
                     .findFirst();

        if ( type.isPresent() )
        {
            if ( Objects.nonNull( append ) )
            {
                append = append + "_ENSEMBLE_"
                         + type.get()
                               .name();
            }
            else
            {
                append = "ENSEMBLE_" + type.get()
                                           .name();
            }
        }

        return append;
    }

    /**
     * Hidden constructor.
     *
     * @param declaration the project configuration
     * @param outputDirectory the directory into which to write
     * @param mapper a mapper function that provides a string representation of the score
     * @throws NullPointerException if either input is null 
     * @throws ProjectConfigException if the project configuration is not valid for writing 
     */

    private CommaSeparatedScoreWriter( EvaluationDeclaration declaration,
                                       Path outputDirectory,
                                       Function<S, String> mapper )
    {
        super( declaration, outputDirectory );

        Objects.requireNonNull( mapper );

        this.mapper = mapper;
    }

}
