package wres.io.writing.commaseparated.statistics;

import java.io.IOException;
import java.nio.file.Path;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringJoiner;
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
import wres.datamodel.Slicer;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.ScoreStatistic;
import wres.datamodel.statistics.ScoreStatistic.ScoreComponent;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.time.TimeWindowOuter;
import wres.io.writing.commaseparated.CommaSeparatedUtilities;

/**
 * Helps write scores comprising {@link ScoreStatistic} to a file of Comma Separated Values (CSV).
 * 
 * @param <S>  the score component type
 * @param <T> the score type
 * @author james.brown@hydrosolved.com
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
     * @param projectConfig the project configuration
     * @param durationUnits the time units for durations
     * @param outputDirectory the directory into which to write
     * @param mapper a mapper function that provides a string representation of the score
     * @return a writer
     * @throws NullPointerException if either input is null 
     * @throws ProjectConfigException if the project configuration is not valid for writing
     */

    public static <S extends ScoreComponent<?>, T extends ScoreStatistic<?, S>> CommaSeparatedScoreWriter<S, T>
            of( ProjectConfig projectConfig,
                ChronoUnit durationUnits,
                Path outputDirectory,
                Function<S, String> mapper )
    {
        return new CommaSeparatedScoreWriter<>( projectConfig, durationUnits, outputDirectory, mapper );
    }

    /**
     * Writes all output for one score type.
     *
     * @param output the score output
     * @throws NullPointerException if the input is null
     * @throws CommaSeparatedWriteException if the output cannot be written
     */

    @Override
    public Set<Path> apply( final List<T> output )
    {
        Objects.requireNonNull( output, "Specify non-null input data when writing box plot outputs." );

        // Write output
        // In principle, each destination could have a different formatter, so 
        // the output must be generated separately for each destination
        // #89191
        List<DestinationConfig> numericalDestinations = ProjectConfigs.getDestinationsOfType( super.getProjectConfig(),
                                                                                              DestinationType.NUMERIC,
                                                                                              DestinationType.CSV );

        LOGGER.debug( "Writer {} received {} score statistics to write to the destination types {}.",
                      this,
                      output.size(),
                      numericalDestinations );

        Set<Path> paths = new HashSet<>();

        for ( DestinationConfig destinationConfig : numericalDestinations )
        {
            // Write per time-window
            try
            {
                // Group the statistics by the LRB context in which they appear. There will be one path written
                // for each group (e.g., one path for each window with LeftOrRightOrBaseline.RIGHT data and one for 
                // each window with LeftOrRightOrBaseline.BASELINE data): #48287
                Map<LeftOrRightOrBaseline, List<T>> groups =
                        Slicer.getStatisticsGroupedByContext( output );

                for ( List<T> nextGroup : groups.values() )
                {
                    Set<Path> innerPathsWrittenTo =
                            CommaSeparatedScoreWriter.writeOneScoreOutputType( super.getOutputDirectory(),
                                                                               destinationConfig,
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
     * @param output the score output to iterate through
     * @param durationUnits the time units for durations
     * @param mapper a mapper function that provides a string representation of the score
     * @throws IOException if the output cannot be written
     * @return set of paths actually written to
     */

    private static <S extends ScoreComponent<?>, T extends ScoreStatistic<?, S>> Set<Path>
            writeOneScoreOutputType( Path outputDirectory,
                                     DestinationConfig destinationConfig,
                                     List<T> output,
                                     ChronoUnit durationUnits,
                                     Function<S, String> mapper )
                    throws IOException
    {
        Set<Path> pathsWrittenTo = new HashSet<>( 1 );

        // Loop across metrics
        SortedSet<MetricConstants> metrics = Slicer.discover( output, T::getMetricName );
        for ( MetricConstants m : metrics )
        {
            List<T> nextMetric = Slicer.filter( output, m );

            SortedSet<ThresholdOuter> secondThreshold =
                    Slicer.discover( nextMetric,
                                     next -> next.getMetadata().getThresholds().second() );

            // As many outputs as secondary thresholds if secondary thresholds are defined
            // and the output type is OutputTypeSelection.THRESHOLD_LEAD.
            List<List<T>> allOutputs = new ArrayList<>();
            if ( destinationConfig.getOutputType() == OutputTypeSelection.THRESHOLD_LEAD
                 && !secondThreshold.isEmpty() )
            {
                // Slice by threshold two
                secondThreshold.forEach( next -> allOutputs.add( Slicer.filter( nextMetric,
                                                                                data -> data.getMetadata()
                                                                                            .getThresholds()
                                                                                            .second()
                                                                                            .equals( next ) ) ) );
            }
            // One output only
            else
            {
                allOutputs.add( nextMetric );
            }

            // Process each output
            for ( List<T> nextOutput : allOutputs )
            {
                StringJoiner headerRow = new StringJoiner( "," );

                headerRow.add( "FEATURE DESCRIPTION" );

                StringJoiner timeWindowHeader =
                        CommaSeparatedUtilities.getTimeWindowHeaderFromSampleMetadata( output.get( 0 )
                                                                                             .getMetadata(),
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
                String append = null;

                // Secondary threshold? If yes, only one, as this was sliced above
                SortedSet<ThresholdOuter> secondThresholds =
                        Slicer.discover( nextOutput,
                                         next -> next.getMetadata().getThresholds().second() );
                if ( destinationConfig.getOutputType() == OutputTypeSelection.THRESHOLD_LEAD
                     && !secondThresholds.isEmpty() )
                {
                    append = secondThresholds.iterator().next().toStringSafe();
                }

                PoolMetadata meta = nextOutput.get( 0 ).getMetadata();
                Path outputPath = DataFactory.getPathFromSampleMetadata( outputDirectory,
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
                Slicer.discover( component, meta -> meta.getMetadata().getThresholds() );
        SortedSet<TimeWindowOuter> timeWindows =
                Slicer.discover( component, meta -> meta.getMetadata().getTimeWindow() );

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
                                                   next -> next.getMetadata()
                                                               .getThresholds()
                                                               .equals( t )
                                                           && next.getMetadata()
                                                                  .getTimeWindow()
                                                                  .equals( timeWindow ) );
                if ( !nextScore.isEmpty() )
                {
                    S score = nextScore.get( 0 );
                    String stringScore = mapper.apply( score );

                    CommaSeparatedStatisticsWriter.addRowToInput( rows,
                                                                  nextScore.get( 0 )
                                                                           .getMetadata(),
                                                                  Arrays.asList( stringScore ),
                                                                  null,
                                                                  true,
                                                                  durationUnits );
                }
                // Write no data in place: see #48387
                else
                {
                    CommaSeparatedStatisticsWriter.addRowToInput( rows,
                                                                  PoolMetadata.of( metadata, timeWindow ),
                                                                  Arrays.asList( (Object) null ),
                                                                  null,
                                                                  true,
                                                                  durationUnits );
                }
            }
        }
    }

    /**
     * Hidden constructor.
     * 
     * @param projectConfig the project configuration
     * @param durationUnits the time units for durations
     * @param outputDirectory the directory into which to write
     * @param mapper a mapper function that provides a string representation of the score
     * @throws NullPointerException if either input is null 
     * @throws ProjectConfigException if the project configuration is not valid for writing 
     */

    private CommaSeparatedScoreWriter( ProjectConfig projectConfig,
                                       ChronoUnit durationUnits,
                                       Path outputDirectory,
                                       Function<S, String> mapper )
    {
        super( projectConfig, durationUnits, outputDirectory );

        Objects.requireNonNull( mapper );

        this.mapper = mapper;
    }

}
