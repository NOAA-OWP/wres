package wres.io.writing.commaseparated;

import java.io.IOException;
import java.nio.file.Path;
import java.text.Format;
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
import java.util.function.Consumer;
import java.util.function.Supplier;

import wres.config.ProjectConfigException;
import wres.config.generated.DestinationConfig;
import wres.config.generated.OutputTypeSelection;
import wres.config.generated.ProjectConfig;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.StatisticGroup;
import wres.datamodel.Slicer;
import wres.datamodel.metadata.StatisticMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.statistics.ListOfStatistics;
import wres.datamodel.statistics.ScoreStatistic;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.Threshold;
import wres.io.config.ConfigHelper;

/**
 * Helps write scores comprising {@link ScoreStatistic} to a file of Comma Separated Values (CSV).
 * 
 * @param <T> the score component type
 * @author james.brown@hydrosolved.com
 */

public class CommaSeparatedScoreWriter<T extends ScoreStatistic<?, T>> extends CommaSeparatedWriter
        implements Consumer<ListOfStatistics<T>>, Supplier<Set<Path>>
{
    /**
     * Set of paths that this writer actually wrote to
     */
    private final Set<Path> pathsWrittenTo = new HashSet<>();

    /**
     * Returns an instance of a writer.
     * 
     * @param <T> the score component type
     * @param projectConfig the project configuration
     * @return a writer
     * @throws NullPointerException if the input is null 
     * @throws ProjectConfigException if the project configuration is not valid for writing
     */

    public static <T extends ScoreStatistic<?, T>> CommaSeparatedScoreWriter<T> of( final ProjectConfig projectConfig )
    {
        return new CommaSeparatedScoreWriter<>( projectConfig );
    }

    /**
     * Writes all output for one box plot type.
     *
     * @param output the score output
     * @throws NullPointerException if the input is null
     * @throws CommaSeparatedWriteException if the output cannot be written
     */

    @Override
    public void accept( final ListOfStatistics<T> output )
    {
        Objects.requireNonNull( output, "Specify non-null input data when writing box plot outputs." );

        // Write output
        // In principle, each destination could have a different formatter, so 
        // the output must be generated separately for each destination
        List<DestinationConfig> numericalDestinations = ConfigHelper.getNumericalDestinations( projectConfig );
        for ( DestinationConfig destinationConfig : numericalDestinations )
        {

            // Formatter required?
            Format formatter = null;
            if ( !output.getData().isEmpty()
                 && output.getData()
                          .get( 0 )
                          .getMetadata()
                          .getMetricID()
                          .isInGroup( StatisticGroup.DOUBLE_SCORE ) )
            {
                formatter = ConfigHelper.getDecimalFormatter( destinationConfig );
            }

            // Write per time-window
            try
            {
                Set<Path> innerPathsWrittenTo =
                        CommaSeparatedScoreWriter.writeOneScoreOutputType( destinationConfig,
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
     * Writes all output for one score type.
     *
     * @param <T> the score component type
     * @param destinationConfig the destination configuration    
     * @param output the score output to iterate through
     * @param formatter optional formatter, can be null
     * @throws IOException if the output cannot be written
     * @return set of paths actually written to
     */

    private static <T extends ScoreStatistic<?, T>> Set<Path> writeOneScoreOutputType( DestinationConfig destinationConfig,
                                                                                    ListOfStatistics<T> output,
                                                                                    Format formatter )
            throws IOException
    {
        Set<Path> pathsWrittenTo = new HashSet<>( 1 );

        // Loop across metrics
        SortedSet<MetricConstants> metrics = Slicer.discover( output, next -> next.getMetadata().getMetricID() );
        for ( MetricConstants m : metrics )
        {
            ListOfStatistics<T> nextMetric = Slicer.filter( output, m );

            SortedSet<Threshold> secondThreshold =
                    Slicer.discover( nextMetric,
                                     next -> next.getMetadata().getSampleMetadata().getThresholds().second() );

            // As many outputs as secondary thresholds if secondary thresholds are defined
            // and the output type is OutputTypeSelection.THRESHOLD_LEAD.
            List<ListOfStatistics<T>> allOutputs = new ArrayList<>();
            if ( destinationConfig.getOutputType() == OutputTypeSelection.THRESHOLD_LEAD
                 && !secondThreshold.isEmpty() )
            {
                // Slice by threshold two
                secondThreshold.forEach( next -> allOutputs.add( Slicer.filter( nextMetric,
                                                                                data -> data.getSampleMetadata()
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
            for ( ListOfStatistics<T> nextOutput : allOutputs )
            {
                StringJoiner headerRow = new StringJoiner( "," );
                headerRow.merge( HEADER_DEFAULT );
                List<RowCompareByLeft> rows =
                        CommaSeparatedScoreWriter.getRowsForOneScore( m,
                                                                      nextOutput,
                                                                      headerRow,
                                                                      formatter );

                // Add the header row
                rows.add( RowCompareByLeft.of( HEADER_INDEX, headerRow ) );

                // Write the output
                String append = null;

                // Secondary threshold? If yes, only one, as this was sliced above
                SortedSet<Threshold> secondThresholds =
                        Slicer.discover( nextOutput,
                                         next -> next.getMetadata().getSampleMetadata().getThresholds().second() );
                if ( !secondThresholds.isEmpty() )
                {
                    append = secondThresholds.iterator().next().toStringSafe();
                }
                StatisticMetadata meta = nextOutput.getData().get( 0 ).getMetadata();
                Path outputPath = ConfigHelper.getOutputPathToWrite( destinationConfig, meta, append );

                CommaSeparatedWriter.writeTabularOutputToFile( rows, outputPath );

                // If writeTabularOutputToFile did not throw an exception, assume
                // it succeeded in writing to the file, track outputs now (add must
                // be called after the above call).
                pathsWrittenTo.add( outputPath );
            }
        }

        return Collections.unmodifiableSet( pathsWrittenTo );
    }

    /**
     * Returns the results for one score output.
     *
     * @param <T> the score component type
     * @param scoreName the score name
     * @param output the score output
     * @param headerRow the header row
     * @param formatter optional formatter, can be null
     * @return the rows to write
     */

    private static <T extends ScoreStatistic<?, T>> List<RowCompareByLeft>
            getRowsForOneScore( MetricConstants scoreName,
                                ListOfStatistics<T> output,
                                StringJoiner headerRow,
                                Format formatter )
    {
        // Slice score by components
        Map<MetricConstants, ListOfStatistics<T>> helper = Slicer.filterByMetricComponent( output );

        String outerName = scoreName.toString();
        List<RowCompareByLeft> returnMe = new ArrayList<>();
        // Loop across components
        for ( Entry<MetricConstants, ListOfStatistics<T>> e : helper.entrySet() )
        {
            // Add the component name unless there is only one component named "MAIN"
            String name = outerName;
            if ( helper.size() > 1 || !helper.containsKey( MetricConstants.MAIN ) )
            {
                name = name + HEADER_DELIMITER + e.getKey().toString();
            }
            CommaSeparatedScoreWriter.addRowsForOneScoreComponent( name, e.getValue(), headerRow, returnMe, formatter );
        }
        return returnMe;
    }

    /**
     * Mutates the input header and rows, adding results for one score component.
     *
     * @param <T> the score component type
     * @param name the column name
     * @param component the score component results
     * @param headerRow the header row
     * @param rows the data rows
     * @param formatter optional formatter, can be null
     */

    private static <T extends ScoreStatistic<?, T>> void addRowsForOneScoreComponent( String name,
                                                                                   ListOfStatistics<T> component,
                                                                                   StringJoiner headerRow,
                                                                                   List<RowCompareByLeft> rows,
                                                                                   Format formatter )
    {

        // Discover the time windows and thresholds
        SortedSet<OneOrTwoThresholds> thresholds =
                Slicer.discover( component, meta -> meta.getMetadata().getSampleMetadata().getThresholds() );
        SortedSet<TimeWindow> timeWindows =
                Slicer.discover( component, meta -> meta.getMetadata().getSampleMetadata().getTimeWindow() );

        // Loop across the thresholds
        for ( OneOrTwoThresholds t : thresholds )
        {
            String column = name + HEADER_DELIMITER + t;
            headerRow.add( column );
            // Loop across time windows
            for ( TimeWindow timeWindow : timeWindows )
            {
                // Find the next score
                ListOfStatistics<T> nextScore = Slicer.filter( component,
                                                               next -> next.getSampleMetadata()
                                                                           .getThresholds()
                                                                           .equals( t )
                                                                       && next.getSampleMetadata()
                                                                              .getTimeWindow()
                                                                              .equals( timeWindow ) );
                if ( !nextScore.getData().isEmpty() )
                {
                    CommaSeparatedWriter.addRowToInput( rows,
                                                        timeWindow,
                                                        Arrays.asList( nextScore.getData().get( 0 ).getData() ),
                                                        formatter,
                                                        true );
                }
                // Write no data in place: see #48387
                else
                {
                    CommaSeparatedWriter.addRowToInput( rows,
                                                        timeWindow,
                                                        Arrays.asList( (Object) null ),
                                                        formatter,
                                                        true );
                }
            }
        }
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

    private CommaSeparatedScoreWriter( ProjectConfig projectConfig )
    {
        super( projectConfig );
    }

}
