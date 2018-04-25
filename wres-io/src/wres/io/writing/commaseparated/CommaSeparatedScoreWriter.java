package wres.io.writing.commaseparated;

import java.io.IOException;
import java.nio.file.Path;
import java.text.Format;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Consumer;

import org.apache.commons.lang3.tuple.Pair;

import wres.config.ProjectConfigException;
import wres.config.generated.DestinationConfig;
import wres.config.generated.OutputTypeSelection;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.MapKey;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMultiMapByTimeAndThreshold;
import wres.datamodel.outputs.ScoreOutput;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.io.config.ConfigHelper;

/**
 * Helps write scores comprising {@link ScoreOutput} to a file of Comma Separated Values (CSV).
 * 
 * @param <T> the score component type
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 1.0
 */

public class CommaSeparatedScoreWriter<T extends ScoreOutput<?, T>> extends CommaSeparatedWriter
        implements Consumer<MetricOutputMultiMapByTimeAndThreshold<T>>
{

    /**
     * Returns an instance of a writer.
     * 
     * @param <T> the score component type
     * @param projectConfig the project configuration
     * @return a writer
     * @throws NullPointerException if the input is null 
     * @throws ProjectConfigException if the project configuration is not valid for writing
     */

    public static <T extends ScoreOutput<?, T>> CommaSeparatedScoreWriter<T> of( final ProjectConfig projectConfig )
            throws ProjectConfigException
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
    public void accept( final MetricOutputMultiMapByTimeAndThreshold<T> output )
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
            if ( !output.values().isEmpty()
                 && output.values()
                          .iterator()
                          .next()
                          .getMetadata()
                          .getMetricID()
                          .isInGroup( MetricOutputGroup.DOUBLE_SCORE ) )
            {
                formatter = ConfigHelper.getDecimalFormatter( destinationConfig );
            }

            // Write per time-window
            try
            {
                CommaSeparatedScoreWriter.writeOneScoreOutputType( destinationConfig, output, formatter );
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
     */

    private static <T extends ScoreOutput<?, T>> void writeOneScoreOutputType( DestinationConfig destinationConfig,
                                                                               MetricOutputMultiMapByTimeAndThreshold<T> output,
                                                                               Format formatter )
            throws IOException
    {

        // Loop across scores
        for ( Map.Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<T>> m : output.entrySet() )
        {
            
            // As many outputs as secondary thresholds if secondary thresholds are defined
            // and the output type is OutputTypeSelection.THRESHOLD_LEAD.
            List<MetricOutputMapByTimeAndThreshold<T>> allOutputs = new ArrayList<>();
            if ( destinationConfig.getOutputType() == OutputTypeSelection.THRESHOLD_LEAD
                 && !m.getValue().setOfThresholdTwo().isEmpty() )
            {
                // Slice by threshold two
                m.getValue().setOfThresholdTwo().forEach( next -> allOutputs.add( m.getValue().filterByThresholdTwo( next ) ) );
            }
            // One output only
            else
            {
                allOutputs.add( m.getValue() );
            }

            // Process each output
            for ( MetricOutputMapByTimeAndThreshold<T> nextOutput : allOutputs )
            {
                StringJoiner headerRow = new StringJoiner( "," );
                headerRow.merge( HEADER_DEFAULT );
                List<RowCompareByLeft> rows =
                        CommaSeparatedScoreWriter.getRowsForOneScore( m.getKey().getKey(),
                                                                      nextOutput,
                                                                      headerRow,
                                                                      formatter );

                // Add the header row
                rows.add( RowCompareByLeft.of( HEADER_INDEX, headerRow ) );

                // Write the output
                String append = null;
                
                // Secondary threshold? If yes, only, one as this was sliced above
                if ( !nextOutput.setOfThresholdTwo().isEmpty() )
                {
                    append = nextOutput.setOfThresholdTwo().iterator().next().toStringSafe();
                }
                MetricOutputMetadata meta = m.getValue().getMetadata();
                Path outputPath = ConfigHelper.getOutputPathToWrite( destinationConfig, meta, append );

                CommaSeparatedScoreWriter.writeTabularOutputToFile( rows, outputPath );
            }
        }
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

    private static <T extends ScoreOutput<?, T>> List<RowCompareByLeft>
            getRowsForOneScore( MetricConstants scoreName,
                                MetricOutputMapByTimeAndThreshold<T> output,
                                StringJoiner headerRow,
                                Format formatter )
    {
        // Slice score by components
        Map<MetricConstants, MetricOutputMapByTimeAndThreshold<T>> helper =
                DefaultDataFactory.getInstance()
                                  .getSlicer()
                                  .filterByMetricComponent( output );

        String outerName = scoreName.toString();
        List<RowCompareByLeft> returnMe = new ArrayList<>();
        // Loop across components
        for ( Entry<MetricConstants, MetricOutputMapByTimeAndThreshold<T>> e : helper.entrySet() )
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

    private static <T extends ScoreOutput<?, T>> void addRowsForOneScoreComponent( String name,
                                                                                   MetricOutputMapByTimeAndThreshold<T> component,
                                                                                   StringJoiner headerRow,
                                                                                   List<RowCompareByLeft> rows,
                                                                                   Format formatter )
    {
        // Loop across the thresholds
        for ( OneOrTwoThresholds t : component.setOfThresholdKey() )
        {
            String column = name + HEADER_DELIMITER + t;
            headerRow.add( column );
            // Loop across time windows
            for ( TimeWindow timeWindow : component.setOfTimeWindowKey() )
            {
                Pair<TimeWindow, OneOrTwoThresholds> key = Pair.of( timeWindow, t );
                if ( component.containsKey( key ) )
                {
                    CommaSeparatedWriter.addRowToInput( rows,
                                                        timeWindow,
                                                        Arrays.asList( component.get( key ).getData() ),
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
     * Hidden constructor.
     * 
     * @param projectConfig the project configuration
     * @throws ProjectConfigException if the project configuration is not valid for writing 
     */

    private CommaSeparatedScoreWriter( ProjectConfig projectConfig ) throws ProjectConfigException
    {
        super( projectConfig );
    }

}
