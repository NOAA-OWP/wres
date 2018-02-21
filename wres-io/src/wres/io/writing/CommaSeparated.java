package wres.io.writing;

import java.io.IOException;
import java.text.Format;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.StringJoiner;

import wres.config.generated.DestinationConfig;
import wres.config.generated.Feature;
import wres.config.generated.OutputTypeSelection;
import wres.config.generated.ProjectConfig;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.Threshold;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.BoxPlotOutput;
import wres.datamodel.outputs.MapKey;
import wres.datamodel.outputs.MatrixOutput;
import wres.datamodel.outputs.MetricOutputAccessException;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMultiMapByTimeAndThreshold;
import wres.datamodel.outputs.MultiVectorOutput;
import wres.datamodel.outputs.PairedOutput;
import wres.datamodel.outputs.ScoreOutput;
import wres.io.config.ConfigHelper;
import wres.io.writing.CommaSeparatedHelper.RowCompareByLeft;

/**
 * Helps write files of Comma Separated Values (CSV).
 * 
 * @author jesse
 * @author james.brown@hydrosolved.com
 * @version 0.2
 * @since 0.1
 */
public class CommaSeparated
{

    /**
     * Write numerical outputs to CSV files.
     *
     * @param projectConfig the project configuration
     * @param feature the feature
     * @param storedMetricOutput the stored output
     * @throws IOException when the writing fails
     * @throws NullPointerException when any of the arguments are null
     * @throws IllegalArgumentException when destination has bad decimalFormat
     */

    public static void writeOutputFiles( ProjectConfig projectConfig,
                                         Feature feature,
                                         MetricOutputForProjectByTimeAndThreshold storedMetricOutput )
            throws IOException
    {
        Objects.requireNonNull( storedMetricOutput,
                                "Metric outputs must not be null." );
        Objects.requireNonNull( feature,
                                "The feature must not be null." );

        // Validate project for writing
        CommaSeparatedHelper.validateProjectForWriting( projectConfig );

        // Write output
        // In principle, each destination could have a different formatter, so 
        // the output must be generated separately for each destination
        List<DestinationConfig> numericalDestinations = ConfigHelper.getNumericalDestinations( projectConfig );
        for ( DestinationConfig d : numericalDestinations )
        {
            writeAllOutputsForOneDestination( projectConfig, d, storedMetricOutput );
        }
    }

    /**
     * Writes all diagram outputs to file. This is part of the public API because diagrams can be written to file before
     * a processing pipeline has been completed (which is not true for all output types).
     *
     * @param projectConfig the project configuration    
     * @param output the diagram output
     * @throws IOException if the output cannot be written
     * @throws NullPointerException when any of the arguments are null
     */

    public static void writeDiagramFiles( ProjectConfig projectConfig,
                                          MetricOutputMultiMapByTimeAndThreshold<MultiVectorOutput> output )
            throws IOException
    {
        Objects.requireNonNull( output, "Specify non-null input data when writing diagram outputs." );

        // Validate project for writing
        CommaSeparatedHelper.validateProjectForWriting( projectConfig );

        // Write output
        List<DestinationConfig> numericalDestinations = ConfigHelper.getNumericalDestinations( projectConfig );
        for ( DestinationConfig d : numericalDestinations )
        {
            writeOneDiagramOutputType( projectConfig, d, output, ConfigHelper.getDecimalFormatter( d ) );
        }
    }

    /**
     * Writes all box plot outputs to file. This is part of the public API because box plots can be written to file 
     * before a processing pipeline has been completed (which is not true for all output types).
     *
     * @param projectConfig the project configuration    
     * @param output the box plot output
     * @throws IOException if the output cannot be written
     * @throws NullPointerException when any of the arguments are null
     */

    public static void writeBoxPlotFiles( ProjectConfig projectConfig,
                                          MetricOutputMultiMapByTimeAndThreshold<BoxPlotOutput> output )
            throws IOException
    {
        Objects.requireNonNull( output, "Specify non-null input data when writing diagram outputs." );

        // Validate project for writing
        CommaSeparatedHelper.validateProjectForWriting( projectConfig );

        // Write output
        List<DestinationConfig> numericalDestinations = ConfigHelper.getNumericalDestinations( projectConfig );
        for ( DestinationConfig d : numericalDestinations )
        {
            writeOneBoxPlotOutputType( d, output, ConfigHelper.getDecimalFormatter( d ) );
        }
    }

    /**
     * Writes all matrix outputs to file. This is part of the public API because matrix outputs can be written to file 
     * before a processing pipeline has been completed (which is not true for all output types).
     *
     * @param projectConfig the project configuration    
     * @param output the matrix output
     * @throws IOException if the output cannot be written
     * @throws NullPointerException when any of the arguments are null
     */

    public static void writeMatrixOutputFiles( ProjectConfig projectConfig,
                                               MetricOutputMultiMapByTimeAndThreshold<MatrixOutput> output )
            throws IOException
    {
        Objects.requireNonNull( output, "Specify non-null input data when writing diagram outputs." );

        // Validate project for writing
        CommaSeparatedHelper.validateProjectForWriting( projectConfig );

        // Write output
        List<DestinationConfig> numericalDestinations = ConfigHelper.getNumericalDestinations( projectConfig );
        for ( DestinationConfig d : numericalDestinations )
        {
            writeOneMatrixOutputType( projectConfig, d, output, ConfigHelper.getDecimalFormatter( d ) );
        }
    }   
    
    /**
     * Writes all outputs for one destination.
     *     
     * @param projectConfig the project configuration    
     * @param destinationConfig the destination configuration    
     * @param storedMetricOutput the output to use to build rows
     * @throws IOException if the output cannot be written
     */

    private static void writeAllOutputsForOneDestination( ProjectConfig projectConfig,
                                                          DestinationConfig destinationConfig,
                                                          MetricOutputForProjectByTimeAndThreshold storedMetricOutput )
            throws IOException
    {
        try
        {
            // Scores with double output
            if ( storedMetricOutput.hasOutput( MetricOutputGroup.DOUBLE_SCORE ) )
            {
                CommaSeparated.writeOneScoreOutputType( destinationConfig,
                                                        storedMetricOutput.getDoubleScoreOutput(),
                                                        ConfigHelper.getDecimalFormatter( destinationConfig ) );
            }

            // Scores with duration output
            if ( storedMetricOutput.hasOutput( MetricOutputGroup.DURATION_SCORE ) )
            {
                CommaSeparated.writeOneScoreOutputType( destinationConfig,
                                                        storedMetricOutput.getDurationScoreOutput(),
                                                        null );
            }

            // Metrics with PairedOutput
            if ( storedMetricOutput.hasOutput( MetricOutputGroup.PAIRED ) )
            {
                CommaSeparated.writeOnePairedOutputType( destinationConfig,
                                                         storedMetricOutput.getPairedOutput(),
                                                         null );
            }

            // Diagrams
            if ( storedMetricOutput.hasOutput( MetricOutputGroup.MULTIVECTOR ) )
            {
                CommaSeparated.writeOneDiagramOutputType( projectConfig,
                                                          destinationConfig,
                                                          storedMetricOutput.getMultiVectorOutput(),
                                                          ConfigHelper.getDecimalFormatter( destinationConfig ) );
            }
            // Box plots
            if ( storedMetricOutput.hasOutput( MetricOutputGroup.BOXPLOT ) )
            {
                CommaSeparated.writeOneBoxPlotOutputType( destinationConfig,
                                                          storedMetricOutput.getBoxPlotOutput(),
                                                          ConfigHelper.getDecimalFormatter( destinationConfig ) );
            }
            // Matrix output
            if ( storedMetricOutput.hasOutput( MetricOutputGroup.MATRIX ) )
            {
                CommaSeparated.writeOneMatrixOutputType( projectConfig,
                                                         destinationConfig,
                                                         storedMetricOutput.getMatrixOutput(),
                                                         ConfigHelper.getDecimalFormatter( destinationConfig ) );
            }
        }
        catch ( MetricOutputAccessException e )
        {
            throw new IOException( "While retrieving metric output:", e );
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
            StringJoiner headerRow = new StringJoiner( "," );
            headerRow.merge( CommaSeparatedHelper.HEADER_DEFAULT );
            List<RowCompareByLeft> rows =
                    CommaSeparatedHelper.getRowsForOneScore( m.getKey().getKey(), m.getValue(), headerRow, formatter );
            
            // Add the header row
            rows.add( RowCompareByLeft.of( CommaSeparatedHelper.HEADER_INDEX, headerRow ) );
            
            // Write the output
            MetricOutputMetadata meta = m.getValue().getMetadata();
            List<String> nameList = Arrays.asList( meta.getIdentifier().getGeospatialID(),
                                                   meta.getMetricID().name(),
                                                   meta.getIdentifier().getVariableID() );
            CommaSeparatedHelper.writeTabularOutputToFile( destinationConfig, rows, nameList );
        }
    }

    /**
     * Writes all output for one paired type.
     *
     * @param <S> the left side of the paired output type
     * @param <T> the right side if the paired output type
     * @param destinationConfig the destination configuration    
     * @param output the score output to iterate through
     * @param formatter optional formatter, can be null
     * @throws IOException if the output cannot be written
     */

    private static <S, T> void writeOnePairedOutputType( DestinationConfig destinationConfig,
                                                         MetricOutputMultiMapByTimeAndThreshold<PairedOutput<S, T>> output,
                                                         Format formatter )
            throws IOException
    {
        // Loop across paired output
        for ( Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<PairedOutput<S, T>>> m : output.entrySet() )
        {
            StringJoiner headerRow = new StringJoiner( "," );
            headerRow.merge( CommaSeparatedHelper.HEADER_DEFAULT );
            List<RowCompareByLeft> rows =
                    CommaSeparatedHelper.getRowsForOnePairedOutput( m.getKey().getKey(),
                                                                    m.getValue(),
                                                                    headerRow,
                                                                    formatter );
            
            // Add the header row
            rows.add( RowCompareByLeft.of( CommaSeparatedHelper.HEADER_INDEX, headerRow ) );
            
            // Write the output
            MetricOutputMetadata meta = m.getValue().getMetadata();
            List<String> nameList = Arrays.asList( meta.getIdentifier().getGeospatialID(),
                                                   meta.getMetricID().name(),
                                                   meta.getIdentifier().getVariableID() );
            CommaSeparatedHelper.writeTabularOutputToFile( destinationConfig, rows, nameList );
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

    private static void writeOneDiagramOutputType( ProjectConfig projectConfig,
                                                   DestinationConfig destinationConfig,
                                                   MetricOutputMultiMapByTimeAndThreshold<MultiVectorOutput> output,
                                                   Format formatter )
            throws IOException
    {
        // Obtain the output type configuration with any override for ALL_VALID metrics
        OutputTypeSelection diagramType = ConfigHelper.getOutputTypeSelection( projectConfig, destinationConfig );

        // Loop across diagrams
        for ( Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<MultiVectorOutput>> m : output.entrySet() )
        {
            // Obtain the output type with any local override for this metric
            OutputTypeSelection useType =
                    ConfigHelper.getOutputTypeSelection( projectConfig, diagramType, m.getKey().getKey() );

            StringJoiner headerRow = new StringJoiner( "," );
            headerRow.merge( CommaSeparatedHelper.HEADER_DEFAULT );
            
            // Default, per time-window
            if ( useType == OutputTypeSelection.DEFAULT || useType == OutputTypeSelection.LEAD_THRESHOLD )
            {
                writeOneDiagramOutputTypePerTimeWindow( destinationConfig, m.getValue(), headerRow, formatter );
            }
            // Per threshold
            else if ( useType == OutputTypeSelection.THRESHOLD_LEAD )
            {
                writeOneDiagramOutputTypePerThreshold( destinationConfig, m.getValue(), headerRow, formatter );
            }
        }
    }

    /**
     * Writes one diagram for all thresholds at each time window in the input.
     * 
     * @param destinationConfig the destination configuration    
     * @param output the diagram output
     * @param headerRow the header row
     * @param formatter optional formatter, can be null
     * @throws IOException if the output cannot be written
     */

    private static void writeOneDiagramOutputTypePerTimeWindow( DestinationConfig destinationConfig,
                                                                MetricOutputMapByTimeAndThreshold<MultiVectorOutput> output,
                                                                StringJoiner headerRow,
                                                                Format formatter )
            throws IOException
    {
        // Loop across time windows
        for ( TimeWindow timeWindow : output.setOfTimeWindowKey() )
        {
            MetricOutputMetadata meta = output.getMetadata();
            MetricOutputMapByTimeAndThreshold<MultiVectorOutput> next = output.filterByTime( timeWindow );
            List<RowCompareByLeft> rows = CommaSeparatedHelper.getRowsForOneDiagram( next, formatter );
            
            // Add the header row
            rows.add( RowCompareByLeft.of( CommaSeparatedHelper.HEADER_INDEX,
                                           CommaSeparatedHelper.getDiagramHeader( next, headerRow ) ) );
            
            // Write the output
            List<String> nameList = Arrays.asList( meta.getIdentifier().getGeospatialID(),
                                                   meta.getMetricID().name(),
                                                   meta.getIdentifier().getVariableID(),
                                                   Long.toString( timeWindow.getLatestLeadTimeInHours() ) );
            CommaSeparatedHelper.writeTabularOutputToFile( destinationConfig, rows, nameList );
        }
    }

    /**
     * Writes one diagram for all time windows at each threshold in the input.
     * 
     * @param destinationConfig the destination configuration    
     * @param output the diagram output
     * @param headerRow the header row
     * @param formatter optional formatter, can be null
     * @throws IOException if the output cannot be written
     */

    private static void writeOneDiagramOutputTypePerThreshold( DestinationConfig destinationConfig,
                                                               MetricOutputMapByTimeAndThreshold<MultiVectorOutput> output,
                                                               StringJoiner headerRow,
                                                               Format formatter )
            throws IOException
    {
        // Loop across thresholds
        for ( Threshold threshold : output.setOfThresholdKey() )
        {
            MetricOutputMetadata meta = output.getMetadata();
            MetricOutputMapByTimeAndThreshold<MultiVectorOutput> next = output.filterByThreshold( threshold );
            List<RowCompareByLeft> rows = CommaSeparatedHelper.getRowsForOneDiagram( next, formatter );
            
            // Add the header row
            rows.add( RowCompareByLeft.of( CommaSeparatedHelper.HEADER_INDEX,
                                           CommaSeparatedHelper.getDiagramHeader( next, headerRow ) ) );
            
            // Write the output
            List<String> nameList = Arrays.asList( meta.getIdentifier().getGeospatialID(),
                                                   meta.getMetricID().name(),
                                                   meta.getIdentifier().getVariableID(),
                                                   threshold.toStringSafe() );
            CommaSeparatedHelper.writeTabularOutputToFile( destinationConfig, rows, nameList );
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

    private static void writeOneMatrixOutputType( ProjectConfig projectConfig,
                                                  DestinationConfig destinationConfig,
                                                  MetricOutputMultiMapByTimeAndThreshold<MatrixOutput> output,
                                                  Format formatter )
            throws IOException
    {
        // Obtain the output type configuration with any override for ALL_VALID metrics
        OutputTypeSelection diagramType = ConfigHelper.getOutputTypeSelection( projectConfig, destinationConfig );

        // Loop across diagrams
        for ( Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<MatrixOutput>> m : output.entrySet() )
        {
            // Obtain the output type with any local override for this metric
            OutputTypeSelection useType =
                    ConfigHelper.getOutputTypeSelection( projectConfig, diagramType, m.getKey().getKey() );

            StringJoiner headerRow = new StringJoiner( "," );
            headerRow.merge( CommaSeparatedHelper.HEADER_DEFAULT );
            
            // Default, per time-window
            if ( useType == OutputTypeSelection.DEFAULT || useType == OutputTypeSelection.LEAD_THRESHOLD )
            {
                writeOneMatrixOutputTypePerTimeWindow( destinationConfig, m.getValue(), headerRow, formatter );
            }
            // Per threshold
            else if ( useType == OutputTypeSelection.THRESHOLD_LEAD )
            {
                writeOneMatrixOutputTypePerThreshold( destinationConfig, m.getValue(), headerRow, formatter );
            }
        }
    }

    /**
     * Writes all output for one box plot type.
     *
     * @param destinationConfig the destination configuration    
     * @param output the box plot output to iterate through
     * @param formatter optional formatter, can be null
     * @throws IOException if the output cannot be written
     */

    private static void writeOneBoxPlotOutputType( DestinationConfig destinationConfig,
                                                   MetricOutputMultiMapByTimeAndThreshold<BoxPlotOutput> output,
                                                   Format formatter )
            throws IOException
    {
        // Loop across the box plot output
        for ( Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<BoxPlotOutput>> m : output.entrySet() )
        {
            // Write the output
            writeOneBoxPlotOutputTypePerTimeWindow( destinationConfig, m.getValue(), formatter );
        }
    }

    /**
     * Writes one box plot for all thresholds at each time window in the input.
     * 
     * @param destinationConfig the destination configuration    
     * @param output the box plot output
     * @param formatter optional formatter, can be null
     * @throws IOException if the output cannot be written
     */

    private static void writeOneBoxPlotOutputTypePerTimeWindow( DestinationConfig destinationConfig,
                                                                MetricOutputMapByTimeAndThreshold<BoxPlotOutput> output,
                                                                Format formatter )
            throws IOException
    {
        // Loop across time windows
        for ( TimeWindow timeWindow : output.setOfTimeWindowKey() )
        {
            MetricOutputMetadata meta = output.getMetadata();
            MetricOutputMapByTimeAndThreshold<BoxPlotOutput> next = output.filterByTime( timeWindow );
            StringJoiner headerRow = new StringJoiner( "," );
            headerRow.merge( CommaSeparatedHelper.HEADER_DEFAULT );
            List<RowCompareByLeft> rows = CommaSeparatedHelper.getRowsForOneBoxPlot( next, formatter );
            
            // Add the header row
            rows.add( RowCompareByLeft.of( CommaSeparatedHelper.HEADER_INDEX,
                                           CommaSeparatedHelper.getBoxPlotHeader( next, headerRow ) ) );
            // Write the output
            List<String> nameList = Arrays.asList( meta.getIdentifier().getGeospatialID(),
                                                   meta.getMetricID().name(),
                                                   meta.getIdentifier().getVariableID(),
                                                   Long.toString( timeWindow.getLatestLeadTimeInHours() ) );
            CommaSeparatedHelper.writeTabularOutputToFile( destinationConfig, rows, nameList );
        }
    }

    /**
     * Writes one matrix output for all thresholds at each time window in the input.
     * 
     * @param destinationConfig the destination configuration    
     * @param output the matrix output
     * @param headerRow the header row
     * @param formatter optional formatter, can be null
     * @throws IOException if the output cannot be written
     */

    private static void writeOneMatrixOutputTypePerTimeWindow( DestinationConfig destinationConfig,
                                                               MetricOutputMapByTimeAndThreshold<MatrixOutput> output,
                                                               StringJoiner headerRow,
                                                               Format formatter )
            throws IOException
    {
        // Loop across time windows
        for ( TimeWindow timeWindow : output.setOfTimeWindowKey() )
        {
            MetricOutputMetadata meta = output.getMetadata();
            MetricOutputMapByTimeAndThreshold<MatrixOutput> next = output.filterByTime( timeWindow );
            List<RowCompareByLeft> rows = CommaSeparatedHelper.getRowsForOneMatrixOutput( next, formatter );
            
            // Add the header row
            rows.add( RowCompareByLeft.of( CommaSeparatedHelper.HEADER_INDEX,
                                           CommaSeparatedHelper.getMatrixOutputHeader( next, headerRow ) ) );
            
            // Write the output
            List<String> nameList = Arrays.asList( meta.getIdentifier().getGeospatialID(),
                                                   meta.getMetricID().name(),
                                                   meta.getIdentifier().getVariableID(),
                                                   Long.toString( timeWindow.getLatestLeadTimeInHours() ) );
            CommaSeparatedHelper.writeTabularOutputToFile( destinationConfig, rows, nameList );
        }
    }

    /**
     * Writes one matrix output for all time windows at each threshold in the input.
     * 
     * @param destinationConfig the destination configuration    
     * @param output the matrix output
     * @param headerRow the header row
     * @param formatter optional formatter, can be null
     * @throws IOException if the output cannot be written
     */

    private static void writeOneMatrixOutputTypePerThreshold( DestinationConfig destinationConfig,
                                                              MetricOutputMapByTimeAndThreshold<MatrixOutput> output,
                                                              StringJoiner headerRow,
                                                              Format formatter )
            throws IOException
    {
        // Loop across thresholds
        for ( Threshold threshold : output.setOfThresholdKey() )
        {
            MetricOutputMetadata meta = output.getMetadata();
            MetricOutputMapByTimeAndThreshold<MatrixOutput> next = output.filterByThreshold( threshold );
            List<RowCompareByLeft> rows = CommaSeparatedHelper.getRowsForOneMatrixOutput( next, formatter );
            
            // Add the header row
            rows.add( RowCompareByLeft.of( CommaSeparatedHelper.HEADER_INDEX,
                                           CommaSeparatedHelper.getMatrixOutputHeader( next, headerRow ) ) );
            
            // Write the output
            List<String> nameList = Arrays.asList( meta.getIdentifier().getGeospatialID(),
                                                   meta.getMetricID().name(),
                                                   meta.getIdentifier().getVariableID(),
                                                   threshold.toStringSafe() );
            CommaSeparatedHelper.writeTabularOutputToFile( destinationConfig, rows, nameList );
        }
    }
    
    /**
     * Prevent construction.
     */

    private CommaSeparated()
    {        
    }
    
}
