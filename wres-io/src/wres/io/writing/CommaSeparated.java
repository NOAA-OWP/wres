package wres.io.writing;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.Format;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;

import wres.config.ProjectConfigException;
import wres.config.generated.DestinationConfig;
import wres.config.generated.Feature;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.OutputTypeSelection;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.Threshold;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.BoxPlotOutput;
import wres.datamodel.outputs.MapKey;
import wres.datamodel.outputs.MetricOutputAccessException;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMultiMapByTimeAndThreshold;
import wres.datamodel.outputs.MultiVectorOutput;
import wres.datamodel.outputs.PairedOutput;
import wres.datamodel.outputs.ScoreOutput;
import wres.io.config.ConfigHelper;

/**
 * Helps write Comma Separated files.
 */
public class CommaSeparated
{
    private CommaSeparated()
    {
        // Prevent construction.
    }

    private static final String HEADER_DELIMITER = " ";

    /**
     * Earliest possible time window to index the header.
     */
    
    private static final TimeWindow HEADER_INDEX = TimeWindow.of( Instant.MIN,
                                                                  Instant.MIN,
                                                                  ReferenceTime.VALID_TIME,
                                                                  Duration.ofSeconds( Long.MIN_VALUE ) );
    
    /**
     * Default information for the header.
     */

    private static final StringJoiner HEADER_DEFAULT = new StringJoiner( "," ).add( "EARLIEST" + HEADER_DELIMITER + "TIME" )
                                                                          .add( "LATEST" + HEADER_DELIMITER + "TIME" )
                                                                          .add( "EARLIEST" + HEADER_DELIMITER
                                                                                + "LEAD"
                                                                                + HEADER_DELIMITER
                                                                                + "HOUR" )
                                                                          .add( "LATEST" + HEADER_DELIMITER
                                                                                + "LEAD"
                                                                                + HEADER_DELIMITER
                                                                                + "HOUR" );
    
    /**
     * Default exception message when a destination cannot be established.
     */

    private static final String OUTPUT_CLAUSE_BOILERPLATE = "Please include valid numeric output clause(s) in"
                                                            + " the project configuration. Example: <destination>"
                                                            + "<path>c:/Users/myname/wres_output/</path>"
                                                            + "</destination>";

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
        validateProjectForWriting( projectConfig );

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
        validateProjectForWriting( projectConfig );

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
        validateProjectForWriting( projectConfig );

        // Write output
        List<DestinationConfig> numericalDestinations = ConfigHelper.getNumericalDestinations( projectConfig );
        for ( DestinationConfig d : numericalDestinations )
        {
            writeOneBoxPlotOutputType( d, output, ConfigHelper.getDecimalFormatter( d ) );
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
            StringJoiner headerRow = new  StringJoiner( "," );
            headerRow.merge( HEADER_DEFAULT );
            List<RowCompareByLeft> rows =
                    getRowsForOneScore( m.getKey().getKey(), m.getValue(), headerRow, formatter );
            // Add the header row
            rows.add( RowCompareByLeft.of( HEADER_INDEX, headerRow ) );
            // Write the output
            MetricOutputMetadata meta = m.getValue().getMetadata();
            List<String> nameList = Arrays.asList( meta.getIdentifier().getGeospatialID(),
                                                   meta.getMetricID().name(),
                                                   meta.getIdentifier().getVariableID() );
            writeTabularOutputToFile( destinationConfig, rows, nameList );
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
            headerRow.merge( HEADER_DEFAULT );
            List<RowCompareByLeft> rows =
                    getRowsForOnePairedOutput( m.getKey().getKey(), m.getValue(), headerRow, formatter );
            // Add the header row
            rows.add( RowCompareByLeft.of( HEADER_INDEX, headerRow ) );
            // Write the output
            MetricOutputMetadata meta = m.getValue().getMetadata();
            List<String> nameList = Arrays.asList( meta.getIdentifier().getGeospatialID(),
                                                   meta.getMetricID().name(),
                                                   meta.getIdentifier().getVariableID() );
            writeTabularOutputToFile( destinationConfig, rows, nameList );
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
        // Obtain the plot type configuration
        OutputTypeSelection diagramType = ConfigHelper.getOutputTypeSelection( destinationConfig );

        // Obtain any override configuration
        Map<MetricConfigName, MetricConfig> override = new EnumMap<>( MetricConfigName.class );
        if( Objects.nonNull( projectConfig.getMetrics() ) )
        {
            projectConfig.getMetrics().getMetric().forEach( config -> override.put( config.getName(), config ) );
        }
        // If all valid metrics are required, check for override configuration
        if ( override.containsKey( MetricConfigName.ALL_VALID )
             && Objects.nonNull( override.get( MetricConfigName.ALL_VALID ).getOutputType() ) )
        {
            diagramType = override.get( MetricConfigName.ALL_VALID ).getOutputType();
        }

        // Loop across diagrams
        for ( Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<MultiVectorOutput>> m : output.entrySet() )
        {
            OutputTypeSelection useType = diagramType;
            
            // Is an override present? Find the first MetricConfigName.name() that matches MetricConstants.name()
            Optional<MetricConfigName> match =
                    override.keySet()
                            .stream()
                            .filter( configName -> configName.name().equals( m.getKey().getKey().name() ) )
                            .findFirst();
            
            // Set the type to an override type
            if ( match.isPresent() && Objects.nonNull( override.get( match.get() ).getOutputType() ) )
            {
                useType = override.get( match.get() ).getOutputType();
            }

            StringJoiner headerRow = new StringJoiner( "," );
            headerRow.merge( HEADER_DEFAULT );
            // Default, per time-window
            if ( Objects.isNull( useType ) || useType == OutputTypeSelection.DEFAULT
                 || useType == OutputTypeSelection.LEAD_THRESHOLD )
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
            List<RowCompareByLeft> rows = getRowsForOneDiagram( next, formatter );
            // Add the header row
            rows.add( RowCompareByLeft.of( HEADER_INDEX, getDiagramHeader( next, headerRow ) ) );
            // Write the output
            List<String> nameList = Arrays.asList( meta.getIdentifier().getGeospatialID(),
                                                   meta.getMetricID().name(),
                                                   meta.getIdentifier().getVariableID(),
                                                   Long.toString( timeWindow.getLatestLeadTimeInHours() ) );
            writeTabularOutputToFile( destinationConfig, rows, nameList );
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
            List<RowCompareByLeft> rows = getRowsForOneDiagram( next, formatter );
            // Add the header row
            rows.add( RowCompareByLeft.of( HEADER_INDEX, getDiagramHeader( next, headerRow ) ) );
            // Write the output
            List<String> nameList = Arrays.asList( meta.getIdentifier().getGeospatialID(),
                                                   meta.getMetricID().name(),
                                                   meta.getIdentifier().getVariableID(),
                                                   threshold.toStringSafe() );
            writeTabularOutputToFile( destinationConfig, rows, nameList );
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
            StringJoiner headerRow = new StringJoiner( "," );
            headerRow.merge( HEADER_DEFAULT );

            // Write the output
            writeOneBoxPlotOutputTypePerTimeWindow( destinationConfig, m.getValue(), headerRow, formatter );
        }
    }
    
    /**
     * Writes one box plot for all thresholds at each time window in the input.
     * 
     * @param destinationConfig the destination configuration    
     * @param output the box plot output
     * @param headerRow the header row
     * @param formatter optional formatter, can be null
     * @throws IOException if the output cannot be written
     */

    private static void writeOneBoxPlotOutputTypePerTimeWindow( DestinationConfig destinationConfig,
                                                                MetricOutputMapByTimeAndThreshold<BoxPlotOutput> output,
                                                                StringJoiner headerRow,
                                                                Format formatter )
            throws IOException
    {
        // Loop across time windows
        for ( TimeWindow timeWindow : output.setOfTimeWindowKey() )
        {
            MetricOutputMetadata meta = output.getMetadata();
            MetricOutputMapByTimeAndThreshold<BoxPlotOutput> next = output.filterByTime( timeWindow );
            List<RowCompareByLeft> rows =
                    getRowsForOneBoxPlotOutput( next, headerRow, formatter );
            // Add the header row
            rows.add( RowCompareByLeft.of( HEADER_INDEX, headerRow ) );
            // Write the output
            List<String> nameList = Arrays.asList( meta.getIdentifier().getGeospatialID(),
                                                   meta.getMetricID().name(),
                                                   meta.getIdentifier().getVariableID(),
                                                   Long.toString( timeWindow.getLatestLeadTimeInHours() ) );
            writeTabularOutputToFile( destinationConfig, rows, nameList );
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
            if ( helper.size() > 1 || ! helper.containsKey( MetricConstants.MAIN ) )
            {
                name = name + HEADER_DELIMITER + e.getKey().toString();
            }
            addRowsForOneScoreComponent( name, e.getValue(), headerRow, returnMe, formatter );
        }
        return returnMe;
    }   
    
    /**
     * Returns the results for one paired output.
     *
     * @param <S> the left side of the paired output type
     * @param <T> the right side if the paired output type
     * @param metricName the score name
     * @param output the paired output
     * @param headerRow the header row
     * @param formatter optional formatter, can be null
     * @return the rows to write
     */

    private static <S, T> List<RowCompareByLeft>
            getRowsForOnePairedOutput( MetricConstants metricName,
                                MetricOutputMapByTimeAndThreshold<PairedOutput<S,T>> output,
                                StringJoiner headerRow,
                                Format formatter )
    {
        String outerName = metricName.toString();
        List<RowCompareByLeft> returnMe = new ArrayList<>();

        // Add the rows
        // Loop across the thresholds
        for ( Threshold t : output.setOfThresholdKey() )
        {
            // Append to header
            headerRow.add( HEADER_DELIMITER + outerName + HEADER_DELIMITER + "BASIS TIME" + HEADER_DELIMITER + t );
            headerRow.add( HEADER_DELIMITER + outerName + HEADER_DELIMITER + "DURATION" + HEADER_DELIMITER + t );
            // Loop across time windows
            for ( TimeWindow timeWindow : output.setOfTimeWindowKey() )
            {
                Pair<TimeWindow, Threshold> key = Pair.of( timeWindow, t );
                List<Pair<S, T>> nextValues = output.get( key ).getData();
                for ( Pair<S, T> nextPair : nextValues )
                {
                    addRowToInput( returnMe,
                                   timeWindow,
                                   Arrays.asList( nextPair.getLeft(), nextPair.getRight() ),
                                   formatter,
                                   false );
                }
            }
        }
        
        return returnMe;
    }     
    
    /**
     * Returns the results for one box plot output.
     *
     * @param metricName the score name
     * @param output the box plot output
     * @param headerRow the header row
     * @param formatter optional formatter, can be null
     * @return the rows to write
     */

    private static List<RowCompareByLeft>
            getRowsForOneBoxPlotOutput( MetricOutputMapByTimeAndThreshold<BoxPlotOutput> output,
                                        StringJoiner headerRow,
                                        Format formatter )
    {
        List<RowCompareByLeft> returnMe = new ArrayList<>();

        // Add the rows
        // Loop across the thresholds
        boolean addHeaderProbabilities = true;
        for ( Threshold t : output.setOfThresholdKey() )
        {
            // Loop across time windows
            for ( TimeWindow timeWindow : output.setOfTimeWindowKey() )
            {
                BoxPlotOutput nextValues = output.get( Pair.of( timeWindow, t ) );
                // Add the probabilities to the header. These are constant across time windows
                if ( addHeaderProbabilities )
                {
                    headerRow.add( HEADER_DELIMITER + nextValues.getDomainAxisDimension() + HEADER_DELIMITER + t );
                    VectorOfDoubles headerProbabilities = nextValues.getProbabilities();
                    for ( double nextProb : headerProbabilities.getDoubles() )
                    {
                        headerRow.add( HEADER_DELIMITER + nextValues.getRangeAxisDimension()
                                       + HEADER_DELIMITER
                                       + t
                                       + HEADER_DELIMITER
                                       + "QUANTILE Pr="
                                       + nextProb );
                    }
                }
                // Add each box
                for ( PairOfDoubleAndVectorOfDoubles nextBox : nextValues )
                {
                    List<Double> data = new ArrayList<>();
                    data.add( nextBox.getItemOne() );
                    data.addAll( Arrays.stream( nextBox.getItemTwo() ).boxed().collect( Collectors.toList() ) );
                    addRowToInput( returnMe,
                                   timeWindow,
                                   data,
                                   formatter,
                                   false );
                }
                addHeaderProbabilities = false;
            }
        }

        return returnMe;
    }  
    
    /**
     * Returns the results for one diagram output.
     *
     * @param output the diagram output
     * @param formatter optional formatter, can be null
     * @return the rows to write
     */

    private static List<RowCompareByLeft> getRowsForOneDiagram( MetricOutputMapByTimeAndThreshold<MultiVectorOutput> output,
                                                                Format formatter )
    {
        List<RowCompareByLeft> returnMe = new ArrayList<>();

        // Add the rows
        // Loop across time windows
        for ( TimeWindow timeWindow : output.setOfTimeWindowKey() )
        {
            // Loop across the thresholds, merging results when multiple thresholds occur
            Map<Integer, List<Double>> merge = new TreeMap<>();
            for ( Threshold threshold : output.setOfThresholdKey() )
            {
                MultiVectorOutput next = output.get( Pair.of( timeWindow, threshold ) );
                // For safety, find the largest vector and use Double.NaN in place for vectors of differing size
                // In practice, all should be the same length
                int maxRows = next.getData()
                                  .values()
                                  .stream()
                                  .max( Comparator.comparingInt( VectorOfDoubles::size ) )
                                  .get()
                                  .size();
                // Loop until the maximum row 
                for ( int rowIndex = 0; rowIndex < maxRows; rowIndex++ )
                {
                    // Add the row
                    List<Double> row = getOneRowForOneDiagram( next, rowIndex );
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
            // Add the merged rows
            for ( List<Double> next : merge.values() )
            {
                addRowToInput( returnMe,
                               timeWindow,
                               next,
                               formatter,
                               false );
            }
        }

        return returnMe;
    }

    /**
     * Returns the row from the diagram from the input data at a specified threshold and row index.
     * 
     * @param next the data
     * @param row the row index to generate
     * @return the row data
     */

    private static List<Double> getOneRowForOneDiagram( MultiVectorOutput next,
                                                        int row )
    {
        List<Double> valuesToAdd = new ArrayList<>();
        for ( Entry<MetricDimension, VectorOfDoubles> nextColumn : next.getData().entrySet() )
        {
            // Populate the values
            Double addMe = Double.NaN;
            if ( row < nextColumn.getValue().size() )
            {
                addMe = nextColumn.getValue().getDoubles()[row];
            }
            valuesToAdd.add( addMe );
        }
        return valuesToAdd;
    }
    
    /**
     * Helper that mutates the header for diagrams based on the input.
     * 
     * @param diagramOutput the diagram output
     * @param headerRow the header row
     * @return the mutated header
     */

    private static StringJoiner getDiagramHeader( MetricOutputMapByTimeAndThreshold<MultiVectorOutput> diagramOutput,
                                                  StringJoiner headerRow )
    {
        // Append to header
        StringJoiner returnMe = new StringJoiner( "," );
        returnMe.merge( headerRow );
        String metricName = diagramOutput.getMetadata().getMetricID().toString();
        Entry<Pair<TimeWindow, Threshold>, MultiVectorOutput> data = diagramOutput.entrySet().iterator().next();
        Set<MetricDimension> dimensions = data.getValue().getData().keySet();
        //Add the metric name, dimension, and threshold for each column-vector
        for ( Threshold nextThreshold : diagramOutput.setOfThresholdKey() )
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
     * Mutates the input header and rows, adding results for one score component.
     *
     * @param <T> the score component type
     * @param name the column name
     * @param component the score component results
     * @param headerRow the header row
     * @param rows the data rows
     * @param formatter optional formatter, can be null
     */

    private static <T extends ScoreOutput<?,T>> void addRowsForOneScoreComponent( String name,
                                                     MetricOutputMapByTimeAndThreshold<T> component,
                                                     StringJoiner headerRow,
                                                     List<RowCompareByLeft> rows,
                                                     Format formatter )
    {
        // Loop across the thresholds
        for ( Threshold t : component.setOfThresholdKey() )
        {
            String column = name + HEADER_DELIMITER + t;
            headerRow.add( column );
            // Loop across time windows
            for ( TimeWindow timeWindow : component.setOfTimeWindowKey() )
            {
                Pair<TimeWindow, Threshold> key = Pair.of( timeWindow, t );
                addRowToInput( rows, timeWindow, Arrays.asList( component.get( key ).getData() ), formatter, true );
            }
        }
    }       
    
    /**
     * Writes the raw tabular output to file. Uses the supplied metadata for file naming.
     * 
     * @param destinationConfig the destination configuration
     * @param rows the tabular data to write
     * @param nameElements the elements to use when naming the file
     * @throws IOException if the output cannot be written
     */

    private static void writeTabularOutputToFile( DestinationConfig destinationConfig,
                                                  List<RowCompareByLeft> rows,
                                                  List<String> nameElements )
            throws IOException
    {              
        File outputDirectory = null;
        try 
        {
            outputDirectory = ConfigHelper.getDirectoryFromDestinationConfig( destinationConfig );
        }
        catch ( final ProjectConfigException pce )
        {
            throw new IOException( OUTPUT_CLAUSE_BOILERPLATE, pce );
        }
        StringJoiner joinElements = new StringJoiner("_");
        nameElements.forEach( joinElements::add );
        Path outputPath = Paths.get( outputDirectory.toString(),joinElements.toString()+".csv" );
        // Sort the rows before writing them
        Collections.sort( rows );
        
        try ( BufferedWriter w = Files.newBufferedWriter( outputPath,
                                                          StandardCharsets.UTF_8,
                                                          StandardOpenOption.CREATE,
                                                          StandardOpenOption.TRUNCATE_EXISTING ) )
        {
            for ( RowCompareByLeft row : rows )
            {
                w.write( row.getRight().toString() );
                w.write( System.lineSeparator() );
            }
        }
    }

    /**
     * Mutates the input, adding a new row.
     * 
     * @param <T> the type of values to add
     * @param rows the map of rows to mutate
     * @param timeWindow the time window
     * @param values the values to add, one for each column
     * @param formatter an optional formatter
     * @param append is true to add the values to an existing row with the same time window, false otherwise
     */

    private static <T> void addRowToInput( List<RowCompareByLeft> rows,
                                           TimeWindow timeWindow,                                           
                                           List<T> values,
                                           Format formatter,
                                           boolean append )
    {
        StringJoiner row = null;
        int rowIndex = rows.indexOf( RowCompareByLeft.of( timeWindow, null) );
        // Set the row to append if it exists and appending is required
        if( rowIndex > -1 && append )
        {
            row = rows.get( rowIndex ).getRight();
        }
        // Otherwise, start a new row 
        else 
        {
            row = new StringJoiner( "," );
            row.add( timeWindow.getEarliestTime().toString() );
            row.add( timeWindow.getLatestTime().toString() );
            row.add( Long.toString( timeWindow.getEarliestLeadTimeInHours() ) );
            row.add( Long.toString( timeWindow.getLatestLeadTimeInHours() ) );
            rows.add( RowCompareByLeft.of( timeWindow, row ) );
        }

        for ( T nextColumn : values )
        {

            String toWrite = "NA";

            // Write the current score component at the current window and threshold
            if ( nextColumn != null && !Double.valueOf( Double.NaN ).equals( nextColumn ) )
            {
                if ( formatter != null )
                {
                    toWrite = formatter.format( nextColumn );
                }
                else
                {
                    toWrite = nextColumn.toString();
                }
            }
            row.add( toWrite );
        }
    }
    
    /**
     * Validates the input configuration for writing. Throws an exception if the configuration is invalid.
     * 
     * @param projectConfig the project configuration
     * @throws NullPointerException if the input is null
     * @throws IOException if the project is not correctly configured for writing numerical output
     */

    private static void validateProjectForWriting( ProjectConfig projectConfig ) throws IOException
    {
        Objects.requireNonNull( projectConfig, "Specify non-null project configuration when writing diagram outputs." );

        try
        {
            if ( projectConfig.getOutputs() == null
                 || projectConfig.getOutputs().getDestination() == null
                 || projectConfig.getOutputs().getDestination().isEmpty() )
            {
                String message = "No numeric output files specified for project.";
                throw new ProjectConfigException( projectConfig.getOutputs(),
                                                  message );
            }
        }
        catch ( final ProjectConfigException pce )
        {
            throw new IOException( OUTPUT_CLAUSE_BOILERPLATE, pce );
        }
    }
    
    /**
     * A helper class that contains a single row whose natural order is based on the {@link TimeWindow} of the row
     * and not the contents. All comparisons are based on the left value only.
     * 
     * @author james.brown@hydrosolved.com
     * @version 0.1
     * @since version 0.4
     */
    
    private static class RowCompareByLeft implements Comparable<RowCompareByLeft>
    {        
        /**
         * The row time window.
         */
        private final TimeWindow left;
        
        /**
         * The row value.
         */
        
        private final StringJoiner right;
     
        /**
         * Returns an instance for the given input.
         * 
         * @param timeWindow the time window
         * @param value the row value
         * @return an instance 
         */
        
        private static RowCompareByLeft of( TimeWindow timeWindow, StringJoiner value ) 
        {
            return new RowCompareByLeft( timeWindow, value );
        }      

        /**
         * Returns the left value.
         * 
         * @return the left value
         */

        private TimeWindow getLeft()
        {
            return left;
        }

        /**
         * Returns the right value
         * 
         * @return the right value
         */
        
        private StringJoiner getRight()
        {
            return right;
        }
        
        @Override 
        public int compareTo( RowCompareByLeft compareTo) 
        {
            Objects.requireNonNull( compareTo, "Specify a non-null input row for comparison." );
            return getLeft().compareTo( compareTo.getLeft() );          
        }

        @Override
        public boolean equals( Object o )
        {
            if ( ! ( o instanceof RowCompareByLeft ) )
            {
                return false;
            }
            RowCompareByLeft in = (RowCompareByLeft) o;
            return Objects.equals( in.getLeft(), getLeft() );
        }
        
        @Override 
        public int hashCode()
        {
            return Objects.hashCode( left );
        }
        
        /**
         * Constructor.
         * 
         * @param timeWindow the time window
         * @param value the row value
         */
        
        private RowCompareByLeft( TimeWindow timeWindow, StringJoiner value )
        {           
            Objects.requireNonNull( timeWindow , "Specify a non-null time window for the row." );
            left = timeWindow;
            right = value;
        }       
    }
    
}
