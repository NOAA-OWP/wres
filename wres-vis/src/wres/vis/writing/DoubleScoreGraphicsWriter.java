package wres.vis.writing;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.function.Function;

import org.jfree.chart.JFreeChart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.DatasetOrientation;
import wres.datamodel.DataUtilities;
import wres.datamodel.Slicer;
import wres.config.MetricConstants;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Pool.EnsembleAverageType;
import wres.vis.charts.ChartBuildingException;
import wres.vis.charts.ChartFactory;

/**
 * Helps write charts comprising {@link DoubleScoreStatisticOuter} to graphics formats.
 * 
 * @author James Brown
 */

public class DoubleScoreGraphicsWriter extends GraphicsWriter
        implements Function<List<DoubleScoreStatisticOuter>, Set<Path>>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( DoubleScoreGraphicsWriter.class );

    /**
     * Returns an instance of a writer.
     *
     * @param outputsDescription a description of the required outputs
     * @param outputDirectory the directory into which to write
     * @return a writer
     * @throws NullPointerException if either input is null
     */

    public static DoubleScoreGraphicsWriter of( Outputs outputsDescription,
                                                Path outputDirectory )
    {
        return new DoubleScoreGraphicsWriter( outputsDescription, outputDirectory );
    }

    /**
     * Writes all output for one score type.
     *
     * @param output the score output
     * @return the paths written
     * @throws NullPointerException if the input is null
     * @throws GraphicsWriteException if the output cannot be written
     */

    @Override
    public Set<Path> apply( List<DoubleScoreStatisticOuter> output )
    {
        Objects.requireNonNull( output, "Specify non-null input data when writing duration score outputs." );

        Set<Path> paths = new HashSet<>();

        // Iterate through each metric 
        SortedSet<MetricConstants> metrics = Slicer.discover( output, DoubleScoreStatisticOuter::getMetricName );
        for ( MetricConstants next : metrics )
        {
            if ( next == MetricConstants.CONTINGENCY_TABLE )
            {
                LOGGER.debug( "Discovered contingency table output while writing PNGs: ignoring these outputs." );
            }
            else
            {
                List<DoubleScoreStatisticOuter> filtered = Slicer.filter( output, next );

                // Group the statistics by the LRB context in which they appear. There will be one path written
                // for each group (e.g., one path for each window with LeftOrRightOrBaseline.RIGHT data and one for 
                // each window with LeftOrRightOrBaseline.BASELINE data): #48287
                Map<DatasetOrientation, List<DoubleScoreStatisticOuter>> groups =
                        Slicer.getStatisticsGroupedByContext( filtered );

                for ( List<DoubleScoreStatisticOuter> nextGroup : groups.values() )
                {
                    Set<Path> innerPathsWrittenTo =
                            DoubleScoreGraphicsWriter.writeScoreCharts( super.getOutputDirectory(),
                                                                        super.getOutputsDescription(),
                                                                        nextGroup );
                    paths.addAll( innerPathsWrittenTo );
                }
            }
        }

        return Collections.unmodifiableSet( paths );
    }

    /**
     * Writes a set of charts associated with {@link DoubleScoreStatisticOuter} for a single metric and time window,
     * stored in a {@link List}.
     *
     * @param outputDirectory the directory into which to write
     * @param outputsDescription a description of the outputs required
     * @param statistics the metric output
     * @return the paths written
     * @throws GraphicsWriteException when an error occurs during writing
     */

    private static Set<Path> writeScoreCharts( Path outputDirectory,
                                               Outputs outputsDescription,
                                               List<DoubleScoreStatisticOuter> statistics )
    {
        Set<Path> pathsWrittenTo = new HashSet<>();

        ChartFactory chartFactory = GraphicsWriter.getChartFactory();

        // Build charts
        try
        {
            MetricConstants metricName = statistics.get( 0 ).getMetricName();
            PoolMetadata metadata = statistics.get( 0 ).getMetadata();

            // Collection of graphics parameters, one for each set of charts to write across N formats.
            Collection<Outputs> outputsMap =
                    GraphicsWriter.getOutputsGroupedByGraphicsParameters( outputsDescription );

            for ( Outputs nextOutputs : outputsMap )
            {
                // One helper per set of graphics parameters.
                GraphicsHelper helper = GraphicsHelper.of( nextOutputs );

                // Slice the statistics
                List<List<DoubleScoreStatisticOuter>> allOutputs =
                        DoubleScoreGraphicsWriter.getSlicedStatistics( statistics );

                for ( List<DoubleScoreStatisticOuter> nextOutput : allOutputs )
                {
                    Map<MetricConstants, JFreeChart> engines = chartFactory.getScoreCharts( nextOutput,
                                                                                            helper.getGraphicShape(),
                                                                                            helper.getDurationUnits() );

                    String append = DoubleScoreGraphicsWriter.getPathQualifier( nextOutput );

                    Set<Path> paths = DoubleScoreGraphicsWriter.writeNextGroupOfDestinations( outputDirectory,
                                                                                              metadata,
                                                                                              engines,
                                                                                              metricName,
                                                                                              append,
                                                                                              nextOutputs );
                    pathsWrittenTo.addAll( paths );
                }
            }
        }
        catch ( ChartBuildingException e )
        {
            throw new GraphicsWriteException( "Error while generating double score charts: ", e );
        }

        return Collections.unmodifiableSet( pathsWrittenTo );
    }

    /**
     * Slices the statistics for individual graphics. Returns as many sliced lists of statistics as graphics to create.
     * 
     * @param statistics the statistics to slice
     * @return the sliced statistics to write
     */

    private static List<List<DoubleScoreStatisticOuter>>
            getSlicedStatistics( List<DoubleScoreStatisticOuter> statistics )
    {
        List<List<DoubleScoreStatisticOuter>> sliced = new ArrayList<>();

        SortedSet<ThresholdOuter> secondThreshold =
                Slicer.discover( statistics,
                                 next -> next.getMetadata()
                                             .getThresholds()
                                             .second() );

        // Slice by ensemble averaging function and then by secondary threshold
        for ( EnsembleAverageType type : EnsembleAverageType.values() )
        {
            List<DoubleScoreStatisticOuter> innerSlice = Slicer.filter( statistics,
                                                                        value -> type == value.getMetadata()
                                                                                              .getPool()
                                                                                              .getEnsembleAverageType() );
            // Slice by secondary threshold
            if ( !innerSlice.isEmpty() )
            {
                if ( !secondThreshold.isEmpty() )
                {
                    // Slice by the second threshold
                    secondThreshold.forEach( next -> sliced.add( Slicer.filter( innerSlice,
                                                                                value -> next.equals( value.getMetadata()
                                                                                                           .getThresholds()
                                                                                                           .second() ) ) ) );

                    // Primary thresholds without secondary thresholds
                    List<DoubleScoreStatisticOuter> primaryOnly = innerSlice.stream()
                                                                            .filter( next -> !next.getMetadata()
                                                                                                  .getThresholds()
                                                                                                  .hasTwo() )
                                                                            .toList();

                    sliced.add( primaryOnly );
                }
                // No secondary thresholds
                else
                {
                    sliced.add( innerSlice );
                }
            }
        }

        return Collections.unmodifiableList( sliced );
    }

    /**
     * Generates a path qualifier for the graphic based on the statistics provided.
     * @param statistics the statistics
     * @return a path qualifier or null if non is required
     */

    private static String getPathQualifier( List<DoubleScoreStatisticOuter> statistics )
    {
        String append = null;

        // Secondary threshold? If yes, only one, as this was sliced above
        SortedSet<ThresholdOuter> second =
                Slicer.discover( statistics,
                                 next -> next.getMetadata()
                                             .getThresholds()
                                             .second() );
        if ( !second.isEmpty() )
        {
            append = DataUtilities.toStringSafe( second.iterator()
                                                       .next() );
        }

        // Non-default averaging types that should be qualified?
        // #51670
        SortedSet<EnsembleAverageType> types =
                Slicer.discover( statistics,
                                 next -> next.getMetadata().getPool().getEnsembleAverageType() );

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
     *  Writes a group of destinations.
     * 
     * @param outputDirectory the output directory
     * @param metadata the sample metadata
     * @param engines the graphics engines
     * @param metricName the metric name
     * @param append a string to append to the path
     * @param outputsDescription a description of the outputs required
     * @return the paths written
     * @throws ChartBuildingException if the chart could not be created
     */

    private static Set<Path> writeNextGroupOfDestinations( Path outputDirectory,
                                                           PoolMetadata metadata,
                                                           Map<MetricConstants, JFreeChart> engines,
                                                           MetricConstants metricName,
                                                           String append,
                                                           Outputs outputsDescription )
    {
        Set<Path> pathsWrittenTo = new HashSet<>();

        // Build the outputs
        for ( final Entry<MetricConstants, JFreeChart> nextEntry : engines.entrySet() )
        {

            // Qualify with the component name unless there is one component and it is main
            MetricConstants componentName = null;
            if ( nextEntry.getKey() != MetricConstants.MAIN || engines.size() > 0 )
            {
                componentName = nextEntry.getKey();
            }

            // Build the output file name
            Path outputImage = DataUtilities.getPathFromPoolMetadata( outputDirectory,
                                                                      metadata,
                                                                      append,
                                                                      metricName,
                                                                      componentName );

            JFreeChart chart = nextEntry.getValue();

            // Write formats
            Set<Path> finishedPaths = GraphicsWriter.writeGraphic( outputImage,
                                                                   chart,
                                                                   outputsDescription );

            pathsWrittenTo.addAll( finishedPaths );
        }

        return Collections.unmodifiableSet( pathsWrittenTo );
    }

    /**
     * Hidden constructor.
     * 
     * @param outputsDescription a description of the required outputs
     * @param outputDirectory the directory into which to write
     * @throws NullPointerException if either input is null
     */

    private DoubleScoreGraphicsWriter( Outputs outputsDescription,
                                       Path outputDirectory )
    {
        super( outputsDescription, outputDirectory );
    }

}
