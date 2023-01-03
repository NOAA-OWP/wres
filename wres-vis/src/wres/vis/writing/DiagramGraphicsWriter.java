package wres.vis.writing;

import java.io.IOException;
import java.nio.file.Path;
import java.time.temporal.ChronoUnit;
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

import wres.config.ProjectConfigException;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.datamodel.DataUtilities;
import wres.datamodel.Slicer;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Pool.EnsembleAverageType;
import wres.vis.charts.ChartBuildingException;
import wres.vis.charts.ChartFactory;

/**
 * Helps write charts comprising {@link DiagramStatisticOuter} to graphics formats.
 * 
 * @author James Brown
 */

public class DiagramGraphicsWriter extends GraphicsWriter
        implements Function<List<DiagramStatisticOuter>, Set<Path>>
{

    /**
     * Returns an instance of a writer.
     *
     * @param outputsDescription a description of the required outputs
     * @param outputDirectory the directory into which to write
     * @return a writer
     * @throws NullPointerException if either input is null
     * @throws ProjectConfigException if the project configuration is not valid for writing
     */

    public static DiagramGraphicsWriter of( Outputs outputsDescription,
                                            Path outputDirectory )
    {
        return new DiagramGraphicsWriter( outputsDescription, outputDirectory );
    }

    /**
     * Writes all output for one diagram type.
     *
     * @param output the diagram output
     * @return the paths written
     * @throws NullPointerException if the input is null
     * @throws GraphicsWriteException if the output cannot be written
     */

    @Override
    public Set<Path> apply( List<DiagramStatisticOuter> output )
    {
        Objects.requireNonNull( output, "Specify non-null input data when writing diagram outputs." );

        Set<Path> paths = new HashSet<>();

        // Iterate through each metric 
        SortedSet<MetricConstants> metrics = Slicer.discover( output, DiagramStatisticOuter::getMetricName );
        for ( MetricConstants next : metrics )
        {
            List<DiagramStatisticOuter> filtered = Slicer.filter( output, next );

            // Group the statistics by the LRB context in which they appear. There will be one path written
            // for each group (e.g., one path for each window with LeftOrRightOrBaseline.RIGHT data and one for 
            // each window with LeftOrRightOrBaseline.BASELINE data): #48287
            Map<LeftOrRightOrBaseline, List<DiagramStatisticOuter>> groups =
                    Slicer.getStatisticsGroupedByContext( filtered );

            for ( List<DiagramStatisticOuter> nextGroup : groups.values() )
            {
                // Slice by ensemble averaging type
                List<List<DiagramStatisticOuter>> sliced =
                        DiagramGraphicsWriter.getSlicedStatistics( nextGroup );

                for ( List<DiagramStatisticOuter> nextSlice : sliced )
                {
                    Set<Path> innerPathsWrittenTo =
                            DiagramGraphicsWriter.writeDiagrams( super.getOutputDirectory(),
                                                                 super.getOutputsDescription(),
                                                                 nextSlice );
                    paths.addAll( innerPathsWrittenTo );
                }
            }
        }

        return Collections.unmodifiableSet( paths );
    }

    /**
     * Writes a set of charts associated with {@link DiagramStatisticOuter} for a single metric and time window,
     * stored in a {@link List}.
     *
     * @param outputDirectory the directory into which to write
     * @param outputsDescription a description of the outputs required
     * @param statistics the metric results
     * @return the paths written
     * @throws GraphicsWriteException when an error occurs during writing
     */

    private static Set<Path> writeDiagrams( Path outputDirectory,
                                            Outputs outputsDescription,
                                            List<DiagramStatisticOuter> statistics )
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

            for ( Outputs nextOutput : outputsMap )
            {
                // One helper per set of graphics parameters.
                GraphicsHelper helper = GraphicsHelper.of( nextOutput );

                Map<Object, JFreeChart> engines = chartFactory.getDiagramCharts( statistics,
                                                                                 helper.getGraphicShape(),
                                                                                 helper.getDurationUnits() );

                // Build the outputs
                for ( Entry<Object, JFreeChart> nextEntry : engines.entrySet() )
                {
                    // Build the output file name
                    Object appendObject = nextEntry.getKey();
                    String appendString = DiagramGraphicsWriter.getPathQualifier( appendObject, statistics, helper );
                    Path outputImage = DataUtilities.getPathFromPoolMetadata( outputDirectory,
                                                                              metadata,
                                                                              appendString,
                                                                              metricName,
                                                                              null );

                    JFreeChart chart = nextEntry.getValue();

                    // Write formats
                    Set<Path> finishedPaths = GraphicsWriter.writeGraphic( outputImage,
                                                                           chart,
                                                                           nextOutput );

                    pathsWrittenTo.addAll( finishedPaths );
                }
            }
        }
        catch ( ChartBuildingException | IOException e )
        {
            throw new GraphicsWriteException( "Error while generating diagram charts: ", e );
        }

        return Collections.unmodifiableSet( pathsWrittenTo );
    }

    /**
     * Slices the statistics for individual graphics. Returns as many sliced lists of statistics as graphics to create.
     * 
     * @param the statistics to slice
     * @return the sliced statistics to write
     */

    private static List<List<DiagramStatisticOuter>> getSlicedStatistics( List<DiagramStatisticOuter> statistics )
    {
        List<List<DiagramStatisticOuter>> sliced = new ArrayList<>();

        // Slice by ensemble averaging function
        for ( EnsembleAverageType type : EnsembleAverageType.values() )
        {
            List<DiagramStatisticOuter> innerSlice = Slicer.filter( statistics,
                                                                    value -> type == value.getMetadata()
                                                                                          .getPool()
                                                                                          .getEnsembleAverageType() );
            if ( !innerSlice.isEmpty() )
            {
                sliced.add( innerSlice );
            }
        }

        return Collections.unmodifiableList( sliced );
    }

    /**
     * Generates a path qualifier for the graphic based on the statistics provided.
     * @param appendObject the object to use in the path qualifier
     * @param statistics the statistics
     * @param helper the graphics helper
     * @return a path qualifier or null if non is required
     */

    private static String getPathQualifier( Object appendObject,
                                            List<DiagramStatisticOuter> statistics,
                                            GraphicsHelper helper )
    {
        String append = null;

        if ( appendObject instanceof TimeWindowOuter )
        {
            TimeWindowOuter timeWindow = (TimeWindowOuter) appendObject;

            ChronoUnit leadUnits = helper.getDurationUnits();
            append = DataUtilities.durationToNumericUnits( timeWindow.getLatestLeadDuration(),
                                                           leadUnits )
                     + "_"
                     + leadUnits.name().toUpperCase();
        }
        else if ( appendObject instanceof OneOrTwoThresholds )
        {
            OneOrTwoThresholds threshold = (OneOrTwoThresholds) appendObject;
            append = DataUtilities.toStringSafe( threshold );
        }
        else
        {
            throw new UnsupportedOperationException( "Unexpected situation where WRES could not create "
                                                     + "outputImage path" );
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
            append = "ENSEMBLE_" + type.get()
                                       .name();
        }

        return append;
    }

    /**
     * Hidden constructor.
     *
     * @param outputDirectory the directory into which to write
     * @param outputsDescription a description of the required outputs
     * @throws ProjectConfigException if the project configuration is not valid for writing
     * @throws NullPointerException if either input is null
     */

    private DiagramGraphicsWriter( Outputs outputsDescription,
                                   Path outputDirectory )
    {
        super( outputsDescription, outputDirectory );
    }

}
