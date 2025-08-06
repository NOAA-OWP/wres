package wres.vis.writing;

import java.nio.file.Path;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.function.Function;

import org.jfree.chart.JFreeChart;

import wres.config.MetricConstants;
import wres.config.yaml.components.DatasetOrientation;
import wres.datamodel.DataUtilities;
import wres.datamodel.Slicer;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.PairsStatisticOuter;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Outputs.GraphicFormat.GraphicShape;
import wres.statistics.generated.Pool.EnsembleAverageType;
import wres.vis.charts.ChartBuildingException;
import wres.vis.charts.ChartFactory;

/**
 * Helps write charts comprising {@link PairsStatisticOuter} to graphics formats.
 *
 * @author James Brown
 */

public class PairsStatisticsGraphicsWriter extends GraphicsWriter
        implements Function<List<PairsStatisticOuter>, Set<Path>>
{

    /**
     * Returns an instance of a writer.
     *
     * @param outputsDescription a description of the required outputs
     * @param outputDirectory the directory into which to write
     * @return a writer
     * @throws NullPointerException if either input is null
     */

    public static PairsStatisticsGraphicsWriter of( Outputs outputsDescription,
                                                    Path outputDirectory )
    {
        return new PairsStatisticsGraphicsWriter( outputsDescription, outputDirectory );
    }

    /**
     * Writes all output for one pairs statistic type.
     *
     * @param output the pairs statistics
     * @return the paths written
     * @throws NullPointerException if the input is null
     * @throws GraphicsWriteException if the output cannot be written
     */

    @Override
    public Set<Path> apply( List<PairsStatisticOuter> output )
    {
        Objects.requireNonNull( output, "Specify non-null input data when writing pairs statistics." );

        Set<Path> paths = new HashSet<>();

        // Iterate through each metric
        SortedSet<MetricConstants> metrics = Slicer.discover( output, PairsStatisticOuter::getMetricName );
        for ( MetricConstants next : metrics )
        {
            List<PairsStatisticOuter> filtered = Slicer.filter( output, next );

            // Slice the statistics
            List<List<PairsStatisticOuter>> sliced =
                    PairsStatisticsGraphicsWriter.getSlicedStatistics( filtered );

            for ( List<PairsStatisticOuter> nextSlice : sliced )
            {
                if ( nextSlice.size() > 1 )
                {
                    throw new GraphicsWriteException( "When writing pairs statistics, encountered "
                                                      + nextSlice.size()
                                                      + " pairs statistics, but only 1 was expected. " );
                }

                Set<Path> innerPathsWrittenTo =
                        PairsStatisticsGraphicsWriter.writePlot( super.getOutputDirectory(),
                                                                 super.getOutputsDescription(),
                                                                 nextSlice.get( 0 ) );
                paths.addAll( innerPathsWrittenTo );
            }
        }

        return Collections.unmodifiableSet( paths );
    }

    /**
     * Writes a set of charts associated with {@link PairsStatisticOuter} for a single metric.
     *
     * @param outputDirectory the directory into which to write
     * @param outputsDescription a description of the outputs required
     * @param statistics the metric results
     * @return the paths written
     * @throws GraphicsWriteException when an error occurs during writing
     */

    private static Set<Path> writePlot( Path outputDirectory,
                                        Outputs outputsDescription,
                                        PairsStatisticOuter statistics )
    {
        Set<Path> pathsWrittenTo = new HashSet<>();

        ChartFactory chartFactory = GraphicsWriter.getChartFactory();

        // Build charts
        try
        {
            MetricConstants metricName = statistics.getMetricName();
            PoolMetadata metadata = statistics.getPoolMetadata();

            // Collection of graphics parameters, one for each set of charts to write across N formats.
            Collection<Outputs> outputsMap =
                    GraphicsWriter.getOutputsGroupedByGraphicsParameters( outputsDescription );

            for ( Outputs nextOutput : outputsMap )
            {
                // One helper per set of graphics parameters.
                GraphicsHelper helper = GraphicsHelper.of( nextOutput );

                JFreeChart chart = chartFactory.getPairsChart( statistics, helper.getDurationUnits() );

                // Build the output file name
                TimeWindowOuter appendObject = metadata.getTimeWindow();
                String appendString =
                        PairsStatisticsGraphicsWriter.getPathQualifier( appendObject, statistics, helper );
                Path outputImage = DataUtilities.getPathFromPoolMetadata( outputDirectory,
                                                                          metadata,
                                                                          appendString,
                                                                          metricName,
                                                                          null );
                // Write formats
                Set<Path> finishedPaths = GraphicsWriter.writeGraphic( outputImage,
                                                                       chart,
                                                                       metricName.getCanonicalName(),
                                                                       nextOutput );

                pathsWrittenTo.addAll( finishedPaths );
            }
        }
        catch ( ChartBuildingException e )
        {
            throw new GraphicsWriteException( "Error while generating diagram charts: ", e );
        }

        return Collections.unmodifiableSet( pathsWrittenTo );
    }

    /**
     * Slices the statistics for individual graphics. Returns as many sliced lists of statistics as graphics to create.
     *
     * @param statistics the statistics to slice
     * @return the sliced statistics to write
     */

    private static List<List<PairsStatisticOuter>> getSlicedStatistics( List<PairsStatisticOuter> statistics )
    {
        List<List<PairsStatisticOuter>> sliced = new ArrayList<>();

        // Slice by ensemble averaging function
        for ( EnsembleAverageType type : EnsembleAverageType.values() )
        {
            List<PairsStatisticOuter> innerSlice = Slicer.filter( statistics,
                                                                  value -> type == value.getPoolMetadata()
                                                                                        .getPoolDescription()
                                                                                        .getEnsembleAverageType() );

            // Slice by dataset orientation, i.e., pairs statistics do not support combined graphics
            if ( !innerSlice.isEmpty() )
            {
                Map<DatasetOrientation, List<PairsStatisticOuter>> dataSliced
                        = Slicer.getGroupedStatistics( innerSlice );
                sliced.addAll( dataSliced.values() );
            }
        }

        return Collections.unmodifiableList( sliced );
    }

    /**
     * Generates a path qualifier for the graphic based on the statistics provided.
     * @param timeWindow the time window qualifier
     * @param statistics the statistics
     * @param helper the graphics helper
     * @return a path qualifier or null if non is required
     */

    private static String getPathQualifier( TimeWindowOuter timeWindow,
                                            PairsStatisticOuter statistics,
                                            GraphicsHelper helper )
    {
        String append = "";

        GraphicShape shape = helper.getGraphicShape();
        ChronoUnit leadUnits = helper.getDurationUnits();

        // Qualify pooling windows with the latest reference time and valid time
        if ( shape == GraphicShape.ISSUED_DATE_POOLS
             || shape == GraphicShape.VALID_DATE_POOLS )
        {
            append = DataUtilities.toStringSafe( timeWindow, leadUnits );
        }
        // Needs to be fully qualified, but this would change the file names, which is arguably a breaking change
        // See GitHub ticket #540
        else if ( !timeWindow.getLatestLeadDuration()
                             .equals( TimeWindowOuter.DURATION_MAX ) )
        {
            append = DataUtilities.toStringSafe( timeWindow.getLatestLeadDuration(), leadUnits )
                     + "_"
                     + leadUnits.name()
                                .toUpperCase();
        }

        // Non-default averaging types that should be qualified?
        // #51670
        EnsembleAverageType type = statistics.getPoolMetadata()
                                             .getPoolDescription()
                                             .getEnsembleAverageType();

        if ( type != EnsembleAverageType.MEAN
             && type != EnsembleAverageType.NONE
             && type != EnsembleAverageType.UNRECOGNIZED )
        {
            append += "_ENSEMBLE_" + type.name();
        }

        return append;
    }

    /**
     * Hidden constructor.
     *
     * @param outputDirectory the directory into which to write
     * @param outputsDescription a description of the required outputs
     * @throws NullPointerException if either input is null
     */

    private PairsStatisticsGraphicsWriter( Outputs outputsDescription,
                                           Path outputDirectory )
    {
        super( outputsDescription, outputDirectory );
    }

}
