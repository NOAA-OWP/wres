package wres.metrics.singlevalued;

import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.MetricConstants;
import wres.datamodel.pools.Pool;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.metrics.Diagram;
import wres.statistics.generated.DiagramMetric;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent.DiagramComponentType;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.DiagramStatistic.DiagramStatisticComponent;
import wres.statistics.generated.MetricName;

/**
 * Plots the pairs of observations and predictions.
 *
 * @author James Brown
 */

public class ScatterPlot extends Diagram<Pool<Pair<Double, Double>>, DiagramStatisticOuter>
{
    /** Observations. */
    private static final DiagramMetricComponent OBSERVATIONS =
            DiagramMetricComponent.newBuilder()
                                  .setName( MetricName.OBSERVATIONS )
                                  .setType( DiagramComponentType.PRIMARY_DOMAIN_AXIS )
                                  .setMinimum( MetricConstants.SCATTER_PLOT.getMinimum() )
                                  .setMaximum( MetricConstants.SCATTER_PLOT.getMaximum() )
                                  .build();

    /** Predictions. */
    private static final DiagramMetricComponent PREDICTIONS =
            DiagramMetricComponent.newBuilder()
                                  .setName( MetricName.PREDICTIONS )
                                  .setType( DiagramComponentType.PRIMARY_RANGE_AXIS )
                                  .setMinimum( MetricConstants.SCATTER_PLOT.getMinimum() )
                                  .setMaximum( MetricConstants.SCATTER_PLOT.getMaximum() )
                                  .build();

    /** Basic description of the metric. */
    public static final DiagramMetric BASIC_METRIC = DiagramMetric.newBuilder()
                                                                  .setName( MetricName.SCATTER_PLOT )
                                                                  .setHasDiagonal( true )
                                                                  .build();

    /** Full description of the metric. */
    public static final DiagramMetric METRIC = DiagramMetric.newBuilder()
                                                            .addComponents( ScatterPlot.OBSERVATIONS )
                                                            .addComponents( ScatterPlot.PREDICTIONS )
                                                            .setHasDiagonal( true )
                                                            .setName( MetricName.SCATTER_PLOT )
                                                            .build();

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( ScatterPlot.class );

    /**
     * Returns an instance.
     *
     * @return an instance
     */

    public static ScatterPlot of()
    {
        return new ScatterPlot();
    }

    @Override
    public DiagramStatisticOuter apply( Pool<Pair<Double, Double>> pool )
    {
        Objects.requireNonNull( pool, "Specify non-null input to the '" + this.getMetricNameString() + "'." );

        LOGGER.debug( "Computing the {}.", this );


        // Add the units
        DiagramMetricComponent obsWithUnits = ScatterPlot.OBSERVATIONS.toBuilder()
                                                                      .setUnits( pool.getMetadata()
                                                                                     .getMeasurementUnit()
                                                                                     .toString() )
                                                                      .build();

        DiagramMetricComponent predWithUnits = ScatterPlot.PREDICTIONS.toBuilder()
                                                                      .setUnits( pool.getMetadata()
                                                                                     .getMeasurementUnit()
                                                                                     .toString() )
                                                                      .build();

        DiagramStatisticComponent.Builder observations =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( obsWithUnits );

        DiagramStatisticComponent.Builder predictions =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( predWithUnits );

        for ( Pair<Double, Double> nextPair : pool.get() )
        {
            observations.addValues( nextPair.getLeft() );
            predictions.addValues( nextPair.getRight() );
        }

        DiagramStatistic qqDiagram = DiagramStatistic.newBuilder()
                                                     .addStatistics( observations )
                                                     .addStatistics( predictions )
                                                     .setMetric( ScatterPlot.BASIC_METRIC )
                                                     .build();

        return DiagramStatisticOuter.of( qqDiagram, pool.getMetadata() );
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.SCATTER_PLOT;
    }

    @Override
    public boolean hasRealUnits()
    {
        return true;
    }

    /**
     * Hidden constructor.
     */

    private ScatterPlot()
    {
        super();
    }
}
