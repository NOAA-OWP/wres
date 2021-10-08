package wres.metrics.singlevalued;

import java.util.List;
import java.util.Objects;
import java.util.function.DoubleUnaryOperator;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.metrics.Diagram;
import wres.metrics.FunctionFactory;
import wres.datamodel.Slicer;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.statistics.generated.BoxplotMetric;
import wres.statistics.generated.BoxplotMetric.LinkedValueType;
import wres.statistics.generated.BoxplotMetric.QuantileValueType;
import wres.statistics.generated.BoxplotStatistic.Box;
import wres.statistics.generated.BoxplotStatistic;
import wres.statistics.generated.MetricName;

/**
 * A box plot of the errors associated with a pool of single-valued pairs.
 * 
 * @author James Brown
 */

public class BoxPlotError extends Diagram<Pool<Pair<Double, Double>>, BoxplotStatisticOuter>
{

    /**
     * Default probabilities.
     */

    private static final List<Double> DEFAULT_PROBABILITIES = List.of( 0.0, 0.25, 0.5, 0.75, 1.0 );

    /**
     * Empty box.
     */

    private static final List<Double> EMPTY_BOX = List.of( Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN );

    /**
     * Function for rounding the errors.
     */

    private static final DoubleUnaryOperator ROUNDER = v -> FunctionFactory.round().apply( v, 8 );

    /**
     * The canonical representation of the metric.
     */

    private final BoxplotMetric metric;

    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static BoxPlotError of()
    {
        return new BoxPlotError();
    }

    @Override
    public boolean hasRealUnits()
    {
        return true;
    }

    @Override
    public BoxplotStatisticOuter apply( Pool<Pair<Double, Double>> s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new PoolException( "Specify non-null input to the '" + this + "'." );
        }

        BoxplotStatistic.Builder builder = BoxplotStatistic.newBuilder()
                                                           .setMetric( this.getMetric()
                                                                           .toBuilder()
                                                                           .setUnits( s.getMetadata()
                                                                                       .getMeasurementUnit()
                                                                                       .toString() ) );

        // Empty output for empty input
        if ( s.get().isEmpty() )
        {
            // Add an empty box: #62863
            builder.addStatistics( Box.newBuilder().addAllQuantiles( BoxPlotError.EMPTY_BOX ) );

            return BoxplotStatisticOuter.of( builder.build(), s.getMetadata() );
        }

        // Get the sorted errors
        List<Double> probs = this.getMetric().getQuantilesList();
        double[] sortedErrors = s.get()
                                 .stream()
                                 .mapToDouble( a -> a.getRight() - a.getLeft() )
                                 .sorted()
                                 .toArray();

        // Compute the quantiles of the errors at a rounded precision
        List<Double> box = probs.stream()
                                .mapToDouble( Double::doubleValue )
                                .map( Slicer.getQuantileFunction( sortedErrors ) )
                                .map( BoxPlotError.ROUNDER )
                                .boxed()
                                .collect( Collectors.toList() );

        BoxplotStatistic statistic = builder.addStatistics( Box.newBuilder().addAllQuantiles( box ) )
                                            .build();

        return BoxplotStatisticOuter.of( statistic, s.getMetadata() );
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.BOX_PLOT_OF_ERRORS;
    }

    /**
     * Returns the metric.
     * 
     * @return the metric
     */

    private BoxplotMetric getMetric()
    {
        return this.metric;
    }

    /**
     * Hidden constructor.
     */

    private BoxPlotError()
    {
        super();
        this.metric = BoxplotMetric.newBuilder()
                                   .setName( MetricName.BOX_PLOT_OF_ERRORS )
                                   .setLinkedValueType( LinkedValueType.NONE )
                                   .setQuantileValueType( QuantileValueType.FORECAST_ERROR )
                                   .addAllQuantiles( BoxPlotError.DEFAULT_PROBABILITIES )
                                   .setMinimum( Double.NEGATIVE_INFINITY )
                                   .setMaximum( Double.POSITIVE_INFINITY )
                                   .build();
    }

}
