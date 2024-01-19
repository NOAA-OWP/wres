package wres.metrics.singlevalued;

import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;

import wres.config.MetricConstants;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.metrics.DoubleErrorFunction;
import wres.metrics.FunctionFactory;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Computes the mean error of a single-valued prediction as a fraction of the mean observed value or, in other words,
 * the sum of right values divided by the sum of left values minus one.
 *
 * @author James Brown
 */
public class BiasFraction extends DoubleErrorScore<Pool<Pair<Double, Double>>>
{

    /**
     * Basic description of the metric.
     */

    public static final DoubleScoreMetric BASIC_METRIC = DoubleScoreMetric.newBuilder()
                                                                          .setName( MetricName.BIAS_FRACTION )
                                                                          .build();

    /**
     * Main score component.
     */

    public static final DoubleScoreMetricComponent MAIN =
            DoubleScoreMetricComponent.newBuilder()
                                      .setMinimum( MetricConstants.BIAS_FRACTION.getMinimum() )
                                      .setMaximum( MetricConstants.BIAS_FRACTION.getMaximum() )
                                      .setOptimum( MetricConstants.BIAS_FRACTION.getOptimum() )
                                      .setName( ComponentName.MAIN )
                                      .setUnits( MeasurementUnit.DIMENSIONLESS )
                                      .build();

    /**
     * Full description of the metric.
     */

    public static final DoubleScoreMetric METRIC_INNER = DoubleScoreMetric.newBuilder()
                                                                          .addComponents( BiasFraction.MAIN )
                                                                          .setName( MetricName.BIAS_FRACTION )
                                                                          .build();

    /**
     * Returns an instance.
     *
     * @return an instance
     */

    public static BiasFraction of()
    {
        return new BiasFraction();
    }

    @Override
    public DoubleScoreStatisticOuter apply( Pool<Pair<Double, Double>> pool )
    {
        if ( Objects.isNull( pool ) )
        {
            throw new PoolException( "Specify non-null input to the '" + this + "'." );
        }

        // Compute in error space for improved precision
        double leftSum = 0.0;
        double errorSum = 0.0;
        DoubleErrorFunction error = FunctionFactory.error();
        for ( Pair<Double, Double> pair : pool.get() )
        {
            leftSum += pair.getLeft();
            errorSum += error.applyAsDouble( pair );
        }

        double result = Double.NaN;
        if ( leftSum > 0 )
        {
            result = errorSum / leftSum;
        }

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( BiasFraction.MAIN )
                                                                               .setValue( result )
                                                                               .build();

        DoubleScoreStatistic score =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( BiasFraction.BASIC_METRIC )
                                    .addStatistics( component )
                                    .build();

        return DoubleScoreStatisticOuter.of( score, pool.getMetadata() );
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.BIAS_FRACTION;
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    /**
     * Hidden constructor.
     */

    private BiasFraction()
    {
        super( BiasFraction.METRIC_INNER );
    }

}
