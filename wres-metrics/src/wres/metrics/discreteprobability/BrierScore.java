package wres.metrics.discreteprobability;

import java.util.Objects;
import java.util.function.ToDoubleFunction;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.types.Probability;
import wres.config.MetricConstants;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.metrics.DecomposableScore;
import wres.metrics.FunctionFactory;
import wres.metrics.ProbabilityScore;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * <p>
 * The Brier score is the average square difference between a probabilistic predictand and a verifying observation. The
 * verifying observation is also probabilistic, and is typically obtained from the indicator function of a continuous
 * variable (i.e. dichotomous). Optionally, the Brier Score may be factored into two-component or three-component
 * decompositions. By convention, the Brier Score is half of the original Brier Score proposed by Brier (1950):
 * </p>
 * <p>
 * Brier, G. W. (1950) Verification of forecasts expressed in terms of probability. <i> Monthly Weather Review</i>,
 * <b>78</b>, 1-3.
 * </p>
 *
 * @author James Brown
 */
public class BrierScore extends DecomposableScore<Pool<Pair<Probability, Probability>>>
        implements ProbabilityScore<Pool<Pair<Probability, Probability>>, DoubleScoreStatisticOuter>
{

    /**
     * Basic description of the metric.
     */

    public static final DoubleScoreMetric BASIC_METRIC = DoubleScoreMetric.newBuilder()
                                                                          .setName( MetricName.BRIER_SCORE )
                                                                          .build();

    /**
     * Main score component.
     */

    public static final DoubleScoreMetricComponent MAIN =
            DoubleScoreMetricComponent.newBuilder()
                                      .setMinimum( MetricConstants.BRIER_SCORE.getMinimum() )
                                      .setMaximum( MetricConstants.BRIER_SCORE.getMaximum() )
                                      .setOptimum( MetricConstants.BRIER_SCORE.getOptimum() )
                                      .setName( MetricName.MAIN )
                                      .setUnits( MeasurementUnit.DIMENSIONLESS )
                                      .build();

    /**
     * Full description of the metric.
     */

    public static final DoubleScoreMetric METRIC = DoubleScoreMetric.newBuilder()
                                                                    .addComponents( BrierScore.MAIN )
                                                                    .setName( MetricName.BRIER_SCORE )
                                                                    .build();

    /**
     * Returns an instance.
     *
     * @return an instance
     */

    public static BrierScore of()
    {
        return new BrierScore();
    }

    @Override
    public DoubleScoreStatisticOuter apply( Pool<Pair<Probability, Probability>> pool )
    {
        if ( Objects.isNull( pool ) )
        {
            throw new PoolException( "Specify non-null input to the '" + this + "'." );
        }

        ToDoubleFunction<Pool<Pair<Probability, Probability>>> sse =
                FunctionFactory.sumOfSquareErrors( Probability::getProbability );

        double result = sse.applyAsDouble( pool ) / pool.get()
                                                        .size();

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( BrierScore.MAIN )
                                                                               .setValue( result )
                                                                               .build();
        DoubleScoreStatistic score =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( BrierScore.BASIC_METRIC )
                                    .addStatistics( component )
                                    .build();

        return DoubleScoreStatisticOuter.of( score, pool.getMetadata() );
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.BRIER_SCORE;
    }

    @Override
    public boolean isProper()
    {
        return true;
    }

    @Override
    public boolean isStrictlyProper()
    {
        return true;
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    /**
     * Hidden constructor.
     */

    BrierScore()
    {
        super();
    }

}
