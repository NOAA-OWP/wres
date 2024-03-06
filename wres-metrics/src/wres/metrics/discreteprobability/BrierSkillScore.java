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
import wres.metrics.FunctionFactory;
import wres.metrics.singlevalued.MeanSquareErrorSkillScore;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * <p>
 * The Brier Skill Score (SS) measures the reduction in the {@link BrierScore} (i.e. probabilistic Mean Square Error)
 * associated with one set of predictions when compared to another. The BSS is analogous to the
 * {@link MeanSquareErrorSkillScore} or the Nash-Sutcliffe Efficiency for a single-valued input. The perfect BSS is 1.0.
 * </p>
 *
 * @author James Brown
 */
public class BrierSkillScore extends BrierScore
{

    /**
     * Basic description of the metric.
     */

    public static final DoubleScoreMetric BASIC_METRIC = DoubleScoreMetric.newBuilder()
                                                                          .setName( MetricName.BRIER_SKILL_SCORE )
                                                                          .build();

    /**
     * Main score component.
     */

    public static final DoubleScoreMetricComponent MAIN =
            DoubleScoreMetricComponent.newBuilder()
                                      .setMinimum( MetricConstants.BRIER_SKILL_SCORE.getMinimum() )
                                      .setMaximum( MetricConstants.BRIER_SKILL_SCORE.getMaximum() )
                                      .setOptimum( MetricConstants.BRIER_SKILL_SCORE.getOptimum() )
                                      .setName( MetricName.MAIN )
                                      .setUnits( MeasurementUnit.DIMENSIONLESS )
                                      .build();

    /**
     * Full description of the metric.
     */

    public static final DoubleScoreMetric METRIC = DoubleScoreMetric.newBuilder()
                                                                    .addComponents( BrierSkillScore.MAIN )
                                                                    .setName( MetricName.BRIER_SKILL_SCORE )
                                                                    .build();

    /**
     * Returns an instance.
     *
     * @return an instance
     */

    public static BrierSkillScore of()
    {
        return new BrierSkillScore();
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

        // Avoid division where possible for numerical accuracy
        double numerator = sse.applyAsDouble( pool );
        double denominator;
        if ( pool.hasBaseline() )
        {
            Pool<Pair<Probability, Probability>> baselinePool = pool.getBaselineData();
            denominator = sse.applyAsDouble( baselinePool );

            // Divide?
            int mainSize = pool.get()
                               .size();
            int baselineSize = baselinePool.get()
                                           .size();
            if ( mainSize != baselineSize )
            {
                numerator = numerator / mainSize;
                denominator = denominator / baselineSize;
            }
        }
        else
        {
            ToDoubleFunction<Pool<Pair<Probability, Probability>>> sseml =
                    FunctionFactory.sumOfSquareErrorsForMeanLeft( Probability::getProbability );
            denominator = sseml.applyAsDouble( pool );
        }

        double result = FunctionFactory.skill()
                                       .applyAsDouble( numerator, denominator );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( BrierSkillScore.MAIN )
                                                                               .setValue( result )
                                                                               .build();
        DoubleScoreStatistic score =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( BrierSkillScore.BASIC_METRIC )
                                    .addStatistics( component )
                                    .build();

        return DoubleScoreStatisticOuter.of( score, pool.getMetadata() );
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.BRIER_SKILL_SCORE;
    }

    @Override
    public boolean isProper()
    {
        return false;
    }

    @Override
    public boolean isStrictlyProper()
    {
        return false;
    }

    /**
     * Hidden constructor.
     */

    private BrierSkillScore()
    {
        super();
    }

}
