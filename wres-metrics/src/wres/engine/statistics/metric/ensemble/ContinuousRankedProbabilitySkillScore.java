package wres.engine.statistics.metric.ensemble;

import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.Ensemble;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricGroup;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.pools.SampleData;
import wres.datamodel.pools.SampleDataException;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricParameterException;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * <p>
 * The Continuous Ranked Probability Skill Score (CRPSS) measures the reduction in the 
 * {@link ContinuousRankedProbabilityScore} associated with one set of predictions when compared to another. The perfect
 * score is 1.0. 
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 */
public class ContinuousRankedProbabilitySkillScore extends ContinuousRankedProbabilityScore
{

    /**
     * Basic description of the metric.
     */

    public static final DoubleScoreMetric BASIC_METRIC = DoubleScoreMetric.newBuilder()
                                                                          .setName( MetricName.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE )
                                                                          .build();

    /**
     * Main score component.
     */

    public static final DoubleScoreMetricComponent MAIN = DoubleScoreMetricComponent.newBuilder()
                                                                                    .setMinimum( Double.NEGATIVE_INFINITY )
                                                                                    .setMaximum( 1 )
                                                                                    .setOptimum( 1 )
                                                                                    .setName( ComponentName.MAIN )
                                                                                    .setUnits( MeasurementUnit.DIMENSIONLESS )
                                                                                    .build();

    /**
     * Full description of the metric.
     */

    public static final DoubleScoreMetric METRIC = DoubleScoreMetric.newBuilder()
                                                                    .addComponents( ContinuousRankedProbabilitySkillScore.MAIN )
                                                                    .setName( MetricName.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE )
                                                                    .build();

    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static ContinuousRankedProbabilitySkillScore of()
    {
        return new ContinuousRankedProbabilitySkillScore();
    }

    /**
     * Returns an instance.
     * 
     * @param decompositionId the decomposition identifier
     * @return an instance
     * @throws MetricParameterException if one or more parameters is invalid 
     */

    public static ContinuousRankedProbabilitySkillScore of( MetricGroup decompositionId )
            throws MetricParameterException
    {
        return new ContinuousRankedProbabilitySkillScore( decompositionId );
    }

    @Override
    public DoubleScoreStatisticOuter apply( SampleData<Pair<Double, Ensemble>> s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }
        if ( !s.hasBaseline() )
        {
            throw new SampleDataException( "Specify a non-null baseline for the '" + this + "'." );
        }
        //CRPSS, currently without decomposition
        //TODO: implement the decomposition
        double numerator = super.apply( s ).getComponent( MetricConstants.MAIN )
                                           .getData()
                                           .getValue();
        double denominator = super.apply( s.getBaselineData() ).getComponent( MetricConstants.MAIN )
                                                               .getData()
                                                               .getValue();

        double result = FunctionFactory.skill().applyAsDouble( numerator, denominator );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( ContinuousRankedProbabilitySkillScore.MAIN )
                                                                               .setValue( result )
                                                                               .build();
        DoubleScoreStatistic score =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( ContinuousRankedProbabilitySkillScore.BASIC_METRIC )
                                    .addStatistics( component )
                                    .build();

        return DoubleScoreStatisticOuter.of( score, s.getMetadata() );
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE;
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

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    /**
     * Hidden constructor.
     */

    private ContinuousRankedProbabilitySkillScore()
    {
        super();
    }

    /**
     * Hidden constructor.
     * 
     * @param decompositionId the decomposition identifier
     * @throws MetricParameterException if one or more parameters is invalid 
     */

    private ContinuousRankedProbabilitySkillScore( MetricGroup decompositionId ) throws MetricParameterException
    {
        super( decompositionId );
    }

}
