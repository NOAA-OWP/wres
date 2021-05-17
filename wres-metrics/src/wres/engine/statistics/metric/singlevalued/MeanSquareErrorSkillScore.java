package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.Slicer;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.metrics.MetricConstants.MetricGroup;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.engine.statistics.metric.Collectable;
import wres.engine.statistics.metric.DecomposableScore;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * The Mean Square Error (MSE) Skill Score (SS) measures the reduction in MSE associated with one set of predictions
 * when compared to another. The MSE-SS is equivalent to the Nash-Sutcliffe Efficiency. The perfect MSE-SS is 1.0.
 * 
 * @author james.brown@hydrosolved.com
 */
public class MeanSquareErrorSkillScore extends DecomposableScore<Pool<Pair<Double, Double>>>
        implements Collectable<Pool<Pair<Double, Double>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter>
{

    /**
     * Basic description of the metric.
     */

    public static final DoubleScoreMetric BASIC_METRIC = DoubleScoreMetric.newBuilder()
                                                                          .setName( MetricName.MEAN_SQUARE_ERROR_SKILL_SCORE )
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
                                                                    .addComponents( MeanSquareErrorSkillScore.MAIN )
                                                                    .setName( MetricName.MEAN_SQUARE_ERROR_SKILL_SCORE )
                                                                    .build();

    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static MeanSquareErrorSkillScore of()
    {
        return new MeanSquareErrorSkillScore();
    }

    /**
     * Instance of {@link SumOfSquareError}.
     */

    private final SumOfSquareError sse;

    @Override
    public DoubleScoreStatisticOuter apply( final Pool<Pair<Double, Double>> s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new PoolException( "Specify non-null input to the '" + this + "'." );
        }

        if ( this.getScoreOutputGroup() != MetricGroup.NONE )
        {
            throw new MetricCalculationException( "Decomposition is not currently implemented for the '" + this
                                                  + "'." );
        }

        //TODO: implement any required decompositions, based on the instance parameters and return the decomposition
        //template as the componentID in the metadata

        double result = Double.NaN;

        // Some data, proceed
        if ( !s.getRawData().isEmpty() )
        {
            double numerator = this.sse.apply( s )
                                       .getComponent( MetricConstants.MAIN )
                                       .getData()
                                       .getValue();

            double denominator = 0.0;
            if ( s.hasBaseline() )
            {
                denominator = this.sse.apply( s.getBaselineData() )
                                      .getComponent( MetricConstants.MAIN )
                                      .getData()
                                      .getValue();
            }
            else
            {
                double meanLeft =
                        FunctionFactory.mean().applyAsDouble( VectorOfDoubles.of( Slicer.getLeftSide( s ) ) );
                for ( Pair<Double, Double> next : s.getRawData() )
                {
                    denominator += Math.pow( next.getLeft() - meanLeft, 2 );
                }
            }
            result = FunctionFactory.skill().applyAsDouble( numerator, denominator );
        }

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( MeanSquareErrorSkillScore.MAIN )
                                                                               .setValue( result )
                                                                               .build();

        DoubleScoreStatistic score =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( MeanSquareErrorSkillScore.BASIC_METRIC )
                                    .addStatistics( component )
                                    .build();

        return DoubleScoreStatisticOuter.of( score, s.getMetadata() );
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE;
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    @Override
    public DoubleScoreStatisticOuter aggregate( DoubleScoreStatisticOuter output )
    {
        if ( Objects.isNull( output ) )
        {
            throw new PoolException( "Specify non-null input to the '" + this + "'." );
        }

        return output;
    }

    @Override
    public DoubleScoreStatisticOuter getInputForAggregation( Pool<Pair<Double, Double>> input )
    {
        return this.apply( input );
    }

    @Override
    public MetricConstants getCollectionOf()
    {
        return MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE;
    }

    /**
     * Hidden constructor.
     */

    MeanSquareErrorSkillScore()
    {
        super();
        sse = SumOfSquareError.of();
    }

}
