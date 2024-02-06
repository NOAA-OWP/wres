package wres.metrics.singlevalued;

import java.util.Objects;
import java.util.function.ToDoubleFunction;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.config.MetricConstants;
import wres.config.MetricConstants.MetricGroup;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.metrics.Collectable;
import wres.metrics.DecomposableScore;
import wres.metrics.FunctionFactory;
import wres.metrics.MetricCalculationException;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * The Mean Square Error (MSE) Skill Score (SS) measures the reduction in MSE associated with one set of predictions
 * when compared to another. The MSE-SS is equivalent to the Nash-Sutcliffe Efficiency when using the average
 * observation as the baseline. The perfect MSE-SS is 1.0.
 *
 * @author James Brown
 */
public class MeanSquareErrorSkillScore extends DecomposableScore<Pool<Pair<Double, Double>>>
        implements Collectable<Pool<Pair<Double, Double>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter>
{
    /** Basic description of the metric. */
    public static final DoubleScoreMetric BASIC_METRIC = DoubleScoreMetric.newBuilder()
                                                                          .setName( MetricName.MEAN_SQUARE_ERROR_SKILL_SCORE )
                                                                          .build();

    /** Main score component. */
    public static final DoubleScoreMetricComponent MAIN =
            DoubleScoreMetricComponent.newBuilder()
                                      .setMinimum( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE.getMinimum() )
                                      .setMaximum( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE.getMaximum() )
                                      .setOptimum( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE.getOptimum() )
                                      .setName( MetricName.MAIN )
                                      .setUnits( MeasurementUnit.DIMENSIONLESS )
                                      .build();

    /** Full description of the metric.*/
    public static final DoubleScoreMetric METRIC = DoubleScoreMetric.newBuilder()
                                                                    .addComponents( MeanSquareErrorSkillScore.MAIN )
                                                                    .setName( MetricName.MEAN_SQUARE_ERROR_SKILL_SCORE )
                                                                    .build();

    /** Instance of {@link SumOfSquareError}.*/
    private final SumOfSquareError sse;

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( MeanSquareErrorSkillScore.class );

    /**
     * Returns an instance.
     *
     * @return an instance
     */

    public static MeanSquareErrorSkillScore of()
    {
        return new MeanSquareErrorSkillScore();
    }

    @Override
    public DoubleScoreStatisticOuter apply( final Pool<Pair<Double, Double>> pool )
    {
        LOGGER.debug( "Computing the {}.", this );

        if ( Objects.isNull( pool ) )
        {
            throw new PoolException( "Specify non-null input to the '" + this + "'." );
        }

        return this.applyIntermediate( this.getIntermediate( pool ), pool );
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
    public DoubleScoreStatisticOuter applyIntermediate( DoubleScoreStatisticOuter output,
                                                        Pool<Pair<Double, Double>> pool )
    {
        LOGGER.debug( "Computing the {} from the intermediate statistic, {}.", this, this.getCollectionOf() );

        if ( Objects.isNull( output ) )
        {
            throw new PoolException( "Specify a non-null statistic for the '" + this + "'." );
        }

        if ( this.getScoreOutputGroup() != MetricGroup.NONE )
        {
            throw new MetricCalculationException( "Decomposition is not currently implemented for the '" + this
                                                  + "'." );
        }

        // TODO: implement any required decompositions, based on the instance parameters and return the decomposition
        // template as the componentID in the metadata
        double result = Double.NaN;

        // Some data, proceed
        if ( !pool.get()
                  .isEmpty() )
        {
            double sseInner = output.getComponent( MetricConstants.MAIN )
                                    .getStatistic()
                                    .getValue();

            double numerator = FunctionFactory.finiteOrMissing()
                                              .applyAsDouble( sseInner );

            ToDoubleFunction<Pool<Pair<Double, Double>>> sses = FunctionFactory.sumOfSquareErrors( a -> a );

            double denominator;
            if ( pool.hasBaseline() )
            {
                Pool<Pair<Double, Double>> baselinePool = pool.getBaselineData();
                denominator = sses.applyAsDouble( baselinePool );

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
            // Default baseline is the average observation, which results in the so-called Nash-Sutcliffe Efficiency
            else
            {
                ToDoubleFunction<Pool<Pair<Double, Double>>> sseml =
                        FunctionFactory.sumOfSquareErrorsForMeanLeft( a -> a );
                denominator = sseml.applyAsDouble( pool );
            }

            result = FunctionFactory.skill()
                                    .applyAsDouble( numerator, denominator );
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

        return DoubleScoreStatisticOuter.of( score, output.getPoolMetadata() );
    }

    @Override
    public DoubleScoreStatisticOuter getIntermediate( Pool<Pair<Double, Double>> input )
    {
        return this.sse.apply( input );
    }

    @Override
    public MetricConstants getCollectionOf()
    {
        return MetricConstants.SUM_OF_SQUARE_ERROR;
    }

    /**
     * Hidden constructor.
     */

    MeanSquareErrorSkillScore()
    {
        super();
        this.sse = SumOfSquareError.of();
    }
}
