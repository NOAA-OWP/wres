package wres.metrics.singlevalued;

import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.Slicer;
import wres.datamodel.VectorOfDoubles;
import wres.config.MetricConstants;
import wres.config.MetricConstants.MetricGroup;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.metrics.Collectable;
import wres.metrics.FunctionFactory;
import wres.metrics.MetricCalculationException;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * The mean absolute error skill score (MAESS) applies the general equation for skill to the MAE of a main set of 
 * prediction (MAE) and a baseline set of predictions (MAE_baseline), {@code MAESS = 1.0 - MAE / MAE_baseline}. When no
 * explicit baseline is provided, a default baseline is used, which corresponds to the average observation or 
 * "climatology".
 *
 * @author James Brown
 */
public class MeanAbsoluteErrorSkillScore extends DoubleErrorScore<Pool<Pair<Double, Double>>>
        implements Collectable<Pool<Pair<Double, Double>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter>
{
    /** Main score component. */
    public static final DoubleScoreMetricComponent MAIN = DoubleScoreMetricComponent.newBuilder()
                                                                                    .setMinimum( Double.NEGATIVE_INFINITY )
                                                                                    .setMaximum( 1 )
                                                                                    .setOptimum( 1 )
                                                                                    .setName( ComponentName.MAIN )
                                                                                    .setUnits( MeasurementUnit.DIMENSIONLESS )
                                                                                    .build();

    /** Full description of the metric.*/
    public static final DoubleScoreMetric METRIC_INNER
            = DoubleScoreMetric.newBuilder()
                               .addComponents( MeanAbsoluteErrorSkillScore.MAIN )
                               .setName( MetricName.MEAN_ABSOLUTE_ERROR_SKILL_SCORE )
                               .build();

    /** Basic description of the metric. */
    public static final DoubleScoreMetric BASIC_METRIC = DoubleScoreMetric.newBuilder()
                                                                          .setName( MetricName.MEAN_ABSOLUTE_ERROR_SKILL_SCORE )
                                                                          .build();

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( MeanAbsoluteErrorSkillScore.class );

    /**
     * Returns an instance.
     *
     * @return an instance
     */

    public static MeanAbsoluteErrorSkillScore of()
    {
        return new MeanAbsoluteErrorSkillScore();
    }

    @Override
    public DoubleScoreStatisticOuter apply( final Pool<Pair<Double, Double>> pool )
    {
        LOGGER.debug( "Computing the {}.", this );

        if ( Objects.isNull( pool ) )
        {
            throw new PoolException( "Specify non-null input to the '" + this + "'." );
        }

        return this.aggregate( this.getIntermediateStatistic( pool ), pool );
    }

    @Override
    public DoubleScoreStatisticOuter aggregate( DoubleScoreStatisticOuter output, Pool<Pair<Double, Double>> pool )
    {
        LOGGER.debug( "Computing the {} from the intermediate statistic, {}.", this, this.getCollectionOf() );

        if ( Objects.isNull( output ) )
        {
            throw new MetricCalculationException( "Specify a non-null statistic for the '" + this + "'." );
        }

        if ( this.getScoreOutputGroup() != MetricGroup.NONE )
        {
            throw new MetricCalculationException( "Decomposition is not currently implemented for the '" + this
                                                  + "'." );
        }

        double result = Double.NaN;

        // Some data, proceed
        if ( !pool.get().isEmpty() )
        {
            double mae = output.getComponent( MetricConstants.MAIN )
                               .getData()
                               .getValue();

            double numerator = FunctionFactory.finiteOrMissing()
                                              .applyAsDouble( mae );

            double denominator = 0.0;
            if ( pool.hasBaseline() )
            {
                Pool<Pair<Double, Double>> baseline = pool.getBaselineData();
                DoubleScoreStatisticOuter maeBase = super.apply( baseline );
                denominator = maeBase.getComponent( MetricConstants.MAIN )
                                     .getData()
                                     .getValue();
            }
            // Default baseline is the average observation or so-called climatology
            else
            {
                double meanLeft =
                        FunctionFactory.mean().applyAsDouble( VectorOfDoubles.of( Slicer.getLeftSide( pool ) ) );
                for ( Pair<Double, Double> next : pool.get() )
                {
                    denominator += Math.abs( next.getLeft() - meanLeft );
                }

                denominator = denominator / pool.get()
                                                .size();
            }

            result = FunctionFactory.skill().applyAsDouble( numerator, denominator );
        }

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( MeanAbsoluteErrorSkillScore.MAIN )
                                                                               .setValue( result )
                                                                               .build();

        DoubleScoreStatistic score =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( MeanAbsoluteErrorSkillScore.BASIC_METRIC )
                                    .addStatistics( component )
                                    .build();

        return DoubleScoreStatisticOuter.of( score, output.getMetadata() );
    }

    @Override
    public DoubleScoreStatisticOuter getIntermediateStatistic( Pool<Pair<Double, Double>> input )
    {
        return super.apply( input );
    }

    @Override
    public MetricConstants getCollectionOf()
    {
        return MetricConstants.MEAN_ABSOLUTE_ERROR;
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.MEAN_ABSOLUTE_ERROR_SKILL_SCORE;
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    /**
     * Hidden constructor.
     */

    private MeanAbsoluteErrorSkillScore()
    {
        super( FunctionFactory.absError(), MeanAbsoluteErrorSkillScore.METRIC_INNER );
    }

}
