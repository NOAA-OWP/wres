package wres.metrics.singlevalued;

import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.MetricConstants;
import wres.config.MetricConstants.MetricGroup;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.metrics.Collectable;
import wres.metrics.DecomposableScore;
import wres.metrics.MetricCalculationException;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Computes the Mean Square Error Skill Score Normalized (MSESSN) to (0,1]. It is related to the 
 * {@link MeanSquareErrorSkillScore} (MSESS) as: MSESSN = 1.0 / (2.0 - MSESS). 
 * 
 * @author James Brown
 */
public class MeanSquareErrorSkillScoreNormalized extends DecomposableScore<Pool<Pair<Double, Double>>>
        implements Collectable<Pool<Pair<Double, Double>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter>
{
    /** Basic description of the metric. */
    public static final DoubleScoreMetric BASIC_METRIC = DoubleScoreMetric.newBuilder()
                                                                          .setName( MetricName.MEAN_SQUARE_ERROR_SKILL_SCORE_NORMALIZED )
                                                                          .build();

    /** Main score component. */
    public static final DoubleScoreMetricComponent MAIN = DoubleScoreMetricComponent.newBuilder()
                                                                                    .setMinimum( 0 )
                                                                                    .setMaximum( 1 )
                                                                                    .setOptimum( 1 )
                                                                                    .setName( ComponentName.MAIN )
                                                                                    .setUnits( MeasurementUnit.DIMENSIONLESS )
                                                                                    .build();

    /** Full description of the metric. */
    public static final DoubleScoreMetric METRIC = DoubleScoreMetric.newBuilder()
                                                                    .addComponents( MeanSquareErrorSkillScoreNormalized.MAIN )
                                                                    .setName( MetricName.MEAN_SQUARE_ERROR_SKILL_SCORE_NORMALIZED )
                                                                    .build();

    /** Instance of {@link SumOfSquareError}.*/
    private final SumOfSquareError sse;

    /** Instance of {@link MeanSquareErrorSkillScore}.*/
    private final MeanSquareErrorSkillScore msess;

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( MeanSquareErrorSkillScoreNormalized.class );

    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static MeanSquareErrorSkillScoreNormalized of()
    {
        return new MeanSquareErrorSkillScoreNormalized();
    }

    @Override
    public DoubleScoreStatisticOuter apply( Pool<Pair<Double, Double>> s )
    {
        LOGGER.debug( "Computing the {}.", this );

        if ( Objects.isNull( s ) )
        {
            throw new PoolException( "Specify non-null input to the '" + this + "'." );
        }

        return this.aggregate( this.getIntermediateStatistic( s ), s );
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE_NORMALIZED;
    }

    @Override
    public MetricGroup getScoreOutputGroup()
    {
        return MetricGroup.NONE;
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    @Override
    public DoubleScoreStatisticOuter aggregate( DoubleScoreStatisticOuter output, Pool<Pair<Double, Double>> pool )
    {
        LOGGER.debug( "Computing the {} from the intermediate statistic, {}.", this, this.getCollectionOf() );

        DoubleScoreStatisticOuter resultOuter = this.msess.aggregate( output, pool );

        if ( this.getScoreOutputGroup() != MetricGroup.NONE )
        {
            throw new MetricCalculationException( "Decomposition is not currently implemented for the '" + this
                                                  + "'." );
        }

        double resultInner = resultOuter.getComponent( MetricConstants.MAIN )
                                        .getData()
                                        .getValue();

        double result = 1.0 / ( 2.0 - resultInner );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( MeanSquareErrorSkillScoreNormalized.MAIN )
                                                                               .setValue( result )
                                                                               .build();

        DoubleScoreStatistic score =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( MeanSquareErrorSkillScoreNormalized.BASIC_METRIC )
                                    .addStatistics( component )
                                    .build();

        return DoubleScoreStatisticOuter.of( score, output.getMetadata() );
    }

    @Override
    public DoubleScoreStatisticOuter getIntermediateStatistic( Pool<Pair<Double, Double>> input )
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

    MeanSquareErrorSkillScoreNormalized()
    {
        super();
        this.sse = SumOfSquareError.of();
        this.msess = MeanSquareErrorSkillScore.of();
    }

}
