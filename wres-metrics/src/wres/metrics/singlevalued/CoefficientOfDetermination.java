package wres.metrics.singlevalued;

import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.MetricConstants;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Computes the square of Pearson's product-moment correlation coefficient between the left and right sides of the
 * {SingleValuedPairs} input.
 *
 * @author James Brown
 */
public class CoefficientOfDetermination extends CorrelationPearsons
{
    /** Basic description of the metric. */
    public static final DoubleScoreMetric BASIC_METRIC = DoubleScoreMetric.newBuilder()
                                                                          .setName( MetricName.COEFFICIENT_OF_DETERMINATION )
                                                                          .build();

    /** Main score component. */
    public static final DoubleScoreMetricComponent MAIN =
            DoubleScoreMetricComponent.newBuilder()
                                      .setMinimum( MetricConstants.COEFFICIENT_OF_DETERMINATION.getMinimum() )
                                      .setMaximum( MetricConstants.COEFFICIENT_OF_DETERMINATION.getMaximum() )
                                      .setOptimum( MetricConstants.COEFFICIENT_OF_DETERMINATION.getOptimum() )
                                      .setName( ComponentName.MAIN )
                                      .setUnits( MeasurementUnit.DIMENSIONLESS )
                                      .build();

    /** Full description of the metric. */
    public static final DoubleScoreMetric METRIC = DoubleScoreMetric.newBuilder()
                                                                    .addComponents( CoefficientOfDetermination.MAIN )
                                                                    .setName( MetricName.COEFFICIENT_OF_DETERMINATION )
                                                                    .build();

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( CoefficientOfDetermination.class );

    /**
     * Returns an instance.
     *
     * @return an instance
     */

    public static CoefficientOfDetermination of()
    {
        return new CoefficientOfDetermination();
    }

    @Override
    public DoubleScoreStatisticOuter apply( Pool<Pair<Double, Double>> pool )
    {
        LOGGER.debug( "Computing the {}.", this );

        return this.applyIntermediate( this.getIntermediate( pool ), pool );
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.COEFFICIENT_OF_DETERMINATION;
    }

    @Override
    public DoubleScoreStatisticOuter applyIntermediate( DoubleScoreStatisticOuter output,
                                                        Pool<Pair<Double, Double>> pool )
    {
        LOGGER.debug( "Computing the {} from the intermediate statistic, {}.", this, this.getCollectionOf() );

        if ( Objects.isNull( output ) )
        {
            throw new PoolException( "Specify non-null input to the '" + this + "'." );
        }

        double input = output.getComponent( MetricConstants.MAIN )
                             .getStatistic()
                             .getValue();

        double result = Math.pow( input, 2 );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( CoefficientOfDetermination.MAIN )
                                                                               .setValue( result )
                                                                               .build();

        DoubleScoreStatistic score =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( CoefficientOfDetermination.BASIC_METRIC )
                                    .addStatistics( component )
                                    .build();

        return DoubleScoreStatisticOuter.of( score, output.getPoolMetadata() );
    }

    @Override
    public DoubleScoreStatisticOuter getIntermediate( Pool<Pair<Double, Double>> input )
    {
        if ( Objects.isNull( input ) )
        {
            throw new PoolException( "Specify non-null input to the '" + this + "'." );
        }
        return super.apply( input );
    }

    /**
     * Hidden constructor.
     */

    private CoefficientOfDetermination()
    {
        super();
    }

}
