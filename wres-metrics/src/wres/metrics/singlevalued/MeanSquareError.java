package wres.metrics.singlevalued;

import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.MetricConstants;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.metrics.FunctionFactory;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * The mean square error (MSE) measures the accuracy of a single-valued predictand. It comprises the average square
 * difference between the predictand and verifying observation. Optionally, the MSE may be factored into two-component
 * or three-component decompositions.
 *
 * @author James Brown
 */
public class MeanSquareError extends SumOfSquareError
{
    /** Basic description of the metric. */
    public static final DoubleScoreMetric BASIC_METRIC = DoubleScoreMetric.newBuilder()
                                                                          .setName( MetricName.MEAN_SQUARE_ERROR )
                                                                          .build();

    /** Main score component. */
    public static final DoubleScoreMetricComponent MAIN =
            DoubleScoreMetricComponent.newBuilder()
                                      .setMinimum( MetricConstants.MEAN_SQUARE_ERROR.getMinimum() )
                                      .setMaximum( MetricConstants.MEAN_SQUARE_ERROR.getMaximum() )
                                      .setOptimum( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE.getOptimum() )
                                      .setName( MetricName.MAIN )
                                      .build();

    /** Full description of the metric. */
    public static final DoubleScoreMetric METRIC = DoubleScoreMetric.newBuilder()
                                                                    .addComponents( MeanSquareError.MAIN )
                                                                    .setName( MetricName.MEAN_SQUARE_ERROR )
                                                                    .build();

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( MeanSquareError.class );

    /**
     * Returns an instance.
     *
     * @return an instance
     */

    public static MeanSquareError of()
    {
        return new MeanSquareError();
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
        return MetricConstants.MEAN_SQUARE_ERROR;
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

        double mse = FunctionFactory.finiteOrMissing()
                                    .applyAsDouble( input / output.getStatistic().getSampleSize() );

        // Set the real-valued measurement units
        DoubleScoreMetricComponent.Builder metricCompBuilder = MeanSquareError.MAIN.toBuilder()
                                                                                   .setUnits( output.getPoolMetadata()
                                                                                                    .getMeasurementUnit()
                                                                                                    .toString() );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( metricCompBuilder )
                                                                               .setValue( mse )
                                                                               .build();

        DoubleScoreStatistic score =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( MeanSquareError.BASIC_METRIC )
                                    .addStatistics( component )
                                    .build();

        return DoubleScoreStatisticOuter.of( score, output.getPoolMetadata() );
    }

    /**
     * Hidden constructor.
     */

    MeanSquareError()
    {
        super();
    }
}
