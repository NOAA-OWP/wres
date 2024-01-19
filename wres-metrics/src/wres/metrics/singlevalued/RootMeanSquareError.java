package wres.metrics.singlevalued;

import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.MetricConstants;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.metrics.Collectable;
import wres.metrics.FunctionFactory;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * As with the MSE, the Root Mean Square Error (RMSE) or Root Mean Square Deviation (RMSD) is a measure of accuracy.
 * However, the RMSE is expressed in the original (unsquared) units of the predictand and no decompositions are
 * available for the RMSE.
 *
 * @author James Brown
 */
public class RootMeanSquareError extends DoubleErrorScore<Pool<Pair<Double, Double>>>
        implements Collectable<Pool<Pair<Double, Double>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter>
{

    /** Basic description of the metric. */
    public static final DoubleScoreMetric BASIC_METRIC = DoubleScoreMetric.newBuilder()
                                                                          .setName( MetricName.ROOT_MEAN_SQUARE_ERROR )
                                                                          .build();

    /** Main score component. */
    public static final DoubleScoreMetricComponent MAIN =
            DoubleScoreMetricComponent.newBuilder()
                                      .setMinimum( MetricConstants.ROOT_MEAN_SQUARE_ERROR.getMinimum() )
                                      .setMaximum( MetricConstants.ROOT_MEAN_SQUARE_ERROR.getMaximum() )
                                      .setOptimum( MetricConstants.ROOT_MEAN_SQUARE_ERROR.getOptimum() )
                                      .setName( ComponentName.MAIN )
                                      .build();

    /** Full description of the metric. */
    public static final DoubleScoreMetric METRIC_INNER = DoubleScoreMetric.newBuilder()
                                                                          .addComponents( RootMeanSquareError.MAIN )
                                                                          .setName( MetricName.ROOT_MEAN_SQUARE_ERROR )
                                                                          .build();

    /** Instance of {@link SumOfSquareError}. */
    private final SumOfSquareError sse;

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( RootMeanSquareError.class );

    /**
     * Returns an instance.
     *
     * @return an instance
     */

    public static RootMeanSquareError of()
    {
        return new RootMeanSquareError();
    }

    @Override
    public DoubleScoreStatisticOuter apply( final Pool<Pair<Double, Double>> pool )
    {
        LOGGER.debug( "Computing the {}.", this );

        return this.applyIntermediate( this.getIntermediate( pool ), pool );
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.ROOT_MEAN_SQUARE_ERROR;
    }

    @Override
    public boolean hasRealUnits()
    {
        return true;
    }

    @Override
    public DoubleScoreStatisticOuter applyIntermediate( DoubleScoreStatisticOuter output,
                                                        Pool<Pair<Double, Double>> pool )
    {
        LOGGER.debug( "Computing the {} from the intermediate statistic, {}.", this, this.getCollectionOf() );

        double input = output.getComponent( MetricConstants.MAIN )
                             .getStatistic()
                             .getValue();

        double sampleSize = output.getStatistic().getSampleSize();

        double result = Math.sqrt( input / sampleSize );

        // Set the real-valued measurement units
        DoubleScoreMetricComponent.Builder metricCompBuilder = RootMeanSquareError.MAIN.toBuilder()
                                                                                       .setUnits( output.getPoolMetadata()
                                                                                                        .getMeasurementUnit()
                                                                                                        .toString() );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( metricCompBuilder )
                                                                               .setValue( result )
                                                                               .build();

        DoubleScoreStatistic score =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( RootMeanSquareError.BASIC_METRIC )
                                    .addStatistics( component )
                                    .build();

        return DoubleScoreStatisticOuter.of( score, output.getPoolMetadata() );
    }

    @Override
    public DoubleScoreStatisticOuter getIntermediate( Pool<Pair<Double, Double>> input )
    {
        LOGGER.debug( "Computing an intermediate statistic of {} for the {}.", this.getCollectionOf(), this );

        if ( Objects.isNull( input ) )
        {
            throw new PoolException( "Specify non-null input to the '" + this + "'." );
        }

        return this.sse.apply( input );
    }

    @Override
    public MetricConstants getCollectionOf()
    {
        return MetricConstants.SUM_OF_SQUARE_ERROR;
    }

    /**
     * Constructor.
     */

    RootMeanSquareError()
    {
        super( FunctionFactory.squareError(), RootMeanSquareError.METRIC_INNER );

        this.sse = SumOfSquareError.of();
    }

}
