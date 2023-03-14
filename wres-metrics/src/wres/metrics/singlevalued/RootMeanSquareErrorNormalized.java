package wres.metrics.singlevalued;

import java.util.Objects;
import java.util.function.ToDoubleFunction;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.MissingValues;
import wres.datamodel.VectorOfDoubles;
import wres.config.MetricConstants;
import wres.datamodel.pools.MeasurementUnit;
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
 * The Root Mean Square Error (RMSE) normalized by the standard deviation of the observations (SDO), also known as
 * the RMSE Standard Deviation Ratio (RSR): RSR = RMSE / SDO.
 * 
 * @author James Brown
 */
public class RootMeanSquareErrorNormalized extends DoubleErrorScore<Pool<Pair<Double, Double>>>
        implements Collectable<Pool<Pair<Double, Double>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter>
{

    /** Basic description of the metric. */
    public static final DoubleScoreMetric BASIC_METRIC = DoubleScoreMetric.newBuilder()
                                                                          .setName( MetricName.ROOT_MEAN_SQUARE_ERROR_NORMALIZED )
                                                                          .build();

    /** Main score component. */
    public static final DoubleScoreMetricComponent MAIN = DoubleScoreMetricComponent.newBuilder()
                                                                                    .setMinimum( 0 )
                                                                                    .setMaximum( Double.POSITIVE_INFINITY )
                                                                                    .setOptimum( 0 )
                                                                                    .setName( ComponentName.MAIN )
                                                                                    .setUnits( MeasurementUnit.DIMENSIONLESS )
                                                                                    .build();

    /** Full description of the metric. */
    public static final DoubleScoreMetric METRIC_INNER = DoubleScoreMetric.newBuilder()
                                                                          .addComponents( RootMeanSquareErrorNormalized.MAIN )
                                                                          .setName( MetricName.ROOT_MEAN_SQUARE_ERROR_NORMALIZED )
                                                                          .build();

    /** Instance of a standard deviation. */
    private final ToDoubleFunction<VectorOfDoubles> stdev;

    /** Instance of {@link SumOfSquareError}. */
    private final SumOfSquareError sse;

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( RootMeanSquareErrorNormalized.class );

    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static RootMeanSquareErrorNormalized of()
    {
        return new RootMeanSquareErrorNormalized();
    }

    @Override
    public DoubleScoreStatisticOuter apply( final Pool<Pair<Double, Double>> t )
    {
        LOGGER.debug( "Computing the {}.", this );

        DoubleScoreStatisticOuter statistic = this.getIntermediateStatistic( t );
        return this.aggregate( statistic, t );
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.ROOT_MEAN_SQUARE_ERROR_NORMALIZED;
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    @Override
    public DoubleScoreStatisticOuter aggregate( DoubleScoreStatisticOuter statistic, Pool<Pair<Double, Double>> pool )
    {
        LOGGER.debug( "Computing the {} from the intermediate statistic, {}.", this, this.getCollectionOf() );

        double input = statistic.getComponent( MetricConstants.MAIN )
                                .getData()
                                .getValue();

        double sampleSize = statistic.getData()
                                     .getSampleSize();

        double rmse = Math.sqrt( input / sampleSize );

        double result = MissingValues.DOUBLE;

        if ( !pool.get().isEmpty() )
        {
            // Standard deviation of the left
            double[] sdLeft = pool.get()
                                  .stream()
                                  .mapToDouble( Pair::getLeft )
                                  .toArray();

            VectorOfDoubles sdVector = VectorOfDoubles.of( sdLeft );
            double stdevValue = this.stdev.applyAsDouble( sdVector );

            result = rmse / stdevValue;
        }

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( RootMeanSquareErrorNormalized.MAIN )
                                                                               .setValue( result )
                                                                               .build();

        DoubleScoreStatistic score =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( RootMeanSquareErrorNormalized.BASIC_METRIC )
                                    .addStatistics( component )
                                    .build();

        return DoubleScoreStatisticOuter.of( score, statistic.getMetadata() );
    }

    @Override
    public DoubleScoreStatisticOuter getIntermediateStatistic( Pool<Pair<Double, Double>> pool )
    {
        LOGGER.debug( "Computing an intermediate statistic of {} for the {}.", this.getCollectionOf(), this );

        if ( Objects.isNull( pool ) )
        {
            throw new PoolException( "Specify non-null input to the '" + this + "'." );
        }

        return this.sse.apply( pool );
    }

    @Override
    public MetricConstants getCollectionOf()
    {
        return MetricConstants.SUM_OF_SQUARE_ERROR;
    }

    /**
     * Hidden constructor.
     */

    private RootMeanSquareErrorNormalized()
    {
        super( FunctionFactory.squareError(), FunctionFactory.mean(), RootMeanSquareErrorNormalized.METRIC_INNER );

        this.stdev = FunctionFactory.standardDeviation();
        this.sse = SumOfSquareError.of();
    }

}
