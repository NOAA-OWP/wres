package wres.metrics.singlevalued;

import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.MissingValues;
import wres.config.MetricConstants;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.metrics.Collectable;
import wres.metrics.DecomposableScore;
import wres.metrics.FunctionFactory;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Base class for decomposable scores that involve a sum-of-square errors.
 * 
 * @author James Brown
 */
public class SumOfSquareError extends DecomposableScore<Pool<Pair<Double, Double>>>
        implements Collectable<Pool<Pair<Double, Double>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter>
{

    /** Basic description of the metric. */
    public static final DoubleScoreMetric BASIC_METRIC = DoubleScoreMetric.newBuilder()
                                                                          .setName( MetricName.SUM_OF_SQUARE_ERROR )
                                                                          .build();

    /** Main score component.*/
    public static final DoubleScoreMetricComponent MAIN = DoubleScoreMetricComponent.newBuilder()
                                                                                    .setMinimum( 0 )
                                                                                    .setMaximum( Double.POSITIVE_INFINITY )
                                                                                    .setOptimum( 0 )
                                                                                    .setName( ComponentName.MAIN )
                                                                                    .build();

    /** Full description of the metric.*/
    public static final DoubleScoreMetric METRIC = DoubleScoreMetric.newBuilder()
                                                                    .addComponents( SumOfSquareError.MAIN )
                                                                    .setName( MetricName.SUM_OF_SQUARE_ERROR )
                                                                    .build();
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( SumOfSquareError.class );

    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static SumOfSquareError of()
    {
        return new SumOfSquareError();
    }

    @Override
    public DoubleScoreStatisticOuter apply( Pool<Pair<Double, Double>> s )
    {
        LOGGER.debug( "Computing the {}.", this );

        return this.aggregate( this.getIntermediateStatistic( s ), s );
    }

    @Override
    public boolean hasRealUnits()
    {
        return true;
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.SUM_OF_SQUARE_ERROR;
    }

    @Override
    public DoubleScoreStatisticOuter getIntermediateStatistic( Pool<Pair<Double, Double>> input )
    {
        LOGGER.debug( "Computing the {}, which may be used as an intermediate statistic for other statistics.",
                      MetricConstants.SUM_OF_SQUARE_ERROR );

        if ( Objects.isNull( input ) )
        {
            throw new PoolException( "Specify non-null input to the '" + this + "'." );
        }

        double returnMe = MissingValues.DOUBLE;

        // Data available
        if ( !input.get().isEmpty() )
        {
            // Sort the stream, as this improves accuracy according to the API docs
            // See #71343
            returnMe = input.get()
                            .stream()
                            .mapToDouble( FunctionFactory.squareError() )
                            .sorted()
                            .sum();
        }

        // Set the real-valued measurement units
        DoubleScoreMetricComponent.Builder metricCompBuilder = SumOfSquareError.MAIN.toBuilder()
                                                                                    .setUnits( input.getMetadata()
                                                                                                    .getMeasurementUnit()
                                                                                                    .toString() );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( metricCompBuilder )
                                                                               .setValue( returnMe )
                                                                               .build();

        DoubleScoreStatistic score =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( SumOfSquareError.BASIC_METRIC )
                                    .addStatistics( component )
                                    .setSampleSize( input.get().size() )
                                    .build();

        return DoubleScoreStatisticOuter.of( score, input.getMetadata() );
    }

    @Override
    public DoubleScoreStatisticOuter aggregate( DoubleScoreStatisticOuter output, Pool<Pair<Double, Double>> pool )
    {
        if ( Objects.isNull( output ) )
        {
            throw new PoolException( "Specify non-null input to the '" + this + "'." );
        }

        return DoubleScoreStatisticOuter.of( output.getData(), output.getMetadata() );
    }

    @Override
    public MetricConstants getCollectionOf()
    {
        return MetricConstants.SUM_OF_SQUARE_ERROR;
    }

    /**
     * Hidden constructor.
     */

    SumOfSquareError()
    {
        super();
    }
}
