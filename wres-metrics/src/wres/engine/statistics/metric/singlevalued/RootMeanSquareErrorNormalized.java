package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;
import java.util.function.ToDoubleFunction;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants;
import wres.datamodel.MissingValues;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.pools.SampleData;
import wres.datamodel.pools.SampleDataException;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.engine.statistics.metric.FunctionFactory;
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
 * @author james.brown@hydrosolved.com
 */
public class RootMeanSquareErrorNormalized extends DoubleErrorScore<SampleData<Pair<Double, Double>>>
{

    /**
     * Basic description of the metric.
     */

    public static final DoubleScoreMetric BASIC_METRIC = DoubleScoreMetric.newBuilder()
                                                                          .setName( MetricName.ROOT_MEAN_SQUARE_ERROR_NORMALIZED )
                                                                          .build();

    /**
     * Main score component.
     */

    public static final DoubleScoreMetricComponent MAIN = DoubleScoreMetricComponent.newBuilder()
                                                                                    .setMinimum( 0 )
                                                                                    .setMaximum( Double.POSITIVE_INFINITY )
                                                                                    .setOptimum( 0 )
                                                                                    .setName( ComponentName.MAIN )
                                                                                    .setUnits( MeasurementUnit.DIMENSIONLESS )
                                                                                    .build();

    /**
     * Full description of the metric.
     */

    public static final DoubleScoreMetric METRIC = DoubleScoreMetric.newBuilder()
                                                                    .addComponents( RootMeanSquareErrorNormalized.MAIN )
                                                                    .setName( MetricName.ROOT_MEAN_SQUARE_ERROR_NORMALIZED )
                                                                    .build();

    /**
     * Instance of a standard deviation.
     */

    private final ToDoubleFunction<VectorOfDoubles> stdev;

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
    public DoubleScoreStatisticOuter apply( final SampleData<Pair<Double, Double>> t )
    {
        if ( Objects.isNull( t ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }

        double returnMe = MissingValues.DOUBLE;

        // Data available
        if ( !t.getRawData().isEmpty() )
        {

            double mse = super.apply( t ).getComponent( MetricConstants.MAIN )
                                         .getData()
                                         .getValue();

            //Compute the observation standard deviation
            double[] obs = t.getRawData()
                            .stream()
                            .mapToDouble( Pair::getLeft )
                            .toArray();

            double stdevValue = this.stdev.applyAsDouble( VectorOfDoubles.of( obs ) );

            returnMe = Math.sqrt( mse ) / stdevValue;
        }

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( RootMeanSquareErrorNormalized.MAIN )
                                                                               .setValue( returnMe )
                                                                               .build();

        DoubleScoreStatistic score =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( RootMeanSquareErrorNormalized.BASIC_METRIC )
                                    .addStatistics( component )
                                    .build();

        return DoubleScoreStatisticOuter.of( score, t.getMetadata() );
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

    /**
     * Hidden constructor.
     */

    private RootMeanSquareErrorNormalized()
    {
        super( FunctionFactory.squareError(), FunctionFactory.mean(), RootMeanSquareErrorNormalized.METRIC );

        stdev = FunctionFactory.standardDeviation();
    }

}
