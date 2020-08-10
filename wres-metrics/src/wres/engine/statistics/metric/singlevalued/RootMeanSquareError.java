package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.engine.statistics.metric.Collectable;
import wres.engine.statistics.metric.FunctionFactory;
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
 * @author james.brown@hydrosolved.com
 */
public class RootMeanSquareError extends DoubleErrorScore<SampleData<Pair<Double, Double>>>
        implements Collectable<SampleData<Pair<Double, Double>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter>
{

    /**
     * Basic description of the metric.
     */

    public static final DoubleScoreMetric BASIC_METRIC = DoubleScoreMetric.newBuilder()
                                                                          .setName( MetricName.ROOT_MEAN_SQUARE_ERROR )
                                                                          .build();

    /**
     * Main score component.
     */

    public static final DoubleScoreMetricComponent MAIN = DoubleScoreMetricComponent.newBuilder()
                                                                                    .setMinimum( 0 )
                                                                                    .setMaximum( Double.POSITIVE_INFINITY )
                                                                                    .setOptimum( 0 )
                                                                                    .setName( ComponentName.MAIN )
                                                                                    .build();

    /**
     * Full description of the metric.
     */

    public static final DoubleScoreMetric METRIC = DoubleScoreMetric.newBuilder()
                                                                    .addComponents( RootMeanSquareError.MAIN )
                                                                    .setName( MetricName.ROOT_MEAN_SQUARE_ERROR )
                                                                    .build();

    /**
     * Instance of {@link SumOfSquareError}.
     */

    private final SumOfSquareError sse;

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
    public DoubleScoreStatisticOuter apply( final SampleData<Pair<Double, Double>> t )
    {
        return this.aggregate( this.getInputForAggregation( t ) );
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
    public DoubleScoreStatisticOuter aggregate( DoubleScoreStatisticOuter output )
    {
        double input = output.getComponent( MetricConstants.MAIN )
                             .getData()
                             .getValue();

        double sampleSize = output.getData().getSampleSize();

        double result = Math.sqrt( input / sampleSize );

        // Set the real-valued measurement units
        DoubleScoreMetricComponent.Builder metricCompBuilder = RootMeanSquareError.MAIN.toBuilder()
                                                                                       .setUnits( output.getMetadata()
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

        return DoubleScoreStatisticOuter.of( score, output.getMetadata() );
    }

    @Override
    public DoubleScoreStatisticOuter getInputForAggregation( SampleData<Pair<Double, Double>> input )
    {
        if ( Objects.isNull( input ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }

        return sse.apply( input );
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
        super( FunctionFactory.squareError(), RootMeanSquareError.METRIC );

        this.sse = SumOfSquareError.of();
    }

}
