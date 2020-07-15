package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
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
 * @author james.brown@hydrosolved.com
 */
public class CoefficientOfDetermination extends CorrelationPearsons
{

    /**
     * Canonical description of the metric.
     */

    public static final DoubleScoreMetric METRIC =
            DoubleScoreMetric.newBuilder()
                             .addComponents( DoubleScoreMetricComponent.newBuilder()
                                                                       .setMinimum( 0 )
                                                                       .setMaximum( 1 )
                                                                       .setOptimum( 1 )
                                                                       .setName( ComponentName.MAIN ) )
                             .setName( MetricName.COEFFICIENT_OF_DETERMINATION )
                             .build();

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
    public DoubleScoreStatisticOuter apply( SampleData<Pair<Double, Double>> s )
    {
        return aggregate( getInputForAggregation( s ) );
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.COEFFICIENT_OF_DETERMINATION;
    }

    @Override
    public DoubleScoreStatisticOuter aggregate( DoubleScoreStatisticOuter output )
    {
        if ( Objects.isNull( output ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }

        double input = output.getComponent( MetricConstants.MAIN )
                             .getData()
                             .getValue();

        double result = Math.pow( input, 2 );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setName( ComponentName.MAIN )
                                                                               .setValue( result )
                                                                               .build();

        DoubleScoreStatistic score =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( CoefficientOfDetermination.METRIC )
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
        return super.apply( input );
    }

    @Override
    public MetricConstants getCollectionOf()
    {
        return MetricConstants.PEARSON_CORRELATION_COEFFICIENT;
    }

    /**
     * Hidden constructor.
     */

    private CoefficientOfDetermination()
    {
        super();
    }

}
