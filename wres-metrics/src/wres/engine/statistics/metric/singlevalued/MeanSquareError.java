package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricGroup;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricParameterException;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * The mean square error (MSE) measures the accuracy of a single-valued predictand. It comprises the average square
 * difference between the predictand and verifying observation. Optionally, the MSE may be factored into two-component
 * or three-component decompositions.
 * 
 * @author james.brown@hydrosolved.com
 */
public class MeanSquareError extends SumOfSquareError
{

    /**
     * Canonical description of the metric.
     */

    public static final DoubleScoreMetric METRIC =
            DoubleScoreMetric.newBuilder()
                             .addComponents( DoubleScoreMetricComponent.newBuilder()
                                                                       .setMinimum( 0 )
                                                                       .setMaximum( Double.POSITIVE_INFINITY )
                                                                       .setOptimum( 0 )
                                                                       .setName( ComponentName.MAIN ) )
                             .setName( MetricName.MEAN_SQUARE_ERROR )
                             .build();

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
    public DoubleScoreStatisticOuter apply( SampleData<Pair<Double, Double>> s )
    {
        switch ( this.getScoreOutputGroup() )
        {
            case NONE:
                return this.aggregate( this.getInputForAggregation( s ) );
            case CR:
            case LBR:
            case CR_AND_LBR:
            default:
                throw new MetricCalculationException( "Decomposition is not currently implemented for the '" + this
                                                      + "'." );
        }
    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.MEAN_SQUARE_ERROR;
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

        double mse = FunctionFactory.finiteOrMissing()
                                    .applyAsDouble( input / output.getData().getSampleSize() );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setName( ComponentName.MAIN )
                                                                               .setValue( mse )
                                                                               .build();

        DoubleScoreStatistic score =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( MeanSquareError.METRIC )
                                    .addStatistics( component )
                                    .build();

        return DoubleScoreStatisticOuter.of( score, output.getMetadata() );
    }

    /**
     * Hidden constructor.
     */

    MeanSquareError()
    {
        super();
    }

    /**
     * Hidden constructor.
     * 
     * @param decompositionId the decomposition identifier
     * @throws MetricParameterException if one or more parameters is invalid 
     */

    MeanSquareError( MetricGroup decompositionId ) throws MetricParameterException
    {
        super( decompositionId );
    }

}
