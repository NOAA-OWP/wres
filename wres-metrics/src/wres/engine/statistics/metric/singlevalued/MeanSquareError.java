package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreGroup;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.pairs.SingleValuedPairs;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricParameterException;

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
     * Returns an instance.
     * 
     * @return an instance
     */

    public static MeanSquareError of()
    {
        return new MeanSquareError();
    }

    @Override
    public DoubleScoreStatistic apply( final SingleValuedPairs s )
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
    public MetricConstants getID()
    {
        return MetricConstants.MEAN_SQUARE_ERROR;
    }

    @Override
    public DoubleScoreStatistic aggregate( DoubleScoreStatistic output )
    {
        if ( Objects.isNull( output ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }

        StatisticMetadata meta = StatisticMetadata.of( output.getMetadata().getSampleMetadata(),
                                                       this.getID(),
                                                       MetricConstants.MAIN,
                                                       this.hasRealUnits(),
                                                       output.getMetadata().getSampleSize(),
                                                       null );

        double mse = FunctionFactory.finiteOrMissing()
                                    .applyAsDouble( output.getData() / output.getMetadata().getSampleSize() );

        return DoubleScoreStatistic.of( mse, meta );
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

    MeanSquareError( ScoreGroup decompositionId ) throws MetricParameterException
    {
        super( decompositionId );
    }

}
