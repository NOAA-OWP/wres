package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DoubleScoreOutput;
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
    public DoubleScoreOutput apply( final SingleValuedPairs s )
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
    public DoubleScoreOutput aggregate( DoubleScoreOutput output )
    {
        if ( Objects.isNull( output ) )
        {
            throw new MetricInputException( "Specify non-null input to the '" + this + "'." );
        }

        final MetricOutputMetadata metIn = output.getMetadata();

        MetricOutputMetadata meta = MetricOutputMetadata.of( metIn,
                                                             this.getID(),
                                                             MetricConstants.MAIN,
                                                             this.hasRealUnits(),
                                                             metIn.getSampleSize(),
                                                             null );

        double mse = FunctionFactory.finiteOrMissing()
                                    .applyAsDouble( output.getData() / metIn.getSampleSize() );

        return DoubleScoreOutput.of( mse, meta );
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

    MeanSquareError( ScoreOutputGroup decompositionId ) throws MetricParameterException
    {
        super( decompositionId );
    }

}
