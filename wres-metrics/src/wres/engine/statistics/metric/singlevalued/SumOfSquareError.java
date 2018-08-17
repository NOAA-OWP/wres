package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MissingValues;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.engine.statistics.metric.Collectable;
import wres.engine.statistics.metric.DecomposableScore;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * Base class for decomposable scores that involve a sum-of-square errors.
 * 
 * @author james.brown@hydrosolved.com
 */
public class SumOfSquareError extends DecomposableScore<SingleValuedPairs>
        implements Collectable<SingleValuedPairs, DoubleScoreOutput, DoubleScoreOutput>
{

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
    public DoubleScoreOutput apply( SingleValuedPairs s )
    {
        return this.aggregate( this.getInputForAggregation( s ) );
    }

    @Override
    public boolean hasRealUnits()
    {
        return true;
    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.SUM_OF_SQUARE_ERROR;
    }

    @Override
    public DoubleScoreOutput getInputForAggregation( SingleValuedPairs input )
    {
        if ( Objects.isNull( input ) )
        {
            throw new MetricInputException( "Specify non-null input to the '" + this + "'." );
        }

        double returnMe = MissingValues.MISSING_DOUBLE;

        // Data available
        if ( !input.getRawData().isEmpty() )
        {
            returnMe = input.getRawData().stream().mapToDouble( FunctionFactory.squareError() ).sum();
        }

        //Metadata
        MetricOutputMetadata metOut = MetricOutputMetadata.of( input.getMetadata(),
                                                               MetricConstants.SUM_OF_SQUARE_ERROR,
                                                               MetricConstants.MAIN,
                                                               this.hasRealUnits(),
                                                               input.getRawData().size(),
                                                               null );

        return DoubleScoreOutput.of( returnMe, metOut );
    }

    @Override
    public DoubleScoreOutput aggregate( DoubleScoreOutput output )
    {
        if ( Objects.isNull( output ) )
        {
            throw new MetricInputException( "Specify non-null input to the '" + this + "'." );
        }

        final MetricOutputMetadata metIn = output.getMetadata();

        // Set the output dimension
        MetricOutputMetadata meta = MetricOutputMetadata.of( metIn,
                                                             this.getID(),
                                                             MetricConstants.MAIN,
                                                             this.hasRealUnits(),
                                                             metIn.getSampleSize(),
                                                             null );

        return DoubleScoreOutput.of( output.getData(), meta );
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

    /**
     * Hidden constructor.
     * 
     * @param decompositionId the decomposition identifier
     * @throws MetricParameterException if one or more parameters is invalid 
     */

    SumOfSquareError( ScoreOutputGroup decompositionId ) throws MetricParameterException
    {
        super( decompositionId );
    }

}
