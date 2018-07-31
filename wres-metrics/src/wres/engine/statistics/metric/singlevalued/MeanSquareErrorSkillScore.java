package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;

import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.Slicer;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.SingleValuedPair;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.DatasetIdentifier;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.engine.statistics.metric.DecomposableScore;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * The Mean Square Error (MSE) Skill Score (SS) measures the reduction in MSE associated with one set of predictions
 * when compared to another. The MSE-SS is equivalent to the Nash-Sutcliffe Efficiency. The perfect MSE-SS is 1.0.
 * 
 * @author james.brown@hydrosolved.com
 */
public class MeanSquareErrorSkillScore extends DecomposableScore<SingleValuedPairs>
{

    /**
     * Returns an instance.
     * 
     * @return an instance
     */
    
    public static MeanSquareErrorSkillScore of()
    {
        return new MeanSquareErrorSkillScore();
    }
    
    /**
     * Instance if {@link SumOfSquareError}.
     */

    private final SumOfSquareError sse;

    @Override
    public DoubleScoreOutput apply( final SingleValuedPairs s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new MetricInputException( "Specify non-null input to the '" + this + "'." );
        }

        if ( this.getScoreOutputGroup() != ScoreOutputGroup.NONE )
        {
            throw new MetricCalculationException( "Decomposition is not currently implemented for the '" + this
                                                  + "'." );
        }

        //TODO: implement any required decompositions, based on the instance parameters and return the decomposition
        //template as the componentID in the metadata

        double result = Double.NaN;

        // Some data, proceed
        if ( !s.getRawData().isEmpty() )
        {
            double numerator = sse.apply( s ).getData();
            double denominator = 0.0;
            if ( s.hasBaseline() )
            {
                denominator = sse.apply( s.getBaselineData() ).getData();
            }
            else
            {
                double meanLeft =
                        FunctionFactory.mean().applyAsDouble( VectorOfDoubles.of( Slicer.getLeftSide( s ) ) );
                for ( SingleValuedPair next : s.getRawData() )
                {
                    denominator += Math.pow( next.getLeft() - meanLeft, 2 );
                }
            }
            result = FunctionFactory.skill().applyAsDouble( numerator, denominator );
        }

        // Metadata
        DatasetIdentifier baselineIdentifier = null;
        if ( s.hasBaseline() )
        {
            baselineIdentifier = s.getMetadataForBaseline().getIdentifier();
        }
        final MetricOutputMetadata metOut =
                this.getMetadata( s, s.getRawData().size(), MetricConstants.MAIN, baselineIdentifier );
        return DoubleScoreOutput.of( result, metOut );
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE;
    }

    @Override
    public boolean isSkillScore()
    {
        return true;
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    /**
     * Hidden constructor.
     */

    private MeanSquareErrorSkillScore()
    {
        super();
        sse = SumOfSquareError.of();
    }
    
    /**
     * Hidden constructor.
     * 
     * @param decompositionId the decomposition identifier
     * @throws MetricParameterException if one or more parameters is invalid 
     */

    private MeanSquareErrorSkillScore( ScoreOutputGroup decompositionId ) throws MetricParameterException
    {
        super( decompositionId );
        sse = SumOfSquareError.of();
    }

}
