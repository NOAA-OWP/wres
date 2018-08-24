package wres.engine.statistics.metric;

import java.util.Objects;

import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.sampledata.MetricInput;
import wres.datamodel.statistics.DoubleScoreOutput;

/**
 * A generic implementation of an error score that is decomposable.
 * 
 * @author james.brown@hydrosolved.com
 */

public abstract class DecomposableScore<S extends MetricInput<?>> extends OrdinaryScore<S, DoubleScoreOutput>
{

    /**
     * The decomposition identifier.
     */

    private final ScoreOutputGroup decompositionId;

    @Override
    public boolean isDecomposable()
    {
        return true;
    }

    @Override
    public ScoreOutputGroup getScoreOutputGroup()
    {
        return decompositionId;
    }

    /**
     * Hidden constructor for a score with no decomposition, i.e. {@link ScoreOutputGroup#NONE}.
     */

    protected DecomposableScore()
    {
        super();
        
        this.decompositionId = ScoreOutputGroup.NONE;        
    }
    
    /**
     * Hidden constructor.
     * 
     * @param decompositionId the decomposition identifier
     * @throws MetricParameterException if one or more parameters is invalid
     */

    protected DecomposableScore( ScoreOutputGroup decompositionId ) throws MetricParameterException
    {
        super();

        if ( Objects.isNull( decompositionId ) )
        {
            throw new MetricParameterException( "Specify a non-null decomposition identifier." );
        }
        
        this.decompositionId = decompositionId;
        
    }

}
