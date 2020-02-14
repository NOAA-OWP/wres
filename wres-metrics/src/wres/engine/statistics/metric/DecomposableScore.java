package wres.engine.statistics.metric;

import java.util.Objects;

import wres.datamodel.MetricConstants.MetricGroup;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.statistics.DoubleScoreStatistic;

/**
 * A generic implementation of an error score that is decomposable.
 * 
 * @author james.brown@hydrosolved.com
 */

public abstract class DecomposableScore<S extends SampleData<?>> extends OrdinaryScore<S, DoubleScoreStatistic>
{

    /**
     * The decomposition identifier.
     */

    private final MetricGroup decompositionId;

    @Override
    public boolean isDecomposable()
    {
        return true;
    }

    @Override
    public MetricGroup getScoreOutputGroup()
    {
        return decompositionId;
    }

    /**
     * Hidden constructor for a score with no decomposition, i.e. {@link MetricGroup#NONE}.
     */

    protected DecomposableScore()
    {
        super();
        
        this.decompositionId = MetricGroup.NONE;        
    }
    
    /**
     * Hidden constructor.
     * 
     * @param decompositionId the decomposition identifier
     * @throws MetricParameterException if one or more parameters is invalid
     */

    protected DecomposableScore( MetricGroup decompositionId ) throws MetricParameterException
    {
        super();

        if ( Objects.isNull( decompositionId ) )
        {
            throw new MetricParameterException( "Specify a non-null decomposition identifier." );
        }
        
        this.decompositionId = decompositionId;
        
    }

}
