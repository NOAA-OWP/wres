package wres.metrics;

import java.util.Objects;

import wres.datamodel.metrics.MetricConstants.MetricGroup;
import wres.datamodel.pools.Pool;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;

/**
 * A generic implementation of an error score that is decomposable.
 * 
 * @author James Brown
 */

public abstract class DecomposableScore<S extends Pool<?>> implements Score<S, DoubleScoreStatisticOuter>
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

    @Override
    public String toString()
    {
        return this.getMetricName()
                   .toString();
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

    protected DecomposableScore( MetricGroup decompositionId )
    {
        super();

        if ( Objects.isNull( decompositionId ) )
        {
            throw new MetricParameterException( "Specify a non-null decomposition identifier." );
        }
        
        this.decompositionId = decompositionId;
        
    }

}
