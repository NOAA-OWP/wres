package wres.pipeline;

import java.util.Objects;

import wres.datamodel.pools.PoolRequest;

/**
 * Some metadata about the statistics associated with a pool.
 * 
 * @author James Brown
 */

class PoolProcessingResult
{
    /** The pool description. */
    private final PoolRequest poolRequest;

    /** Is <code>true</code> if statistics were produced for the pool, otherwise <code>false</code>. */
    private final boolean hasStatistics;

    /**
     * @param poolrequest the pool description
     * @param hasStatistics is true if the group produced statistics
     */
    PoolProcessingResult( PoolRequest poolRequest,
                          boolean hasStatistics )
    {
        Objects.requireNonNull( poolRequest );
        this.hasStatistics = hasStatistics;
        this.poolRequest = poolRequest;
    }

    /**
     * Returns <code>true</code> if statistics were produced for the pool, otherwise <code>false</code>.
     * 
     * @return true if statistics were produced, false if no statistics were produced
     */

    boolean hasStatistics()
    {
        return this.hasStatistics;
    }
    
    /**
     * @return the pool request
     */
    
    PoolRequest getPoolRequest()
    {
        return this.poolRequest;
    }

    @Override
    public String toString()
    {
        return "Pool "
               + this.poolRequest
               + " produced statistics: "
               + this.hasStatistics()
               + ".";
    }

}
