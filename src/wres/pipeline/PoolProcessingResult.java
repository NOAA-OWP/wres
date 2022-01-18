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
    enum Status
    {
        /** Nominal situation. */
        STATISTICS_PUBLISHED,
        /** Exceptional situation where an evaluation has failed and some statistics were created and not published. */
        STATISTICS_AVAILABLE_NOT_PUBLISHED,
        /** No data situation. */
        STATISTICS_NOT_AVAILABLE;
    }
    
    /** The pool description. */
    private final PoolRequest poolRequest;

    /** Pool status. */
    private final Status status;

    /**
     * @param poolRequest the pool description
     * @param hasStatistics is true if statistics were published
     */
    PoolProcessingResult( PoolRequest poolRequest,
                          Status status )
    {
        Objects.requireNonNull( poolRequest );
        this.status = status;
        this.poolRequest = poolRequest;
    }

    /**
     * @return the status of the pool that completed
     */

    Status getStatus()
    {
        return this.status;
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
               + " has status: "
               + this.getStatus()
               + ".";
    }

}
