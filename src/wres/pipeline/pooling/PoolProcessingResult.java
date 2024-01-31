package wres.pipeline.pooling;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import wres.datamodel.messages.EvaluationStatusMessage;
import wres.datamodel.pools.PoolRequest;

/**
 * Some metadata about the statistics associated with a pool.
 * 
 * @author James Brown
 */

public class PoolProcessingResult
{
    enum Status
    {
        /** Nominal situation. */
        STATISTICS_PUBLISHED,
        /** Exceptional situation where an evaluation has failed and some statistics were created and not published. */
        STATISTICS_AVAILABLE_NOT_PUBLISHED_ERROR_STATE,
        /** No data situation. */
        STATISTICS_NOT_AVAILABLE,
        /** Publication of statistics skipped because they are only required for an intermediary step, such as summary
         * statistics. */
        STATISTICS_PUBLICATION_SKIPPED
    }
    
    /** The pool description. */
    private final PoolRequest poolRequest;

    /** Pool status. */
    private final Status status;
    
    /** Status events encountered when creating the pool. */
    private final List<EvaluationStatusMessage> statusEvents;

    /**
     * @param poolRequest the pool description
     * @param status the status the status
     * @param statusEvents the evaluation status events encountered while creating the pool, if any
     * @throws NullPointerException if any input is null
     */
    PoolProcessingResult( PoolRequest poolRequest,
                          Status status,
                          List<EvaluationStatusMessage> statusEvents )
    {
        Objects.requireNonNull( poolRequest );
        Objects.requireNonNull( statusEvents );
        Objects.requireNonNull( status );
        
        this.status = status;
        this.poolRequest = poolRequest;
        this.statusEvents = Collections.unmodifiableList( statusEvents );
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
    
    /**
     * @return the evaluation status events
     */
    
    List<EvaluationStatusMessage> getEvaluationStatusEvents()
    {
        return this.statusEvents;
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
