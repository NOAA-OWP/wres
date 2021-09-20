package wres.control;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.tuple.Pair;

import jdk.jfr.Category;
import jdk.jfr.DataAmount;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolSlicer;
import wres.datamodel.time.TimeSeries;

/**
 * A custom event for monitoring and exposing an evaluation to the Java Flight Recorder.
 * 
 * @author James Brown
 */

@Name( "wres.control.EvaluationEvent" )
@Label( "Evaluation Event" )
@Category( { "Java Application", "Water Resources Evaluation Service", "Core" } )
class EvaluationEvent extends Event
{
    @Label( "Evaluation Identifier" )
    @Description( "The unique identifier of the evaluation." )
    private String evaluationId;

    @Label( "Data Hash" )
    @Description( "The MD5 checksum or top hash of the time-series data, including all left, right and baseline "
                  + "datasets." )
    private String hash;

    @Label( "Pool Count" )
    @Description( "The number of pools in the evaluation." )
    private int poolCount;

    @Label( "Time-Series Count" )
    @Description( "The number of time-series in the evaluation, including all (left, right and baseline) data sources. "
                  + "Each time-series may contain one or more traces." )
    private long seriesCount;

    @Label( "Time-Series Trace Count" )
    @Description( "The number of time-series traces in the evaluation, including all (left, right and baseline) data "
                  + "sources. A time-series of ensemble traces contains up to N traces, whereas a single-valued time-"
                  + "series always contains one trace." )
    private long traceCount;

    @Label( "Pair Count" )
    @Description( "The number of paired values in the evaluation." )
    private long pairCount;

    @Label( "Time-Series Value Count" )
    @Description( "The number of individual time-series values in the evaluation, including all (left, right and "
                  + "baseline) data sources. Each value is represented as a 64-bit float." )
    private long valueCount;

    @Label( "Resource Count" )
    @Description( "The number of resources created." )
    private int resourceCount;

    @Label( "Resource Size (Estimated)" )
    @DataAmount( DataAmount.BYTES )
    @Description( "The estimated size in storage of all resources created by this evaluation." )
    private long resourceBytes;

    @Label( "Succeeded" )
    @Description( "Is true if the evaluation succeeded, false if the evaluation failed." )
    private boolean succeeded;

    /** Internal/incremental state of {@link #seriesCount} for left/right data during an evaluation. */
    private final AtomicLong seriesCountRightInternal;

    /** Internal/incremental state of {@link #pairCount} for left/right data during an evaluation. */
    private final AtomicLong pairCountRightInternal;

    /** Internal/incremental state of {@link #traceCount} for left/right data during an evaluation. */
    private final AtomicLong traceCountRightInternal;

    /** Internal/incremental state of {@link #seriesCount} for left/baseline data during an evaluation. */
    private final AtomicLong seriesCountBaselineInternal;

    /** Internal/incremental state of {@link #pairCount} for left/baseline data during an evaluation. */
    private final AtomicLong pairCountBaselineInternal;

    /** Internal/incremental state of {@link #traceCount} for left/baseline data during an evaluation. */
    private final AtomicLong traceCountBaselineInternal;

    /**
     * @return an instance
     */

    static EvaluationEvent of()
    {
        return new EvaluationEvent();
    }

    /**
     * Sets the evaluation identifier.
     * @param evaluationId, the evaulationId
     */

    void setEvaluationId( String evaluationId )
    {
        this.evaluationId = evaluationId;
    }

    /**
     * Flags that the evaluation succeeded.
     */
    void setSucceeded()
    {
        this.succeeded = true;

        // Register incremental state
        this.copyStateOnCompletion();
    }

    /**
     * Sets an error message on failure.
     * @param error, the error
     */

    void setFailed()
    {
        this.succeeded = false;

        // Register incremental state
        this.copyStateOnCompletion();
    }

    /**
     * Sets the hash of the evaluation data.
     * @param hash, the data hash
     */

    void setDataHash( String hash )
    {
        this.hash = hash;
    }

    /**
     * Sets information about the resources created.
     * 
     * @param resources the resources
     */

    void setResources( Set<Path> resources )
    {
        if ( Objects.nonNull( resources ) )
        {
            this.resourceCount = resources.size();
            long resourceBytesLocal = 0;

            for ( Path resource : resources )
            {
                try
                {
                    resourceBytesLocal += Files.size( resource );
                }
                catch ( IOException e )
                {
                    // Do nothing.
                }
            }

            this.resourceBytes = resourceBytesLocal;
        }
    }

    /**
     * Set the pool count.
     * @param poolCount the pool count
     */
    void setPoolCount( int poolCount )
    {
        this.poolCount = poolCount;
    }

    /**
     * Registers a new pool with the evaluation and increments related statistics. 
     * 
     * @param pool a pool
     * @param traceCount an estimate of the number of traces in the pool for left/right data
     * @param traceCountBaseline an estimate of the number of traces in the pool for left/baseline data
     */

    <L, R> void registerPool( Pool<TimeSeries<Pair<L, R>>> pool, long traceCount, long traceCountBaseline )
    {
        if ( Objects.nonNull( pool ) )
        {
            this.seriesCountRightInternal.addAndGet( pool.get().size() );
            this.pairCountRightInternal.addAndGet( PoolSlicer.getPairCount( pool ) );
            this.traceCountRightInternal.addAndGet( traceCount );

            if ( pool.hasBaseline() )
            {
                this.seriesCountBaselineInternal.addAndGet( pool.getBaselineData().get().size() );
                this.pairCountBaselineInternal.addAndGet( PoolSlicer.getPairCount( pool.getBaselineData() ) );
                this.traceCountBaselineInternal.addAndGet( traceCountBaseline );
            }
        }
    }

    /**
     * Copies the incremental state to the final state upon completion.
     */

    private void copyStateOnCompletion()
    {
        this.seriesCount = this.seriesCountRightInternal.get() + this.seriesCountBaselineInternal.get();
        this.pairCount = this.pairCountRightInternal.get() + this.pairCountBaselineInternal.get();
        this.traceCount = this.traceCountRightInternal.get() + this.traceCountBaselineInternal.get();

        // Estimate the number of values, assuming a rectangular shape for each set of pairs
        long rightMembersPerPair = 0;
        long baselineMembersPerPair = 0;

        if ( this.seriesCountRightInternal.get() > 0 )
        {
            rightMembersPerPair = this.traceCountRightInternal.get() / this.seriesCountRightInternal.get();
        }

        if ( this.seriesCountBaselineInternal.get() > 0 )
        {
            baselineMembersPerPair = this.traceCountBaselineInternal.get() / this.seriesCountBaselineInternal.get();
        }

        this.valueCount = ( rightMembersPerPair * this.pairCountRightInternal.get() )
                          + ( baselineMembersPerPair * this.pairCountBaselineInternal.get() );
    }

    /**
     * Hidden constructor.
     */

    private EvaluationEvent()
    {
        this.seriesCountRightInternal = new AtomicLong();
        this.pairCountRightInternal = new AtomicLong();
        this.traceCountRightInternal = new AtomicLong();
        this.seriesCountBaselineInternal = new AtomicLong();
        this.pairCountBaselineInternal = new AtomicLong();
        this.traceCountBaselineInternal = new AtomicLong();
    }
}
