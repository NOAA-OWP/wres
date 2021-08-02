package wres.io.pooling;

import java.util.Objects;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.Threshold;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.datamodel.pools.PoolMetadata;

/**
 * A custom event for monitoring time-series data retrievals and exposing them to the Java Flight Recorder.
 * 
 * @author james.brown@hydrosolved.com
 */

@Name( "wres.io.pooling.RetrievalEvent" )
@Label( "Retrieval Event" )
@Category( { "Java Application", "Water Resources Evaluation Service", "Core", "Pooling", "Retrieval" } )
class RetrievalEvent extends Event
{
    @Label( "Orientation" )
    @Description( "The side of the pairing from which the time series data originates." )
    private final String lrb;

    @Label( "Pool Description" )
    @Description( "The boundaries of the pool from which the time series data originates." )
    private final String poolMetadata;

    /**
     * @param lrb the orientation of the time-series data
     * @param poolMetadata the pool metadata
     * @return an instance
     * @throws NullPointerException if either input is null
     */

    static RetrievalEvent of( LeftOrRightOrBaseline lrb, PoolMetadata poolMetadata )
    {
        return new RetrievalEvent( lrb, poolMetadata );
    }

    /**
     * Hidden constructor.
     * @param lrb the orientation of the time-series data
     * @param poolMetadata the pool metadata
     * @throws NullPointerException if either input is null
     */

    private RetrievalEvent( LeftOrRightOrBaseline lrb, PoolMetadata poolMetadata )
    {
        Objects.requireNonNull( lrb );
        Objects.requireNonNull( poolMetadata );

        this.lrb = lrb.toString();
        this.poolMetadata = poolMetadata.toString();
    }
}
