package wres.io.pooling;

import java.util.Objects;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import wres.datamodel.pools.PoolMetadata;

/**
 * A custom event for monitoring and exposing pool creation to the Java Flight Recorder. Pool creation includes several
 * steps, notably:
 * 
 * <ol>
 * <li>Time-series data retrieval;</li>
 * <li>Rescaling of time-series data, where applicable;</li>
 * <li>Transforming time-series data, where applicable; and</li>
 * <li>Pairing.</li>
 * </ol>
 * 
 * @author james.brown@hydrosolved.com
 */

@Name( "wres.io.pooling.PoolCreationEvent" )
@Label( "Pool Creation Event" )
@Category( { "Java Application", "Water Resources Evaluation Service", "Core", "Pooling" } )
class PoolCreationEvent extends Event
{
    @Label( "Pool Description" )
    @Description( "The boundaries of the pool that contains time series data." )
    private final String poolMetadata;

    /**
     * @param poolMetadata the pool metadata
     * @return an instance
     * @throws NullPointerException if the poolMetadata is null
     */

    static PoolCreationEvent of( PoolMetadata poolMetadata )
    {
        return new PoolCreationEvent( poolMetadata );
    }

    /**
     * Hidden constructor.
     * @param poolMetadata the pool metadata
     * @throws NullPointerException if the poolMetadata is null
     */

    private PoolCreationEvent( PoolMetadata poolMetadata )
    {
        Objects.requireNonNull( poolMetadata );

        this.poolMetadata = poolMetadata.toString();
    }
}
