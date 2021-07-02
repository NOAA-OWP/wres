package wres.tasker;

import org.redisson.api.annotation.REntity;
import org.redisson.api.annotation.RId;

/**
 * A dummy redisson class to test redisson live object service availability.
 */

@REntity
public class DummyLiveObject
{
    @RId
    private String id;

    public DummyLiveObject( String id )
    {
        this.id = id;
    }

    public DummyLiveObject()
    {
        // Left for Redisson (subclass?) to fill in.
    }

    public String getId()
    {
        return this.id;
    }

    public void setId( String id )
    {
        this.id = id;
    }
}
