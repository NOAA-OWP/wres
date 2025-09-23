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

    /**
     * For Redisson use.
     * @param id the id
     */

    public DummyLiveObject( String id )
    {
        this.id = id;
    }

    /**
     * For Redisson use.
     */
    public DummyLiveObject()
    {
        // Left for Redisson (subclass?) to fill in.
    }

    /**
     * @return the id
     */

    public String getId()
    {
        return this.id;
    }

    /**
     * Set the id.
     * @param id the id
     */
    public void setId( String id )
    {
        this.id = id;
    }
}
