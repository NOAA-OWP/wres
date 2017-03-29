/**
 * 
 */
package wres.configcontrol.datamodel.spacetimeobject;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author ctubbs
 */
public class SpaceTimeObjectStore
{
    private final Map<Identifier, SpaceTimeObject> store;

    /**
     * 
     */
    public SpaceTimeObjectStore()
    {
        this.store = new TreeMap<Identifier, SpaceTimeObject>();
    }

    /**
     * This is just a stub; Adds a SpaceTimeObject to the store
     * 
     * @param obj the object
     * @return the object
     */
    public SpaceTimeObject add(final SpaceTimeObject obj)
    {
        return this.store.put(obj.getId(), obj);
    }

    /**
     * Like the above, this is just a stub.
     * 
     * @param id the identifier
     * @return the object
     */
    public SpaceTimeObject read(final Identifier id)
    {
        return this.store.get(id);
    }

    /**
     * This is just a stub; currently returns all SpaceTimeObjects
     * 
     * @return the collection of objects
     */
    public Collection<SpaceTimeObject> readAll()
    {
        return this.store.values();
    }
}
