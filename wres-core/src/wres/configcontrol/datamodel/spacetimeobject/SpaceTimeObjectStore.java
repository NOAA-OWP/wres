/**
 * 
 */
package wres.configcontrol.datamodel.spacetimeobject;
import java.util.TreeMap;
import java.util.Map;
import java.util.Collection;

/**
 * @author ctubbs
 *
 */
public class SpaceTimeObjectStore {
	private final Map<Identifier, SpaceTimeObject> store;	

	/**
	 * 
	 */
	public SpaceTimeObjectStore() {
		this.store = new TreeMap<Identifier, SpaceTimeObject>();
	}
	
	/**
	 * This is just a stub; Adds a SpaceTimeObject to the store
	 * @param obj
	 */
	public SpaceTimeObject add(SpaceTimeObject obj) {
		return this.store.put(obj.getId(), obj);
	}
	
	/**
	 * Like the above, this is just a stub.
	 * @param id
	 */
	public SpaceTimeObject read(Identifier id) {
		return this.store.get(id);
	}

	/**
	 * This is just a stub; currently returns all SpaceTimeObjects
	 * @return
	 */
	public Collection<SpaceTimeObject> readAll() {
		return this.store.values();
	}
}
