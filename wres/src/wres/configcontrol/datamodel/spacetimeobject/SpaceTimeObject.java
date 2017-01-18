/**
 * 
 */
package wres.configcontrol.datamodel.spacetimeobject;

import java.util.Set;

/**
 * @author ctubbs
 */
public abstract class SpaceTimeObject implements wres.util.DeepCopy, Comparable<SpaceTimeObject> {
	private final Identifier id;
	
	protected SpaceTimeObject(String identifier) {
		id = new Identifier(identifier);
	}
	
	protected SpaceTimeObject() {
		id = new Identifier(generateID());
	}
	
	private final String generateID() {
		return "";
	}
	
	protected Identifier getId() {
		return this.id;
	}
	
	protected String getIdentifierKey() {
		return id.getID();
	}
	
	protected Set<Integer> getKeys() {
		return this.id.getKeys();
	}
	
	protected int setValue(int key, int value) {
		return this.id.setValue(key, value);
	}
	
	protected int getValue(int key) {
		return this.id.getValue(key);
	}

	@Override
	public int compareTo(SpaceTimeObject arg0) {
		return this.id.compareTo(arg0.getId());
	}
}
