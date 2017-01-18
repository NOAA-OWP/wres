/**
 * 
 */
package wres.configcontrol.datamodel.spacetimeobject;

import java.util.HashMap;
import java.util.Set;

/**
 * @author ctubbs
 *
 */
public class Identifier implements Comparable<Identifier> {

	private final String id;
	private final HashMap<Integer, Integer> map;
	
	public Identifier(String id) {
		this.id = id;
		this.map = new HashMap<Integer, Integer>();
	}
	
	public String getID() {
		return this.id;
	}
	
	public Integer setValue(int key, int value) {
		this.map.put(key, value);
		return value;
	}
	
	public Integer getValue(int key) {
		return this.map.get(key);
	}
	
	public Integer removeValue(int key) {
		return this.map.remove(key);
	}
	
	public Boolean containsKey(int key) {
		return map.containsKey(key);
	}
	
	public void clear() {
		this.map.clear();
	}
	
	public boolean containsValue(int value) {
		return this.map.containsValue(value);
	}
	
	public Set<Integer> getKeys() {
		return this.map.keySet();
	}
	
	@Override
	public int compareTo(Identifier arg0) {
		// TODO Auto-generated method stub
		return this.id.compareTo(arg0.id);
	}

}
