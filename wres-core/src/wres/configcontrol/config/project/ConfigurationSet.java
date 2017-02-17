/**
 * 
 */
package wres.configcontrol.config.project;

// Java util dependencies
import java.util.*;

//WRES dependencies
import wres.configcontrol.config.*;

/**
 * A set of {@link ConfigurationUnit} of a prescribed type. Each {@link ConfigurationUnit} is stored with a unique
 * {@link Identifier}.
 * 
 * @author james.brown@hydrosolved.com
 */
public class ConfigurationSet<T extends ConfigurationUnit<?>> implements Configurable {

	/**
	 * The unique identifier associated with the configuration.
	 */

	private Identifier id = null;

	/**
	 * A map of {@link ConfigurationUnit} by unique {@link Identifier}.
	 */

	private Map<Identifier, T> configs = new TreeMap<>();

	/**
	 * Construct with a default identifier.
	 */

	public ConfigurationSet() {
		id = new Identifier(Identifier.CONFIGURATION_IDENTIFIER, System.currentTimeMillis() + "");
	}

	/**
	 * Construct with a unique identifier.
	 * 
	 * @param id
	 *            the identifier
	 */

	public ConfigurationSet(Identifier id) {
		if (id == null) {
			throw new ConfigurationException("Specify a non-null identifier for the configuration.");
		}
		if (!id.contains(Identifier.CONFIGURATION_IDENTIFIER)) {
			throw new ConfigurationException("Specify a configuration identifier.");
		}
		this.id = id;
	}
	
	@Override
	public Identifier getID() {
		return id;
	}	

	/**
	 * Returns a {@link ConfigurationUnit} associated with an identifier or null if the configuration does not exist.
	 * 
	 * @param id
	 *            the identifier
	 * @return a {@link ConfigurationUnit}
	 */

	public T get(Identifier id) {
		return (T) configs.get(id);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Configurable deepCopy() {
		ConfigurationSet<T> returnMe = new ConfigurationSet<>();
		for(Identifier next : configs.keySet()) {
			returnMe.set((T)configs.get(next).deepCopy());
		}
		return returnMe;
	}	
	
	/**
	 * Sets a {@link ConfigurationUnit}
	 * 
	 * @param add
	 *            the {@link ConfigurationUnit} to add
	 * @throws ConfigurationException
	 *             if the input is null
	 */

	public void set(T add) {
		if (add == null) {
			throw new ConfigurationException("Cannot store null configuration.");
		}
		configs.put(add.getID(), add);
	}

	/**
	 * Removes a {@link ConfigurationUnit} for a prescribed identifier.
	 * 
	 * @param id
	 *            the identifier
	 * @return the item removed or null
	 */

	public T remove(Identifier id) {
		return configs.remove(id);
	}	

}
