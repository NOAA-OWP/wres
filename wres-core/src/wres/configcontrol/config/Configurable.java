package wres.configcontrol.config;

//WRES dependencies
import wres.util.*;

/**
 * Identifiers a block of configuration with a unique {@link Identifier}. 
 * 
 * @author james.brown@hydrosolved.com
 */
public interface Configurable extends DeepCopy<Configurable> {

	/**
	 * Returns the unique identifier for the configuration.
	 * 
	 * @return the unique identifier
	 */

	Identifier getID();

}
