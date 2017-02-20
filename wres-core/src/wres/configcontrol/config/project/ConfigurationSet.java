/**
 * 
 */
package wres.configcontrol.config.project;

// Java util dependencies
import java.util.Map;
import java.util.TreeMap;

// WRES dependencies
import wres.configcontrol.config.Configurable;
import wres.configcontrol.config.SimpleIdentifier;

/**
 * A set of {@link ConfigurationUnit} of a prescribed type. Each {@link ConfigurationUnit} is stored with a unique
 * {@link SimpleIdentifier}.
 * 
 * @author james.brown@hydrosolved.com
 */
public class ConfigurationSet<T extends ConfigurationUnit<?>> implements Configurable
{

    /**
     * The unique identifier associated with the configuration.
     */

    private SimpleIdentifier id = null;

    /**
     * A map of {@link ConfigurationUnit} by unique {@link SimpleIdentifier}.
     */

    private final Map<SimpleIdentifier, T> configs = new TreeMap<>();

    /**
     * Construct with a default identifier.
     */

    public ConfigurationSet()
    {
        id = new SimpleIdentifier(System.currentTimeMillis() + "");
    }

    /**
     * Construct with a unique identifier.
     * 
     * @param id the identifier
     */

    public ConfigurationSet(final SimpleIdentifier id)
    {
        if(id == null)
        {
            throw new ConfigurationException("Specify a non-null identifier for the configuration.");
        }
        this.id = id;
    }

    @Override
    public SimpleIdentifier getID()
    {
        return id;
    }

    /**
     * Returns a {@link ConfigurationUnit} associated with an identifier or null if the configuration does not exist.
     * 
     * @param id the identifier
     * @return a {@link ConfigurationUnit}
     */

    public T get(final SimpleIdentifier id)
    {
        return configs.get(id);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Configurable deepCopy()
    {
        final ConfigurationSet<T> returnMe = new ConfigurationSet<>();
        for(final SimpleIdentifier next: configs.keySet())
        {
            returnMe.set((T)configs.get(next).deepCopy());
        }
        return returnMe;
    }

    /**
     * Sets a {@link ConfigurationUnit}
     * 
     * @param add the {@link ConfigurationUnit} to add
     * @throws ConfigurationException if the input is null
     */

    public void set(final T add)
    {
        if(add == null)
        {
            throw new ConfigurationException("Cannot store null configuration.");
        }
        configs.put(add.getID(), add);
    }

    /**
     * Removes a {@link ConfigurationUnit} for a prescribed identifier.
     * 
     * @param id the identifier
     * @return the item removed or null
     */

    public T remove(final SimpleIdentifier id)
    {
        return configs.remove(id);
    }

}
