/**
 * 
 */
package wres.configcontrol.config.project;

// WRES dependencies
import wres.configcontrol.config.Configurable;
import wres.configcontrol.config.Identifier;

/**
 * An elementary block of configuration. The configuration applies to a nominated {@link DataIdentifierSet}. The
 * {@link DataIdentifierSet} uniquely identifies all of the datasets to which the configuration refers.
 * 
 * @author james.brown@hydrosolved.com
 */
public abstract class ConfigurationUnit<T> implements Configurable
{

    /**
     * A unique identifier for the configuration block.
     */

    private Identifier id = null;

    /**
     * The context for the configuration, comprising the datasets to which it refers.
     */

    private final DataIdentifierSet idSet = null;

    /**
     * Constructs a {@link ConfigurationUnit} with a default {@link Identifier}.
     */

    public ConfigurationUnit()
    {
        id = new Identifier(Identifier.CONFIGURATION_IDENTIFIER, System.currentTimeMillis() + "");
    }

    /**
     * Constructs a {@link ConfigurationUnit} with an {@link Identifier}.
     * 
     * @param id the unique identifier for the configuration
     * @throws ConfigurationException if the identifier is null or does not contain an
     *             {@link Identifier#CONFIGURATION_IDENTIFIER}
     */

    public ConfigurationUnit(final Identifier id)
    {
        if(id == null)
        {
            throw new ConfigurationException("Enter a non-null identifier for the configuration.");
        }
        if(!id.contains(Identifier.CONFIGURATION_IDENTIFIER))
        {
            throw new ConfigurationException("Specify a configuration identifier.");
        }
        this.id = id;
    }

    @Override
    public Identifier getID()
    {
        return id;
    }

    /**
     * Sets the {@link DataIdentifierSet} associated with the configuration.
     * 
     * @param idSet the set of identifiers
     * @throws ConfigurationException if the input is null
     */

    public void setContext(final DataIdentifierSet idSet)
    {
        if(idSet == null)
        {
            throw new ConfigurationException("Specify a non-null context for the configuration.");
        }
    }

    /**
     * Returns the {@link DataIdentifierSet} associated with the configuration.
     * 
     * @return the set of identifiers
     */

    public DataIdentifierSet getContext()
    {
        return idSet;
    }

}
