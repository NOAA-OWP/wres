package wres.configcontrol.config.project;

// WRES dependencies
import wres.configcontrol.config.Configurable;
import wres.configcontrol.config.SimpleIdentifier;

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

    private final SimpleIdentifier id;

    /**
     * The context for the configuration, comprising the datasets to which it refers.
     */

    private DataIdentifierSet idSet = null;

    /**
     * Constructs a {@link ConfigurationUnit} with a default {@link SimpleIdentifier}.
     */

    public ConfigurationUnit()
    {
        id = new SimpleIdentifier(System.currentTimeMillis() + "");
    }

    /**
     * Constructs a {@link ConfigurationUnit} with an {@link SimpleIdentifier}.
     * 
     * @param id the unique identifier for the configuration
     * @throws ConfigurationException if the identifier is null
     */

    public ConfigurationUnit(final SimpleIdentifier id)
    {
        if(id == null)
        {
            throw new ConfigurationException("Enter a non-null identifier for the configuration.");
        }
        this.id = id;
    }

    @Override
    public SimpleIdentifier getID()
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
        this.idSet = idSet;
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
