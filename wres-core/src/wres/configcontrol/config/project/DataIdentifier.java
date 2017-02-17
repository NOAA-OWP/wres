/**
 * 
 */
package wres.configcontrol.config.project;

// Java net dependencies
import java.net.URI;

// WRES dependencies
import wres.configcontrol.config.Identifier;

/**
 * An immutable identifier for a unique dataset that is accessible in a particular context. The dataset is identifier by
 * its {@link Identifier} and the context is given by a {@link URI} to the data store.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class DataIdentifier implements Comparable<DataIdentifier>
{

    /**
     * The URI to the data store.
     */

    private URI context = null;

    /**
     * A unique identifier for the dataset.
     */

    private Identifier id = null;

    /**
     * Construct a data identifier.
     * 
     * @param id the Identifier
     * @param context the URI to the data store that contains the {@link #id}
     * @throws ConfigurationException if one or both of the inputs are null
     */

    public DataIdentifier(final Identifier id, final URI context)
    {
        if(id == null)
        {
            throw new ConfigurationException("Specify a non-null ID from which to construct the data identifier.");
        }
        if(context == null)
        {
            throw new ConfigurationException("Specify a non-null URI from which to construct the data identifier.");
        }
        this.id = id;
        this.context = context;
    }

    @Override
    public boolean equals(final Object o)
    {
        return o != null && o.hashCode() == hashCode();
    }

    @Override
    public int hashCode()
    {
        return id.hashCode() + context.hashCode();
    }

    @Override
    public String toString()
    {
        final StringBuilder b = new StringBuilder();
        b.append(id).append(" in ").append(context);
        return b.toString();
    }

    /**
     * Returns the dataset identifier.
     * 
     * @return the dataset identifier
     */

    public Identifier getID()
    {
        return id;
    }

    /**
     * Returns the context for the dataset, comprising the URI to the data store that contains it.
     * 
     * @return the URI to the data store
     */

    public URI getContext()
    {
        return context;
    }

    @Override
    public int compareTo(final DataIdentifier o)
    {
        return o.id.compareTo(id) + o.context.compareTo(context);
    }

}
