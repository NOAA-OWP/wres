package wres.configcontrol.config.project;

// Java net dependencies
import java.net.URI;

// WRES dependencies
import wres.configcontrol.config.CompoundIdentifier;

/**
 * An immutable identifier for one or more unique datasets that are accessible in a single context. Each dataset is
 * identified by its {@link CompoundIdentifier} and the context is given by a {@link URI} to the data store. Use a
 * {@link DataIdentifier} to reference one or more datasets in a single context and {@link DataIdentifierSet} to
 * identify multiple datasets in multiple contexts.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class DataIdentifier implements Comparable<DataIdentifier>
{

    /**
     * The URI to the data store.
     */

    private final URI context;

    /**
     * The unique identifiers for the datasets.
     */

    private final CompoundIdentifier[] ids;

    /**
     * Construct a data identifier.
     * 
     * @param id the Identifier
     * @param context the URI to the data store that contains the {@link #id}
     * @throws ConfigurationException if one or both of the inputs are null
     */

    public DataIdentifier(final CompoundIdentifier id, final URI context)
    {
        if(id == null)
        {
            throw new ConfigurationException("Specify a non-null ID from which to construct the data identifier.");
        }
        if(context == null)
        {
            throw new ConfigurationException("Specify a non-null URI from which to construct the data identifier.");
        }
        ids = new CompoundIdentifier[]{id};
        this.context = context;
    }

    @Override
    public boolean equals(final Object o)
    {
        return o != null && o instanceof DataIdentifier && o.hashCode() == hashCode();
    }

    @Override
    public int hashCode()
    {
        int hash = 0;
        for(final CompoundIdentifier id: ids)
        {
            hash += id.hashCode();
        }
        return hash + context.hashCode();
    }

    @Override
    public String toString()
    {
        final StringBuilder b = new StringBuilder();
        b.append("<");
        for(final CompoundIdentifier id: ids)
        {
            b.append(id).append(", ");
        }
        final int length = b.length();
        b.delete(length - 2, length);
        b.append(">");
        b.append(" in ").append(context);
        return b.toString();
    }

    /**
     * Returns the dataset identifiers.
     * 
     * @return the dataset identifiers
     */

    public CompoundIdentifier[] getIDs()
    {
        return ids;
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
        return o.toString().compareTo(toString());
    }

}
