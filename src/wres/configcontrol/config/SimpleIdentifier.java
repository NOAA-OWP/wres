package wres.configcontrol.config;

// JAXB dependencies
import javax.xml.bind.annotation.XmlValue;

/**
 * Concrete implementation of a simple, immutable, identifier that contains a single identifier element.
 *
 * @author james.brown@hydrosolved.com
 */
public final class SimpleIdentifier implements Identifier, Comparable<SimpleIdentifier>
{
    private String id = null;

    /**
     * Construct the simple identifier.
     * 
     * @param id the identifier
     */

    public SimpleIdentifier(final String id)
    {
        this.id = id;
    }

    @Override
    @XmlValue
    public String getID()
    {
        return id;
    }

    @Override
    public boolean equals(final Object o)
    {
        return o != null && o instanceof SimpleIdentifier && ((SimpleIdentifier)o).id.equals(id);
    }

    @Override
    public int hashCode()
    {
        return id.hashCode();
    }

    @Override
    public int compareTo(final SimpleIdentifier compareMe)
    {
        return id.compareTo(compareMe.id);
    }

    /**
     * No argument constructor for marshalling.
     */
    @SuppressWarnings("unused")
    private SimpleIdentifier()
    {
    }

}
