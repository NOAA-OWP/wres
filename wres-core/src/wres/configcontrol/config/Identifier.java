package wres.configcontrol.config;

// Java util dependencies
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * An immutable identifier that comprises {@link String} elements of a prescribed {@link Integer} type.
 * 
 * @author james.brown@hydrosolved.com
 */
@XmlRootElement(name = "identifier")
@XmlAccessorType(XmlAccessType.FIELD)
public final class Identifier implements Comparable<Identifier>
{

    /**
     * Identifier for a geographic location, area or region/domain that represents an object.
     */

    public static final int OBJECT_IDENTIFIER = 101;

    /**
     * Identifier for an attribute.
     */

    public static final int ATTRIBUTE_IDENTIFIER = 102;

    /**
     * Identifier for a scenario.
     */

    public static final int SCENARIO_IDENTIFIER = 103;

    /**
     * Identifier for a configuration component.
     */

    public static final int CONFIGURATION_IDENTIFIER = 104;

    /**
     * Default separator.
     */

    public static final String DEFAULT_SEPARATOR = ".";

    /**
     * The composite identifier.
     */

    private String id = null;

    /**
     * The components of the identifier.
     */

    private final Map<Integer, String> elements = new TreeMap<>();

    /**
     * Separator character. This is {#DEFAULT_SEPARATOR} by default.
     */

    private String separator = DEFAULT_SEPARATOR;

    /**
     * Construct the identifier with a single element and the element type.
     * 
     * @param type the type of element
     * @param element the identifier element
     */

    public Identifier(final int type, final String element)
    {
        elements.put(type, element);
        buildString();
    }

    /**
     * Construct the identifier with a map of elements and a separator.
     * 
     * @param elements the elements of the identifier
     * @param separator the separator
     */

    public Identifier(final Map<Integer, String> elements, final String separator)
    {
        this.separator = separator;
        this.elements.putAll(elements);
        buildString();
    }

    /**
     * Construct the identifier with a list of string elements.
     * 
     * @param elements the elements of the identifier
     */

    public Identifier(final Map<Integer, String> elements)
    {
        this(elements, DEFAULT_SEPARATOR);
    }

    @Override
    public String toString()
    {
        return id;
    }

    @Override
    public boolean equals(final Object o)
    {
        return o != null && o.hashCode() == hashCode();
    }

    @Override
    public int hashCode()
    {
        return elements.hashCode();
    }

    @Override
    public int compareTo(final Identifier compareMe)
    {
        int returnMe = 0;
        final Map<Integer, String> in = compareMe.elements;
        final int s1 = in.size();
        final int s2 = elements.size();
        if(s2 < s1)
        {
            return -1;
        }
        else if(s2 > s1)
        {
            return 1;
        }
        final Iterator<Integer> i1 = in.keySet().iterator();
        final Iterator<Integer> i2 = elements.keySet().iterator();
        while(i1.hasNext())
        {
            final Integer k1 = i1.next();
            final Integer k2 = i2.next();
            returnMe += k2.compareTo(k1);
            returnMe += elements.get(k2).compareTo(in.get(k1));
        }
        return returnMe;
    }

    /**
     * Returns a prescribed element of the identifier or null if the element does not exist.
     * 
     * @param element the element
     * @return the identifier element or null
     */

    public String get(final Integer element)
    {
        return elements.get(element);
    }

    /**
     * Returns true if the prescribed element is contained in the identifier, false otherwise.
     * 
     * @param element the element
     * @return true if the prescribed elements is contained in the identifier, false otherwise
     */

    public boolean contains(final Integer element)
    {
        return get(element) != null;
    }

    /**
     * No argument constructor for marshalling.
     */

    private Identifier()
    {
    }

    /**
     * Builds a string representation of the identifier.
     */

    private void buildString()
    {
        final StringBuilder b = new StringBuilder();
        for(final String s: elements.values())
        {
            b.append(s).append(separator);
        }
        b.deleteCharAt(b.length() - 1);
        id = b.toString();
    }

}
