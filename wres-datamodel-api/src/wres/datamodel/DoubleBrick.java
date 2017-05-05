package wres.datamodel;

/**
 * Low level, but common interface, to be used across the system.
 * 
 * The building block type of the rest of the type hierarchy.
 * 
 * Allows the ability to get an array of doubles. Avoiding wrappers or boxing.
 * 
 * @author jesse
 *
 */
public interface DoubleBrick
{
    double[] getDoubles();
}
