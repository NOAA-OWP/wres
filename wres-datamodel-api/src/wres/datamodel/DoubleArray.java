package wres.datamodel;

/**
 * Provides an array of primitive doubles.
 *
 * Low level, but common interface, to be used across the system.
 *
 * The building block type of the rest of the type hierarchy.
 *
 * Helps avoid use of boxed Double for large (gt 1m values) datasets.
 *
 * @author jesse
 *
 */
public interface DoubleArray
{
    double[] getDoubles();
}
