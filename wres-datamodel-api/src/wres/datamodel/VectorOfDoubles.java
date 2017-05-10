package wres.datamodel;

/**
 * Provides a 1D array of primitive doubles.
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
public interface VectorOfDoubles
{
    double[] getDoubles();
}
