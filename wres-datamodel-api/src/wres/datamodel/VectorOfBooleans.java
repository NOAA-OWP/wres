package wres.datamodel;

/**
 * Provides a 1D array of primitive booleans.
 *
 * Low level, but common interface, to be used across the system.
 *
 * A type used mostly by the metrics engine.
 *
 * Helps avoid use of boxed Boolean for large (gt 1m values) datasets.
 *
 * @author jesse
 *
 */
public interface VectorOfBooleans
{
    boolean[] getBooleans();
}
