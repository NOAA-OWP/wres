package gov.noaa.wres.datamodel;

/**
 * Low level interface to be used by higher-level interfaces.
 * 
 * @author jesse
 *
 * @param <T>
 */
public interface ByIndex<T>
{
    T getByIndex(int index) throws IndexOutOfBoundsException;
    int getLength();
}
