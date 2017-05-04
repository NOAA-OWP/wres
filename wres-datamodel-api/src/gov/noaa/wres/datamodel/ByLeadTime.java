package gov.noaa.wres.datamodel;

/**
 * Low level interface to be used by other interfaces
 * 
 * @author jesse
 *
 * @param <T>
 */
public interface ByLeadTime<T>
{
    T getByLeadTime(int leadtime) throws IndexOutOfBoundsException;
    int getLength();
}
