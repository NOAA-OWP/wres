package wres.datamodel;

/**
 * Low level interface to be used by other interfaces
 * 
 * @author jesse
 *
 * @param <T>
 */
public interface ByLeadTime<T>
{
    /** @return null if not found */
    T getByLeadTime(int leadtime);
    int getLeadTime(int index) throws IndexOutOfBoundsException;
    /** @return the number of T elements available */
    int getLength();
}
