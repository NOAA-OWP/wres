package wres.datamodel;

/**
 * Provides getting an element of a collection or array by a lead time.
 * 
 * Low level interface to be used by other interfaces
 * 
 * @author jesse
 *
 * @param <T>
 */
public interface ByLeadTime<T>
{
    /**
     * Get the value at the given lead time.
     * @return null if not found
     */
    public T getByLeadTime(int leadtime);
    /** 
     * Get the lead time at the given index.
     * @throws IndexOutOfBoundsException
     */
    public int getLeadTime(int index);
    /**
     * Get the number of elements and lead times available.
     * @return the number of T elements and lead times available
     */
    public int getLength();
}
