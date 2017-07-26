package wres.configcontrol.datamodel.attribute;

/**
 * Generic interface for all axes defining the margins of data arrays within space time objects. Though methods
 * {@link #getValue(int...)} and {@link #setValue(Object, int...)} are specified, if possible a caller may want to call
 * lower level, implementation specific methods that return primitives which most or all implementations of this
 * interface will include. Using the methods herein, which require wrapping primitives in Java objects, will likely be
 * slow.
 * 
 * @author Hank.Herr
 * @param <T>
 */
public interface Axis<T> extends Iterable
{
    /**
     * @param indices Number of indices provided must match the number of indices expected by the concrete
     *            implementation. It is up to the implementation to ensure this.
     * @return The value at the indices.
     */
    public T getValue(int... indices);

    /**
     * @param value The value to store at the provided indices.
     * @param indices Number of indices provided must match the number of indices expected by the concrete
     *            implementation. It is up to the implementation to ensure this.
     */
    public void setValue(T value, int... indices);

    //TODO Add methods for searching, such as...
    //
    //    public int findValue(T value)
    //
    //which returns the index of the provided value.  There may be other variations, as well.
    //The underlying implementation will need to determine how best to search itself, since some
    //axes may be able to assume sorted values and some may not.  
}
