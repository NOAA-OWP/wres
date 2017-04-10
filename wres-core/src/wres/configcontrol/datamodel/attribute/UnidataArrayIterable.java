package wres.configcontrol.datamodel.attribute;

import java.util.Iterator;

import ucar.ma2.Array;
import ucar.ma2.IndexIterator;

/**
 * Wrapper on {@link IndexIterator}, which is the Unidata CDM iterator that does not implement {@link Iterable}.
 * 
 * @author Hank.Herr
 * @param <T> The data type for the {@link UnidataArrayIterable} which must match the {@link Array} provided upon construction.
 */
public class UnidataArrayIterable<T> implements Iterable<T>
{
    /**
     * The {@link IndexIterator} that the {@link Iterator} returned by {@link #iterator()} will use under the hood.
     */
    private final IndexIterator indexIterator;

    /**
     * @param array The {@link Array} from which this will acquire an {@link IndexIterator} via
     *            {@link Array#getIndexIterator()}.
     */
    public UnidataArrayIterable(final Array array)
    {
        indexIterator = array.getIndexIterator();
    }

    @Override
    public Iterator<T> iterator()
    {
        return new Iterator<T>()
        {
            @Override
            public boolean hasNext()
            {
                return indexIterator.hasNext();
            }

            @Override
            public T next()
            {
                return (T)indexIterator.next();
            }
        };
    }

}
