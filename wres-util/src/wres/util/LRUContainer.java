package wres.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.locks.ReentrantLock;

import wres.util.functional.ExceptionalPredicate;

/**
 * A fixed size container that removes the least recently used item when a new one is added
 * <br><br>
 * <strong>Thread Safe</strong>
 */
public class LRUContainer<U>
{
    private final LinkedList<U> innerList = new LinkedList<>(  );
    private final int limit;
    private ReentrantLock lock = new ReentrantLock(  );

    /**
     * Constructs the container with the given limit
     * @param limit The maximum number of objects that may be in the container
     */
    public LRUContainer(final int limit)
    {
        this.limit = limit;
    }

    /**
     * Constructs the container with the given items. If the number of items
     * exceeds the size of the container, the first items added are removed
     * @param limit The maximum number of objects that may be in the container
     * @param values Values to add to the collection
     */
    public LRUContainer(final int limit, Collection<U> values)
    {
        this.limit = limit;

        for (U value : values)
        {
            this.add(value);
        }
    }

    /**
     * Adds an item to the container
     * <br><br>
     * If the new length exceeds the limit of the number of values, the least recently used value is removed
     * <br><br>
     * Uniqueness is not maintained
     * @param value A value to add to the container
     */
    public void add(U value)
    {
        this.lock.lock();
        try
        {
            this.innerList.add( value );

            if ( this.innerList.size() > this.limit )
            {
                this.innerList.pop();
            }
        }
        finally
        {
            if (this.lock.isHeldByCurrentThread())
            {
                this.lock.unlock();
            }
        }
    }

    /**
     * Retrieve the least recently used object that matches the given search function
     * <br><br>
     * If an object is found, it becomes the least recently used object in the collection
     * @param search A function that will find a value
     * @param <E> An exception that may be thrown by the search function
     * @return The least recently used object that matches the given search function, null if no object matches
     * @throws E An exception thrown by the search
     */
    public <E extends Throwable> U get( ExceptionalPredicate<U, E> search) throws E
    {
        this.lock.lock();

        try
        {
            U value = null;

            Iterator<U> descendingIterator = this.innerList.descendingIterator();

            while ( descendingIterator.hasNext() )
            {
                U entry = descendingIterator.next();

                if ( search.test( entry ) )
                {
                    value = entry;
                    break;
                }
            }

            if ( value != null )
            {
                this.innerList.remove( value );
                this.innerList.add( value );
            }

            return value;
        }
        finally
        {
            if (this.lock.isHeldByCurrentThread())
            {
                this.lock.unlock();
            }
        }
    }

    /**
     * @return The number of contained objects
     */
    public int getSize()
    {
        return this.innerList.size();
    }
}
