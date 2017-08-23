package wres.datamodel;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.BiConsumer;

/**
 * A sorted map of {@link MetricOutput} stored by a {@link Comparable} key in their natural order. Implements the same
 * read-only API as the {@link SortedMap}. However, for an immutable implementation, changes in the returned values are
 * not backed by this map.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface MetricOutputMap<S extends Comparable<?>, T extends MetricOutput<?>>
{

    /**
     * Returns true if the map is empty, false otherwise.
     * 
     * @return true if the map is empty, false otherwise
     */

    default boolean isEmpty()
    {
        return size() > 0;
    }

    /**
     * Consume each pair in the map.
     * 
     * @param consumer the consumer
     */

    default void forEach(BiConsumer<S, T> consumer)
    {
        for(Entry<S, T> entry: entrySet())
        {
            consumer.accept(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Returns the {@link MetricOutput} associated with the specified {@link Comparable} key or null.
     * 
     * @param key the key
     * @return the output for the specified key or null
     */

    T get(S key);

    /**
     * Returns the key at the specified index in the sorted map.
     * 
     * @param index the index
     * @return the key
     * @throws IndexOutOfBoundsException if the index is out of range
     */

    S getKey(int index);

    /**
     * Returns the value at the specified index in the sorted map.
     * 
     * @param index the index
     * @return the value
     * @throws IndexOutOfBoundsException if the index is out of range
     */

    T getValue(int index);

    /**
     * Returns true if the mapping contains the specified key, false otherwise.
     * 
     * @param key the key
     * @return true if the map contains the key, false otherwise
     */

    boolean containsKey(S key);

    /**
     * Returns true if the mapping contains the specified value, false otherwise.
     * 
     * @param value the value
     * @return true if the map contains the value, false otherwise
     */

    boolean containsValue(T value);

    /**
     * Returns a collection view of the values in the map.
     * 
     * @return a collection view of the values
     */

    Collection<T> values();

    /**
     * Returns a view of the keys in the map for iteration.
     * 
     * @return a view of the keys
     */

    Set<S> keySet();

    /**
     * Returns a view of the entries in the map for iteration.
     * 
     * @return a view of the map entries
     */

    Set<Entry<S, T>> entrySet();

    /**
     * Returns the number of element in the map.
     * 
     * @return the size of the mapping
     */

    int size();

    /**
     * Returns a view of the portion of this map whose keys range from fromKey, inclusive, to toKey, exclusive. If
     * fromKey and toKey are equal, the returned map is empty.
     * 
     * @param fromKey low endpoint (inclusive) of the keys in the returned map
     * @param toKey high endpoint (exclusive) of the keys in the returned map
     * @return a view of the portion of this map whose keys range from fromKey, inclusive, to toKey, exclusive
     */

    SortedMap<S, T> subMap(S fromKey, S toKey);

    /**
     * Returns a view of the portion of this map whose keys are strictly less than toKey.
     * 
     * @param toKey high endpoint (exclusive) of the keys in the returned map
     * @return a view of the portion of this map whose keys are strictly less than toKey
     */

    SortedMap<S, T> headMap(S toKey);

    /**
     * Returns a view of the portion of this map whose keys are greater than or equal to fromKey.
     * 
     * @param fromKey low endpoint (inclusive) of the keys in the returned map
     * @return a view of the portion of this map whose keys are greater than or equal to fromKey
     */

    SortedMap<S, T> tailMap(S fromKey);

    /**
     * Returns the first (lowest) key currently in this map.
     * 
     * @return the first (lowest) key currently in this map
     */

    S firstKey();

    /**
     * Returns the last (highest) key currently in this map.
     * 
     * @return the last (highest) key currently in this map
     */

    S lastKey();

}
