package wres.io.utilities;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Map that only contains a specified number of elements before shedding the
 * least recently used entry
 * @param <K> The type of the key
 * @param <U> The type of the value
 */
public class LRUMap<K, U> extends LinkedHashMap<K, U>
{
    /**
     * The total number of entries allowable before entries are removed
     */
    private final int maximumEntries;

    /**
     * An optional method to call upon an entry's expiration
     */
    private Consumer<Map.Entry<K, U>> expireAction;

    /**
     * Creates the map with the given number of entries
     * @param maximumEntries The maximum number of entries that may be held in
     *                       the map until the least recently used entry is removed.
     */
    public LRUMap( int maximumEntries)
    {
        super();
        this.maximumEntries = maximumEntries;
    }

    /**
     * Creates the map with the given number of entries and an action to
     * perform upon entry expiration
     * @param maximumEntries The maximum number of entries that may be held in
     *                       the map until the least recently used entry is removed
     * @param expireAction An action to perform when an entry is removed
     */
    public LRUMap(int maximumEntries, Consumer<Map.Entry<K, U>> expireAction)
    {
        super();
        this.maximumEntries = maximumEntries;
        this.expireAction = expireAction;
    }

    /**
     * Creates the map with the given number of maximum entries and with the
     * contents of another map
     * @param maximumEntries The maximum number of entries that may be held in
     *                       the map until the least recently used entry is removed
     * @param otherMap A map with data that will be added to the new LRUMap
     */
    public LRUMap(int maximumEntries, Map<K, U> otherMap)
    {
        super();
        this.maximumEntries = maximumEntries;
        this.putAll( otherMap );
    }

    /**
     * Creates the map with the given number of maximum entries, with the
     * contents of another map, and an action to perform upon entry expiration
     * @param maximumEntries The maximum number of entries that may be held in
     *                       the map until the least recently used entry is removed
     * @param otherMap A map with data that will be added to the new LRUMap
     * @param expireAction An action to perform when an entry is removed
     */
    public LRUMap(int maximumEntries, Map<K, U> otherMap, Consumer<Map.Entry<K, U>> expireAction)
    {
        super();
        this.maximumEntries = maximumEntries;
        this.putAll( otherMap );
        this.expireAction = expireAction;
    }

    @Override
    protected boolean removeEldestEntry( Map.Entry<K, U> eldest )
    {
        boolean remove = this.size() > this.maximumEntries;

        if (remove && this.expireAction != null)
        {
            this.expireAction.accept( eldest );
        }

        return remove;
    }
}
