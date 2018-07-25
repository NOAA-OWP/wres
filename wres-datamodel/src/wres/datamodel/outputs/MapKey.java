package wres.datamodel.outputs;

import java.util.Objects;

/**
 * A {@link MapKey} for storing a {@link MetricOutput} in a map. The key comprises a single component.
 * 
 * @author james.brown@hydrosolved.com
 */

public class MapKey<S extends Comparable<S>> implements Comparable<MapKey<S>>
{

    /**
     * The map key.
     */

    private final S key;

    /**
     * Build a map key.
     * 
     * @param <S> the type of data for the key
     * @param key the key
     * @return an instance of the output
     */

    public static <S extends Comparable<S>> MapKey<S> of( S key )
    {
        return new MapKey<>( key );
    }

    @Override
    public int compareTo( final MapKey<S> o )
    {
        //Compare the keys
        Objects.requireNonNull( o, "Specify a non-null map key for comparison." );

        return this.getKey().compareTo( o.getKey() );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof MapKey ) )
        {
            return false;
        }
        MapKey<?> check = (MapKey<?>) o;
        return key.equals( check.key );
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode( key );
    }

    /**
     * Returns the raw key.
     * 
     * @return the raw key
     */

    public S getKey()
    {
        return key;
    }

    @Override
    public String toString()
    {
        return "[" + getKey() + "]";
    }

    /**
     * Build a map key.
     * 
     * @param key the key
     */

    private MapKey( S key )
    {
        Objects.requireNonNull( key, "Specify a non-null map key." );
        this.key = key;
    }

}
