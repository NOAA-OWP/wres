package wres.datamodel;

import java.util.Arrays;

/**
 * Provides a 1D array of primitive booleans. Low level, but common interface, to be used across the system. A type used
 * mostly by the metrics engine. Helps avoid use of boxed Boolean for large (gt 1m values) datasets. This implementation
 * is immutable.
 *
 * @author jesse
 * @author james.brown@hydrosolved.com
 */
public class VectorOfBooleans
{
    /**
     * The primitive array.
     */

    private final boolean[] booleans;

    /**
     * Returns a {@link VectorOfBooleans} from a primitive array.
     * 
     * @param booleans the input booleans
     * @return the vector of booleans
     */

    public static VectorOfBooleans of( final boolean[] booleans )
    {
        return new VectorOfBooleans( booleans );
    }

    /**
     * Returns a copy of the boolean array.
     * 
     * @return the boolean array
     */

    public boolean[] getBooleans()
    {
        return booleans.clone();
    }

    /**
     * Return the number of booleans present.
     * 
     * @return the size of the boolean vector
     */

    public int size()
    {
        return booleans.length;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof VectorOfBooleans ) )
        {
            return false;
        }
        boolean[] in = ( (VectorOfBooleans) o ).getBooleans();
        return Arrays.equals( this.getBooleans(), in );
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode( this.getBooleans() );
    }

    /**
     * Hidden constructor.
     * 
     * @param booleans the booleans
     */

    private VectorOfBooleans( final boolean[] booleans )
    {
        this.booleans = booleans.clone();
    }
}
