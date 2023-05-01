package wres.datamodel;

import java.util.Arrays;
import java.util.stream.Stream;

/**
 * Provides a 1D array of primitive doubles. Low level, but common interface, to be used across the system. The building
 * block type of the rest of the type hierarchy. Helps avoid use of boxed Double for large (gt 1m values) datasets. This
 * implementation is immutable.

 * @author Jesse Bickel
 * @author James Brown
 */
public class VectorOfDoubles implements Comparable<VectorOfDoubles>
{
    /**
     * The primitive array of doubles.
     */

    private final double[] doubles;

    /**
     * Returns a {@link VectorOfDoubles} from a primitive array.
     * 
     * @param doubles the input doubles
     * @return the vector of doubles
     */

    public static VectorOfDoubles of( final double... doubles )
    {
        return new VectorOfDoubles( doubles );
    }

    /**
     * Returns a {@link VectorOfDoubles} from a primitive array of wrapped {@link Double} values.
     * 
     * @param doubles the input wrapped doubles
     * @return the vector of doubles
     */

    public static VectorOfDoubles of( final Double[] doubles )
    {
        return of( Stream.of( doubles ).mapToDouble( Double::doubleValue ).toArray() );
    }

    /**
     * Returns a copy of the double array.
     * 
     * @return the double array
     */

    public double[] getDoubles()
    {
        return this.doubles.clone();
    }

    /**
     * Return the number of doubles present.
     * 
     * @return the size of the double vector
     */

    public int size()
    {
        return this.doubles.length;
    }

    @Override
    public int compareTo( VectorOfDoubles other )
    {
        double[] otherDoubles = other.getDoubles();
        return VectorOfDoubles.compareDoubleArray( this.getDoubles(), otherDoubles );
    }

    @Override
    public boolean equals( Object other )
    {
        if ( other instanceof VectorOfDoubles otherVec )
        {
            return 0 == VectorOfDoubles.compareDoubleArray( this.getDoubles(), otherVec.getDoubles() );
        }
        else
        {
            return false;
        }
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode( this.getDoubles() );
    }

    @Override
    public String toString()
    {
        return Arrays.toString( this.doubles );
    }

    /**
     * Hidden constructor.
     * 
     * @param doubles the primitive array of doubles
     */

    private VectorOfDoubles( final double[] doubles )
    {
        this.doubles = doubles.clone();
    }

    /**
     * <p>Consistent comparison of double arrays, first checks count of elements,
     * next goes through values.
     *
     * <p>If first has fewer values, return -1, if first has more values, return 1.
     *
     * <p>If value count is equal, go through in order until an element is less
     * or greater than another. If all values are equal, return 0.
     *
     * @param first the first array
     * @param second the second array
     * @return -1 if first is less than second, 0 if equal, 1 otherwise.
     */
    private static int compareDoubleArray( final double[] first,
                                           final double[] second )
    {
        // this one has fewer elements
        if ( first.length < second.length )
        {
            return -1;
        }
        // this one has more elements
        else if ( first.length > second.length )
        {
            return 1;
        }
        // compare values until we diverge
        else // assumption here is lengths are equal
        {
            for ( int i = 0; i < first.length; i++ )
            {
                int safeComparisonResult = Double.compare( first[i], second[i] );
                if ( safeComparisonResult != 0 )
                {
                    return safeComparisonResult;
                }
            }
            // all values were equal
            return 0;
        }
    }
}
