package wres.datamodel;

import java.util.Arrays;
import java.util.stream.Stream;

/**
 * Provides a 1D array of primitive doubles. Low level, but common interface, to be used across the system. The building
 * block type of the rest of the type hierarchy. Helps avoid use of boxed Double for large (gt 1m values) datasets. This
 * implementation is immutable.

 * @author jesse
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
        return DataFactory.compareDoubleArray( this.getDoubles(), otherDoubles );
    }

    @Override
    public boolean equals( Object other )
    {
        if ( other instanceof VectorOfDoubles )
        {
            VectorOfDoubles otherVec = (VectorOfDoubles) other;
            return 0 == DataFactory.compareDoubleArray( this.getDoubles(), otherVec.getDoubles() );
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
}
