package wres.datamodel;

import java.util.Arrays;
import java.util.Objects;

/**
 * Immutable two-dimensional array of double values.
 *
 * @author james.brown@hydrosolved.com
 */
public class MatrixOfDoubles
{

    /**
     * Store of doubles.
     */

    private final double[][] doubles;

    /**
     * Build a matrix of doubles with a primitive array.
     * 
     * @param doubles the array of doubles
     * @return a matrix
     */

    public static MatrixOfDoubles of( final double[][] doubles )
    {
        return new MatrixOfDoubles( doubles );
    }

    /**
     * Return the underlying array of primitive doubles.
     * 
     * @return the double array
     */

    public double[][] getDoubles()
    {
        return doubles.clone();
    }

    /**
     * Return the number of rows in the matrix.
     * 
     * @return the number of rows
     */

    public int rows()
    {
        return doubles.length;
    }

    /**
     * Return the number of columns in the matrix.
     * 
     * @return the number of columns
     */

    public int columns()
    {
        return doubles[0].length;
    }

    /**
     * Returns true if {@link #rows()} == {@link #columns()}.
     * 
     * @return true if the matrix is square, false otherwise.
     */

    public boolean isSquare()
    {
        return this.rows() == this.columns();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof MatrixOfDoubles ) )
        {
            return false;
        }
        return Arrays.deepEquals( doubles, ( (MatrixOfDoubles) o ).doubles );
    }

    @Override
    public int hashCode()
    {
        return Arrays.deepHashCode( doubles );
    }

    @Override
    public String toString()
    {
        return Arrays.deepToString( doubles );
    }

    /**
     * Hidden constructor.
     * 
     * @param doubles the array of doubles
     * @throws NullPointerException if the input is null
     */

    private MatrixOfDoubles( final double[][] doubles )
    {
        Objects.requireNonNull( doubles, "Specify non-null input from which to create the matrix." );

        this.doubles = doubles.clone();
    }
}
