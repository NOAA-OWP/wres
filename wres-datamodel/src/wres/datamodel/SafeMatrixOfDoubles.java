package wres.datamodel;

import java.util.Arrays;

/**
 * Default implementation of an immutable matrix of double values.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.2
 * @since 0.1
 */

class SafeMatrixOfDoubles implements MatrixOfDoubles
{
    private final double[][] doubles;

    private SafeMatrixOfDoubles(final double[][] doubles)
    {
        this.doubles = doubles.clone();
    }

    public static MatrixOfDoubles of(final double[][] doubles)
    {
        return new SafeMatrixOfDoubles(doubles);
    }

    @Override
    public double[][] getDoubles()
    {
        return doubles.clone();
    }

    @Override
    public int rows()
    {
        return doubles.length;
    }

    @Override
    public int columns()
    {
        return doubles[0].length;
    }

    @Override
    public boolean isSquare()
    {
        return rows() == columns();
    }
    
    @Override
    public boolean equals( Object o )
    {
        if( !(o instanceof SafeMatrixOfDoubles) )
        {
            return false;
        }
        return Arrays.deepEquals( doubles, ( (SafeMatrixOfDoubles) o ).doubles );
    }
    
    @Override
    public int hashCode( )
    {
        return Arrays.deepHashCode( doubles );
    }    
    
    @Override
    public String toString()
    {
        return Arrays.deepToString( doubles );
    }
}
