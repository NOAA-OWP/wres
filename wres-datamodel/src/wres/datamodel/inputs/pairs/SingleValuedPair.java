package wres.datamodel.inputs.pairs;

import java.util.Objects;

/**
 * <p>Immutable pair of primitive double values.</p>
 * 
 * <p>An example might be the simplest forecast/observation timeseries data, but stripped of 
 * any/all time information, and containing only the values, i.e. {obs, fcst}.</p>
 *
 * @author jesse
 * @author james.brown@hydrosolved.com
 */
public class SingleValuedPair implements Comparable<SingleValuedPair>
{

    /**
     * The left side.
     */

    private final double left;

    /**
     * The right side.
     */

    private final double right;

    /**
     * Returns an instance from the input.
     * 
     * @param left the left side of the pair
     * @param right the right side of the pair
     * @return a pair of booleans
     */

    public static SingleValuedPair of( double left, double right )
    {
        return new SingleValuedPair( left, right );
    }

    /**
     * Returns the left side of the pair.
     * 
     * @return the left value
     */

    public double getLeft()
    {
        return left;
    }

    /**
     * Returns the right side of the pair.
     * 
     * @return the right value
     */

    public double getRight()
    {
        return right;
    }

    @Override
    public int compareTo( SingleValuedPair other )
    {
        if ( Double.compare( this.getLeft(), other.getLeft() ) == 0 )
        {
            if ( Double.compare( this.getRight(), other.getRight() ) == 0 )
            {
                return 0;
            }
            else if ( this.getRight() < other.getRight() )
            {
                return -1;
            }
            else
            {
                return 1;
            }
        }
        else if ( this.getLeft() < other.getLeft() )
        {
            return -1;
        }
        else
        {
            return 1;
        }
    }

    @Override
    public boolean equals( Object other )
    {
        if ( other instanceof SingleValuedPair )
        {
            SingleValuedPair otherPair = (SingleValuedPair) other;
            return 0 == this.compareTo( otherPair );
        }
        else
        {
            return false;
        }
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( left, right );
    }

    @Override
    public String toString()
    {
        return left + "," + right;
    }
    
    /**
     * Hidden constructor.
     * 
     * @param left the first item
     * @param right the second item
     */

    SingleValuedPair( double left, double right )
    {
        this.left = left;
        this.right = right;
    }

}
