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
public class PairOfDoubles implements Comparable<PairOfDoubles>
{

    /**
     * The first item.
     */

    private final double itemOne;

    /**
     * The second item.
     */

    private final double itemTwo;

    /**
     * Returns an instance from the input.
     * 
     * @param left the left side of the pair
     * @param right the right side of the pair
     * @return a pair of booleans
     */

    public static PairOfDoubles of( double left, double right )
    {
        return new PairOfDoubles( left, right );
    }

    /**
     * Returns the first value, i.e. the obs in the above example.
     * 
     * @return the first value
     */

    public double getItemOne()
    {
        return itemOne;
    }

    /**
     * Returns the second value, i.e. the fcst in the above example.
     * 
     * @return the second value
     */

    public double getItemTwo()
    {
        return itemTwo;
    }

    @Override
    public int compareTo( PairOfDoubles other )
    {
        if ( Double.compare( this.getItemOne(), other.getItemOne() ) == 0 )
        {
            if ( Double.compare( this.getItemTwo(), other.getItemTwo() ) == 0 )
            {
                return 0;
            }
            else if ( this.getItemTwo() < other.getItemTwo() )
            {
                return -1;
            }
            else
            {
                return 1;
            }
        }
        else if ( this.getItemOne() < other.getItemOne() )
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
        if ( other instanceof PairOfDoubles )
        {
            PairOfDoubles otherPair = (PairOfDoubles) other;
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
        return Objects.hash( itemOne, itemTwo );
    }

    @Override
    public String toString()
    {
        return itemOne + "," + itemTwo;
    }

    /**
     * Hidden constructor.
     * 
     * @param itemOne the first item
     * @param itemTwo the second item
     */

    private PairOfDoubles( double itemOne, double itemTwo )
    {
        this.itemOne = itemOne;
        this.itemTwo = itemTwo;
    }

}
