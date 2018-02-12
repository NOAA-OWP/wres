package wres.datamodel;

import java.util.Objects;

import wres.datamodel.inputs.pairs.PairOfDoubles;

/**
 * Immutable implementation of a pair of doubles.
 * 
 * @author jesse
 * @author james.brown@hydrosolved.com
 */

class SafePairOfDoubles implements PairOfDoubles
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
     * Construct a pair of doubles
     * 
     * @param itemOne the first item
     * @param itemTwo the second item
     */
    
    SafePairOfDoubles(double itemOne, double itemTwo)
    {
        this.itemOne = itemOne;
        this.itemTwo = itemTwo;
    }

    @Override
    public double getItemOne()
    {
        return itemOne;
    }

    @Override
    public double getItemTwo()
    {
        return itemTwo;
    }

    @Override
    public int compareTo(PairOfDoubles other)
    {
        if (Double.compare(this.getItemOne(), other.getItemOne()) == 0)
        {
            if (Double.compare(this.getItemTwo(), other.getItemTwo()) == 0)
            {
                return 0;
            }
            else if (this.getItemTwo() < other.getItemTwo())
            {
                return -1;
            }
            else
            {
                return 1;
            }
        }
        else if (this.getItemOne() < other.getItemOne())
        {
            return -1;
        }
        else
        {
            return 1;
        }
    }

    @Override
    public boolean equals(Object other)
    {
        if (other instanceof PairOfDoubles)
        {
            PairOfDoubles otherPair = (PairOfDoubles) other;
            return 0 == this.compareTo(otherPair);
        }
        else
        {
            return false;
        }
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(itemOne, itemTwo);
    }
    
    @Override
    public String toString()
    {
        return itemOne+","+itemTwo;
    }
    
}
