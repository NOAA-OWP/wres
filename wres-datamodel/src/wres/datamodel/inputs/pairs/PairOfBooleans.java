package wres.datamodel.inputs.pairs;

/**
 * Immutable pair of primitive boolean values.
 *
 * Requested for metrics
 *
 * @author jesse
 * @author james.brown@hydrosolved.com
 */

public class PairOfBooleans
{
    /**
     * The left side.
     */

    private final boolean left;

    /**
     * The right side.
     */

    private final boolean right;

    /**
     * Returns an instance from the input.
     * 
     * @param left the left side of the pair
     * @param right the right side of the pair
     * @return a pair of booleans
     */

    public static PairOfBooleans of( boolean left, boolean right )
    {
        return new PairOfBooleans( left, right );
    }

    /**
     * Returns the left side of the pair.
     * 
     * @return the left side
     */

    public boolean getItemOne()
    {
        return left;
    }

    /**
     * Returns the right side of the pair.
     * 
     * @return the right side
     */

    public boolean getItemTwo()
    {
        return right;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof PairOfBooleans ) )
        {
            return false;
        }
        PairOfBooleans b = (PairOfBooleans) o;
        return b.getItemOne() == getItemOne() && b.getItemTwo() == getItemTwo();
    }

    @Override
    public int hashCode()
    {
        return Boolean.hashCode( getItemOne() ) + Boolean.hashCode( getItemTwo() );
    }

    /**
     * Hidden constructor.
     * 
     * @param left the left
     * @param right the right
     */

    private PairOfBooleans( boolean left, boolean right )
    {
        this.left = left;
        this.right = right;
    }

}
