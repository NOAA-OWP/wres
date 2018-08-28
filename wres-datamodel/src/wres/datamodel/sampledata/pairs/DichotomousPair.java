package wres.datamodel.sampledata.pairs;

/**
 * An immutable pair of values associated with a dichotomous variable.
 *
 * @author james.brown@hydrosolved.com
 */

public class DichotomousPair
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

    public static DichotomousPair of( boolean left, boolean right )
    {
        return new DichotomousPair( left, right );
    }

    /**
     * Returns the left side of the pair.
     * 
     * @return the left side
     */

    public boolean getLeft()
    {
        return left;
    }

    /**
     * Returns the right side of the pair.
     * 
     * @return the right side
     */

    public boolean getRight()
    {
        return right;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof DichotomousPair ) )
        {
            return false;
        }
        DichotomousPair b = (DichotomousPair) o;
        return b.getLeft() == getLeft() && b.getRight() == getRight();
    }

    @Override
    public int hashCode()
    {
        return Boolean.hashCode( getLeft() ) + Boolean.hashCode( getRight() );
    }

    /**
     * Hidden constructor.
     * 
     * @param left the left
     * @param right the right
     */

    private DichotomousPair( boolean left, boolean right )
    {
        this.left = left;
        this.right = right;
    }

}
