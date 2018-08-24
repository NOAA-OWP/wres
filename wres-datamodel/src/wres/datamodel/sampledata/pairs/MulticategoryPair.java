package wres.datamodel.sampledata.pairs;

import java.util.Arrays;
import java.util.Objects;
import java.util.StringJoiner;

import wres.datamodel.sampledata.MetricInputException;

/**
 * Immutable pair of multicategory outcomes. The left and right sides contain an equivalent number of elements. When
 * the inputs contain more than one element, exactly one of them must be <code>true</code>, and all others must be 
 * <code>false</code>.
 *
 * @author james.brown@hydrosolved.com
 */
public class MulticategoryPair
{

    /**
     * The left side of the pair.
     */

    private final boolean[] left;

    /**
     * The right side of the pair.
     */

    private final boolean[] right;

    /**
     * Builds a pair from a primitive array of the left and right outcomes.
     * 
     * @param left the left side of the pair
     * @param right the right side of the pair
     * @throws NullPointerException if either input is null
     * @throws MetricInputException if the left and right sides are not equal in size or one input contains more than 
     *            one category, but does not contain exactly one occurrence among them (<code>true</code> value)
     * @return a multicategory pair
     */

    public static MulticategoryPair of( boolean[] left, boolean[] right )
    {
        return new MulticategoryPair( left, right );
    }

    /**
     * Returns the left side of the pair.
     * 
     * @return the left value
     */

    public boolean[] getLeft()
    {
        return left.clone();
    }

    /**
     * Returns the right side of the pair.
     * 
     * @return the right value
     */

    public boolean[] getRight()
    {
        return right.clone();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof MulticategoryPair ) )
        {
            return false;
        }
        MulticategoryPair b = (MulticategoryPair) o;
        return Arrays.equals( b.getLeft(), getLeft() ) && Arrays.equals( b.getRight(), getRight() );
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode( getLeft() ) + Arrays.hashCode( getRight() );
    }
    
    @Override
    public String toString()
    {
        StringJoiner s = new StringJoiner( ",","{", "}" );
        for ( final boolean d : getLeft() )
        {
            s.add( Boolean.toString( d ) );
        }
        for ( final boolean d : getRight() )
        {
            s.add( Boolean.toString( d ) );
        }
        
        return s.toString();
    }

    /**
     * Hidden constructor.
     * 
     * @param left the left side of the pair
     * @param right the right side of the pair
     * @throws NullPointerException if either input is null
     * @throws MetricInputException if the left and right sides are not equal in size or one input contains more than 
     *            one category, but does not contain exactly one occurrence among them (<code>true</code> value)
     */

    private MulticategoryPair( final boolean[] left, final boolean[] right )
    {
        Objects.requireNonNull( left, "Specify a non-null left side for the multicategory pair." );

        Objects.requireNonNull( right, "Specify a non-null right side for the multicategory pair." );

        if ( left.length != right.length )
        {
            throw new MetricInputException( "The input should have an equivalent number of outcomes on each "
                                            + "side: ["
                                            + left.length
                                            + ", "
                                            + right.length
                                            + "]." );
        }

        // One occurrence is required for outcomes that comprise more than one category
        if ( left.length > 1 )
        {
            int leftCount = 0;
            for ( boolean next : left )
            {
                if ( next )
                {
                    leftCount++;
                }
            }
            if ( leftCount != 1 )
            {
                throw new MetricInputException( "The left input must contain exactly one occurrence: " + leftCount
                                                + "." );
            }
            int rightCount = 0;
            for ( boolean next : right )
            {
                if ( next )
                {
                    rightCount++;
                }
            }
            if ( rightCount != 1 )
            {
                throw new MetricInputException( "The right input must contain exactly one occurrence: " + rightCount
                                                + "." );
            }
        }
        this.left = left;
        this.right = right;
    }

}
