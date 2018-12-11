package wres.datamodel.sampledata.pairs;

import java.util.Arrays;
import java.util.Objects;
import java.util.StringJoiner;

import wres.datamodel.sampledata.SampleDataException;

/**
 * Immutable pair of multicategory outcomes. The left and right sides contain an equivalent number of elements. When
 * the inputs contain more than one element, exactly one of them must be <code>true</code>, and all others must be 
 * <code>false</code>.
 *
 * @author james.brown@hydrosolved.com
 */
public class MulticategoryPair implements Pair<boolean[],boolean[]>
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
     * @throws SampleDataException if the left and right sides are not equal in size or one input contains more than 
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
     * @throws SampleDataException if the left and right sides are not equal in size or one input contains more than 
     *            one category, but does not contain exactly one occurrence among them (<code>true</code> value)
     */

    private MulticategoryPair( final boolean[] left, final boolean[] right )
    {
        Objects.requireNonNull( left, "Specify a non-null left side for the multicategory pair." );

        Objects.requireNonNull( right, "Specify a non-null right side for the multicategory pair." );

        if ( left.length != right.length )
        {
            throw new SampleDataException( "The input should have an equivalent number of outcomes on each "
                                            + "side: ["
                                            + left.length
                                            + ", "
                                            + right.length
                                            + "]." );
        }

        // One occurrence is required for outcomes that comprise more than one category
        if ( left.length > 1 )
        {
            checkInput( left, "left" );
            checkInput( right, "right" );
        }
        this.left = left;
        this.right = right;
    }

    /**
     * Checks the input for precisely one occurrence and throws an exception otherwise. 
     * 
     * @param checkMe the array to check
     * @param messageHelper the string used to identify the input
     * @throws SampleDataException if the input array does not contain exactly one occurrence
     */
    
    private void checkInput( boolean[] checkMe, String messageHelper )
    {
        int count = 0;
        for ( boolean next : checkMe )
        {
            if ( next )
            {
                count++;
            }
        }
        if ( count != 1 )
        {
            throw new SampleDataException( "The "+messageHelper+" input must contain exactly one occurrence: " + count
                                            + "." );
        }        
    }
    
}
