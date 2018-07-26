package wres.datamodel.inputs.pairs;

import java.util.Arrays;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.stream.Stream;

import wres.datamodel.DataFactory;

/**
 * Immutable pair composed of a primitive double on the left side and an ensemble of doubles on the right side.
 *
 * @author jesse
 * @author james.brown@hydrosolved.com
 */
public class EnsemblePair implements Comparable<EnsemblePair>
{

    /**
     * The left side of the pair.
     */

    private final double left;

    /**
     * The right side of the pair.
     */

    private final double[] right;

    /**
     * Builds a pair from primitive left and right values.
     * 
     * @param left the left side
     * @param right the right side
     * @return a pair
     * @throws NullPointerException if the right input is null
     */

    public static EnsemblePair of( final double left, final double[] right )
    {
        return new EnsemblePair( left, right );
    }

    /**
     * Builds a pair from boxed left and right values.
     * 
     * @param left the left side
     * @param right the right side
     * @return a pair
     * @throws NullPointerException if either input is null
     */

    public static EnsemblePair of( final Double left, final Double[] right )
    {       
        Objects.requireNonNull( right, "Specify a non-null right side for the pair." );
        
        final double[] unboxedDoubles = Stream.of( right )
                                              .mapToDouble( Double::doubleValue )
                                              .toArray();
        return new EnsemblePair( left.doubleValue(), unboxedDoubles );
    }

    /**
     * Return the left value.
     * 
     * @return the left value
     */

    public double getLeft()
    {
        return left;
    }

    /**
     * Return the right value.
     * 
     * @return the right value
     */
    public double[] getRight()
    {
        return right.clone();
    }

    @Override
    public String toString()
    {
        StringJoiner s = new StringJoiner( ",",
                                           "key: " + getLeft() + " value: [",
                                           "]" );
        for ( final double d : getRight() )
        {
            s.add( Double.toString( d ) );
        }
        return s.toString();
    }

    @Override
    public int compareTo( EnsemblePair other )
    {
        // if the instances are the same...
        if ( this == other )
        {
            return 0;
        }
        else if ( Double.compare( this.getLeft(), other.getLeft() ) == 0 )
        {
            return DataFactory.compareDoubleArray( this.getRight(),
                                                   other.getRight() );
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
        if ( other instanceof EnsemblePair )
        {
            EnsemblePair otherPair =
                    (EnsemblePair) other;
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
        return Objects.hash( this.getLeft(), Arrays.hashCode( this.getRight() ) );
    }

    /**
     * Hidden constructor.
     * 
     * @param itemOne the left side
     * @param itemTwo the right side
     * @throws NullPointerException if the right input is null
     */

    private EnsemblePair( final double itemOne, final double[] itemTwo )
    {
        Objects.requireNonNull( itemTwo, "Specify a non-null right side for the pair." );

        this.left = itemOne;
        this.right = itemTwo.clone();
    }
    
}
