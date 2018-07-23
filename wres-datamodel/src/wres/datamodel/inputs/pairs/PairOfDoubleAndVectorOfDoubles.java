package wres.datamodel.inputs.pairs;

import java.util.Arrays;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.stream.Stream;

import wres.datamodel.DataFactory;

/**
 * Immutable pair composed of a primitive double on the left side and a primitive double[] on the right side.
 *
 * @author jesse
 * @author james.brown@hydrosolved.com
 */
public class PairOfDoubleAndVectorOfDoubles implements Comparable<PairOfDoubleAndVectorOfDoubles>
{

    /**
     * The left side of the pair.
     */

    private final double itemOne;

    /**
     * The right side of the pair.
     */

    private final double[] itemTwo;

    /**
     * Builds a pair from primitive left and right values.
     * 
     * @param itemOne the left side
     * @param itemTwo the right side
     * @return a pair
     * @throws NullPointerException if the right input is null
     */

    public static PairOfDoubleAndVectorOfDoubles of( final double itemOne, final double[] itemTwo )
    {
        return new PairOfDoubleAndVectorOfDoubles( itemOne, itemTwo );
    }

    /**
     * Builds a pair from boxed left and right values.
     * 
     * @param itemOne the left side
     * @param itemTwo the right side
     * @return a pair
     * @throws NullPointerException if either input is null
     */

    public static PairOfDoubleAndVectorOfDoubles of( final Double itemOne, final Double[] itemTwo )
    {
        Objects.requireNonNull( itemOne, "Specify a non-null left side for the pair." );  
        
        Objects.requireNonNull( itemTwo, "Specify a non-null right side for the pair." );
        
        final double[] unboxedDoubles = Stream.of( itemTwo )
                                              .mapToDouble( Double::doubleValue )
                                              .toArray();
        return new PairOfDoubleAndVectorOfDoubles( itemOne.doubleValue(), unboxedDoubles );
    }

    /**
     * Return the left value.
     * 
     * @return the left value
     */

    public double getItemOne()
    {
        return itemOne;
    }

    /**
     * Return the right value.
     * 
     * @return the right value
     */
    public double[] getItemTwo()
    {
        return itemTwo.clone();
    }

    @Override
    public String toString()
    {
        StringJoiner s = new StringJoiner( ",",
                                           "key: " + getItemOne() + " value: [",
                                           "]" );
        for ( final double d : getItemTwo() )
        {
            s.add( Double.toString( d ) );
        }
        return s.toString();
    }

    @Override
    public int compareTo( PairOfDoubleAndVectorOfDoubles other )
    {
        // if the instances are the same...
        if ( this == other )
        {
            return 0;
        }
        else if ( Double.compare( this.getItemOne(), other.getItemOne() ) == 0 )
        {
            return DataFactory.compareDoubleArray( this.getItemTwo(),
                                                   other.getItemTwo() );
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
        if ( other instanceof PairOfDoubleAndVectorOfDoubles )
        {
            PairOfDoubleAndVectorOfDoubles otherPair =
                    (PairOfDoubleAndVectorOfDoubles) other;
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
        return Objects.hash( this.getItemOne(), Arrays.hashCode( this.getItemTwo() ) );
    }


    /**
     * Hidden constructor.
     * 
     * @param itemOne the left side
     * @param itemTwo the right side
     * @throws NullPointerException if the right input is null
     */

    private PairOfDoubleAndVectorOfDoubles( final double itemOne, final double[] itemTwo )
    {
        Objects.requireNonNull( itemTwo, "Specify a non-null right side for the pair." );

        this.itemOne = itemOne;
        this.itemTwo = itemTwo.clone();
    }
    
}
