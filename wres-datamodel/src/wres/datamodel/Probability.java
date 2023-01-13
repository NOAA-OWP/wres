package wres.datamodel;

import java.util.Objects;

import org.apache.commons.math3.util.Precision;

/**
 * A double value that falls in the unit interval.
 * 
 * @author James Brown
 */
public class Probability implements Comparable<Probability>
{

    /**
     * Lower bound.
     */
    
    public static final Probability ZERO = new Probability( 0.0 );
    
    /**
     * Upper bound.
     */
    
    public static final Probability ONE = new Probability( 1.0 );

    /**
     * The probability.
     */

    private final double prob;

    /**
     * Returns an instance.
     * 
     * @param probability a value within the unit interval
     * @return the probability
     * @throws IllegalArgumentException if the probability falls outside the unit interval
     */

    public static Probability of( final double probability )
    {
        // De-duplicate boundary probabilities
        if( Precision.equals( probability, 0.0 ) )
        {
            return Probability.ZERO;
        }
        else if( Precision.equals( probability, 1.0 ) )
        {
            return Probability.ONE;
        }
        
        return new Probability( probability );
    }

    /**
     * Returns the probability value.
     * 
     * @return the probability
     */

    public double getProbability()
    {
        return this.prob;
    }

    @Override
    public int compareTo( Probability other )
    {
        Objects.requireNonNull( other );

        return Double.compare( this.getProbability(), other.getProbability() );
    }

    @Override
    public boolean equals( Object other )
    {
        if( this == other )
        {
            return true;
        }
        
        if ( ! ( other instanceof Probability ) )
        {
            return false;
        }

        Probability input = (Probability) other;

        return Double.compare( this.getProbability(), input.getProbability() ) == 0;
    }

    @Override
    public int hashCode()
    {
        return Double.hashCode( this.getProbability() );
    }

    @Override
    public String toString()
    {
        return Double.toString( this.getProbability() );
    }

    /**
     * Hidden constructor.
     * 
     * @param members the ensemble members
     * @param labels the ensemble member labels
     * @throws IllegalArgumentException if the labels are non-null and differ in size to the number of members
     */

    private Probability( double probability )
    {
        if( !Double.isFinite( probability ) || probability < 0.0 || probability > 1.0 )
        {
            throw new IllegalArgumentException( "The input probability is not in the unit interval: "+ probability );
        }
    
        this.prob = probability;
    }

}
