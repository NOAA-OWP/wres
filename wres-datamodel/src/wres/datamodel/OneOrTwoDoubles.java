package wres.datamodel;

import java.util.Objects;

/**
 * Immutable store of one or two double values.
 * 
 * @author james.brown@hydrosolved.com
 */

public class OneOrTwoDoubles implements Comparable<OneOrTwoDoubles>
{

    /**
     * First value.
     */

    private final Double first;

    /**
     * Second value.
     */

    private final Double second;

    /**
     * Returns a default implementation of {@link OneOrTwoDoubles}.
     * 
     * @param first the first double
     * @return a default implementation
     * @throws NullPointerException if the value is null
     */

    public static OneOrTwoDoubles of( Double first )
    {
        return OneOrTwoDoubles.of( first, null );
    }

    /**
     * Returns a default implementation of {@link OneOrTwoDoubles}.
     * 
     * @param first the first double
     * @param second the second double
     * @return a default implementation
     * @throws NullPointerException if the first value is null
     */

    public static OneOrTwoDoubles of( Double first, Double second )
    {
        return new OneOrTwoDoubles( first, second );
    }

    /**
     * Returns <code>true</code> if the store contains two values, <code>false</code> if it contains only one value.
     * 
     * @return true if the store contains two values, otherwise false
     */

    public boolean hasTwo()
    {
        return Objects.nonNull( second );
    }

    /**
     * Returns the first value in the store. This is always defined.
     * 
     * @return the first value
     */

    public Double first()
    {
        return first;
    }

    /**
     * Returns the second value in the store or null. This is always null when {@link #hasTwo()} is <code>false</code>,
     * otherwise always non-null.
     * 
     * @return the second value or null
     */

    public Double second()
    {
        return second;
    }

    @Override
    public int compareTo( OneOrTwoDoubles o )
    {
        Objects.requireNonNull( o, "Specify non-null input for comparison." );

        // Same structure
        int returnMe = Boolean.compare( this.hasTwo(), o.hasTwo() );
        if ( returnMe != 0 )
        {
            return returnMe;
        }

        // Same values
        returnMe = Double.compare( this.first(), o.first() );
        if ( returnMe != 0 )
        {
            return returnMe;
        }

        if ( this.hasTwo() )
        {
            return Double.compare( this.second(), o.second() );
        }

        return 0;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof OneOrTwoDoubles ) )
        {
            return false;
        }

        OneOrTwoDoubles in = (OneOrTwoDoubles) o;

        return Objects.equals( first(), in.first() ) && Objects.equals( second(), in.second() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( first(), second() );
    }

    /**
     * Construct.
     * 
     * @param first the first value, cannot be null
     * @param second the second value
     */

    private OneOrTwoDoubles( Double first, Double second )
    {
        Objects.requireNonNull( first, "Specify a non-null first value." );

        this.first = first;
        this.second = second;
    }

}
