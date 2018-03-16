package wres.datamodel;

import java.util.Objects;

/**
 * An immutable implementation of {@link OneOrTwoDoubles}.
 * 
 * @author james.brown@hydrosolved.com
 */

class SafeOneOrTwoDoubles implements OneOrTwoDoubles
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

    static SafeOneOrTwoDoubles of( Double first )
    {
        return of( first, null );
    }    
    
    /**
     * Returns a default implementation of {@link OneOrTwoDoubles}.
     * 
     * @param first the first double
     * @param second the second double
     * @return a default implementation
     * @throws NullPointerException if the first value is null
     */

    static SafeOneOrTwoDoubles of( Double first, Double second )
    {
        return new SafeOneOrTwoDoubles( first, second );
    }

    @Override
    public boolean hasTwo()
    {
        return Objects.nonNull( second );
    }

    @Override
    public Double first()
    {
        return first;
    }

    @Override
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
        
        if( this.hasTwo() )
        {
            return Double.compare( this.second(), o.second() );
        }
        
        return 0;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof SafeOneOrTwoDoubles ) )
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

    private SafeOneOrTwoDoubles( Double first, Double second )
    {
        Objects.requireNonNull( first, "Specify a non-null first value." );

        this.first = first;
        this.second = second;
    }

}