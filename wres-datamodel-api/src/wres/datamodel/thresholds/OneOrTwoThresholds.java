package wres.datamodel.thresholds;

import java.util.Comparator;
import java.util.Objects;

/**
 * A composition of one or two {@link Threshold}.
 * 
 * @author james.brown@hydrosolved.com
 */

public interface OneOrTwoThresholds extends Comparable<OneOrTwoThresholds>
{

    /**
     * Return the first {@link Threshold}.
     * 
     * @return the first threshold
     */

    Threshold first();

    /**
     * Returns the second {@link Threshold} or null.
     * 
     * @return the second threshold or null
     */

    Threshold second();

    /**
     * Returns <code>true</code> if the composition contains two {@link Threshold}, <code>false</code> if it contains
     * only one.
     * 
     * @return true if the composition has two thresholds, otherwise false
     */

    boolean hasTwo();

    /**
     * Returns a string representation of the {@link OneOrTwoThresholds} that contains only alphanumeric characters A-Z, a-z, 
     * and 0-9 and, additionally, the underscore character to separate between elements, and the period character as
     * a decimal separator.
     * 
     * @return a safe string representation
     */

    String toStringSafe();
    
    /**
     * Returns a string representation of the {@link OneOrTwoThresholds} without any units. This is useful when forming string
     * representions of a collection of {@link Threshold} and abstracting the common units to a higher level.
     * 
     * @return a string without any units
     */

    String toStringWithoutUnits();

    /**
     * Returns an instance with one {@link Threshold}.
     * 
     * @param first the first threshold
     * @return a composition of one threshold
     * @throws NullPointerException if the threshold is null
     */

    static OneOrTwoThresholds of( Threshold first )
    {
        return OneOrTwoThresholds.of( first, null );
    }

    /**
     * Returns an instance with one or two {@link Threshold}.
     * 
     * @param first the first threshold
     * @param second the second threshold, may be null
     * @return a composition of one or two thresholds
     * @throws NullPointerException if the first threshold is null
     */

    static OneOrTwoThresholds of( Threshold first, Threshold second )
    {
        final class DefaultThresholds implements OneOrTwoThresholds
        {
            /**
             * The first threshold.
             */

            Threshold first;

            /**
             * The second threshold.
             */

            Threshold second;


            /**
             * Build a container with one or two {@link Threshold}.
             * 
             * @param first the first threshold
             * @param second the second threshold, may be null
             * @return a composition of one or two thresholds
             * @throws NullPointerException if the first threshold is null
             */

            public DefaultThresholds( Threshold first, Threshold second )
            {
                Objects.requireNonNull( first, "Specify a non-null primary threshold." );
                this.first = first;
                this.second = second;
            }

            @Override
            public Threshold first()
            {
                return this.first;
            }

            @Override
            public Threshold second()
            {
                return this.second;
            }

            @Override
            public String toString()
            {
                if ( hasTwo() )
                {
                    return first.toString() + " AND " + second.toString();
                }
                return first.toString();
            }           

            @Override
            public String toStringWithoutUnits()
            {
                if ( hasTwo() )
                {
                    return first.toStringWithoutUnits() + " AND " + second.toStringWithoutUnits();
                }
                return first.toStringWithoutUnits();
            }

            @Override
            public String toStringSafe()
            {
                if ( hasTwo() )
                {
                    return first.toStringSafe() + "_AND_" + second.toStringSafe();
                }
                return first.toStringSafe();
            }

            @Override
            public boolean equals( Object o )
            {
                if ( ! ( o instanceof DefaultThresholds ) )
                {
                    return false;
                }
                DefaultThresholds in = (DefaultThresholds) o;

                return Objects.equals( this.first, in.first ) 
                        && Objects.equals( this.second, in.second );
            }

            @Override
            public int hashCode()
            {
                return Objects.hash( this.first, this.second );
            }

            @Override
            public boolean hasTwo()
            {
                return Objects.nonNull( second );
            }

            @Override
            public int compareTo( OneOrTwoThresholds o )
            {
                Objects.requireNonNull( o, "Specify a non-null instance of thresholds for comparison." );
                
                int returnMe = this.first.compareTo( o.first() );
                
                if( returnMe!= 0 )
                {
                    return returnMe;
                }
                
                return Objects.compare( this.second,
                                        o.second(),
                                        Comparator.nullsFirst( Comparator.naturalOrder() ) );
            }
        }
        return new DefaultThresholds( first, second );
    }

}

