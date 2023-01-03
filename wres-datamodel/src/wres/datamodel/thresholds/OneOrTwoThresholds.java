package wres.datamodel.thresholds;

import java.util.Comparator;
import java.util.Objects;

/**
 * An immutable composition of one or two {@link ThresholdOuter}.
 * 
 * @author James Brown
 */

public class OneOrTwoThresholds implements Comparable<OneOrTwoThresholds>
{

    /**
     * The first threshold.
     */

    ThresholdOuter first;

    /**
     * The second threshold.
     */

    ThresholdOuter second;

    /**
     * Returns an instance with one {@link ThresholdOuter}.
     * 
     * @param first the first threshold
     * @return a composition of one threshold
     * @throws NullPointerException if the threshold is null
     */

    public static OneOrTwoThresholds of( ThresholdOuter first )
    {
        return OneOrTwoThresholds.of( first, null );
    }

    /**
     * Returns an instance with one or two {@link ThresholdOuter}.
     * 
     * @param first the first threshold
     * @param second the second threshold, may be null
     * @return a composition of one or two thresholds
     * @throws NullPointerException if the first threshold is null
     */

    public static OneOrTwoThresholds of( ThresholdOuter first, ThresholdOuter second )
    {
        return new OneOrTwoThresholds( first, second );
    }

    /**
     * Return the first {@link ThresholdOuter}.
     * 
     * @return the first threshold
     */

    public ThresholdOuter first()
    {
        return this.first;
    }

    /**
     * Returns the second {@link ThresholdOuter} or null.
     * 
     * @return the second threshold or null
     */

    public ThresholdOuter second()
    {
        return this.second;
    }

    /**
     * Returns <code>true</code> if the composition contains two {@link ThresholdOuter}, <code>false</code> if it contains
     * only one.
     * 
     * @return true if the composition has two thresholds, otherwise false
     */

    public boolean hasTwo()
    {
        return Objects.nonNull( second );
    }

    @Override
    public String toString()
    {
        if ( hasTwo() )
        {
            return this.first.toString() + " AND " + this.second.toString();
        }
        return this.first.toString();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof OneOrTwoThresholds ) )
        {
            return false;
        }
        OneOrTwoThresholds in = (OneOrTwoThresholds) o;

        return Objects.equals( this.first, in.first )
               && Objects.equals( this.second, in.second );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.first, this.second );
    }

    @Override
    public int compareTo( OneOrTwoThresholds o )
    {
        Objects.requireNonNull( o, "Specify a non-null instance of thresholds for comparison." );

        int returnMe = this.first.compareTo( o.first() );

        if ( returnMe != 0 )
        {
            return returnMe;
        }

        return Objects.compare( this.second,
                                o.second(),
                                Comparator.nullsFirst( Comparator.naturalOrder() ) );
    }

    /**
     * Build a container with one or two {@link ThresholdOuter}.
     * 
     * @param first the first threshold
     * @param second the second threshold, may be null
     * @return a composition of one or two thresholds
     * @throws NullPointerException if the first threshold is null
     */

    private OneOrTwoThresholds( ThresholdOuter first, ThresholdOuter second )
    {
        Objects.requireNonNull( first, "Specify a non-null primary threshold." );
        this.first = first;
        this.second = second;
    }

}

