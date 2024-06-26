package wres.datamodel.pools.pairs;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import wres.datamodel.time.TimeSeries;

/**
 * A collection of cross-pairs whose pairs are accessible.
 *
 * @param <S> the time-series event value types for the first time-series
 * @param <T> the time-series event value type for the second time-series
 * @author James Brown
 */

public class CrossPairs<S, T>
{
    /** The first pairs. */
    private final List<TimeSeries<S>> firstPairs;

    /** The second pairs. */
    private final List<TimeSeries<T>> secondPairs;

    /**
     * Hidden constructor.
     *
     * @param <S> the time-series event value type for the first series
     * @param <T> the time-series event value type for the second series
     * @param firstPairs the first pairs
     * @param secondPairs the second pairs
     * @return a lightweight container of the two sets of cross-pairs
     * @throws NullPointerException if either input is null
     */

    public static <S, T> CrossPairs<S, T> of( List<TimeSeries<S>> firstPairs,
                                              List<TimeSeries<T>> secondPairs )
    {
        return new CrossPairs<>( firstPairs, secondPairs );
    }

    /**
     * Returns the first pairs, cross paired against the second pairs.
     *
     * @return the first pairs
     */

    public List<TimeSeries<S>> getFirstPairs()
    {
        return this.firstPairs;
    }

    /**
     * Returns the second pairs, cross paired against the first pairs.
     *
     * @return the second pairs
     */

    public List<TimeSeries<T>> getSecondPairs()
    {
        return this.secondPairs;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( !( o instanceof CrossPairs<?, ?> in ) )
        {
            return false;
        }

        if ( o == this )
        {
            return true;
        }

        return in.getFirstPairs()
                 .equals( this.getFirstPairs() )
               && in.getSecondPairs()
                    .equals( this.getSecondPairs() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.getFirstPairs(), this.getSecondPairs() );
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                .append( "firstPairs", this.getFirstPairs() )
                .append( "secondPairs", this.getSecondPairs() )
                .toString();
    }

    /**
     * Hidden constructor.
     *
     * @param firstPairs the first pairs
     * @param secondPairs the second pairs
     * @throws NullPointerException if either input is null
     */

    private CrossPairs( List<TimeSeries<S>> firstPairs, List<TimeSeries<T>> secondPairs )
    {
        Objects.requireNonNull( firstPairs );
        Objects.requireNonNull( secondPairs );

        this.firstPairs = Collections.unmodifiableList( firstPairs );
        this.secondPairs = Collections.unmodifiableList( secondPairs );
    }
}