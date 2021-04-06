package wres.datamodel.pools.pairs;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.time.TimeSeries;

/**
 * A collection of cross-pairs whose main pairs and baseline pairs are accessible.
 * 
 * @param <L> the left type of event value
 * @param <R> the right type of event value
 */

public class CrossPairs<L, R>
{
    /**
     * The main pairs.
     */

    private final List<TimeSeries<Pair<L, R>>> mainPairs;

    /**
     * The baseline pairs.
     */

    private final List<TimeSeries<Pair<L, R>>> baselinePairs;

    /**
     * Hidden constructor.
     * 
     * @param <L> the left type of data on one side of a pairing
     * @param <R> the right type of data on one side of a pairing
     * @param mainPairs the mains pairs
     * @param baselinePairs the baseline pairs
     * @return a lightweight container of the two sets of pairs
     * @throws NullPointerException if either input is null
     */

    public static <L, R> CrossPairs<L, R> of( List<TimeSeries<Pair<L, R>>> mainPairs,
                                              List<TimeSeries<Pair<L, R>>> baselinePairs )
    {
        return new CrossPairs<>( mainPairs, baselinePairs );
    }

    /**
     * Returns the main pairs, cross paired against the baseline pairs.
     * 
     * @return the main pairs
     */

    public List<TimeSeries<Pair<L, R>>> getMainPairs()
    {
        return this.mainPairs;
    }

    /**
     * Returns the baseline pairs, cross paired against the main pairs.
     * 
     * @return the baseline pairs
     */

    public List<TimeSeries<Pair<L, R>>> getBaselinePairs()
    {
        return this.baselinePairs;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof CrossPairs ) )
        {
            return false;
        }

        if ( o == this )
        {
            return true;
        }

        CrossPairs<?, ?> in = (CrossPairs<?, ?>) o;

        return in.getMainPairs().equals( this.getMainPairs() )
               && in.getBaselinePairs().equals( this.getBaselinePairs() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.getMainPairs(), this.getBaselinePairs() );
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                                                                            .append( "mainPairs", this.getMainPairs() )
                                                                            .append( "baselinePairs",
                                                                                     this.getBaselinePairs() )
                                                                            .toString();
    }

    /**
     * Hidden constructor.
     * 
     * @param <L> the left type of data on one side of a pairing
     * @param <R> the right type of data on one side of a pairing
     * @param mainPairs the main pairs
     * @param baselinePairs the baseline pairs
     * @throws NullPointerException if either input is null
     */

    private CrossPairs( List<TimeSeries<Pair<L, R>>> mainPairs, List<TimeSeries<Pair<L, R>>> baselinePairs )
    {
        Objects.requireNonNull( mainPairs );
        Objects.requireNonNull( baselinePairs );

        this.mainPairs = Collections.unmodifiableList( mainPairs );
        this.baselinePairs = Collections.unmodifiableList( baselinePairs );
    }
}