package wres.datamodel.statistics;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import org.apache.commons.lang3.tuple.Pair;

/**
 * A statistic that comprises a list of pairs.
 * 
 * @author james.brown@hydrosolved.com
 */

public class PairedStatistic<S, T> implements Statistic<List<Pair<S, T>>>, Iterable<Pair<S, T>>
{
    /**
     * Line separator for printing.
     */

    private static final String NEWLINE = System.lineSeparator();

    /**
     * The output.
     */

    private final List<Pair<S, T>> statistic;

    /**
     * The metadata associated with the statistic.
     */

    private final StatisticMetadata meta;

    /**
     * Construct the statistic.
     * 
     * @param <S> the type for the left side of the pair
     * @param <T> the type for the right side of the pair
     * @param statistic the verification statistic
     * @param meta the metadata
     * @throws StatisticException if any of the inputs are invalid
     * @return an instance of the output
     */

    public static <S, T> PairedStatistic<S, T> of( final List<Pair<S, T>> statistic, final StatisticMetadata meta )
    {
        return new PairedStatistic<>( statistic, meta );
    }

    @Override
    public StatisticMetadata getMetadata()
    {
        return meta;
    }

    @Override
    public boolean equals( final Object o )
    {
        if ( ! ( o instanceof PairedStatistic ) )
        {
            return false;
        }
        final PairedStatistic<?, ?> v = (PairedStatistic<?, ?>) o;
        boolean start = meta.equals( v.getMetadata() );
        start = start && v.statistic.equals( statistic );
        return start;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( statistic, meta );
    }

    @Override
    public List<Pair<S, T>> getData()
    {
        return statistic;
    }

    @Override
    public Iterator<Pair<S, T>> iterator()
    {
        return statistic.iterator();
    }

    @Override
    public String toString()
    {
        StringJoiner b = new StringJoiner( NEWLINE );
        statistic.forEach( pair -> b.add( pair.toString() ) );
        return b.toString();
    }

    /**
     * Construct the output.
     * 
     * @param statistic the verification output
     * @param meta the metadata
     * @throws StatisticException if any of the inputs are invalid
     */

    private PairedStatistic( final List<Pair<S, T>> statistic, final StatisticMetadata meta )
    {
        //Validate
        if ( Objects.isNull( statistic ) )
        {
            throw new StatisticException( "Specify a non-null output." );
        }
        if ( Objects.isNull( meta ) )
        {
            throw new StatisticException( "Specify non-null metadata." );
        }
        //Set content
        this.statistic = Collections.unmodifiableList( statistic );
        this.meta = meta;

        //Validate content
        statistic.forEach( pair -> {
            if ( Objects.isNull( pair ) )
            {
                throw new StatisticException( "Cannot prescribe a null pair for the input map." );
            }
            if ( Objects.isNull( pair.getLeft() ) )
            {
                throw new StatisticException( "Cannot prescribe a null value for the left of a pair." );
            }
            if ( Objects.isNull( pair.getRight() ) )
            {
                throw new StatisticException( "Cannot prescribe a null value for the right of a pair." );
            }
        } );
    }

}
