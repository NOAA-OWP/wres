package wres.datamodel.statistics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;

import wres.datamodel.metadata.StatisticMetadata;

/**
 * An immutable list of {@link Statistic}. A thread-safe builder is included to build a {@link ListOfStatistics}
 * incrementally.
 * 
 * @param <T> the statistic type
 * @author james.brown@hydrosolved.com
 */

public class ListOfStatistics<T extends Statistic<?>> implements Iterable<T>
{

    /**
     * The immutable internal list of statistic.
     */

    private final List<T> statistics;

    /**
     * Returns an instance from the inputs.
     * 
     * @param <T> the statistic type
     * @param statistic the statistic used to populate the list
     * @return an instance of the container
     * @throws NullPointerException if the statistic is null
     * @throws StatisticException if the outputs contain one or more null entries
     */

    public static <T extends Statistic<?>> ListOfStatistics<T> of( List<T> statistic )
    {
        return new ListOfStatistics<>( statistic );
    }

    /**
     * Returns an immutable iterator over the statistic. An exception will be thrown on attempting to remove elements
     * from the iterator.
     * 
     * @return an immutable iterator over the statistic
     */

    @Override
    public Iterator<T> iterator()
    {
        return statistics.iterator();
    }

    /**
     * Returns an immutable copy of the underlying data.
     * 
     * @return an immutable copy of the data
     */

    public List<T> getData()
    {
        return Collections.unmodifiableList( statistics );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof ListOfStatistics ) )
        {
            return false;
        }

        ListOfStatistics<?> in = (ListOfStatistics<?>) o;

        return this.getData().equals( in.getData() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.getData() );
    }

    @Override
    public String toString()
    {
        StringBuilder b = new StringBuilder();

        // Add the data
        this.forEach( element -> b.append( "{" )
                                  .append( element.getMetadata() )
                                  .append( ": " )
                                  .append( element.getData() )
                                  .append( "}" )
                                  .append( System.lineSeparator() ) );

        // Remove trailing newline
        int lines = b.length();
        if( lines > 0 )
        {
            b.delete( lines - System.lineSeparator().length(), lines );
        }
        
        return b.toString();
    }

    /**
     * <p>A thread-safe builder that allows for the incremental construction of a {@link ListOfStatistics}. 
     * For convenience, the outputs may be sorted immediately prior to construction using a prescribed sorter. 
     * For example, to sort the final list by order of the {@link StatisticMetadata}:</p>
     * 
     * <p><code>
     * builder.setSorter( ( first, second ) { {@literal ->} first.getMetadata().compareTo( second.getMetadata() ) )
     * </code></p>;
     * 
     * @author james.brown@hydrosolved.com
     */

    public static class ListOfStatisticsBuilder<T extends Statistic<?>>
    {

        /**
         * The thread-safe queue of metric outputs.
         */

        private ConcurrentLinkedQueue<T> statistics = new ConcurrentLinkedQueue<>();

        /**
         * An optional sorter to sort the list on construction.
         */

        private Comparator<? super T> sorter;

        /**
         * Adds an statistic to the list.
         * 
         * @param statistic the statistic to add
         * @return the builder
         */

        public ListOfStatisticsBuilder<T> addStatistic( T statistic )
        {
            statistics.add( statistic );

            return this;
        }

        /**
         * Sets a sorter to sort the output on construction. 
         * 
         * @param sorter the sorter
         * @return the builder
         */

        public ListOfStatisticsBuilder<T> setSorter( Comparator<? super T> sorter )
        {
            this.sorter = sorter;

            return this;
        }

        /**
         * Return the container.
         * 
         * @return the list of outputs
         */

        public ListOfStatistics<T> build()
        {
            List<T> sorted = new ArrayList<>( this.statistics );

            if ( Objects.nonNull( this.sorter ) )
            {
                Collections.sort( sorted, this.sorter );
            }

            return new ListOfStatistics<>( sorted );
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param statistics the statistics used to populate the list
     * @throws NullPointerException if the output is null
     * @throws StatisticException if the outputs contain one or more null entries
     */

    ListOfStatistics( List<T> statistics )
    {
        Objects.requireNonNull( statistics, "Specify a non-null list of outputs." );

        // Set first, then validate contents
        this.statistics = Collections.unmodifiableList( statistics );
        
        if ( this.statistics.contains( (T) null ) )
        {
            throw new StatisticException( "Cannot build a list of outputs with one or more null entries." );
        }

    }

}
