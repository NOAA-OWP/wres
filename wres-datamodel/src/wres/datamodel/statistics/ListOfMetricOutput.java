package wres.datamodel.statistics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;

import wres.datamodel.metadata.MetricOutputMetadata;

/**
 * An immutable list of {@link MetricOutput}. A thread-safe builder is included to build a {@link ListOfMetricOutput}
 * incrementally.
 * 
 * @param <T> the metric output type
 * @author james.brown@hydrosolved.com
 */

public class ListOfMetricOutput<T extends MetricOutput<?>> implements Iterable<T>
{

    /**
     * The immutable internal list of outputs.
     */

    private final List<T> outputs;

    /**
     * Returns an instance from the inputs.
     * 
     * @param <T> the metric output type
     * @param outputs the outputs used to populate the list
     * @return an instance of the container
     * @throws NullPointerException if the output is null
     * @throws MetricOutputException if the outputs contain one or more null entries
     */

    public static <T extends MetricOutput<?>> ListOfMetricOutput<T> of( List<T> outputs )
    {
        return new ListOfMetricOutput<>( outputs );
    }

    /**
     * Returns an immutable iterator over the outputs. An exception will be thrown on attempting to remove elements
     * from the iterator.
     * 
     * @return an immutable iterator over the outputs
     */

    @Override
    public Iterator<T> iterator()
    {
        return outputs.iterator();
    }

    /**
     * Returns an immutable copy of the underlying data.
     * 
     * @return an immutable copy of the data
     */

    public List<T> getData()
    {
        return Collections.unmodifiableList( outputs );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof ListOfMetricOutput ) )
        {
            return false;
        }

        ListOfMetricOutput<?> in = (ListOfMetricOutput<?>) o;

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
        b.delete( lines - System.lineSeparator().length(), lines );

        return b.toString();
    }

    /**
     * <p>A thread-safe builder that allows for the incremental construction of a {@link ListOfMetricOutput}. 
     * For convenience, the outputs may be sorted immediately prior to construction using a prescribed sorter. 
     * For example, to sort the final list by order of the {@link MetricOutputMetadata}:</p>
     * 
     * <p><code>
     * builder.setSorter( ( first, second ) { {@literal ->} first.getMetadata().compareTo( second.getMetadata() ) )
     * </code></p>;
     * 
     * @author james.brown@hydrosolved.com
     */

    public static class ListOfMetricOutputBuilder<T extends MetricOutput<?>>
    {

        /**
         * The thread-safe queue of metric outputs.
         */

        private ConcurrentLinkedQueue<T> outputs = new ConcurrentLinkedQueue<>();

        /**
         * An optional sorter to sort the list on construction.
         */

        private Comparator<? super T> sorter;

        /**
         * Adds an output to the list.
         * 
         * @param output the output to add
         * @return the builder
         */

        public ListOfMetricOutputBuilder<T> addOutput( T output )
        {
            outputs.add( output );

            return this;
        }

        /**
         * Sets a sorter to sort the output on construction. 
         * 
         * @param sorter the sorter
         * @return the builder
         */

        public ListOfMetricOutputBuilder<T> setSorter( Comparator<? super T> sorter )
        {
            this.sorter = sorter;

            return this;
        }

        /**
         * Return the container.
         * 
         * @return the list of outputs
         */

        public ListOfMetricOutput<T> build()
        {
            List<T> sorted = new ArrayList<>( this.outputs );

            if ( Objects.nonNull( this.sorter ) )
            {
                Collections.sort( sorted, this.sorter );
            }

            return new ListOfMetricOutput<>( sorted );
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param outputs the outputs used to populate the list
     * @throws NullPointerException if the output is null
     * @throws MetricOutputException if the outputs contain one or more null entries
     */

    private ListOfMetricOutput( List<T> outputs )
    {
        Objects.requireNonNull( outputs, "Specify a non-null list of outputs." );

        // Set first, then validate contents
        this.outputs = Collections.unmodifiableList( outputs );
        
        if ( this.outputs.contains( null ) )
        {
            throw new MetricOutputException( "Cannot build a list of outputs with one or more null entries." );
        }

    }

}
