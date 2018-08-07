package wres.datamodel.outputs;

import java.util.ArrayList;
import java.util.Collections;
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
     * Optional metadata that summarizes the collection of outputs.
     */

    private final MetricOutputMetadata metadata;

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
        return ListOfMetricOutput.of( outputs, null );
    }    
    
    /**
     * Returns an instance from the inputs.
     * 
     * @param <T> the metric output type
     * @param outputs the outputs used to populate the list
     * @param metadata optional metadata that summarizes the list of outputs
     * @return an instance of the container
     * @throws NullPointerException if the output is null
     * @throws MetricOutputException if the outputs contain one or more null entries
     */

    public static <T extends MetricOutput<?>> ListOfMetricOutput<T> of( List<T> outputs, MetricOutputMetadata metadata )
    {
        return new ListOfMetricOutput<>( outputs, metadata );
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

    /**
     * Returns the optional metadata that summarizes the list of outputs.
     * 
     * @return the metadata or null
     */

    public MetricOutputMetadata getMetadata()
    {
        return metadata;
    }

    /**
     * Returns <code>true</code> if summary metadata is set for the list of outputs, otherwise <code>false</code>.
     * 
     * @return <code>true</code> if summary metadata is available, otherwise <code>false</code>
     */

    public boolean hasMetadata()
    {
        return Objects.nonNull( metadata );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof ListOfMetricOutput ) )
        {
            return false;
        }

        ListOfMetricOutput<?> in = (ListOfMetricOutput<?>) o;

        return this.getData().equals( in.getData() ) && Objects.equals( this.getMetadata(), in.getMetadata() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.getData(), this.getMetadata() );
    }

    @Override
    public String toString()
    {
        StringBuilder b = new StringBuilder();

        // Add the metadata
        if ( this.hasMetadata() )
        {
            b.append( this.getMetadata() )
             .append( System.lineSeparator() );
        }

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
     * A thread-safe builder that allows for the incremental construction of a {@link ListOfMetricOutput} using one or 
     * more threads.
     */

    public static class ListOfMetricOutputBuilder<T extends MetricOutput<?>>
    {

        /**
         * The metadata.
         */

        private MetricOutputMetadata metadata;

        /**
         * The thread-safe queue of metric outputs.
         */

        private ConcurrentLinkedQueue<T> outputs = new ConcurrentLinkedQueue<>();

        /**
         * Sets the metadata.
         * 
         * @param metadata the metadata
         * @return the builder
         */

        public ListOfMetricOutputBuilder<T> setMetadata( MetricOutputMetadata metadata )
        {
            this.metadata = metadata;

            return this;
        }

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
         * Return the container.
         * 
         * @return the list of outputs
         */

        public ListOfMetricOutput<T> build()
        {
            return ListOfMetricOutput.of( Collections.unmodifiableList( new ArrayList<>( outputs ) ), metadata );
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param outputs the outputs used to populate the list
     * @param metadata the output metadata
     * @throws NullPointerException if the output is null
     * @throws MetricOutputException if the outputs contain one or more null entries
     */

    private ListOfMetricOutput( List<T> outputs, MetricOutputMetadata metadata )
    {
        Objects.requireNonNull( outputs, "Specify a non-null list of outputs." );

        if ( outputs.contains( null ) )
        {
            throw new MetricOutputException( "Cannot build a list of outputs with one or more null entries." );
        }

        this.outputs = Collections.unmodifiableList( outputs );
        
        this.metadata = metadata;
    }

}
