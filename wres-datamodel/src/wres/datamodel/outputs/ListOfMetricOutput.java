package wres.datamodel.outputs;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import wres.datamodel.metadata.MetricOutputMetadata;

/**
 * An immutable list of {@link MetricOutput}.
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
     * Metadata that summarizes the collection of outputs.
     */

    private final MetricOutputMetadata metadata;

    /**
     * Returns an instance from the inputs.
     * 
     * @param <T> the metric output type
     * @param outputs the outputs used to populate the list
     * @param metadata the output metadata
     * @return an instance of the container
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
     * Returns the metadata that summarizes the collection of outputs.
     * 
     * @return the metadata
     */

    public MetricOutputMetadata getMetadata()
    {
        return metadata;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof ListOfMetricOutput ) )
        {
            return false;
        }

        ListOfMetricOutput<?> in = (ListOfMetricOutput<?>) o;

        return this.getData().equals( in.getData() ) && this.getMetadata().equals( in.getMetadata() );
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
        
        // Add the metdata
        b.append( this.getMetadata() )
         .append( System.lineSeparator() );
        
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
     * Hidden constructor.
     * 
     * @param outputs the outputs used to populate the list
     * @param metadata the output metadata
     */

    private ListOfMetricOutput( List<T> outputs, MetricOutputMetadata metadata )
    {
        this.outputs = Collections.unmodifiableList( outputs );
        this.metadata = metadata;
    }

}
