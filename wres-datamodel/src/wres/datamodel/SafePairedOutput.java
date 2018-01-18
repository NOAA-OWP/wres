package wres.datamodel;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.MetricOutputException;
import wres.datamodel.outputs.PairedOutput;

/**
 * A read-only list of paired outputs. The pairs may or may not be immutable, but the list is read only.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.4
 */

class SafePairedOutput<S, T> implements PairedOutput<S, T>
{

    /**
     * Line separator for printing.
     */

    private static final String NEWLINE = System.lineSeparator();

    /**
     * The output.
     */

    private final List<Pair<S, T>> output;

    /**
     * The metadata associated with the output.
     */

    private final MetricOutputMetadata meta;

    @Override
    public MetricOutputMetadata getMetadata()
    {
        return meta;
    }

    @Override
    public boolean equals( final Object o )
    {
        if ( ! ( o instanceof SafePairedOutput ) )
        {
            return false;
        }
        final SafePairedOutput<?, ?> v = (SafePairedOutput<?, ?>) o;
        boolean start = meta.equals( v.getMetadata() );
        start = start && v.output.equals( output );
        return start;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( output, meta );
    }

    @Override
    public List<Pair<S, T>> getData()
    {
        return output;
    }

    @Override
    public Iterator<Pair<S, T>> iterator()
    {
        return output.iterator();
    }

    @Override
    public String toString()
    {
        StringJoiner b = new StringJoiner( NEWLINE );
        output.forEach( pair -> b.add( pair.toString() ) );
        return b.toString();
    }

    /**
     * Construct the output.
     * 
     * @param output the verification output
     * @param meta the metadata
     * @throws MetricOutputException if any of the inputs are invalid
     */

    SafePairedOutput( final List<Pair<S, T>> output, final MetricOutputMetadata meta )
    {
        //Validate
        if ( Objects.isNull( output ) )
        {
            throw new MetricOutputException( "Specify a non-null output." );
        }
        if ( Objects.isNull( meta ) )
        {
            throw new MetricOutputException( "Specify non-null metadata." );
        }
        //Set content
        this.output = Collections.unmodifiableList( output );
        this.meta = meta;

        //Validate content
        output.forEach( pair -> {
            if ( Objects.isNull( pair ) )
            {
                throw new MetricOutputException( "Cannot prescribe a null pair for the input map." );
            }
            if ( Objects.isNull( pair.getLeft() ) )
            {
                throw new MetricOutputException( "Cannot prescribe a null value for the left of a pair." );
            }
            if ( Objects.isNull( pair.getRight() ) )
            {
                throw new MetricOutputException( "Cannot prescribe a null value for the right of a pair." );
            }
        } );

    }

}
