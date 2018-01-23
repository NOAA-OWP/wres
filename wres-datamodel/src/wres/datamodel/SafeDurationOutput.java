package wres.datamodel;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DurationScoreOutput;
import wres.datamodel.outputs.MetricOutputException;

/**
 * An immutable {@link Duration} output produced by a metric.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.4
 */

class SafeDurationOutput implements DurationScoreOutput
{

    /**
     * The output.
     */

    private final Duration output;

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
        if ( ! ( o instanceof SafeDurationOutput ) )
        {
            return false;
        }
        final SafeDurationOutput v = (SafeDurationOutput) o;
        boolean start = meta.equals( v.getMetadata() );
        start = start && ( (SafeDurationOutput) o ).getData().equals( output );
        return start;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( output, meta );
    }

    @Override
    public Duration getData()
    {
        return output;
    }

    @Override
    public Duration getValue( MetricConstants component )
    {
        if ( component == MetricConstants.MAIN )
        {
            return output;
        }
        return null;
    }

    @Override
    public boolean hasComponent( MetricConstants component )
    {
        return component == MetricConstants.MAIN;
    }

    @Override
    public Set<MetricConstants> getComponents()
    {
        return new HashSet<>( Arrays.asList( MetricConstants.MAIN ) );
    }

    @Override
    public Iterator<Pair<MetricConstants, Duration>> iterator()
    {
        return Collections.unmodifiableList( Arrays.asList( Pair.of( MetricConstants.MAIN, output ) ) ).iterator();
    }

    @Override
    public String toString()
    {
        StringJoiner format = new StringJoiner( ",", "[", "]" );
        format.add( MetricConstants.MAIN.toString() ).add( output.toString() );
        return format.toString();
    }

    /**
     * Construct the output.
     * 
     * @param output the verification output
     * @param meta the metadata
     * @throws MetricOutputException if any of the inputs are invalid
     */

    SafeDurationOutput( final Duration output, final MetricOutputMetadata meta )
    {
        if ( Objects.isNull( output ) )
        {
            throw new MetricOutputException( "Specify a non-null output." );
        }
        if ( Objects.isNull( meta ) )
        {
            throw new MetricOutputException( "Specify non-null metadata." );
        }
        this.output = output;
        this.meta = meta;
    }

}
