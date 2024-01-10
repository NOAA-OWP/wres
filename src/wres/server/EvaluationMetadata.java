package wres.server;

import java.io.Serializable;
import java.net.URI;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents the metadata about a single WRES execution submission aka job.
 * Mutable but intended to be thread safe.
 */

public class EvaluationMetadata implements Serializable
{
    protected static final Logger LOGGER = LoggerFactory.getLogger( EvaluationMetadata.class );

    private String id;

    private SortedSet<URI> outputs;

    private String stdout;

    private String stderr;

    public EvaluationMetadata( String id )
    {
        Objects.requireNonNull( id );

        if ( id.isBlank() )
        {
            throw new IllegalArgumentException( "The id must not be blank!" );
        }

        this.setId( id );
    }

    /**
     * Required for Redisson, do not use!
     */
    public EvaluationMetadata()
    {
        // Left for Redisson (subclass?) to fill in.
    }

    public String getId()
    {
        return this.id;
    }

    /**
     * Required for redisson, do not use!
     * @param id The id.
     */

    public void setId( String id )
    {
        this.id = id;
    }


    /**
     * Get an unmodifiable view of the outputs Set
     * @return The Set of output URIs.
     */

    public Set<Path> getOutputs()
    {
        return this.outputs
                .stream()
                .map( java.nio.file.Path::of )
                .collect( Collectors.toSet() );
    }

    public void setOutputs( Set<Path> outputs )
    {
        this.outputs = outputs
                .stream()
                .map( Path::toUri )
                .collect( Collectors.toCollection( TreeSet::new ) );
    }

    public String getStdout()
    {
        return this.stdout;
    }

    public void setStdout( String stdout )
    {
        this.stdout = stdout;
    }

    public String getStderr()
    {
        return this.stderr;
    }

    public void setStderr( String stderr )
    {
        this.stderr = stderr;
    }

    @Override
    public String toString()
    {
        return new org.apache.commons.lang3.builder.ToStringBuilder( this )
                .append( "id", this.getId() )
                .append( "outputs", this.getOutputs() )
                .toString();
    }

}

