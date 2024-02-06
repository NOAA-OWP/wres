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

import wres.messages.generated.EvaluationStatusOuterClass;

/**
 * Represents the metadata about a single WRES execution submission aka job.
 * Mutable but intended to be thread safe.
 */

public class EvaluationMetadata implements Serializable
{
    protected static final Logger LOGGER = LoggerFactory.getLogger( EvaluationMetadata.class );

    /**
     * The internal id of the job being executed
     */
    private String id;

    /**
     * The outputs produced by the evaluation if there are any
     */
    private SortedSet<URI> outputs;

    /**
     * The stdout produced by the job
     */
    private String stdout;

    /**
     * The stderr produced by the job
     */
    private String stderr;

    /**
     * The current status of the job related to cached object
     */
    private EvaluationStatusOuterClass.EvaluationStatus status;

    /**
     * Creates a new EvaluationMetadata object
     * @param id the id of the cached object
     */
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

    /**
     * Get the ID
     * @return the id as a string
     */
    public String getId()
    {
        return this.id;
    }

    /**
     * Sets the ID of the cached object
     * @param id the id to set
     */
    public void setId( String id )
    {
        this.id = id;
    }

    /**
     * Gets the status of the cached object
     * @return the EvaluationStatus of this object
     */
    public EvaluationStatusOuterClass.EvaluationStatus getStatus()
    {
        return this.status;
    }

    /**
     * Sets the status of the object
     * @param status the status to set to
     */
    public void setStatus( EvaluationStatusOuterClass.EvaluationStatus status )
    {
        this.status = status;
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

    /**
     * Sets the outputs of this object
     * @param outputs the outputs to set to
     */
    public void setOutputs( Set<Path> outputs )
    {
        this.outputs = outputs
                .stream()
                .map( Path::toUri )
                .collect( Collectors.toCollection( TreeSet::new ) );
    }

    /**
     * Get the stdout of the object
     * @return String representation of the stdout
     */
    public String getStdout()
    {
        return this.stdout;
    }

    /**
     * Sets the stdout of this object
     * @param stdout the stdout to set to
     */
    public void setStdout( String stdout )
    {
        this.stdout = stdout;
    }

    /**
     * Gets the stderr of the object
     * @return a string representing the stderr
     */
    public String getStderr()
    {
        return this.stderr;
    }

    /**
     * Sets the stderr of the object
     * @param stderr the stderr to set too
     */
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
                .append( "status", this.getStatus().toString() )
                .toString();
    }

}

