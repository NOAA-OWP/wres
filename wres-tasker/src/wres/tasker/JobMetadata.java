package wres.tasker;

import java.net.URI;
import java.util.Objects;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

import org.redisson.api.RCascadeType;
import org.redisson.api.annotation.RCascade;
import org.redisson.api.annotation.REntity;
import org.redisson.api.annotation.RId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents the metadata about a single WRES execution submission aka job.
 * Mutable but intended to be thread safe.
 */

@REntity
public class JobMetadata
{
    protected static final Logger LOGGER = LoggerFactory.getLogger( JobMetadata.class );

    @RId
    private String id;

    private Integer exitCode;

    @RCascade( RCascadeType.ALL )
    private SortedSet<URI> outputs;

    @RCascade( RCascadeType.ALL )
    private ConcurrentMap<Integer,String> stdout;

    @RCascade( RCascadeType.ALL )
    private ConcurrentMap<Integer,String> stderr;

    /** Optional: only set when posting job input via tasker */
    private String projectDeclaration;

    /** Inputs to be added to the above declaration when posting job input */
    @RCascade( RCascadeType.ALL )
    private SortedSet<URI> leftInputs;

    /** Inputs to be added to the above declaration when posting job input */
    @RCascade( RCascadeType.ALL )
    private SortedSet<URI> rightInputs;

    /** Inputs to be added to the above declaration when posting job input */
    @RCascade( RCascadeType.ALL )
    private SortedSet<URI> baselineInputs;

    public JobMetadata( String id )
    {
        Objects.requireNonNull( id );

        if ( id.isBlank() )
        {
            throw new IllegalArgumentException( "The id must not be blank!" );
        }

        this.setId( id );
        this.exitCode = null;
        this.outputs = new ConcurrentSkipListSet<>();
        this.stdout = new ConcurrentHashMap<>();
        this.stderr = new ConcurrentHashMap<>();
        this.projectDeclaration = null;
        this.leftInputs = new ConcurrentSkipListSet<>();
        this.rightInputs = new ConcurrentSkipListSet<>();
        this.baselineInputs = new ConcurrentSkipListSet<>();
    }

    /**
     * Required for Redisson, do not use!
     */
    public JobMetadata()
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
     * @return Exit code or null when not yet exited.
     */
    public Integer getExitCode()
    {
        return this.exitCode;
    }

    public void setExitCode( Integer exitCode )
    {
        this.exitCode = exitCode;
    }


    public String getProjectDeclaration()
    {
        return this.projectDeclaration;
    }

    public void setProjectDeclaration( String projectDeclaration )
    {
        this.projectDeclaration = projectDeclaration;
    }


    public SortedSet<URI> getLeftInputs()
    {
        return this.leftInputs;
    }

    public void setLeftInputs( SortedSet<URI> leftInputs )
    {
        this.leftInputs = leftInputs;
    }

    public SortedSet<URI> getRightInputs()
    {
        return this.rightInputs;
    }

    public void setRightInputs( SortedSet<URI> rightInputs )
    {
        this.rightInputs = rightInputs;
    }

    public SortedSet<URI> getBaselineInputs()
    {
        return this.baselineInputs;
    }

    public void setBaselineInputs( SortedSet<URI> baselineInputs )
    {
        this.baselineInputs = baselineInputs;
    }

    /**
     * Get an unmodifiable view of the outputs Set
     * @return The Set of output URIs.
     */

    public SortedSet<URI> getOutputs()
    {
        return this.outputs;
    }


    public void setOutputs( SortedSet<URI> outputs )
    {
        this.outputs = outputs;
    }

    /**
     * Get an unmodifiable view of the standard output stream Map.
     * @return The Map of stdout.
     */

    public ConcurrentMap<Integer,String> getStdout()
    {
        return this.stdout;
    }

    public void setStdout( ConcurrentMap<Integer, String> stdout )
    {
        this.stdout = stdout;
    }

    public ConcurrentMap<Integer,String> getStderr()
    {
        return this.stderr;
    }

    public void setStderr( ConcurrentMap<Integer, String> stderr )
    {
        this.stderr = stderr;
    }

    void addStdout( Integer index, String line )
    {
        String result = this.getStdout()
                            .put( index, line );

        if ( Objects.nonNull( result ) )
        {
            LOGGER.warn( "Overwrote stdout index={} old value={} with new value={}",
                         index, result, line );
        }
    }

    void addStderr( Integer index, String line )
    {
        String result = this.getStderr()
                            .put( index, line );

        if ( Objects.nonNull( result ) )
        {
            LOGGER.warn( "Overwrote stderr for id={} index={} old value={} with new value={}",
                         this.id, index, result, line );
        }
    }

    void addOutput( URI uri )
    {
        boolean result = this.getOutputs()
                             .add( uri );

        if ( !result )
        {
            LOGGER.warn( "Outputs for {} already had a value {} in it.",
                         this.id, uri );
        }
    }

    void addLeftInput( URI input )
    {
        SortedSet<URI> uris = this.getLeftInputs();
        uris.add( input );
    }

    void addRightInput( URI input )
    {
        SortedSet<URI> uris = this.getRightInputs();
        uris.add( input );
    }

    void addBaselineInput( URI input )
    {
        SortedSet<URI> uris = this.getBaselineInputs();
        uris.add( input );
    }

    /**
     * Return whether the exit code has been set or not, aka whether this job
     * representation is aware that the job has finished.
     * @return Whether or not the job is finished according to reports to this.
     */

    boolean isFinished()
    {
        return Objects.nonNull( this.getExitCode() );
    }

    @Override
    public String toString()
    {
        return new org.apache.commons.lang3.builder.ToStringBuilder( this )
                .append( "id", this.getId() )
                .append( "exitCode", this.getExitCode() )
                .append( "outputs", this.getOutputs() )
                .append( "projectDeclaration", this.getProjectDeclaration() )
                .append( "leftInputs", this.getLeftInputs() )
                .append( "rightInputs", this.getRightInputs() )
                .append( "baselineInputs", this.getBaselineInputs() )
                .toString();
    }
}
