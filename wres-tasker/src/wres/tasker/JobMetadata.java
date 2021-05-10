package wres.tasker;

import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;

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
    private static final String CAN_ONLY_TRANSITION_FROM = "Can only transition from ";
    private static final String TO = " to ";

    /** Descriptions of the state of a WRES evaluation job */
    public enum JobState
    {
        /** The job was created by tasker, but not yet sent anywhere */
        CREATED,
        /** For when data has yet to be posted */
        AWAITING_DATA,
        /** For when no more data can be posted, but job not yet submitted */
        NO_LONGER_AWAITING_DATA,
        /** When the job has been submitted to the job queue */
        IN_QUEUE,
        /** When the job has been started by a worker */
        IN_PROGRESS,
        /** The job has finished and the worker reported successful */
        COMPLETED_REPORTED_SUCCESS,
        /** The job has finshed and the worker reported failure */
        COMPLETED_REPORTED_FAILURE,
        /** For use by other classes when no JobMetadata exists */
        NOT_FOUND
    }

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
    private byte[] jobMessage;

    /** Inputs to be added to the above declaration when posting job input */
    @RCascade( RCascadeType.ALL )
    private List<URI> leftInputs;

    /** Inputs to be added to the above declaration when posting job input */
    @RCascade( RCascadeType.ALL )
    private List<URI> rightInputs;

    /** Inputs to be added to the above declaration when posting job input */
    @RCascade( RCascadeType.ALL )
    private List<URI> baselineInputs;

    private JobState jobState;

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
        this.jobMessage = null;
        this.leftInputs = new CopyOnWriteArrayList<>();
        this.rightInputs = new CopyOnWriteArrayList<>();
        this.baselineInputs = new CopyOnWriteArrayList<>();
        this.jobState = JobState.CREATED;
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


    public byte[] getJobMessage()
    {
        return this.jobMessage;
    }

    public void setJobMessage( byte[] jobMessage )
    {
        this.jobMessage = jobMessage;
    }

    public List<URI> getLeftInputs()
    {
        return this.leftInputs;
    }

    public void setLeftInputs( List<URI> leftInputs )
    {
        this.leftInputs = leftInputs;
    }

    public List<URI> getRightInputs()
    {
        return this.rightInputs;
    }

    public void setRightInputs( List<URI> rightInputs )
    {
        this.rightInputs = rightInputs;
    }

    public List<URI> getBaselineInputs()
    {
        return this.baselineInputs;
    }

    public void setBaselineInputs( List<URI> baselineInputs )
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

    public JobState getJobState()
    {
        return this.jobState;
    }

    /**
     * When setting job state, verify the state transition is valid. Very picky
     * about state transitions, expecting only transitions when calling this.
     *
     * @param jobState The new job state (not NOT_FOUND)
     * @throws IllegalStateException When an illegal job state transition occurs
     */

    public void setJobState( JobState jobState )
    {
        if ( this.jobState == null )
        {
            if ( !jobState.equals( JobState.CREATED ) )
            {
                throw new IllegalStateException( CAN_ONLY_TRANSITION_FROM
                                                 + " null " + TO
                                                 + JobState.CREATED );
            }
        }
        else if ( jobState.equals( JobState.NOT_FOUND ) )
        {
            throw new IllegalArgumentException( "Cannot set to "
                                                + JobState.NOT_FOUND );
        }
        else if ( this.jobState.equals( JobState.CREATED ) )
        {
            Set<JobState> createdToThese = Set.of( JobState.IN_QUEUE,
                                                   JobState.AWAITING_DATA );
            if ( !createdToThese.contains( jobState ) )
            {
                throw new IllegalStateException( CAN_ONLY_TRANSITION_FROM
                                                 + JobState.CREATED + TO
                                                 + createdToThese );
            }
        }
        else if ( this.jobState.equals( JobState.AWAITING_DATA ) )
        {
            if ( !jobState.equals( JobState.NO_LONGER_AWAITING_DATA ) )
            {
                throw new IllegalStateException( CAN_ONLY_TRANSITION_FROM
                                                 + JobState.AWAITING_DATA + TO
                                                 + JobState.NO_LONGER_AWAITING_DATA );
            }
        }
        else if ( this.jobState.equals( JobState.NO_LONGER_AWAITING_DATA ) )
        {
            if ( !jobState.equals( JobState.IN_QUEUE ) )
            {
                throw new IllegalStateException( CAN_ONLY_TRANSITION_FROM
                                                 + JobState.NO_LONGER_AWAITING_DATA
                                                 + TO + JobState.IN_QUEUE );
            }
        }
        else if ( this.jobState.equals( JobState.IN_QUEUE ) )
        {
            Set<JobState> inQueueToThese = Set.of( JobState.IN_PROGRESS,
                                                   JobState.COMPLETED_REPORTED_FAILURE,
                                                   JobState.COMPLETED_REPORTED_SUCCESS );
            if ( !inQueueToThese.contains( JobState.IN_PROGRESS) )
            {
                throw new IllegalStateException( CAN_ONLY_TRANSITION_FROM
                                                 + JobState.IN_QUEUE
                                                 + TO + inQueueToThese );
            }
        }
        else if ( this.jobState.equals( JobState.IN_PROGRESS ) )
        {
            Set<JobState> inProgressToThese = Set.of( JobState.COMPLETED_REPORTED_FAILURE,
                                                      JobState.COMPLETED_REPORTED_SUCCESS );
            if ( !inProgressToThese.contains( JobState.IN_PROGRESS) )
            {
                throw new IllegalStateException( CAN_ONLY_TRANSITION_FROM
                                                 + JobState.IN_PROGRESS
                                                 + TO + inProgressToThese );
            }
        }

        this.jobState = jobState;
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
        List<URI> uris = this.getLeftInputs();
        uris.add( input );
    }

    void addRightInput( URI input )
    {
        List<URI> uris = this.getRightInputs();
        uris.add( input );
    }

    void addBaselineInput( URI input )
    {
        List<URI> uris = this.getBaselineInputs();
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
                .append( "jobMessage", this.getJobMessage() )
                .append( "leftInputs", this.getLeftInputs() )
                .append( "rightInputs", this.getRightInputs() )
                .append( "baselineInputs", this.getBaselineInputs() )
                .toString();
    }

}
