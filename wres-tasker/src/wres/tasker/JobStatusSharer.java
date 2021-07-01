package wres.tasker;

import java.util.function.Consumer;

import com.google.protobuf.GeneratedMessageV3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static wres.messages.generated.JobStatus.job_status.Report.*;
import static wres.tasker.JobMetadata.JobState.COMPLETED_REPORTED_FAILURE;
import static wres.tasker.JobMetadata.JobState.IN_PROGRESS;

import wres.messages.generated.JobStatus;

/**
 * Saves job Status information to a given shared location.
 *
 * The purpose is to genericize waitForAllMessages method in JobMessageHelper
 * rather than re-implementing it over and over for each message type.
 * Instead, we have this boilerplate helper class for each message type.
 */

class JobStatusSharer implements Consumer<GeneratedMessageV3>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( JobStatusSharer.class );
    private final JobMetadata jobMetadata;

    JobStatusSharer( JobMetadata jobMetadata )
    {
        this.jobMetadata = jobMetadata;
    }

    /**
     * Saves a parsed Status message to a shared location.
     * Creator of this instance and callers must pass in the correct type,
     * because it will be immediately cast to a specific type.
     * @param mustBeJobStatus specific type must be a JobStatus.job_Status
     */
    @Override
    public void accept( GeneratedMessageV3 mustBeJobStatus )
    {
        JobStatus.job_status jobStatus =
                (JobStatus.job_status) mustBeJobStatus;
        JobStatus.job_status.Report report = jobStatus.getReport();
        JobMetadata.JobState existingState = this.jobMetadata.getJobState();
        String jobId = this.jobMetadata.getId();

        if ( report.equals( RECEIVED ) || report.equals( ALIVE ) )
        {
            if ( existingState.equals( JobMetadata.JobState.IN_QUEUE ) )
            {
                // TODO: this should probably be something like compare-and-set
                this.jobMetadata.setJobState( IN_PROGRESS );
            }
            else if ( !existingState.equals( IN_PROGRESS ) )
            {
                LOGGER.warn( "Existing job {} was not marked {} but worker reported {}",
                             jobId, IN_PROGRESS, report );
            }
        }
        else
        {
            if ( !JobMetadata.COMPLETED_STATES.contains( existingState ) )
            {
                // Wait two minutes to double-check state
                try
                {
                    LOGGER.info( "Saw job {} was marked {} but worker reported status {}, waiting two minutes and re-checking",
                                 jobId, existingState, report );
                    Thread.sleep( 120000 );
                }
                catch ( InterruptedException ie )
                {
                    LOGGER.warn( "Interrupted while waiting to double-check state of dead process for {}",
                                 jobId );
                }

                existingState = this.jobMetadata.getJobState();

                if ( !JobMetadata.COMPLETED_STATES.contains( existingState ) )
                {
                    if ( LOGGER.isWarnEnabled() )
                    {
                        LOGGER.warn( "Marking job {} reported failure because it was marked dead but not yet marked in a completed state.",
                                     this.jobMetadata.getId() );
                    }

                    this.jobMetadata.setJobState( COMPLETED_REPORTED_FAILURE );
                }
                else
                {
                    LOGGER.info( "Ignoring report {} for job {} because other mechanisms (eventually) already handled it.",
                                 report, jobId );
                }
            }
            else
            {
                LOGGER.info( "Ignoring report {} for job {} because other mechanisms already handled it.",
                             report, jobId );
            }
        }
    }
}
