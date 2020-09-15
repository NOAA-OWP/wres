package wres.tasker;

import java.util.Objects;
import java.util.function.Consumer;

import com.google.protobuf.GeneratedMessageV3;

import static wres.tasker.JobResults.WhichStream;
import static wres.tasker.JobResults.WhichStream.STDERR;
import static wres.tasker.JobResults.WhichStream.STDOUT;
import wres.messages.generated.JobStandardStream;

/**
 * Saves stream messages to a given shared location.
 *
 * The purpose is to genericize waitForAllMessages method in JobMessageHelper
 * rather than re-implementing it over and over for each message type.
 * Instead, we have this boilerplate helper class for each message type.
 */

class JobStandardStreamSharer implements Consumer<GeneratedMessageV3>
{
    private final JobMetadata sharedMetadata;
    private final WhichStream whichStream;

    JobStandardStreamSharer( JobMetadata sharedMetadata,
                             WhichStream whichStream )
    {
        Objects.requireNonNull( sharedMetadata );
        Objects.requireNonNull( whichStream );
        this.sharedMetadata = sharedMetadata;
        this.whichStream = whichStream;
    }

    /**
     * Saves a parsed message to a shared location.
     * Creator of this instance and callers must pass in the correct type,
     * because it will be immediately cast to a specific type.
     * @param mustBeJobStandardStream specific type must be a JobStandardStream.job_standard_stream
     */
    @Override
    public void accept( GeneratedMessageV3 mustBeJobStandardStream )
    {
        JobStandardStream.job_standard_stream job_standard_stream =
                ( JobStandardStream.job_standard_stream ) mustBeJobStandardStream;
        int index = job_standard_stream.getIndex();
        String text = job_standard_stream.getText();

        if ( this.whichStream.equals( STDERR ) )
        {
            this.sharedMetadata.addStderr( index, text );
        }
        else if ( this.whichStream.equals( STDOUT ) )
        {
            this.sharedMetadata.addStdout( index, text );
        }
        else
        {
            throw new UnsupportedOperationException( "Stream must be either "
                                                     + STDERR.name()
                                                     + " or " +
                                                     STDOUT.name() );
        }
    }
}
