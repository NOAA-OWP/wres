package wres.tasker;

import java.net.URI;
import java.util.Set;
import java.util.function.Consumer;

import com.google.protobuf.GeneratedMessageV3;

import wres.messages.generated.JobOutput;

/**
 * Saves job output information to a given shared location.
 *
 * The purpose is to genericize waitForAllMessages method in JobMessageHelper
 * rather than re-implementing it over and over for each message type.
 * Instead, we have this boilerplate helper class for each message type.
 */

class JobOutputSharer implements Consumer<GeneratedMessageV3>
{
    private final JobMetadata jobMetadata;

    JobOutputSharer( JobMetadata jobMetadata )
    {
        this.jobMetadata = jobMetadata;
    }

    /**
     * Saves a parsed output message to a shared location.
     * Creator of this instance and callers must pass in the correct type,
     * because it will be immediately cast to a specific type.
     * @param mustBeJobOutput specific type must be a JobOutput.job_output
     */
    @Override
    public void accept( GeneratedMessageV3 mustBeJobOutput )
    {
        JobOutput.job_output jobOutput =
                (JobOutput.job_output) mustBeJobOutput;
        URI jobOutputUri = URI.create( jobOutput.getResource() );
        this.jobMetadata.addOutput( jobOutputUri );
    }
}
