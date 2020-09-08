package wres.tasker;

import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

import com.google.protobuf.GeneratedMessageV3;

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
    private final ConcurrentMap<Integer,String> sharedMap;

    JobStandardStreamSharer( ConcurrentMap<Integer,String> sharedMap )
    {
        this.sharedMap = sharedMap;
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
                (JobStandardStream.job_standard_stream) mustBeJobStandardStream;
        this.sharedMap.put( job_standard_stream.getIndex(),
                            job_standard_stream.getText() );
    }
}
