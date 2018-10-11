package wres.tasker;

import java.net.URI;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Consumer;

import com.google.protobuf.GeneratedMessageV3;

import wres.messages.generated.JobOutput;

class JobOutputSharer implements Consumer<GeneratedMessageV3>
{
    private final ConcurrentSkipListSet<URI> sharedSet;

    JobOutputSharer( ConcurrentSkipListSet<URI> sharedSet )
    {
        this.sharedSet = sharedSet;
    }

    @Override
    public void accept( GeneratedMessageV3 mustBeJobOutput )
    {
        JobOutput.job_output jobOutput =
                (JobOutput.job_output) mustBeJobOutput;
        URI jobOutputUri = Paths.get( jobOutput.getResource() ).toUri();
        this.sharedSet.add( jobOutputUri );
    }
}
