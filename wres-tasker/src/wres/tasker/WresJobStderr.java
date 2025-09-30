package wres.tasker;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;

/**
 * The job standard error.
 */

@Path( "/job/{jobId}/stderr")
public class WresJobStderr
{
    /**
     * Returns the job standard error.
     * @param jobId the job id
     * @return the standard error
     */

    @GET
    @Produces( "text/plain; charset=utf-8" )
    public String getWresJobStderr( @PathParam( "jobId" ) String jobId )
    {
        return WresJob.getSharedJobResults()
                      .getJobStderr( jobId );
    }
}
