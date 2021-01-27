package wres.tasker;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

@Path( "/job/{jobId}/stderr")
public class WresJobStderr
{
    @GET
    @Produces( MediaType.TEXT_PLAIN )
    public String getWresJobStderr( @PathParam( "jobId" ) String jobId )
    {
        return WresJob.getSharedJobResults()
                      .getJobStderr( jobId );
    }
}
