package wres.tasker;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path( "/job/{jobId}/stderr")
public class WresJobStderr
{
    @GET
    @Produces( MediaType.TEXT_PLAIN )
    public String getWresJobStderr( @PathParam( "jobId" ) String jobId )
    {
        return JobResults.getJobStderr( jobId );
    }
}
