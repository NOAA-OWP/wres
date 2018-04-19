package wres.tasker;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path( "/jobResult/{jobId}")
public class WresJobResult
{
    @GET
    @Produces( MediaType.TEXT_PLAIN )
    public String getWresJobResult( @PathParam( "jobId" ) String jobId )
    {
        return JobResults.getJobResult( jobId );
    }
}
