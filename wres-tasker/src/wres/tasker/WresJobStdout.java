package wres.tasker;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;

@Path( "/job/{jobId}/stdout")
public class WresJobStdout
{
    @GET
    @Produces( MediaType.TEXT_PLAIN )
    public Response getWresJobStdout( @PathParam( "jobId" ) String jobId )
    {
        StreamingOutput streamingOutput = WresJob.getSharedJobResults()
                                                 .getJobStdout( jobId );
        return Response.ok( streamingOutput )
                       .build();
    }
}
