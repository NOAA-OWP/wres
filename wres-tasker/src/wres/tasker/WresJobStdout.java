package wres.tasker;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

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
