package wres.tasker;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;

/**
 * The standard output.
 */

@Path( "/job/{jobId}/stdout" )
public class WresJobStdout
{
    /**
     * Gets the standard output.
     * @param jobId the job id
     * @return the standard output
     */

    @GET
    @Produces( "text/plain; charset=utf-8" )
    public Response getWresJobStdout( @PathParam( "jobId" ) String jobId )
    {
        StreamingOutput streamingOutput = WresJob.getSharedJobResults()
                                                 .getJobStdout( jobId );
        return Response.ok( streamingOutput )
                       .build();
    }
}
