package wres.tasker;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Models the present status of a previously-submitted WRES evaluation job.
 */
@Path( "/job/{jobId}/status")
public class WresJobResult
{
    /**
     * Return a text representation of the status of a given WRES evaluation job
     *
     * Mostly machine-friendly (if machines know the enum) and somewhat
     * human-friendly.
     *
     * Will be the text of one of the enum JobResults.JobState
     * @param jobId the evaluation job to look for
     * @return the status of the job
     */

    @GET
    @Produces( MediaType.TEXT_PLAIN )
    public Response getWresJobResult( @PathParam( "jobId" ) String jobId )
    {
        JobResults.JobState jobState = WresJob.getSharedJobResults()
                                              .getJobResult( jobId );

        if ( jobState.equals( JobResults.JobState.NOT_FOUND ) )
        {
            return Response.status( Response.Status.NOT_FOUND )
                           .entity( jobState.toString() )
                           .build();
        }

        return Response.ok( jobState.toString() )
                       .build();
    }
}
