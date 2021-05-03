package wres.tasker;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Response;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static jakarta.ws.rs.core.MediaType.MULTIPART_FORM_DATA;
import static jakarta.ws.rs.core.MediaType.TEXT_PLAIN;

@Path( "/job/{jobId}/input/{dataset}" )
public class WresJobInput
{
    private static final Logger LOGGER = LoggerFactory.getLogger( WresJobInput.class );

    @POST
    @Consumes( MULTIPART_FORM_DATA )
    @Produces( TEXT_PLAIN )
    public Response putSourceFileInDataset( @PathParam( "jobId" ) String id,
                                            @PathParam( "dataset" ) String dataset,
                                            @FormDataParam( "data" ) InputStream data )
    {
        LOGGER.debug( "Data being put in job {}, on {} side.", id, dataset );
        // TODO: validate job id, side name (l/r/b)
        // TODO: detect (or read given) content type of the data

        try
        {
            String result = new String( data.readAllBytes(),
                                        StandardCharsets.UTF_8 );
            LOGGER.debug( "Data: {}", result );
            // TODO: put the data somewhere
        }
        catch ( IOException ioe )
        {
            LOGGER.warn( "While reading InputStream:", ioe );

            // TODO: attempt to delete the data if anything was created.
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR )
                           .entity( "Failed to read from stream: " + ioe.getMessage() )
                           .build();
        }

        return Response.status( Response.Status.OK )
                       .build();
    }
}
