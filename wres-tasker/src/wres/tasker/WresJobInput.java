package wres.tasker;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

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
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

@Path( "/job/{jobId}/input/{dataset}" )
public class WresJobInput
{
    private static final Logger LOGGER = LoggerFactory.getLogger( WresJobInput.class );

    @POST
    @Consumes( MULTIPART_FORM_DATA )
    @Produces( TEXT_PLAIN )
    public Response putSourceFileInDataset( @PathParam( "jobId" ) String jobId,
                                            @PathParam( "dataset" ) String dataset,
                                            @FormDataParam( "data" ) InputStream data )
    {
        LOGGER.debug( "Data might be put in job {}, on {} side.", jobId, dataset );

        // Round-about way of validating job id: look for job state
        JobResults.JobState jobState = WresJob.getSharedJobResults()
                                              .getJobResult( jobId );

        if ( jobState.equals( JobResults.JobState.NOT_FOUND ) )
        {
            return Response.status( Response.Status.NOT_FOUND )
                           .entity( jobId + " not found." )
                           .build();
        }

        // TODO: detect (or read given) content type of the data

        FileAttribute<Set<PosixFilePermission>> posixAttributes;

        // Indirect way of detecting "is this unix or not"
        if ( System.getProperty( "file.separator" )
                   .equals( "/" ) )
        {
            Set<PosixFilePermission> permissions;
            LOGGER.debug( "Detected unix system." );
            permissions = EnumSet.of(
                    PosixFilePermission.OWNER_READ,
                    PosixFilePermission.OWNER_WRITE,
                    PosixFilePermission.OWNER_EXECUTE,
                    PosixFilePermission.GROUP_READ,
                    PosixFilePermission.GROUP_WRITE,
                    PosixFilePermission.GROUP_EXECUTE );
                    PosixFilePermissions.asFileAttribute( permissions );
            posixAttributes = PosixFilePermissions.asFileAttribute( permissions );
        }
        else
        {
            LOGGER.debug( "Detected windows system." );
            posixAttributes = null;
        }

        java.nio.file.Path temp = null;

        try
        {
            if ( Objects.nonNull( posixAttributes  ) )
            {
                temp = Files.createTempFile( jobId, "", posixAttributes );
            }
            else
            {
                temp = Files.createTempFile( jobId, "" );
            }

            Files.copy( data, temp, REPLACE_EXISTING );
            LOGGER.debug( "Data in: {}", temp );
        }
        catch ( IOException ioe )
        {
            LOGGER.warn( "While reading InputStream:", ioe );

            if ( Objects.nonNull( temp ) )
            {
                try
                {
                    Files.deleteIfExists( temp );
                }
                catch ( IOException ioe2 )
                {
                    LOGGER.warn( "Failed to delete {}", temp );
                }
            }

            return Response.status( Response.Status.INTERNAL_SERVER_ERROR )
                           .entity( "Failed to read from stream: " + ioe.getMessage() )
                           .build();
        }

        JobResults results = WresJob.getSharedJobResults();
        URI uri = temp.toUri();
        results.addInput( jobId, dataset, uri );
        return Response.status( Response.Status.OK )
                       .build();
    }
}
