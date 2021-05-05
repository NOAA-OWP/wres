package wres.tasker;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import javax.xml.bind.JAXBException;

import com.google.protobuf.InvalidProtocolBufferException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.FormParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Response;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static jakarta.ws.rs.core.MediaType.APPLICATION_FORM_URLENCODED;
import static jakarta.ws.rs.core.MediaType.MULTIPART_FORM_DATA;
import static jakarta.ws.rs.core.MediaType.TEXT_PLAIN;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import wres.config.ProjectConfigs;
import wres.config.generated.DataSourceConfig;
import wres.messages.generated.Job;

@Path( "/job/{jobId}/input" )
public class WresJobInput
{
    private static final Logger LOGGER = LoggerFactory.getLogger( WresJobInput.class );

    /**
     * Add input data to the evaluation associated with the given job id.
     *
     * The declaration must have previously been posted with parameter postInput
     * set to true.
     *
     * After all input data has been posted, the caller must post to
     * /job/{jobId}/input with query parameter postInputDone=true.
     * @param jobId The job for which to add input data.
     * @param dataset The side on which to add data: left, right or baseline.
     * @param data The data stream.
     * @return HTTP 200 on success, 4XX on client error, 5XX on server error.
     */
    @POST
    @Path( "/{dataset}" )
    @Consumes( MULTIPART_FORM_DATA )
    @Produces( TEXT_PLAIN )
    public Response addSourceToDataset( @PathParam( "jobId" ) String jobId,
                                        @PathParam( "dataset" ) String dataset,
                                        @FormDataParam( "data" ) InputStream data )
    {
        LOGGER.debug( "Data might be put in job {}, on {} side.", jobId, dataset );

        // Round-about way of validating job id: look for job state
        JobResults.JobState jobState = WresJob.getSharedJobResults()
                                              .getJobResult( jobId );

        // TODO: only allow posting inputs with job state of "AWAITING_DATA"
        // (Once that job state is accurately reported)
        if ( jobState.equals( JobResults.JobState.NOT_FOUND ) )
        {
            return Response.status( Response.Status.NOT_FOUND )
                           .entity( jobId + " not found." )
                           .build();
        }

        // TODO: detect (or read given) content type of the data, add suffix.

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


    /**
     * Indicates all inputs have been posted when postInputDone is set to true.
     * Causes the job to be submitted to a worker for processing. When HTTP 503
     * is returned, do retry with backoff (wait a few seconds, try again, if 503
     * is still returned, wait a few more seconds, try again, and so forth).
     *
     * No-op when postInputDone is not set to true.
     *
     * @param jobId The job identifier for which to indicate inputs posted.
     * @param postInputDone True when all inputs have been posted.
     * @return HTTP 200 on success, 503 when there are too many jobs already in
     * the queue and retry is needed, 4XX on client error, other 5XX on other
     * server errors.
     */

    @POST
    @Consumes( APPLICATION_FORM_URLENCODED )
    @Produces( TEXT_PLAIN )
    public Response noMoreInput( @PathParam( "jobId" ) String jobId,
                                 @FormParam( "postInputDone" )
                                 @DefaultValue( "false" )
                                 boolean postInputDone )
    {
        // Round-about way of validating job id: look for job state
        JobResults.JobState jobState = WresJob.getSharedJobResults()
                                              .getJobResult( jobId );

        // TODO: only allow posting inputs with job state of "AWAITING_DATA"
        // (Once that job state is accurately reported)
        if ( jobState.equals( JobResults.JobState.NOT_FOUND ) )
        {
            return Response.status( Response.Status.NOT_FOUND )
                           .entity( jobId + " not found." )
                           .build();
        }

        if ( !postInputDone )
        {
            LOGGER.debug( "Inputs have not yet been fully posted." );
            return Response.status( Response.Status.OK )
                           .build();
        }

        LOGGER.debug( "Inputs have been fully posted." );
        JobResults results = WresJob.getSharedJobResults();
        byte[] jobMessageBytes = results.getJobMessage( jobId );
        Job.job jobMessage;

        try
        {
            jobMessage = Job.job.parseFrom( jobMessageBytes );
        }
        catch ( InvalidProtocolBufferException ipbe )
        {
            LOGGER.warn( "Failed to parse job bytes from {}",
                         jobMessageBytes, ipbe );
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR )
                           .entity( "Failed to parse job bytes: "
                                    + ipbe.getMessage() )
                           .build();
        }

        String declaration = jobMessage.getProjectConfig();
        List<URI> leftUris = results.getLeftInputs( jobId );
        List<URI> rightUris = results.getRightInputs( jobId );
        List<URI> baselineUris = results.getBaselineInputs( jobId );
        List<DataSourceConfig.Source> leftDataset = new ArrayList<>( leftUris.size() );
        List<DataSourceConfig.Source> rightDataset = new ArrayList<>( rightUris.size() );
        List<DataSourceConfig.Source> baselineDataset = new ArrayList<>( baselineUris.size() );

        for ( URI uri : leftUris )
        {
            DataSourceConfig.Source source = new DataSourceConfig.Source( uri,
                                                                          null,
                                                                          null,
                                                                          null,
                                                                          null,
                                                                          null );
            leftDataset.add( source );
        }

        for ( URI uri : rightUris )
        {
            DataSourceConfig.Source source = new DataSourceConfig.Source( uri,
                                                                          null,
                                                                          null,
                                                                          null,
                                                                          null,
                                                                          null );
            rightDataset.add( source );
        }

        for ( URI uri : baselineUris )
        {
            DataSourceConfig.Source source = new DataSourceConfig.Source( uri,
                                                                          null,
                                                                          null,
                                                                          null,
                                                                          null,
                                                                          null );
            baselineDataset.add( source );
        }

        String newDeclaration;

        try
        {
            // Add the sources
            newDeclaration = ProjectConfigs.addSources( declaration,
                                                        "web post",
                                                        leftDataset,
                                                        rightDataset,
                                                        baselineDataset );
            LOGGER.debug( "Created new project declaration:\n{}",
                          newDeclaration );
        }
        catch ( JAXBException | IOException e )
        {
            LOGGER.warn( "Failed to add inputs to posted declaration:{}{}",
                         declaration, "\n", e );
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR )
                           .entity( "Failed to add inputs to declaration: "
                                    + e.getMessage() )
                           .build();
        }

        // Replace the original declaration with the one having inputs.
        Job.job newJob = jobMessage.toBuilder()
                                   .setProjectConfig( newDeclaration )
                                   .build();

        try
        {
            // Send the new job.
            WresJob.sendDeclarationMessage( jobId, newJob.toByteArray() );
        }
        catch( IOException | TimeoutException e )
        {
            LOGGER.warn( "Failed to send declaration for job {}", jobId, e );
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR )
                           .entity( "Failed to send declaration: "
                                    + e.getMessage() )
                           .build();
        }

        return Response.status( Response.Status.OK )
                       .build();
    }
}
