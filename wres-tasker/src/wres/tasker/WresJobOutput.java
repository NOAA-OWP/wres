package wres.tasker;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Set;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static javax.ws.rs.core.MediaType.TEXT_PLAIN;
import static javax.ws.rs.core.MediaType.TEXT_HTML;

@Path( "/job/{jobId}/output" )
public class WresJobOutput
{
    private static final Logger LOGGER = LoggerFactory.getLogger( WresJobOutput.class );

    @GET
    @Produces( TEXT_PLAIN )
    public Response getProjectResourcesPlain( @PathParam( "jobId" ) String id )
    {
        LOGGER.debug( "Retrieving resource list form job {} to create plain response", id );
        Set<URI> jobOutputs = WresJob.getSharedJobResults()
                                     .getJobOutputs( id );

        if ( jobOutputs == null )
        {
            return Response.status( Response.Status.NOT_FOUND )
                           .entity( "Could not find job " + id )
                           .build();
        }

        StreamingOutput streamingOutput = output -> {
            try ( OutputStreamWriter outputStreamWriter =  new OutputStreamWriter( output );
                  BufferedWriter writer = new BufferedWriter( outputStreamWriter ) )
            {
                for ( URI outputResource : jobOutputs )
                {
                    java.nio.file.Path path =
                            Paths.get( outputResource.getPath() );
                    java.nio.file.Path resourceName = path.getFileName();
                    writer.write( resourceName.toString() );
                    writer.newLine();
                }
            }
        };

        return Response.ok( streamingOutput )
                       .build();
    }


    @GET
    @Produces( TEXT_HTML )
    public Response getProjectResourcesHtml( @PathParam( "jobId" ) String id )
    {
        LOGGER.debug( "Retrieving resource list from job {} to create html response", id );
        Set<URI> jobOutputs = WresJob.getSharedJobResults()
                                     .getJobOutputs( id );

        if ( jobOutputs == null )
        {
            return Response.status( Response.Status.NOT_FOUND )
                           .entity( "Could not find job " + id )
                           .build();
        }

        String header = "<!DOCTYPE html><html><head><title>Resources created by job id "
                        + id + "</title></head><body><h1>Resources created by job id "
                        + id + "</h1><ul>";
        String footer = "</ul></body></html>";

        StreamingOutput streamingOutput = output -> {
            try ( OutputStreamWriter outputStreamWriter =  new OutputStreamWriter( output );
                  BufferedWriter writer = new BufferedWriter( outputStreamWriter ) )
            {
                writer.write( header );
                writer.newLine();

                for ( URI outputResource : jobOutputs )
                {
                    java.nio.file.Path path =
                            Paths.get( outputResource.getPath() );
                    String resourceName = path.getFileName()
                                              .toString();
                    writer.write( "<li><a href=\"output/" );
                    writer.write( resourceName );
                    writer.write(  "\">" );
                    writer.write( resourceName );
                    writer.write( "</a></li>" );
                }

                writer.write( footer );
                writer.newLine();
            }
        };

        return Response.ok( streamingOutput )
                       .build();
    }


    @GET
    @Path( "/{resourceName}" )
    public Response getProjectResource( @PathParam( "jobId" ) String id,
                                        @PathParam( "resourceName" ) String resourceName )
    {
        LOGGER.debug( "Retrieving resource {} from job {}", resourceName, id );

        Set<URI> jobOutputs = WresJob.getSharedJobResults()
                                     .getJobOutputs( id );

        String type = MediaType.TEXT_PLAIN_TYPE.getType();

        if ( jobOutputs == null )
        {
            return Response.status( Response.Status.NOT_FOUND )
                           .entity( "Could not find job " + id )
                           .build();
        }

        for ( URI outputResource : jobOutputs )
        {
            java.nio.file.Path path = Paths.get( outputResource.getPath() );

            if ( path.getFileName().toString().equals( resourceName ) )
            {
                File actualFile = path.toFile();

                if ( !actualFile.exists() )
                {
                    return Response.status( Response.Status.NOT_FOUND )
                                   .entity( "Could not find resource "
                                            + resourceName + " at "
                                            + actualFile + " from  uri "
                                            + outputResource )
                                   .build();
                }

                if ( !actualFile.canRead() )
                {
                    return Response.status( Response.Status.INTERNAL_SERVER_ERROR )
                                   .entity( "Found but could not read resource "
                                            + resourceName + "." )
                                   .build();
                }

                String probedType;
                Metadata metadata = new Metadata();
                metadata.set( Metadata.RESOURCE_NAME_KEY,
                              actualFile.toString() );

                try
                {
                    TikaConfig tikaConfig = new TikaConfig();
                    Detector detector = tikaConfig.getDetector();
                    InputStream inputStream = TikaInputStream.get( path );
                    org.apache.tika.mime.MediaType mediaType =
                            detector.detect( inputStream,
                                             metadata );
                    probedType = mediaType.toString();

                    if ( probedType != null )
                    {
                        type = probedType;
                    }
                }
                catch ( IOException | TikaException e )
                {
                    LOGGER.warn( "Could not probe content type of {}", path, e );
                }

                return Response.ok( actualFile )
                               .type( type )
                               .build();
            }
        }

        return Response.status( Response.Status.NOT_FOUND )
                       .type( type )
                       .entity( "Could not find resource " + resourceName
                                + " from project " + id )
                       .build();
    }


    /**
     * Afford the client the ability to remove all resources after the client is
     * finished reading the resources it cares about.
     *
     * It is important that the client not specify an arbitrary path, and that
     * the server here do the job of looking for the resources the *server* has
     * associated with the job. Otherwise you could imagine a malicious client
     * deleting anything on the server machine that the server process has
     * access to. (The server process also should have limited privilege.)
     *
     * @param id the id of the job whose outputs to delete
     * @return 200 when successful (idempotent), 500 when IOException occurs
     */

    @DELETE
    @Produces( TEXT_PLAIN )
    public Response deleteProjectResourcesPlain( @PathParam( "jobId" ) String id )
    {
        LOGGER.debug( "Retrieving resources from job {}", id );

        Set<URI> jobOutputs = WresJob.getSharedJobResults()
                                     .getJobOutputs( id );

        if ( jobOutputs == null )
        {
            return Response.status( Response.Status.NOT_FOUND )
                           .entity( "Could not find job " + id )
                           .build();
        }

        try
        {
            for ( URI outputResource : jobOutputs )
            {
                java.nio.file.Path path = Paths.get( outputResource.getPath() );

                boolean deleted = Files.deleteIfExists( path );

                if ( !deleted )
                {
                    LOGGER.warn( "Failed to delete {} -- was it previously deleted?",
                                 path );
                }
                else
                {
                    LOGGER.debug( "Deleted {}", path );
                }
            }

        }
        catch ( IOException ioe )
        {
            LOGGER.error( "Failed to delete resources for job {} at request of client.",
                          id, ioe );

            return Response.serverError()
                    .entity( "Failed to delete resources for job " + id +
                             " (internal server error)" )
                    .build();
        }

        return Response.ok( "Successfully deleted resources for job " + id )
                       .build();
    }
}
