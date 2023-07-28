package wres.tasker;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;

import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.codec.digest.DigestUtils;

@Path( "/job/{jobId}/output" )
public class WresJobOutput
{
    private static final Logger LOGGER = LoggerFactory.getLogger( WresJobOutput.class );

    @GET
    @Produces( "text/plain; charset=utf-8" )
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
            try ( OutputStreamWriter outputStreamWriter =  new OutputStreamWriter( output, StandardCharsets.UTF_8 );
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
    @Produces( "text/html; charset=utf-8" )
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
            try ( OutputStreamWriter outputStreamWriter =  new OutputStreamWriter( output, StandardCharsets.UTF_8 );
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

        String type = "text/plain; charset=utf-8";

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
                metadata.set( TikaCoreProperties.RESOURCE_NAME_KEY,
                              actualFile.toString() );
                String md5 = "not_computed";

                try ( InputStream inputStream = TikaInputStream.get( path ) )
                {
                    //Probing the file type.
                    TikaConfig tikaConfig = new TikaConfig();
                    Detector detector = tikaConfig.getDetector();
                    org.apache.tika.mime.MediaType mediaType =
                            detector.detect( inputStream,
                                             metadata );
                    probedType = mediaType.toString();
                    if ( probedType != null )
                    {
                        type = probedType;
                    }

                    //Testing shows that the inputStream open for tika is reset and can be resused here. 
                    md5 = DigestUtils.md5Hex(inputStream);

                    LOGGER.debug( "The path {} found to have type {} and MD5 {}.", path, type, md5);
                }
                catch ( IOException | TikaException e )
                {
                    LOGGER.warn( "Could not probe content type and compute MD5 of {}", path, e );
                }

                return Response.ok( actualFile )
                               .header( "content-md5", md5 )
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
    @Produces( "text/plain; charset=utf-8" )
    public Response deleteProjectResourcesPlain( @PathParam( "jobId" ) String id )
    {
        LOGGER.debug( "Retrieving resources from job {}", id );

        Set<URI> jobOutputs = WresJob.getSharedJobResults()
                                     .getJobOutputs( id );
        Set<URI> deletedOutputs = new ConcurrentSkipListSet<>(); 

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
                    deletedOutputs.add( outputResource );
                    LOGGER.debug( "Deleted {}", path );
                }
            }

        }
        catch ( IOException ioe )
        {
            LOGGER.error( "Failed to delete resources for job {} at request of client.",
                          id, ioe );

            return Response.serverError()
                    .entity( "Failed to delete all resources for job " + id +
                             " though some may have been deleted before the exception occurred." )
                    .build();
        }
        catch ( IllegalStateException ise )
        {
            LOGGER.error( "Internal error deleting resources for job {}.",
                          id, ise );

            return Response.serverError()
                    .entity( "Internal error deleting all resources for job " + id +
                             " though some may have been deleted before the exception occurred." )
                    .build();
        }
        finally
        {
            WresJob.getSharedJobResults().removeJobOutputs( id, deletedOutputs );
        }

        return Response.ok( "Successfully deleted resources for job " + id )
                       .build();
    }
}
