package wres.tasker;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

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
        Set<URI> jobOutputs = JobResults.getJobOutputs( id );

        if ( jobOutputs == null )
        {
            return Response.status( Response.Status.NOT_FOUND )
                           .entity( "Could not find job " + id )
                           .build();
        }

        List<String> resourceNames = new ArrayList<>();

        for ( URI outputResource : jobOutputs )
        {
            java.nio.file.Path path = Paths.get( outputResource.getPath() );
            java.nio.file.Path resourceName = path.getFileName();
            resourceNames.add( resourceName.toString() );
        }

        return Response.ok( resourceNames.toString() )
                       .build();
    }


    @GET
    @Produces( TEXT_HTML )
    public Response getProjectResourcesHtml( @PathParam( "jobId" ) String id )
    {
        LOGGER.debug( "Retrieving resource list form job {} to create html response", id );
        Set<URI> jobOutputs = JobResults.getJobOutputs( id );

        if ( jobOutputs == null )
        {
            return Response.status( Response.Status.NOT_FOUND )
                           .entity( "Could not find job " + id )
                           .build();
        }

        List<String> resourceNames = new ArrayList<>();

        for ( URI outputResource : jobOutputs )
        {
            java.nio.file.Path path = Paths.get( outputResource.getPath() );
            java.nio.file.Path resourceName = path.getFileName();
            resourceNames.add( resourceName.toString() );
        }

        // Since we're going to the trouble of showing html, may as well sort it
        Collections.sort( resourceNames );

        String header = "<!DOCTYPE html><html><head><title>Resources created by job id "
                        + id + "</title></head><body><h1>Resources created by job id "
                        + id + "</h1><ul><li>";
        String footer = "</li></ul></body></html>";

        StringJoiner fullDocument = new StringJoiner( "</li><li>", header, footer );

        for ( String resourceName : resourceNames )
        {
            String resource = "<a href=\"output/" + resourceName + "\">" + resourceName
                              + "</a>";
            fullDocument.add( resource );
        }

        return Response.ok( fullDocument.toString() )
                       .build();
    }


    @GET
    @Path( "/{resourceName}" )
    public Response getProjectResource( @PathParam( "jobId" ) String id,
                                        @PathParam( "resourceName" ) String resourceName )
    {
        LOGGER.debug( "Retrieving resource {} from job {}", resourceName, id );

        Set<URI> jobOutputs = JobResults.getJobOutputs( id );

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

                try
                {
                    // Successfully translates .nc to application/x-netcdf
                    // Successfully translates .csv to text/csv
                    String probedType = Files.probeContentType( path );

                    if ( probedType != null )
                    {
                        type = probedType;
                    }
                }
                catch ( IOException ioe )
                {
                    LOGGER.warn( "Could not probe content type of {}", path, ioe );
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

        Set<URI> jobOutputs = JobResults.getJobOutputs( id );

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
