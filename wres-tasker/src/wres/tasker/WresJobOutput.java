package wres.tasker;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static javax.ws.rs.core.MediaType.TEXT_PLAIN;

@Path( "/job/{jobId}/output" )
public class WresJobOutput
{
    private static final Logger LOGGER = LoggerFactory.getLogger( WresJobOutput.class );

    @GET
    @Produces( TEXT_PLAIN )
    public Response getProjectResources( @PathParam( "jobId" ) String id )
    {
        LOGGER.debug( "Retrieving resource list form job {}", id );
        Set<URI> jobOutputs = JobResults.getJobOutputs( id );

        if ( jobOutputs == null )
        {
            return Response.status( Response.Status.NOT_FOUND )
                           .entity( "Could not find project " + id )
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
                           .entity( "Could not find project " + id )
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

}
