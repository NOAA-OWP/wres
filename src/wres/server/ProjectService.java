package wres.server;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.Set;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import wres.config.ProjectConfigPlus;
import wres.control.Control;
import wres.control.InternalWresException;
import wres.control.UserInputException;


/**
 * Accepts projects in the body of http request and executes them.
 * Allows retrieval of some CSV outputs for recent stuff.
 */

@Path( "/project")
public class ProjectService
{
    private static final Random RANDOM =
            new Random( System.currentTimeMillis() );

    /** A shared bag of output file names by request id */
    private static final Cache<Long, Set<java.nio.file.Path>> OUTPUTS
            = Caffeine.newBuilder()
                      .maximumSize( 100 )
                      .build();

    @POST
    @Consumes( MediaType.TEXT_XML )
    @Produces( MediaType.TEXT_PLAIN )
    public Response postProjectConfig( String rawProjectConfig )
    {
        long projectId;

        try
        {
            ProjectConfigPlus projectPlus =
                    ProjectConfigPlus.from( rawProjectConfig,
                                            "a web request" );
            projectId = RANDOM.nextLong();

            Control control = new Control();
            control.accept( projectPlus );
            Set<java.nio.file.Path> outputPaths = control.get();
            OUTPUTS.put( projectId, outputPaths );
        }
        catch ( IOException | UserInputException e )
        {
            return Response.status( Response.Status.BAD_REQUEST )
                           .entity(
                                   "I received something I could not parse. The top-level exception was: "
                                   + e.getMessage() )
                           .build();
        }
        catch ( InternalWresException iwe )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR )
                           .entity(
                                   "WRES experienced an internal issue. The top-level exception was: "
                                   + iwe.getMessage() )
                           .build();
        }
        catch ( Exception e )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR )
                           .entity(
                                   "WRES experienced an unexpected internal issue. The top-level exception was: "
                                   + e.getMessage() )
                           .build();
        }

        return Response.ok( "I received project " + projectId
                            + ", and successfully ran it. Visit /project/"
                            + projectId + " for more." )
                       .build();
    }

    @GET
    @Path( "/{id}" )
    @Produces( MediaType.TEXT_PLAIN )
    public Response getProjectResults( @PathParam( "id" ) Long id )
    {
        Set<java.nio.file.Path> paths = OUTPUTS.getIfPresent( id );

        if ( paths == null )
        {
            return Response.status( Response.Status.NOT_FOUND )
                           .entity( "Could not find job " + id )
                           .build();
        }

        return Response.ok( "Here is a list of files: " + paths )
                       .build();
    }

    @GET
    @Path( "/{id}/{fileName}" )
    @Produces( MediaType.TEXT_PLAIN )
    public Response getProjectFile( @PathParam( "id" ) Long id,
                                    @PathParam( "fileName" ) String fileName )
    {
        Set<java.nio.file.Path> paths = OUTPUTS.getIfPresent( id );

        if ( paths == null )
        {
            return Response.status( Response.Status.NOT_FOUND )
                           .entity( "Could not find project " + id )
                           .build();
        }

        for ( java.nio.file.Path path : paths )
        {
            if ( path.getFileName().toString().equals( fileName ) )
            {
                File actualFile = path.toFile();

                if ( !actualFile.exists() )
                {
                    return Response.status( Response.Status.NOT_FOUND )
                                   .entity( "Could not see file " + fileName
                                            + "." )
                                   .build();
                }

                if ( !actualFile.canRead() )
                {
                        return Response.status( Response.Status.INTERNAL_SERVER_ERROR )
                                       .entity( "Found but could not read file "
                                                + fileName + "." )
                                       .build();
                }

                return Response.ok( actualFile )
                               .build();
            }
        }

        return Response.status( Response.Status.NOT_FOUND )
                       .entity( "Could not find fileName " + fileName
                                + " in project " + id )
                       .build();
    }

}
