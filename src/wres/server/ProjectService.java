package wres.server;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashSet;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.ExecutionResult;
import wres.config.ProjectConfigPlus;
import wres.eventsbroker.BrokerConnectionFactory;
import wres.io.concurrency.Executor;
import wres.io.utilities.Database;
import wres.pipeline.Evaluator;
import wres.pipeline.InternalWresException;
import wres.pipeline.UserInputException;
import wres.system.SystemSettings;

/**
 * Accepts projects in the body of http request and executes them.
 * Allows retrieval of some CSV outputs for recent stuff.
 */

@Path( "/project")
public class ProjectService
{
    private static final Logger LOGGER = LoggerFactory.getLogger( ProjectService.class );

    private static final SystemSettings SYSTEM_SETTINGS = SystemSettings.fromDefaultClasspathXmlFile();
    private static final Database DATABASE = new Database( SYSTEM_SETTINGS );
    private static final Executor EXECUTOR = new Executor( SYSTEM_SETTINGS );

    private static final Random RANDOM =
            new Random( System.currentTimeMillis() );

    /** A shared bag of output resource references by request id */
    // The cache is here for expedience, this information could be persisted
    // elsewhere, such as a database or a local file. Cleanup of outputs is a
    // related concern.
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

        // TODO: abstract out the connection factory, really only need one per ProjectService
        try( BrokerConnectionFactory broker = BrokerConnectionFactory.of( false ) )
        {
            Evaluator evaluator = new Evaluator( ProjectService.SYSTEM_SETTINGS,
                                                 ProjectService.DATABASE,
                                                 ProjectService.EXECUTOR,
                                                 broker );
                    
            ProjectConfigPlus projectPlus =
                    ProjectConfigPlus.from( rawProjectConfig,
                                            "a web request" );

            // Guarantee a positive number. Using Math.abs would open up failure
            // in edge cases. A while loop seems complex. Thanks to Ted Hopp
            // on StackOverflow question id 5827023.
            projectId = RANDOM.nextLong() & Long.MAX_VALUE;

            ExecutionResult result = evaluator.evaluate( projectPlus );
            Set<java.nio.file.Path> outputPaths = result.getResources();
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

        Set<String> resources = getSetOfResources( id, paths );

        return Response.ok( "Here are result resources: " + resources )
                       .build();
    }

    @GET
    @Path( "/{id}/{resourceName}" )
    public Response getProjectResource( @PathParam( "id" ) Long id,
                                        @PathParam( "resourceName" ) String resourceName )
    {
        Set<java.nio.file.Path> paths = OUTPUTS.getIfPresent( id );

        String type = MediaType.TEXT_PLAIN_TYPE.getType();

        if ( paths == null )
        {
            return Response.status( Response.Status.NOT_FOUND )
                           .entity( "Could not find project " + id )
                           .build();
        }

        for ( java.nio.file.Path path : paths )
        {
            if ( path.getFileName().toString().equals( resourceName ) )
            {
                File actualFile = path.toFile();

                if ( !actualFile.exists() )
                {
                    return Response.status( Response.Status.NOT_FOUND )
                                   .entity( "Could not see resource "
                                            + resourceName + "." )
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

    private static Set<String> getSetOfResources( long projectId,
                                                  Set<java.nio.file.Path> pathSet )
    {
        Set<String> resources = new HashSet<>();

        for ( java.nio.file.Path path : pathSet )
        {
            resources.add( "project/" + Long.toString( projectId )
                           + "/" + path.getFileName() );
        }

        return Collections.unmodifiableSet( resources );
    }
}
