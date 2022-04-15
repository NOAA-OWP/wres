package wres.server;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.ExecutionResult;
import wres.config.ProjectConfigPlus;
import wres.eventsbroker.BrokerConnectionFactory;
import wres.eventsbroker.BrokerUtilities;
import wres.eventsbroker.embedded.EmbeddedBroker;
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

        // TODO: the embedded broker and broker connections for statistics messaging should be one per service instance,
        // not one per evaluation, but this class does not currently provide a hook for closing its resources, so 
        // creating them and destroying them here for now.
        
        // Create the broker connections for statistics messaging
        Properties brokerConnectionProperties =
                BrokerUtilities.getBrokerConnectionProperties( BrokerConnectionFactory.DEFAULT_PROPERTIES );
        
        // Create an embedded broker for statistics messages, if needed
        EmbeddedBroker broker = null;
        if( BrokerUtilities.isEmbeddedBrokerRequired( brokerConnectionProperties ) )
        {
            broker = EmbeddedBroker.of( brokerConnectionProperties, false );
        }
        
        // TODO: abstract out the connection factory, only need one per ProjectService
        try( BrokerConnectionFactory brokerConnections = BrokerConnectionFactory.of( brokerConnectionProperties ) )
        {
            Evaluator evaluator = new Evaluator( ProjectService.SYSTEM_SETTINGS,
                                                 ProjectService.DATABASE,
                                                 ProjectService.EXECUTOR,
                                                 brokerConnections );
                    
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
        finally
        {
            if( Objects.nonNull( broker ) )
            {
                try
                {
                    broker.close();
                }
                catch ( IOException e )
                {
                    LOGGER.warn( "Failed to destroy the embedded broker used for statistics messaging.", e );
                }
            }
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
