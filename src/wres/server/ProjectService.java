package wres.server;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.Writer;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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
import jakarta.ws.rs.core.StreamingOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static wres.messages.generated.EvaluationStatusOuterClass.EvaluationStatus.*;

import wres.ExecutionResult;
import wres.messages.generated.EvaluationStatusOuterClass;
import wres.pipeline.Evaluator;
import wres.pipeline.InternalWresException;
import wres.pipeline.UserInputException;

/**
 * Accepts projects in the body of http request and executes them.
 * Allows retrieval of some CSV outputs for recent stuff.
 */

@Path( "/project" )
public class ProjectService
{
    private static final Logger LOGGER = LoggerFactory.getLogger( ProjectService.class );

    private static final Random RANDOM =
            new Random( System.currentTimeMillis() );

    private static final String LINE_TERMINATOR = "\n";

    private static final AtomicReference<EvaluationStatusOuterClass.EvaluationStatus> evaluationStage = new AtomicReference<>( AWAITING );

    private static final AtomicLong evaluationId = new AtomicLong(-1);

    private final Evaluator evaluator;

    /** A shared bag of output resource references by request id */
    // The cache is here for expedience, this information could be persisted
    // elsewhere, such as a database or a local file. Cleanup of outputs is a
    // related concern.
    private static final Cache<Long, Set<java.nio.file.Path>> OUTPUTS = Caffeine.newBuilder()
                                                                                .maximumSize( 100 )
                                                                                .build();

    /**
     * ProjectService constructor
     * @param evaluator The servers evaluator used to evaluate projects
     */
    public ProjectService( Evaluator evaluator )
    {
        this.evaluator = evaluator;
    }


    @GET
    @Path( "/up" )
    @Produces( MediaType.TEXT_PLAIN )
    public Response isUp()
    {
        evaluationStage.set( OPENED );
        return Response.ok( "The Server is Up\n" )
                       .build();
    }

    @GET
    @Path( "/status/{id}" )
    @Produces( MediaType.TEXT_PLAIN )
    public Response getStatus(@PathParam( "id" ) Long id)

    {
        return Response.ok( evaluationStage.get().toString() )
                       .build();
    }

    @GET
    @Path( "/stdout" )
    @Produces("application/octet-stream")
    public Response getOutStream()
    {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream( byteArrayOutputStream );

        System.setOut( printStream );

        StreamingOutput streamingOutput = outputStream -> {
            long offset = 0;
            while (!evaluationStage.get().equals( COMPLETED ))
            {
                ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream( byteArrayOutputStream.toByteArray() );
                long skip = byteArrayInputStream.skip( offset );
                if (offset == skip)
                {
                    outputStream.write( byteArrayInputStream.read() );
                    outputStream.flush();
                    offset++;
                }
            }
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream( byteArrayOutputStream.toByteArray() );
            byteArrayInputStream.skip( offset );
            outputStream.write( byteArrayInputStream.readAllBytes() );
            outputStream.flush();
            outputStream.close();
        };

        return Response.ok( streamingOutput )
                .build();
    }

    @GET
    @Path( "/stderr" )
    @Produces("application/octet-stream")
    public Response getErrorStream()
    {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream( byteArrayOutputStream );

        System.setErr( printStream );

        StreamingOutput streamingOutput = outputStream -> {
            long offset = 0;
            while (!evaluationStage.get().equals( COMPLETED ))
            {
                ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream( byteArrayOutputStream.toByteArray() );
                long skip = byteArrayInputStream.skip( offset );
                if (offset == skip)
                {
                    outputStream.write( byteArrayInputStream.read() );
                    outputStream.flush();
                    offset++;
                }
            }
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream( byteArrayOutputStream.toByteArray() );
            byteArrayInputStream.skip( offset );
            outputStream.write( byteArrayInputStream.readAllBytes() );
            outputStream.flush();
            outputStream.close();
        };

        return Response.ok( streamingOutput )
                       .build();
    }

    @GET
    @Path( "/prepareEvaluation" )
    @Produces( MediaType.TEXT_PLAIN )
    public Response prepareEvaluation() {
        // Guarantee a positive number. Using Math.abs would open up failure
        // in edge cases. A while loop seems complex. Thanks to Ted Hopp
        // on StackOverflow question id 5827023.
        long projectId = RANDOM.nextLong() & Long.MAX_VALUE;
        evaluationStage.set( OPENED );
        evaluationId.set( projectId );

        return Response.ok( Response.Status.CREATED )
                       .entity( projectId )
                       .build();
    }

    /**
     * @param projectConfig the evaluation project declaration string
     * @return the state of the evaluation
     */

    @POST
    @Path(  "/startEvaluation/{id}"  )
    @Consumes( MediaType.TEXT_XML )
    @Produces("application/octet-stream")
    public Response postProjectConfig(String projectConfig, @PathParam( "id" ) Long id )
    {
        long projectId = id;
        Set<java.nio.file.Path> outputPaths;
        try
        {
            evaluationStage.set( ONGOING );
            ExecutionResult result = evaluator.evaluate( projectConfig );
            outputPaths = result.getResources();
            OUTPUTS.put( projectId, outputPaths );
        }
        catch ( UserInputException e )
        {
            return Response.status( Response.Status.BAD_REQUEST )
                           .entity( "I received something I could not parse. The top-level exception was: "
                                    + e.getMessage() )
                           .build();
        }
        catch ( InternalWresException iwe )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR )
                           .entity( "WRES experienced an internal issue. The top-level exception was: "
                                    + iwe.getMessage() )
                           .build();
        }
        catch ( Exception e )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR )
                           .entity( "WRES experienced an unexpected internal issue. The top-level exception was: "
                                    + e.getMessage() )
                           .build();
        }

        evaluationStage.set( COMPLETED );

        if ( outputPaths == null )
        {
            return Response.status( Response.Status.NOT_FOUND )
                           .entity( "Could not find job " + id )
                           .build();
        }

        StreamingOutput streamingOutput = outputSream -> {
            Writer writer = new BufferedWriter( new OutputStreamWriter( outputSream ) );
            for ( java.nio.file.Path path : outputPaths) {
                writer.write( path.toString() + LINE_TERMINATOR );
            }
            writer.flush();
            writer.close();
        };

        return Response.ok( streamingOutput )
                       .build();
    }

    /**
     * @param id the evaluation job identifier
     * @return the evaluation results
     */

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

    /**
     * @param id the evaluation job identifier
     * @param resourceName the resource name
     * @return the resource
     */

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
                                            + resourceName
                                            + "." )
                                   .build();
                }

                if ( !actualFile.canRead() )
                {
                    return Response.status( Response.Status.INTERNAL_SERVER_ERROR )
                                   .entity( "Found but could not read resource "
                                            + resourceName
                                            + "." )
                                   .build();
                }

                type = ProjectService.getContentType( path );

                return Response.ok( actualFile )
                               .type( type )
                               .build();
            }
        }

        return Response.status( Response.Status.NOT_FOUND )
                       .type( type )
                       .entity( "Could not find resource " + resourceName
                                + " from project "
                                + id )
                       .build();
    }

    /**
     * Examines the content type of the resource at the path.
     * @param path the path
     * @return the content type
     */

    private static String getContentType( java.nio.file.Path path )
    {
        String type = MediaType.TEXT_PLAIN_TYPE.getType();

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

        return type;
    }

    /**
     * Get a set of resources connected to a project.
     * @param projectId the project identifier
     * @param pathSet the paths
     * @return the resources
     */

    private static Set<String> getSetOfResources( long projectId,
                                                  Set<java.nio.file.Path> pathSet )
    {
        Set<String> resources = new HashSet<>();

        for ( java.nio.file.Path path : pathSet )
        {
            resources.add( "project/"
                           + projectId
                           + "/"
                           + path.getFileName() );
        }

        return Collections.unmodifiableSet( resources );
    }
}
