package wres.server;

import java.io.IOException;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import wres.config.ProjectConfigPlus;

/**
 * Accepts projects in the body of http request and executes them one at a time
 * (In other words, will return 429 Too Many Requests if more than 1 project
 * is attempted at once)
 */
@Path( "/project")
public class ProjectService
{

    @POST
    @Consumes( MediaType.TEXT_XML )
    @Produces( MediaType.TEXT_PLAIN )
    public Response postProjectConfig( String rawProjectConfig )
    {
        try
        {
            ProjectConfigPlus.from( rawProjectConfig, "a web request" );
        }
        catch ( IOException ioe )
        {
            return Response.status( 400 )
                           .entity( "I received something I could not parse. The top-level exception was: "
                                    + ioe.getMessage() )
                           .build();
        }
        return Response.ok( "I received some xml and successfully parsed it. Xml looked like this:"
                            + System.lineSeparator()
                            + rawProjectConfig )
                       .build();
    }
}
