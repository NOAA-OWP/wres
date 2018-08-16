package wres.server;

import java.io.IOException;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import wres.config.ProjectConfigPlus;
import wres.control.Control;
import wres.control.InternalWresException;
import wres.control.UserInputException;

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
            ProjectConfigPlus projectPlus = ProjectConfigPlus.from( rawProjectConfig,
                                                                    "a web request" );
            Control control = new Control();
            control.accept( projectPlus );
        }
        catch ( IOException | UserInputException e )
        {
            return Response.status( 400 )
                           .entity( "I received something I could not parse. The top-level exception was: "
                                    + e.getMessage() )
                           .build();
        }
        catch ( InternalWresException iwe )
        {
            return Response.status( 500 )
                           .entity( "WRES experienced an internal issue. The top-level exception was: "
                             + iwe.getMessage() )
                           .build();
        }

        return Response.ok( "I received a project, and successfully ran it. The config looked like this:"
                            + System.lineSeparator()
                            + rawProjectConfig )
                       .build();
    }
}
