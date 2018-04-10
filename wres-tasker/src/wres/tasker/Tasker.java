package wres.tasker;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Tasker
{
    private static final Logger LOGGER = LoggerFactory.getLogger( Tasker.class );

    // ... in the year 8585...
    private static final int SERVER_PORT = 8585;


    /**
     * Tasker receives requests for wres runs and passes them along to queue.
     * Actual work is done in WresJob restlet class, Tasker sets up a server.
     * @param args unused args
     * @throws Exception when jetty server start fails
     */

    public static void main( String[] args )
            throws Exception
    {
        LOGGER.info( "I will take wres job requests and queue them." );

        // Following example:
        // http://nikgrozev.com/2014/10/16/rest-with-embedded-jetty-and-jersey-in-a-single-jar-step-by-step/

        ServletContextHandler context = new ServletContextHandler( ServletContextHandler.NO_SESSIONS );
        context.setContextPath( "/" );

        ServletHolder dynamicHolder = context.addServlet( ServletContainer.class,
                                                                "/*" );

        dynamicHolder.setInitParameter( "jersey.config.server.provider.classnames",
                                        WresJob.class.getCanonicalName() );

        // Static handler:
        ResourceHandler resourceHandler = new ResourceHandler();
        resourceHandler.setBaseResource( Resource.newClassPathResource( "job_tester.html" ) );

        // Have to chain/wrap the handler this way to get both static/dynamic:
        resourceHandler.setHandler( context );

        Server jettyServer = new Server( Tasker.SERVER_PORT );

        jettyServer.setHandler( resourceHandler );

        try
        {
            // Stinks that start() throws blanket Exception, oh well: propagate.
            jettyServer.start();
            jettyServer.dump( System.err );
            jettyServer.join();
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Server was interrupted.", ie );
            Thread.currentThread().interrupt();
        }
        finally
        {
            jettyServer.destroy();
        }
    }
}
