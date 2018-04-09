package wres.tasker;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
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

        // TODO: What does SESSIONS mean here? Can it be changed to NO_SESSIONS?
        ServletContextHandler context = new ServletContextHandler( ServletContextHandler.SESSIONS );
        context.setContextPath( "/" );

        // Allow static content, use cwd
        String currentDir = System.getProperty( "user.dir" );
        LOGGER.info( "Current directory: ", currentDir );

        context.setResourceBase( currentDir );

        Server jettyServer = new Server( Tasker.SERVER_PORT );
        jettyServer.setHandler( context );

        ServletHolder dynamicHolder = context.addServlet( ServletContainer.class,
                                                                "/jobs/*" );

        dynamicHolder.setInitParameter( "jersey.config.server.provider.classnames",
                                        WresJob.class.getCanonicalName() );

        ServletHolder staticHolder = new ServletHolder( "static-home",
                                                        DefaultServlet.class );
        staticHolder.setInitParameter( "resourceBase", "./static/" );
        staticHolder.setInitParameter( "dirAllowed", "true" );
        staticHolder.setInitParameter( "pathInfoOnly", "true" );
        context.addServlet( staticHolder, "/*" );

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
