package wres.server;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs a web server.
 */
public class WebServer
{
    private static final Logger LOGGER = LoggerFactory.getLogger( WebServer.class );

    // Intentionally choosing a port unlikely to conflict, also intentionally
    // choosing a port that we did *not* ask to be exposed beyond localhost.
    private static final int SERVER_PORT = 8010;

    /**
     * We purposely set a count of server threads for memory predictability
     * in the docker environment and also because we intentionally limit the
     * number of calls to a core WRES process.
     */
    private static final int MAX_SERVER_THREADS = 20;

    public static void main( String[] args )
            throws Exception
    {
        // See comments etc. in wres-tasker/src/wres/tasker/Tasker.java
        // for explanation on steps below.

        ServletContextHandler context = new ServletContextHandler( ServletContextHandler.NO_SESSIONS );
        context.setContextPath( "/" );
        ServletHolder dynamicHolder = context.addServlet( ServletContainer.class,
                                                          "/*" );
        dynamicHolder.setInitParameter( "javax.ws.rs.Application",
                                        JaxRSApplication.class.getCanonicalName() );
        ResourceHandler resourceHandler = new ResourceHandler();
        resourceHandler.setBaseResource( Resource.newClassPathResource( "index.html" ) );
        resourceHandler.setHandler( context );
        QueuedThreadPool threadPool = new QueuedThreadPool( MAX_SERVER_THREADS );
        Server jettyServer = new Server( threadPool );
        jettyServer.setHandler( resourceHandler );

        try ( ServerConnector serverConnector = new ServerConnector( jettyServer ) )
        {
            // Only listen on localhost, this process is intended to be managed
            // by other processes running locally, e.g. a shim or a UI.
            serverConnector.setHost( "127.0.0.1" );
            serverConnector.setPort( WebServer.SERVER_PORT );
            ServerConnector[] serverConnectors = { serverConnector };
            jettyServer.setConnectors( serverConnectors );

            jettyServer.start();
            jettyServer.dump( System.err );
            jettyServer.join();
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "WRES Server was interrupted.", ie );
            Thread.currentThread().interrupt();
        }
        finally
        {
            jettyServer.destroy();
        }
    }
}
