package wres.server;

import org.eclipse.jetty.alpn.server.ALPNServerConnectionFactory;
import org.eclipse.jetty.http2.server.HTTP2ServerConnectionFactory;
import org.eclipse.jetty.server.HttpChannel;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Runs the core application as a long-running instance or web server that accepts evaluation requests
 */
public class WebServer
{
    private static final Logger LOGGER = LoggerFactory.getLogger( WebServer.class );

    // Intentionally choosing a port unlikely to conflict, also intentionally
    // choosing a port that we did *not* ask to be exposed beyond localhost.
    private static final int DEFAULT_SERVER_PORT = 8010;

    /**
     * We purposely set a count of server threads for memory predictability
     * in the docker environment and also because we intentionally limit the
     * number of calls to a core WRES process.
     */
    private static final int MAX_SERVER_THREADS = 20;


    /**
     * Ensure that the X-Frame-Options header element is included in the response.
     * If not already set, then it will be set to DENY. If it is already set when this
     * is called, then it will be left unchanged (there is no check for DENY).
     */
    private static final HttpChannel.Listener HTTP_CHANNEL_LISTENER = new HttpChannel.Listener()
    {
        @Override
        public void onResponseBegin( Request request )
        {
            if ( request.getResponse().getHeader( "X-Frame-Options" ) == null )
            {
                request.getResponse().addHeader( "X-Frame-Options", "DENY" );
            }
        }
    };


    /**
     * Get the port that is passed in from args, if not present then use default port
     * @param args args potentially containing the port
     * @return the int representing the port to use
     */
    private static int getPortOrDefault( String[] args )
    {
        if ( args.length > 0 )
        {
            try
            {
                return Integer.parseInt( args[0] );
            }
            catch ( NumberFormatException ex )
            {
                LOGGER.debug( "Unable to get port with error: %s", ex );
                return DEFAULT_SERVER_PORT;
            }
        }
        else
        {
            return DEFAULT_SERVER_PORT;
        }
    }

    /**
     * Main method of WebServer used to spin up a long-running worker for evaluations
     * @param args the port to run the server on
     * @throws Exception if the web server could not be created for any reason
     */

    public static void main( String[] args ) throws Exception
    {
        ServletContextHandler context = new ServletContextHandler( ServletContextHandler.NO_SESSIONS );
        context.setContextPath( "/" );
        ServletHolder dynamicHolder = context.addServlet( ServletContainer.class, "/*" );

        // Multiple ways of binding using jersey, but Application is standard,
        // see here:
        // https://stackoverflow.com/questions/22994690/which-init-param-to-use-jersey-config-server-provider-packages-or-javax-ws-rs-a#23041643
        dynamicHolder.setInitParameter( "jakarta.ws.rs.Application",
                                        JaxRSApplication.class.getCanonicalName() );

        // Registering the EvaluationService explicitly so stream redirects are sent real time
        ServletContainer servlet = new ServletContainer( new ResourceConfig().register( new EvaluationService() ) );
        dynamicHolder.setServlet( servlet );

        LOGGER.debug( "Setting dynamic holder initialization to {}", JaxRSApplication.class.getCanonicalName() );

        // Static handler:
        ResourceHandler resourceHandler = new ResourceHandler();
        Resource resource = Resource.newClassPathResource( "html" );
        resourceHandler.setBaseResource( resource );

        // Have to chain/wrap the handler this way to get both static/dynamic:
        resourceHandler.setHandler( context );

        LOGGER.debug( "Setting the base resource to: {}", resource );

        // Fix the max server threads for better stack memory predictability,
        // 1 thread = 1000KiB of stack by default.
        QueuedThreadPool threadPool = new QueuedThreadPool( MAX_SERVER_THREADS );

        Server jettyServer = new Server( threadPool );

        ErrorHandler errorHandler = new ErrorHandler();
        errorHandler.setShowStacks( true );
        jettyServer.setErrorHandler( errorHandler );

        jettyServer.setHandler( resourceHandler );

        HttpConfiguration httpConfig = new HttpConfiguration();

        // Support HTTP/1.1
        HttpConnectionFactory httpOne = new HttpConnectionFactory( httpConfig );

        // Support HTTP/2
        HTTP2ServerConnectionFactory httpTwo = new HTTP2ServerConnectionFactory( httpConfig );

        // Support ALPN
        ALPNServerConnectionFactory alpn = new ALPNServerConnectionFactory();
        alpn.setDefaultProtocol( httpOne.getProtocol() );

        try ( ServerConnector serverConnector = new ServerConnector( jettyServer, httpOne, httpTwo, alpn ) )
        {
            int port = getPortOrDefault( args );
            // Only listen on localhost, this process is intended to be managed
            // by other processes running locally, e.g. a shim or a UI.
            serverConnector.setHost( "127.0.0.1" );
            serverConnector.setPort( port );
            serverConnector.addBean( HTTP_CHANNEL_LISTENER );
            serverConnector.setAcceptedSendBufferSize( 0 );
            ServerConnector[] serverConnectors = { serverConnector };
            jettyServer.setConnectors( serverConnectors );

            jettyServer.start();
            jettyServer.dump( System.err );  // NOSONAR
            String helpMessage =
                    String.format( "Server started. Visit localhost:%d/evaluation for usage instructions", port );
            LOGGER.info( helpMessage );

            jettyServer.join();
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "WRES Server was interrupted.", ie );
            Thread.currentThread()
                  .interrupt();
        }
        finally
        {
            LOGGER.info( "DESTROYING SERVER" );
            jettyServer.destroy();
        }
    }
}
