package wres.tasker;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.messages.BrokerHelper;

public class Tasker
{
    private static final Logger LOGGER = LoggerFactory.getLogger( Tasker.class );

    private static final int SERVER_PORT = 8443;

    /**
     * We purposely set a count of server threads for memory predictability
     * in the docker environment. It is probably fine to use Jetty's default of
     * 200 or another value, but starting with something small.
     */
    private static final int MAX_SERVER_THREADS = 100;

    private static final String ENV_VAR_FOR_WRES_ENVIRONMENT = "WRES_ENV_SUFFIX";

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

        // Multiple ways of binding using jersey, but Application is standard,
        // see here:
        // https://stackoverflow.com/questions/22994690/which-init-param-to-use-jersey-config-server-provider-packages-or-javax-ws-rs-a#23041643
        dynamicHolder.setInitParameter( "javax.ws.rs.Application",
                                        JaxRSApplication.class.getCanonicalName() );

        // Static handler:
        ResourceHandler resourceHandler = new ResourceHandler();
        resourceHandler.setBaseResource( Resource.newClassPathResource( "index.html" ) );

        // Have to chain/wrap the handler this way to get both static/dynamic:
        resourceHandler.setHandler( context );

        // Fix the max server threads for better stack memory predictability,
        // 1 thread = 1000KiB of stack by default.
        QueuedThreadPool threadPool = new QueuedThreadPool( MAX_SERVER_THREADS );

        Server jettyServer = new Server( threadPool );

        jettyServer.setHandler( resourceHandler );

        // Use TLS
        SslContextFactory contextFactory = Tasker.getSslContextFactory();

        try ( ServerConnector serverConnector = new ServerConnector( jettyServer, contextFactory ) )
        {
            serverConnector.setPort( Tasker.SERVER_PORT );
            ServerConnector[] serverConnectors = { serverConnector };
            jettyServer.setConnectors( serverConnectors );

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
            WresJob.shutdownNow();
            jettyServer.destroy();
        }
    }

    private static SslContextFactory getSslContextFactory()
    {
        SslContextFactory sslContextFactory = new SslContextFactory();
        String ourServerCertificateFilename = BrokerHelper.getSecretsDir()
                                              + "/***REMOVED***wres"
                                              + Tasker.getEnvironmentSuffix()
                                              + "_server_private_key_and_x509_cert.p12";
        sslContextFactory.setKeyStoreType( "PKCS12" );
        sslContextFactory.setKeyStorePassword( "wres-web-passphrase" );
        sslContextFactory.setKeyStorePath( ourServerCertificateFilename );
        return sslContextFactory;
    }


    /**
     * Returns a string that can be appended to a base hostname in order to
     * vary the hostname used per-environent. If there is a production string
     * it will be blank. Uses the $WRES_ENV_SUFFIX environment variable which
     * contains the dash in it already.
     * @return an always-appendable-to-hostname, non-null string for use in
     * reading a server private key file or server x509 certificate file.
     */

    private static String getEnvironmentSuffix()
    {
        String descriptor = System.getenv( ENV_VAR_FOR_WRES_ENVIRONMENT );

        if ( descriptor == null || descriptor.isEmpty() )
        {
            return "";
        }
        else
        {
            return descriptor;
        }
    }
}
