package wres.tasker;

import java.security.Security;

import org.eclipse.jetty.alpn.server.ALPNServerConnectionFactory;
import org.eclipse.jetty.http2.server.HTTP2ServerConnectionFactory;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
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
    private static final String DH_PARAM_SECURITY_PROPERTY_NAME =
            "jdk.tls.server.defaultDHEParameters";

    // Set a different (i.e. uncommon) diffie-hellman exchange parameters prime
    static
    {
        // Only seems to take effect when setting the Java System Property
        // jdk.tls.ephemeralDHKeySize e.g. '-Djdk.tls.ephemeralDHKeySize=3072'
        // see the build.gradle for where that parameter is set.
        Security.setProperty( DH_PARAM_SECURITY_PROPERTY_NAME,
                              "{00d08a1ef24942257b67802038a29a"
                              + "ec32503e6d9d1bf316b24512f295d3"
                              + "276f5fbb5065eba08d62ab11fe38af"
                              + "1bc6d52ae0ba1d01ae7216dd078e7a"
                              + "5a9098b3bba3ce3c0c26f4c1dfaa3a"
                              + "248ec91c6b89d62dfb313dfda2cc8c"
                              + "3699c0619e00b0fd33cdf7737de18f"
                              + "ceb6d37a9e473983bc0348bee7a799"
                              + "8c3faf7afaa86efd5c3ea62aaf6f83"
                              + "f9fa7b15df9a1905e3d6ccbcfcc545"
                              + "a69e46c3e5a0e2bdf4b2241560b141"
                              + "7785503a93d0c6d4134164301b50d1"
                              + "329044f21b975abb62211724c3ce8a"
                              + "6eab1add04df36d244527b18d79092"
                              + "3f5c51f66fcabb20b0a8caf0f3900b"
                              + "fe0d31b3ed54b761a29b6e300b527d"
                              + "5462cc0211c27cf2a5f109c7dc5598"
                              + "f1ecdf03d069de23265f2dff7deef0"
                              + "cb35c63fc46a4c7844bba5f4e14c1f"
                              + "a562c9733977dfd4185a8b371586f1"
                              + "9527ae438f4749a3971c0a4ed02efc"
                              + "d7dbcc23c81718b0570f13f41a30ad"
                              + "321ccca10d42cf896950530ccf361a"
                              + "a7a2ffa0630e7ed6a0971e15877c3e"
                              + "2da9b86dc042d01c4fce3efcda0a30"
                              + "89d3bb0a4fe7d14900b3,2}" );
    }

    private static final Logger LOGGER = LoggerFactory.getLogger( Tasker.class );

    private static final int SERVER_PORT = 8443;

    /**
     * We purposely set a count of server threads for memory predictability
     * in the docker environment. It is probably fine to use Jetty's default of
     * 200 or another value, but starting with something small.
     */
    static final int MAX_SERVER_THREADS = 100;


    /**
     * Tasker receives requests for wres runs and passes them along to queue.
     * Actual work is done in WresJob restlet class, Tasker sets up a server.
     * @param args unused args
     * @throws TaskerFailedToStartException when jetty server start fails (?)
     */

    public static void main( String[] args )
    {
        // Test connectivity to services (dependencies) by creating WresJob
        WresJob wresJob = new WresJob();

        try
        {
            String result = wresJob.getWresJob();
            LOGGER.info( "{}: I will take wres job requests and queue them.",
                         result );
        }
        catch ( WresJob.ConnectivityException ce )
        {
            LOGGER.error( "Connectivity failure. Shutting down and exiting.",
                          ce );
            WresJob.shutdownNow();
            System.exit( 2 );
        }

        // Following example:
        // http://nikgrozev.com/2014/10/16/rest-with-embedded-jetty-and-jersey-in-a-single-jar-step-by-step/

        ServletContextHandler context = new ServletContextHandler( ServletContextHandler.NO_SESSIONS );
        context.setContextPath( "/" );

        ServletHolder dynamicHolder = context.addServlet( ServletContainer.class,
                                                          "/*" );

        // Multiple ways of binding using jersey, but Application is standard,
        // see here:
        // https://stackoverflow.com/questions/22994690/which-init-param-to-use-jersey-config-server-provider-packages-or-javax-ws-rs-a#23041643
        dynamicHolder.setInitParameter( "jakarta.ws.rs.Application",
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

        HttpConfiguration httpConfig = new HttpConfiguration();

        // Support HTTP/1.1
        HttpConnectionFactory httpOneOne = new HttpConnectionFactory( httpConfig );

        // Support HTTP/2
        HTTP2ServerConnectionFactory httpTwo = new HTTP2ServerConnectionFactory( httpConfig);

        // Support ALPN
        ALPNServerConnectionFactory alpn = new ALPNServerConnectionFactory();
        alpn.setDefaultProtocol( httpOneOne.getProtocol() );

        // Use TLS
        SslContextFactory.Server contextFactory = Tasker.getSslContextFactory();
        httpConfig.addCustomizer( new SecureRequestCustomizer() );
        SslConnectionFactory tlsConnectionFactory =
                new SslConnectionFactory( contextFactory, alpn.getProtocol() );

        try ( ServerConnector serverConnector = new ServerConnector( jettyServer,
                                                                     tlsConnectionFactory,
                                                                     alpn,
                                                                     httpOneOne,
                                                                     httpTwo ) )
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
        catch( Exception e )
        {
            // Wrap and propagate: #84777
            throw new TaskerFailedToStartException( "While attempting to start a jetty server within the WRES Tasker.", 
                                                    e );
        }
        finally
        {
            WresJob.shutdownNow();
            if( jettyServer.isStarted() )
            {
                jettyServer.destroy();
            }
        }
    }
    
    private static class TaskerFailedToStartException extends RuntimeException
    {
        private static final long serialVersionUID = 8797327489673141317L;

        private TaskerFailedToStartException( String message, Throwable cause )
        {
            super( message, cause );
        }
    }

    private static SslContextFactory.Server getSslContextFactory()
    {
        SslContextFactory.Server sslContextFactory = new SslContextFactory.Server();
        String ourServerCertificateFilename = BrokerHelper.getSecretsDir()
                                              + "/***REMOVED***wres"
                                              + Environment.getEnvironmentSuffix()
                                              + "_server_private_key_and_x509_cert.p12";
        sslContextFactory.setKeyStoreType( "PKCS12" );
        sslContextFactory.setKeyStorePassword( "wres-web-passphrase" );
        sslContextFactory.setKeyStorePath( ourServerCertificateFilename );
        return sslContextFactory;
    }


}
