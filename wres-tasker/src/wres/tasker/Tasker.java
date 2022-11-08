package wres.tasker;

import java.io.IOException;
import java.security.Security;
import java.util.EnumSet;

import org.eclipse.jetty.alpn.server.ALPNServerConnectionFactory;
import org.eclipse.jetty.http2.server.HTTP2ServerConnectionFactory;
import org.eclipse.jetty.server.CustomRequestLog;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.Slf4jRequestLogWriter;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.servlet.DispatcherType;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletResponse;

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

    public static final String PATH_TO_SERVER_P12_PNAME = "wres.taskerPathToServerP12";
    private static final String PATH_TO_SERVER_P12;

    /** A format like EXTENDED_NCSA_FORMAT but without an extra timestamp */
    private static final String ACCESS_LOG_FORMAT = "%{client}a - %u \"%r\" %s %O \"%{Referer}i\" \"%{User-Agent}i\"";

    static
    {
        String serverP12 = System.getProperty( PATH_TO_SERVER_P12_PNAME );

        if ( serverP12 != null )
        {
            PATH_TO_SERVER_P12 = serverP12;
        }
        else
        {

            PATH_TO_SERVER_P12 = "wres-tasker_server_private_key_and_x509_cert.p12";
        }
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
        //Set up the ServlentContextHandler and add the ContextFilter for X-Frame-Options for all requests.
        ServletContextHandler context = new ServletContextHandler( ServletContextHandler.NO_SESSIONS );
        context.setContextPath( "/" );
        ServletHolder dynamicHolder = context.addServlet( ServletContainer.class,
                                                          "/*" );
        context.addFilter(ContextFilter.class, "/*",  EnumSet.of(DispatcherType.REQUEST)) ;

        // Multiple ways of binding using jersey, but Application is standard,
        // see here:
        // https://stackoverflow.com/questions/22994690/which-init-param-to-use-jersey-config-server-provider-packages-or-javax-ws-rs-a#23041643
        dynamicHolder.setInitParameter( "jakarta.ws.rs.Application",
                                        JaxRSApplication.class.getCanonicalName() );

        // To handle multipart post data, unfortunately jersey-specific:
        dynamicHolder.setInitParameter( "jersey.config.server.provider.classnames",
                                        MultiPartFeature.class.getCanonicalName() );

        // Static handler:
        ResourceHandler resourceHandler = new ResourceHandler();
        resourceHandler.setBaseResource( Resource.newClassPathResource( "html" ) );

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
        HTTP2ServerConnectionFactory httpTwo = new HTTP2ServerConnectionFactory( httpConfig );

        // Support ALPN
        ALPNServerConnectionFactory alpn = new ALPNServerConnectionFactory();
        alpn.setDefaultProtocol( httpOneOne.getProtocol() );

        // Use TLS
        SslContextFactory.Server contextFactory = Tasker.getSslContextFactory(
                                                                               PATH_TO_SERVER_P12 );
        httpConfig.addCustomizer( new SecureRequestCustomizer() );
        SslConnectionFactory tlsConnectionFactory =
                new SslConnectionFactory( contextFactory, alpn.getProtocol() );
        jettyServer.setRequestLog(
                                   new CustomRequestLog( new Slf4jRequestLogWriter(),
                                                         ACCESS_LOG_FORMAT ) );

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
        catch ( Exception e )
        {
            // Wrap and propagate: #84777
            throw new TaskerFailedToStartException( "While attempting to start a jetty server within the WRES Tasker.",
                                                    e );
        }
        finally
        {
            WresJob.shutdownNow();
            if ( jettyServer.isStarted() )
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
    
    /**
     * A filter that sets X-Frame-Options to DENY on the HTTP header in order to mitigate against clickbait attacks.
     * Note that the capitalization matches that used in the proxy, so I'm sticking with X-Frame-Options for conistency.
     */
    public static class ContextFilter implements Filter
    {
        @Override
        public void doFilter( ServletRequest request, ServletResponse response, FilterChain chain )
                throws IOException, ServletException
        {
            HttpServletResponse res = (HttpServletResponse) response;
            res.addHeader( "X-Frame-Options", "DENY" );
            chain.doFilter( request, response );
        }

        // Hidden constructor. Privacy level needs to be consistent with the class setting.
        public ContextFilter()
        {
        }
    }

    private static SslContextFactory.Server getSslContextFactory( String pathToP12 )
    {
        SslContextFactory.Server sslContextFactory = new SslContextFactory.Server();
        sslContextFactory.setKeyStoreType( "PKCS12" );
        sslContextFactory.setKeyStorePassword( "wres-web-passphrase" );
        sslContextFactory.setKeyStorePath( pathToP12 );
        return sslContextFactory;
    }
}

