package wres.tasker;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.messages.BrokerHelper;

public class Tasker
{
    private static final Logger LOGGER = LoggerFactory.getLogger( Tasker.class );

    private static final int SERVER_PORT = 8443;

    private static final String ENV_PROPERTY_NAME = "wres.env";
    private static final String DEFAULT_ENV = ""; // Production is blank

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
        resourceHandler.setBaseResource( Resource.newClassPathResource( "job_tester.html" ) );

        // Have to chain/wrap the handler this way to get both static/dynamic:
        resourceHandler.setHandler( context );

        Server jettyServer = new Server();

        jettyServer.setHandler( resourceHandler );

        SslContextFactory contextFactory = Tasker.getSslContextFactory();
        ServerConnector serverConnector = new ServerConnector( jettyServer, contextFactory );
        serverConnector.setPort( Tasker.SERVER_PORT );
        ServerConnector[] serverConnectors = { serverConnector };

        jettyServer.setConnectors( serverConnectors );

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
     * Returns an environment descriptor if one has been specified by
     * ENV_PROPERTY_NAME
     * @return ITSG-created descriptor such as "ti" or "dev", non-null.
     */
    private static String getEnvironmentDescriptor()
    {
        String envFromDashD = System.getProperty( ENV_PROPERTY_NAME );

        if ( envFromDashD != null )
        {
            return envFromDashD;
        }
        else
        {
            return DEFAULT_ENV;
        }
    }


    /**
     * Returns a string that can be appended to a base hostname in order to
     * vary the hostname used per-environent. If there is a production string
     * it will be blank. If there is a "ti" or "dev" string it will be "-ti"
     * or "-dev" respectively
     * @return an always-appendable-to-hostname, non-null string
     */

    private static String getEnvironmentSuffix()
    {
        String descriptor = Tasker.getEnvironmentDescriptor();

        if ( descriptor.isEmpty() )
        {
            return "";
        }
        else
        {
            return "-" + descriptor;
        }
    }
}
