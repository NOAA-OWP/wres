package wres.tasker;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.stream.Collectors;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;
import javax.xml.bind.annotation.XmlRootElement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.Credentials;
import okhttp3.OkHttpClient;

import wres.http.WebClient;
import wres.http.WebClientUtils;
import wres.messages.BrokerHelper;

/**
 * Helper for managing broker messaging.
 */

public abstract class BrokerManagerHelper
{
    private static final Logger LOGGER = LoggerFactory.getLogger( BrokerManagerHelper.class );

    /** Monitor authentication. */
    protected static final String WRES_MONITOR_PASSWORD_SYSTEM_PROPERTY_NAME =
            "wres.monitorPassword";

    /** Monitor port. */
    protected static final String BROKER_MANAGER_PORT_SYSTEM_PROPERTY_NAME =
            "wres.brokerManagerPort";

    /** Default port number, if the port is not specified in the environment. */
    private static final int DEFAULT_MANAGER_PORT = 15671;

    /** The port used by the broker manager. */
    private static int managerPort = DEFAULT_MANAGER_PORT;

    /** Client used to interact with the broker manager API. */
    private static final OkHttpClient HTTP_CLIENT;

    /** The base URL for the manager API endpoint. */
    private static final String MANAGER_URL;

    /** Object mapper used to read JSON. */
    private static final ObjectMapper MAPPER = new ObjectMapper();

    //Initialize static variables.
    static
    {
        LOGGER.info( "Initializing the broker manager helper, starting with the monitor password." );

        //Obtain the monitor password if available; use blank otherwise.
        String monitorPassword = System.getProperty( WRES_MONITOR_PASSWORD_SYSTEM_PROPERTY_NAME );
        if ( monitorPassword == null || monitorPassword.isBlank() )
        {
            LOGGER.warn( "Either no or an empty wres-monitor password was provided "
                         + "via environment variable "
                         + WRES_MONITOR_PASSWORD_SYSTEM_PROPERTY_NAME
                         + ". Assuming a blank password which will cause errors later." );
            monitorPassword = "";
        }
        final String monitorPasswordFinal = monitorPassword;

        //Manager port is not required, but must be valid if specified.
        String managerPortStr = System.getProperty( BROKER_MANAGER_PORT_SYSTEM_PROPERTY_NAME );
        if ( managerPortStr != null )
        {
            try
            {
                managerPort = Integer.parseInt( managerPortStr );
                LOGGER.info( "Environment variable specified manager port to be used is {}.", managerPort );
            }
            catch ( NumberFormatException ex )
            {
                LOGGER.warn( "Broker manager port specified by "
                             + BROKER_MANAGER_PORT_SYSTEM_PROPERTY_NAME
                             + " exists, but its value is not an integer: '"
                             + managerPortStr
                             + "'. Using it as is, but connections wil fail later." );
            }
        }

        MANAGER_URL = "https://" + BrokerHelper.getBrokerHost() + ":" + managerPort + "/api";
        LOGGER.info( "Broker manager API URL is {}.", MANAGER_URL );

        //The SSL context is that used by the tasker to talk to the broker.
        String p12Path = System.getProperty( Tasker.PATH_TO_CLIENT_P12_PNAME );
        if ( p12Path == null || p12Path.isEmpty() )
        {
            throw new IllegalStateException( "The system property " + Tasker.PATH_TO_CLIENT_P12_PNAME
                                             + " is not specified. It must be provided and non-empty." );
        }
        SSLContext sslMgmtContext =
                BrokerHelper.getSSLContextWithClientCertificate( p12Path,
                                                                 System.getProperty( Tasker.PASSWORD_TO_CLIENT_P12 ) );

        //The hokey casting below to X509TrustManager is because BrokerHelper.getDefaultTrustManager() is actually
        //guaranteed to return an instance of X509TrustManager, if anything is returned, and that is what this method
        //needs. I'm not sure why the method signature says its just returning a TrustManager.
        X509TrustManager trustManager = ( X509TrustManager ) BrokerHelper.getDefaultTrustManager();

        //The authenticator handles the username/password requirement for accessing the manager.
        HTTP_CLIENT = WebClientUtils.defaultTimeoutHttpClient()
                                    .newBuilder()
                                    .sslSocketFactory( sslMgmtContext.getSocketFactory(),
                                                       trustManager )
                                    .authenticator(
                                            ( route, response ) -> {
                                                String credential =
                                                        Credentials.basic( "wres-monitor",
                                                                           monitorPasswordFinal );
                                                return response.request()
                                                               .newBuilder()
                                                               .header( "Authorization", credential )
                                                               .build();
                                            } )
                                    .build();

    }

    /**
     * @return A count of the number of workers connected to the broker.
     * @throws IOException If this is unable to communicate with the broker manager API
     * or the JSON could not be parsed.
     */
    public static int getBrokerWorkerConnectionCount() throws IOException
    {
        //Determine the URL.
        String connectionsURL = MANAGER_URL + "/connections";
        URI mgmtURI = null;
        try
        {
            mgmtURI = new URI( connectionsURL );
        }
        catch ( URISyntaxException use )
        {
            LOGGER.error( "Failed to create URI from {}", connectionsURL, use );
        }

        //Create the web client.
        WebClient webClient = new WebClient( HTTP_CLIENT );
        WebClient.ClientResponse response = webClient.getFromWeb( mgmtURI );

        //Turn the response into a string.
        String result =
                new BufferedReader( new InputStreamReader( response.getResponse() ) ).lines()
                                                                                     .collect( Collectors.joining( "\n" ) );

        //Check the status code.
        int httpStatus = response.getStatusCode();
        if ( httpStatus >= 400 )
        {
            LOGGER.warn( "Broker manager API returned an error code. Full response: " + result );
            throw new IOException( "Unable to obtain a response from the Broker Manager API." );
        }

        //Read the JSON response.
        CollectionsResponseItem[] items = MAPPER.readValue( result, CollectionsResponseItem[].class );

        //Count the workers.
        int workerCount = 0;
        for ( CollectionsResponseItem item : items )
        {
            if ( item.getUser().equals( "wres-worker" ) )
            {
                workerCount++;
            }
        }
        return workerCount;
    }


    /**
     * Extracts the user from a JSON response for the collections endpoint.
     * @author Hank.Herr
     */
    @XmlRootElement
    @JsonIgnoreProperties( ignoreUnknown = true )
    private static class CollectionsResponseItem
    {
        private String user;

        String getUser()
        {
            return this.user;
        }

        void setUser( String user )
        {
            this.user = user;
        }
    }

}

