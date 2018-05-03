package wres.worker;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

import com.rabbitmq.client.DefaultSaslConfig;
import com.rabbitmq.client.SaslConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A long-running, light-weight process that takes a job from a queue, and runs
 * a single WRES instance for the job taken from the queue, and repeats.
 */

public class Worker
{
    private static final Logger LOGGER = LoggerFactory.getLogger( Worker.class );
    private static final String RECV_QUEUE_NAME = "wres.job";

    private static final String BROKER_HOST_PROPERTY_NAME = "wres.broker";
    private static final String DEFAULT_BROKER_HOST = "localhost";

    private static final String BROKER_VHOST_PROPERTY_NAME = "wres.broker.vhost";
    private static final String DEFAULT_BROKER_VHOST = "wres";

    private static final int BROKER_PORT = 5671;

    /**
     * Expects exactly one arg with a path to WRES executable
     * @param args arguments, but only one is expected, a WRES executable
     * @throws IOException when communication with queue fails or process start fails.
     * @throws IllegalArgumentException when the first argument is not a WRES executable
     * @throws java.net.ConnectException when connection to queue fails.
     * @throws TimeoutException when connection to the queue times out.
     * @throws InterruptedException when interrupted while waiting for work.
     * @throws NoSuchAlgorithmException when TLSv1.2 is unavailable
     * @throws KeyManagementException when creating trust manager fails?
     * @throws IllegalStateException when setting up our custom trust list fails
     */

    public static void main( String[] args )
            throws IOException, TimeoutException, InterruptedException,
            NoSuchAlgorithmException, KeyManagementException
    {
        if ( args.length != 1 )
        {
            throw new IllegalArgumentException( "First arg must be an executable wres path." );
        }

        // Getting as a file allows us to verify it exists
        File wresExecutable = Paths.get( args[0] ).toFile();

        if ( !wresExecutable.exists() )
        {
            throw new IllegalArgumentException( "First arg must be an executable wres *path*." );
        }
        else if ( !wresExecutable.canExecute() )
        {
            throw new IllegalArgumentException( "First arg must be an *executable* wres path." );
        }

        // Determine the actual broker name, whether from -D or default
        String brokerHost = Worker.getBrokerHost();
        String brokerVhost = Worker.getBrokerVhost();
        LOGGER.info( "Using broker at host '{}'", brokerHost );

        // Get work from the queue
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost( brokerHost );
        factory.setVirtualHost( brokerVhost );
        factory.setPort( BROKER_PORT );
        factory.setSaslConfig( DefaultSaslConfig.EXTERNAL );

        factory.useSslProtocol( Worker.getSSLContextWithClientCertificate() );

        try ( Connection connection = factory.newConnection();
              Channel receiveChannel = connection.createChannel() )
        {
            // Take precisely one job at a time:
            receiveChannel.basicQos( 1 );

            receiveChannel.queueDeclare( RECV_QUEUE_NAME, false, false, false, null );

            BlockingQueue<WresProcess> processToLaunch = new ArrayBlockingQueue<>( 1 );

            JobReceiver receiver = new JobReceiver( receiveChannel,
                                                    wresExecutable,
                                                    processToLaunch );

            receiveChannel.basicConsume( RECV_QUEUE_NAME, false, receiver );

            while ( true )
            {
                LOGGER.info( "Waiting for work..." );
                WresProcess wresProcess = processToLaunch.poll( 2, TimeUnit.SECONDS );

                if ( wresProcess != null )
                {
                    // Launch WRES if the consumer found a message saying so.
                    wresProcess.call();
                    // Tell broker it is OK to get more messages by acknowledging
                    receiveChannel.basicAck( wresProcess.getDeliveryTag(), false );
                }
            }
        }
    }

    /**
     * Helper to get the broker host name. Returns what was set in -D args
     * or a default value if -D is not set.
     * @return the broker host name to try connecting to.
     */

    private static String getBrokerHost()
    {
        String brokerFromDashD= System.getProperty( BROKER_HOST_PROPERTY_NAME );

        if ( brokerFromDashD != null )
        {
            return brokerFromDashD;
        }
        else
        {
            return DEFAULT_BROKER_HOST;
        }
    }

    /**
     * Helper to get the broker vhost name. Returns what was set in -D args
     * or a default value if -D is not set.
     * @return the broker host name to try connecting to.
     */

    private static String getBrokerVhost()
    {
        String brokerVhostFromDashD= System.getProperty( BROKER_VHOST_PROPERTY_NAME );

        if ( brokerVhostFromDashD != null )
        {
            return brokerVhostFromDashD;
        }
        else
        {
            return DEFAULT_BROKER_VHOST;
        }
    }

    /**
     * Return an X509 trust manager tied to our custom java trusted certificates
     * @return the default trust manager
     */
    private static TrustManager getDefaultTrustManager()
    {
        KeyStore customTrustStore;
        String ourCustomTrustFileName = "trustedCertificateAuthorities.jks";

        try
        {
            customTrustStore = KeyStore.getInstance( "JKS" );
        }
        catch ( KeyStoreException kse )
        {
            throw new IllegalStateException( "Expected jdk to have KeyStore.getDefaultType()", kse );
        }

        InputStream customTrustStoreFile = Worker.class.getClassLoader()
                                                       .getResourceAsStream( ourCustomTrustFileName );

        try
        {
            customTrustStore.load( customTrustStoreFile,
                                   "changeit".toCharArray() );
        }
        catch ( IOException | NoSuchAlgorithmException | CertificateException e )
        {
            throw new IllegalStateException( "Could not open " + ourCustomTrustFileName, e );
        }

        TrustManagerFactory trustManagerFactory;
        String algorithm = "PKIX";

        try
        {
            trustManagerFactory = TrustManagerFactory.getInstance( algorithm );
        }
        catch ( NoSuchAlgorithmException nsae )
        {
            throw new IllegalStateException( "No " + algorithm + " algorithm existed.", nsae );
        }

        try
        {
            trustManagerFactory.init( customTrustStore );
        }
        catch ( KeyStoreException kse )
        {
            throw new IllegalStateException( "Could not initialize trust manager factory.", kse );
        }

        for ( TrustManager trustManager : trustManagerFactory.getTrustManagers() )
        {
            if ( trustManager instanceof X509TrustManager )
            {
                return trustManager;
            }
        }

        throw new IllegalStateException( "No trust manager was found." );
    }


    /**
     * Get an SSLContext that is set up with a wres-worker client certificate,
     * used to authenticate to the wres-broker
     * @return SSLContext ready to go for connecting to the broker
     * @throws IllegalStateException when anything goes wrong setting up
     * keystores, trust managers, factories, reading files, parsing certificate,
     * decrypting contents, etc.
     */

    private static SSLContext getSSLContextWithClientCertificate()
    {
        String ourClientCertificateFilename = "wres-worker_client_private_key_and_x509_cert.p12";
        char[] keyPassphrase = "wres-worker-passphrase".toCharArray();
        KeyStore keyStore;

        try
        {
            keyStore = KeyStore.getInstance( "PKCS12" );
        }
        catch ( KeyStoreException kse )
        {
            throw new IllegalStateException( "WRES expected JVM to be able to read PKCS#12 keystores.",
                                             kse );
        }

        try
        {
            InputStream clientCertificateInputStream = new FileInputStream( ourClientCertificateFilename );
            keyStore.load( clientCertificateInputStream, keyPassphrase );
        }
        catch ( IOException | NoSuchAlgorithmException | CertificateException e )
        {
            throw new IllegalStateException( "WRES expected to find a file '"
                                             + ourClientCertificateFilename
                                             + "' with PKCS#12 format, with"
                                             + " both a client certificate AND"
                                             + " the private key inside, used "
                                             + "to authenticate to the broker.",
                                             e );
        }

        KeyManagerFactory keyManagerFactory;

        try
        {
            keyManagerFactory = KeyManagerFactory.getInstance( "SunX509" );
        }
        catch ( NoSuchAlgorithmException nsae )
        {
            throw new IllegalStateException( "WRES expected JVM to have SunX509.",
                                             nsae );
        }

        try
        {
            keyManagerFactory.init( keyStore, keyPassphrase );
        }
        catch ( KeyStoreException | NoSuchAlgorithmException | UnrecoverableKeyException e )
        {
            throw new IllegalStateException( "WRES expected to be able to read "
                                             + "and decrypt the file '"
                                             + ourClientCertificateFilename +
                                             "'.",
                                             e );
        }

        SSLContext sslContext;
        String protocol = "TLSv1.2";

        try
        {
            sslContext = SSLContext.getInstance( protocol );
        }
        catch ( NoSuchAlgorithmException nsae )
        {
            throw new IllegalStateException( "WRES expected to be able to use protocol '"
                                             + protocol + "'",
                                             nsae );
        }

        TrustManager[] trustManagers = { Worker.getDefaultTrustManager() };

        try
        {
            sslContext.init( keyManagerFactory.getKeyManagers(),
                             trustManagers,
                             null );
        }
        catch ( KeyManagementException kme )
        {
            throw new IllegalStateException( "WRES expected to be able to initialize SSLContext.", kme );
        }

        return sslContext;
    }
}
