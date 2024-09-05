package wres.messages;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper for worker and tasker to get information about the broker, e.g.
 * hostname, virtual hostname, port, connection setup, etc.
 *
 * For the time being, attempting to avoid a dependency on rabbitmq jars,
 * as this is an optional-for-clients-to-use helper, just like the messages
 * themselves are optional-for-clients-to-use. Keeps the explicit connection
 * setup (however repetitive) in the worker and tasker. If this becomes absurd
 * at some point, maybe a getPreBakedConnectionFactory() method can be added
 * here.
 */

public class BrokerHelper
{
    private static final Logger LOGGER = LoggerFactory.getLogger( BrokerHelper.class );

    static final String BROKER_HOST_PROPERTY_NAME = "wres.broker";
    static final String DEFAULT_BROKER_HOST = "localhost";

    static final String BROKER_VHOST_PROPERTY_NAME = "wres.broker.vhost";
    static final String DEFAULT_BROKER_VHOST = "wres";

    static final int BROKER_PORT = 5671;

    static final String SECRETS_DIR_PROPERTY_NAME = "wres.secrets_dir";
    static final String DEFAULT_SECRETS_DIR = "/wres_secrets";

    static final String TRUST_STORE_PROPERTY_NAME = "wres.trustStore";

    /**
     * The role.
     */
    public enum Role
    {
        /** Worker role. */
        WORKER,
        /** Tasker role. */
        TASKER
    }

    private BrokerHelper()
    {
        // Static helper class, no construction
    }

    /**
     * Helper to get the broker host name. Returns what was set in -D args
     * or a default value if -D is not set.
     * @return the broker host name to try connecting to.
     */

    public static String getBrokerHost()
    {
        String brokerFromDashD = System.getProperty( BROKER_HOST_PROPERTY_NAME );

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

    public static String getBrokerVhost()
    {
        String brokerVhostFromDashD = System.getProperty( BROKER_VHOST_PROPERTY_NAME );

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
     * Return the port of the broker.
     * @return the broker port number to use.
     */
    public static int getBrokerPort()
    {
        return BROKER_PORT;
    }


    /**
     * Return the secrets directory, either from -D args or a default if -D is
     * not set. The secrets directory has PKCS#12 files with client certificate
     * and client key inside each, for clients to authenticate to the broker.
     * The convention for the filename is
     * ${secretsDir}/wres-${role}_client_private_key_and_x509_cert.p12
     * @return the secrets directory containing p12 files for authentication
     */

    public static String getSecretsDir()
    {

        String secretsDirFromDashD = System.getProperty( SECRETS_DIR_PROPERTY_NAME );

        if ( secretsDirFromDashD != null )
        {
            return secretsDirFromDashD;
        }
        else
        {
            return DEFAULT_SECRETS_DIR;
        }
    }

    /**
     * Returns a TrustManager that trusts the certificates contained in a
     * trusted certificates file passed through -Dwres.trustStore and
     * is a file that exists and has no exception when loading.
     * @return the TrustManager
     */

    public static TrustManager getDefaultTrustManager()
    {
        String ourCustomTrustFileName = "trustedCertificateAuthorities.jks";

        // Does KeyStore.getInstance( "JKS" ) but translates checked exception
        // to unchecked exception.
        KeyStore customTrustStore = BrokerHelper.getJKSKeyStore();

        String trustStore = System.getProperty( TRUST_STORE_PROPERTY_NAME );

        Path alternativeTrustStorePath = Paths.get( trustStore );
        File alternativeTrustStoreFile = alternativeTrustStorePath.toFile();

        if ( alternativeTrustStoreFile.isFile()
             && alternativeTrustStoreFile.canRead() )
        {
            try ( InputStream alternativeTrustStream =
                    new FileInputStream( alternativeTrustStoreFile ) )
            {
                customTrustStore.load( alternativeTrustStream,
                                       "changeit".toCharArray() );

                // Only when the alternative exists, is read, is loaded do
                // we skip later step of using truststore from classpath.
                LOGGER.warn( "Trusting alternative Certificate Authority at '{}'",
                             trustStore );
            }
            catch ( IOException | NoSuchAlgorithmException | CertificateException e )
            {
                LOGGER.warn( "Could not use alternative Certificate Authority at '{}'",
                             trustStore,
                             e );
                // Continue and use the default trust store.
            }
        }

        // Does TrustManagerFactory.getInstance( "PKIX" ) but translates checked
        // exceptions to unchecked exception.
        TrustManagerFactory trustManagerFactory = BrokerHelper.getPKIXTrustManagerFactory();

        try
        {
            trustManagerFactory.init( customTrustStore );
        }
        catch ( KeyStoreException kse )
        {
            throw new IllegalStateException( "WRES expected to be able to initialize trust manager factory.",
                                             kse );
        }

        for ( TrustManager trustManager : trustManagerFactory.getTrustManagers() )
        {
            if ( trustManager instanceof X509TrustManager )
            {
                return trustManager;
            }
        }

        throw new IllegalStateException( "WRES expected an X509TrustManager to exist in JRE, but no trust manager was found." );
    }

    /**
     * Calls KeyStore.getInstance( "JKS" ) and translates checked exceptions to
     * unchecked exception.
     * @return a KeyStore instance of type JKS
     * @throws IllegalStateException when KeyStoreException occurs
     */

    private static KeyStore getJKSKeyStore()
    {
        try
        {
            return KeyStore.getInstance( "JKS" );
        }
        catch ( KeyStoreException kse )
        {
            throw new IllegalStateException( "WRES expected JRE to have JKS KeyStore instance", kse );
        }

    }


    /**
     * Calls TrustManagerFactory.getInstance( "PKIX" ) and translates checked
     * exceptions to unchecked exception.
     * @return the TrustManagerFactory
     * @throws IllegalStateException when NoSuchAlgorithmException occurs
     */
    private static TrustManagerFactory getPKIXTrustManagerFactory()
    {
        String algorithm = "PKIX";

        try
        {
            return TrustManagerFactory.getInstance( algorithm );
        }
        catch ( NoSuchAlgorithmException nsae )
        {
            throw new IllegalStateException( "WRES expected JRE to support algorithm '"
                                             + algorithm
                                             + "'.",
                                             nsae );
        }
    }


    /**
     * Get an SSLContext that is set up with a client certificate,
     * used to authenticate to the wres-broker.
     * @param role the role of the module connecting to the broker
     * @param pathToP12 The path to the p12 file to use. Must be non-null and not empty.
     * @param passwordForP12 The password to use. If null or empty, no password isused.
     * @return SSLContext ready to go for connecting to the broker
     * @throws IllegalStateException when anything goes wrong setting up
     * keystores, trust managers, factories, reading files, parsing certificate, 
     * decrypting contents, etc. 
     */

    public static SSLContext getSSLContextWithClientCertificate( String pathToP12, String passwordForP12 )
    {
        if (pathToP12 == null || pathToP12.isEmpty())
        {
            throw new IllegalArgumentException("Argument pathToP12 cannot be null or empty, but was.");
        }
        char[] keyPassphrase = new char[]{};
        if ( passwordForP12 != null && !passwordForP12.isEmpty() )
        {
            keyPassphrase = passwordForP12.toCharArray();
        }
        else
        {
            LOGGER.warn("For file " + pathToP12 + " password provided is null or empty, so no password is assumed.");
        }

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

        try ( InputStream clientCertificateInputStream =
                new FileInputStream( pathToP12 ) )
        {
            keyStore.load( clientCertificateInputStream, keyPassphrase );
        }
        catch ( IOException | NoSuchAlgorithmException | CertificateException e )
        {
            throw new IllegalStateException( "WRES expected to find a file '"
                                             + pathToP12
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
                                             + pathToP12
                                             +
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
                                             + protocol
                                             + "'",
                                             nsae );
        }

        TrustManager[] trustManagers = { BrokerHelper.getDefaultTrustManager() };

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

