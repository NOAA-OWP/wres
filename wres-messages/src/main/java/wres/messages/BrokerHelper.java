package wres.messages;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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
    static final String BROKER_HOST_PROPERTY_NAME = "wres.broker";
    static final String DEFAULT_BROKER_HOST = "localhost";

    static final String BROKER_VHOST_PROPERTY_NAME = "wres.broker.vhost";
    static final String DEFAULT_BROKER_VHOST = "wres";

    static final int BROKER_PORT = 5671;

    static final String SECRETS_DIR_PROPERTY_NAME = "wres.secrets_dir";
    static final String DEFAULT_SECRETS_DIR = "/wres_secrets";

    public enum Role
    {
        WORKER,
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

    public static String getBrokerVhost()
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


    /** Helper to get the broker port number. */
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
     * trusted certificates file available on the classpath named
     * trustedCertificateAuthorities.jks
     * @return the TrustManager
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
            throw new IllegalStateException( "WRES expected JRE to have JKS KeyStore instance", kse );
        }

        InputStream customTrustStoreFile = BrokerHelper.class.getClassLoader()
                                                       .getResourceAsStream( ourCustomTrustFileName );

        try
        {
            customTrustStore.load( customTrustStoreFile,
                                   "changeit".toCharArray() );
        }
        catch ( IOException | NoSuchAlgorithmException | CertificateException e )
        {
            throw new IllegalStateException( "WRES could not open TrustStoreFile " + ourCustomTrustFileName, e );
        }

        TrustManagerFactory trustManagerFactory;
        String algorithm = "PKIX";

        try
        {
            trustManagerFactory = TrustManagerFactory.getInstance( algorithm );
        }
        catch ( NoSuchAlgorithmException nsae )
        {
            throw new IllegalStateException( "WRES expected JRE to support algorithm '"
                                             + algorithm + "'.", nsae );
        }

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
     * Get an SSLContext that is set up with a client certificate,
     * used to authenticate to the wres-broker.
     * @param role the role of the module connecting to the broker
     * @return SSLContext ready to go for connecting to the broker
     * @throws IllegalStateException when anything goes wrong setting up
     * keystores, trust managers, factories, reading files, parsing certificate,
     * decrypting contents, etc.
     */

    public static SSLContext getSSLContextWithClientCertificate( Role role )
    {
        String ourClientCertificateFilename = BrokerHelper.getSecretsDir() + "/"
                                              + "wres-" + role.name()
                                                             .toLowerCase()
                                              + "_client_private_key_and_x509_cert.p12";
        char[] keyPassphrase = ("wres-" + role.name().toLowerCase()
                                + "-passphrase").toCharArray();
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
