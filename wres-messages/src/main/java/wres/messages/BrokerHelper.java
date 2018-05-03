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

public class BrokerHelper
{
    private static final String BROKER_HOST_PROPERTY_NAME = "wres.broker";
    private static final String DEFAULT_BROKER_HOST = "localhost";

    private static final String BROKER_VHOST_PROPERTY_NAME = "wres.broker.vhost";
    private static final String DEFAULT_BROKER_VHOST = "wres";

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

        InputStream customTrustStoreFile = BrokerHelper.class.getClassLoader()
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

    public static SSLContext getSSLContextWithClientCertificate()
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
