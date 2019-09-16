package wres.system;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.Objects;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

/**
 * Suppose you want to make an SSL/TLS connection, suppose you know either the
 * exact certificate or the signer of the certificate that you wish to trust
 * just for this connection, and no others. Create one of these. Now you can
 * get a TrustManager that trusts that certificate. You also can get an
 * SSLContext wrapping that TrustManager. You can also get an SSLSocketFactory
 * from that SSLContext wrapping that TrustManager.
 * Tries to fail early, on construction, by doing its work during construction.
 */

public class SSLStuffThatTrustsOneCertificate
{
    private final TrustManager trustManager;
    private final SSLContext sslContext;

    /**
     * Create JSSE goo that trusts a single specified certificate.
     * (Canonical constructor accepting an InputStream)
     * @param derEncodedCertificate the certificate, intermediate cert, or CA
     * @throws IllegalStateException when any exceptions occur setting up SSL
     */
    public SSLStuffThatTrustsOneCertificate( InputStream derEncodedCertificate )
    {
        this.trustManager = getTrustManagerWithOneAuthority( derEncodedCertificate );
        this.sslContext = getSSLContextWithTrustManager( this.trustManager );
    }

    /**
     * Create JSSE goo that trusts a single specified certificate.
     * (Convenience constructor accepting a File)
     * @param derEncodedCertificate the certificate, intermediate cert, or CA
     * @throws IllegalArgumentException when failing to read the file
     * @throws IllegalStateException when any exceptions occur setting up SSL
     */
    SSLStuffThatTrustsOneCertificate( File derEncodedCertificate )
    {
        try ( InputStream inputStream = new FileInputStream( derEncodedCertificate ) )
        {
            this.trustManager = getTrustManagerWithOneAuthority( inputStream );
        }
        catch ( IOException ioe )
        {
            throw new IllegalArgumentException( "Failed to read file " + derEncodedCertificate,
                                                ioe );
        }
        this.sslContext = getSSLContextWithTrustManager( this.trustManager );
    }

    SSLSocketFactory getSSLSocketFactory()
    {
        return this.getSSLContext()
                   .getSocketFactory();
    }

    TrustManager getTrustManager()
    {
        return this.trustManager;
    }

    public SSLContext getSSLContext()
    {
        return this.sslContext;
    }


    /**
     * Returns a TrustManager that trusts only the certificate passed in
     * @param derEncodedCertificate an InputStream with the certificate
     * @return the TrustManager
     */

    private TrustManager getTrustManagerWithOneAuthority( InputStream derEncodedCertificate )
    {
        Objects.requireNonNull( derEncodedCertificate );
        KeyStore customTrustStore;

        try
        {
            customTrustStore = KeyStore.getInstance( KeyStore.getDefaultType() );
        }
        catch ( KeyStoreException kse )
        {
            throw new IllegalStateException( "WRES expected JRE to have default KeyStore type", kse );
        }

        try
        {
            customTrustStore.load( null,
                                   "changeit".toCharArray() );
        }
        catch ( IOException | NoSuchAlgorithmException | CertificateException e )
        {
            throw new IllegalStateException( "WRES could not create a custom trust store", e );
        }

        try
        {
            CertificateFactory certificateFactory = CertificateFactory.getInstance( "X.509" );
            Certificate certificate = certificateFactory.generateCertificate( derEncodedCertificate );
            customTrustStore.setCertificateEntry( "SoleTrusted", certificate );
        }
        catch ( CertificateException | KeyStoreException e )
        {
            throw new IllegalStateException( "WRES could not add custom certificate to custom trust store.", e );
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
     * Set up a TLS 1.2 context that uses the supplied trustManager
     * @param trustManager the trust manager to use.
     * @return an SSLContext
     */
    private SSLContext getSSLContextWithTrustManager( TrustManager trustManager )
    {
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

        TrustManager[] trustManagers = { trustManager };

        try
        {
            sslContext.init( null, trustManagers, null );
        }
        catch ( KeyManagementException kme )
        {
            throw new IllegalStateException( "WRES unable to initialize SSLContext", kme );
        }

        return sslContext;
    }

}
