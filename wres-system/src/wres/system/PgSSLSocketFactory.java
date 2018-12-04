package wres.system;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import javax.net.ssl.SSLSocketFactory;

/**
 * Class to help set up TLS/SSL with postgres which expects a very specific
 * thing: a name of a class that extends SSLSocketFactory that has a single-arg
 * constructor or a no-arg constructor. Use the single-arg constructor so we can
 * pass in the pem raw string of the certificate for the TLS/SSL stuff to trust.
 *
 * Delegates to the a wrapped socket factory obtained from the
 * SSLStuffThatTrustsOneCertificate.
 */

public class PgSSLSocketFactory extends SSLSocketFactory
{
    private final SSLSocketFactory socketFactory;

    /**
     * Uses an SSLStuffThatTrustsOneCertificate to get a socket factory
     * @param derEncodedCertificateToTrust the path of the certificate to trust,
     *                                     prefixed with "classpath:" if on the
     *                                     classpath, or the absolute path, or
     *                                     a verbatim der-encoded certificate.
     * (The three options are inspired by the excellent example of mariadb:
     * https://mariadb.com/kb/en/library/using-tlsssl-with-mariadb-connectorj/#provide-certificate-directly
     */
    public PgSSLSocketFactory( String derEncodedCertificateToTrust )
    {
        Objects.requireNonNull( derEncodedCertificateToTrust );

        if ( derEncodedCertificateToTrust.startsWith( "classpath:" ) )
        {
            String fileOnClassPath =
                    derEncodedCertificateToTrust.substring( 10 );

            try ( InputStream inputStream = PgSSLSocketFactory.class
                    .getClassLoader().getResourceAsStream( fileOnClassPath ) )
            {
                SSLStuffThatTrustsOneCertificate sslGoo =
                        new SSLStuffThatTrustsOneCertificate( inputStream );
                this.socketFactory = sslGoo.getSSLSocketFactory();
            }
            catch ( IOException ioe )
            {
                throw new IllegalArgumentException( "Failed to read certificate from "
                                                    + derEncodedCertificateToTrust,
                                                    ioe );
            }
        }
        else if ( derEncodedCertificateToTrust.startsWith( "-----BEGIN CERTIFICATE-----") )
        {
            try ( InputStream inputStream =
                          new ByteArrayInputStream(
                                  derEncodedCertificateToTrust.getBytes(
                                          StandardCharsets.US_ASCII ) ) )
            {
                SSLStuffThatTrustsOneCertificate sslGoo
                        = new SSLStuffThatTrustsOneCertificate( inputStream );
                this.socketFactory = sslGoo.getSSLSocketFactory();
            }
            catch ( IOException ioe )
            {
                throw new IllegalArgumentException( "Failed to decode certificate.",
                                                    ioe );
            }
        }
        else
        {
            try ( InputStream inputStream = new FileInputStream( derEncodedCertificateToTrust ) )
            {
                SSLStuffThatTrustsOneCertificate sslGoo
                        = new SSLStuffThatTrustsOneCertificate( inputStream );
                this.socketFactory = sslGoo.getSSLSocketFactory();
            }
            catch ( IOException ioe )
            {
                throw new IllegalArgumentException( "Failed to read certificate from "
                                                    + derEncodedCertificateToTrust,
                                                    ioe );
            }
        }
    }

    private PgSSLSocketFactory()
    {
        throw new UnsupportedOperationException( "Default constructor not supported." );
    }

    @Override
    public String[] getDefaultCipherSuites()
    {
        return this.socketFactory.getDefaultCipherSuites();
    }

    @Override
    public String[] getSupportedCipherSuites()
    {
        return this.socketFactory.getSupportedCipherSuites();
    }

    @Override
    public Socket createSocket( Socket socket, String s, int i, boolean b )
            throws IOException
    {
        return this.socketFactory.createSocket( socket, s, i, b );
    }

    @Override
    public Socket createSocket() throws IOException
    {
        return this.socketFactory.createSocket();
    }

    @Override
    public Socket createSocket( String s, int i )
            throws IOException, UnknownHostException
    {
        return this.socketFactory.createSocket( s, i );
    }

    @Override
    public Socket createSocket( String s,
                                int i,
                                InetAddress inetAddress,
                                int i1 )
            throws IOException, UnknownHostException
    {
        return this.socketFactory.createSocket( s, i, inetAddress, i1 );
    }

    @Override
    public Socket createSocket( InetAddress inetAddress, int i )
            throws IOException
    {
        return this.socketFactory.createSocket( inetAddress, i );
    }

    @Override
    public Socket createSocket( InetAddress inetAddress,
                                int i,
                                InetAddress inetAddress1,
                                int i1 ) throws IOException
    {
        return this.socketFactory.createSocket( inetAddress, i, inetAddress1, i1 );
    }

    @Override
    public Socket createSocket( Socket s,
                                InputStream consumed,
                                boolean autoClose ) throws IOException
    {
        return this.socketFactory.createSocket( s, consumed, autoClose );
    }
}
