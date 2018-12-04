package wres.system;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import javax.net.ssl.SSLHandshakeException;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertNull;

public class SSLStuffThatTrustsOneCertificateTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * The intermediate certificate found that signed hc.apache.org certificate
     * as of 2018-11-15, will expire 2024-07-03. They will probably change certs
     * before then. Feel free to ignore the test if it starts failing due to
     * either a certificate change on https://hc.apache.org or you are reading
     * this in 2024-07.
     */
    private static final String APACHE_INTERMEDIATE_PEM_2018 =
            "-----BEGIN CERTIFICATE-----\n"
            + "MIIF8jCCA9qgAwIBAgIQUWEdv45DMOu87QrYvTIjzTANBgkqhkiG9w0BAQwFADCB\n"
            + "iDELMAkGA1UEBhMCVVMxEzARBgNVBAgTCk5ldyBKZXJzZXkxFDASBgNVBAcTC0pl\n"
            + "cnNleSBDaXR5MR4wHAYDVQQKExVUaGUgVVNFUlRSVVNUIE5ldHdvcmsxLjAsBgNV\n"
            + "BAMTJVVTRVJUcnVzdCBSU0EgQ2VydGlmaWNhdGlvbiBBdXRob3JpdHkwHhcNMTQw\n"
            + "NzA0MDAwMDAwWhcNMjQwNzAzMjM1OTU5WjBZMQswCQYDVQQGEwJVUzEQMA4GA1UE\n"
            + "ChMHU1NMLmNvbTEUMBIGA1UECxMLd3d3LnNzbC5jb20xIjAgBgNVBAMTGVNTTC5j\n"
            + "b20gSGlnaCBBc3N1cmFuY2UgQ0EwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEK\n"
            + "AoIBAQClFxgJFdeMFUw1Y4GEqcVHDiXJp7ukYtO31KKU+j/llv/DHKhRNO4CtbI0\n"
            + "febjCO2gMzYz3cvj9zgwWiX/oaeI5HWBqN3XqULI+UA84sBKzdsD5oELyfqC6Yyy\n"
            + "yPeXrS7SCQYblUZqmcjksbJgL2XA9nZ+iqiched+z4MWRbiU+DEklXssTPyo1f4A\n"
            + "QT033VxgkF6lNw+Q+Rm4FVQV7FrYQYj6mxSTFXtIvepbsszU6iy7iVXPA4RNb3Iu\n"
            + "REz8tBnce7+fwZkq+NPUlaKDUp82WvGHMwAzBULBZkd2iujWzDawFPVeYONb+P70\n"
            + "e///GTDhDvMe4E27zXoSW6ujZGh5AgMBAAGjggGEMIIBgDAfBgNVHSMEGDAWgBRT\n"
            + "eb9aqitKz1SA4dibwJ3ysgNmyzAdBgNVHQ4EFgQUTFQjZyl09ELqmbtoPBafCC8Q\n"
            + "5kEwDgYDVR0PAQH/BAQDAgGGMBIGA1UdEwEB/wQIMAYBAf8CAQAwHQYDVR0lBBYw\n"
            + "FAYIKwYBBQUHAwEGCCsGAQUFBwMCMCEGA1UdIAQaMBgwDAYKKwYBBAGCqTABATAI\n"
            + "BgZngQwBAgIwVQYDVR0fBE4wTDBKoEigRoZEaHR0cDovL2NybC50cnVzdC1wcm92\n"
            + "aWRlci5jb20vVVNFUlRydXN0UlNBQ2VydGlmaWNhdGlvbkF1dGhvcml0eS5jcmww\n"
            + "gYAGCCsGAQUFBwEBBHQwcjBEBggrBgEFBQcwAoY4aHR0cDovL2NydC50cnVzdC1w\n"
            + "cm92aWRlci5jb20vVVNFUlRydXN0UlNBQWRkVHJ1c3RDQS5jcnQwKgYIKwYBBQUH\n"
            + "MAGGHmh0dHA6Ly9vY3NwLnRydXN0LXByb3ZpZGVyLmNvbTANBgkqhkiG9w0BAQwF\n"
            + "AAOCAgEAKBjIAycQLrBawRYWFoRJIo4tP7ZkyEVRgd491E3beayYpTs+wLzbBOve\n"
            + "+v6wGx4ohIOACncEDgVy4bkBgYulsix6xWeVG1ZmF9WXhhFdP+3ZipAttMSh5s7m\n"
            + "U/5hAg5NvZ5h5VN/3EOJD8+0FOfEaUcgQFGorYXeL+/2+E/ixNWtTFl4ipZ7AU2k\n"
            + "+lSPxnsc4+3H1+EEea9xTmanMx+gY/tXEnyQ7iVtry4uUjHF+8ts0vvAlesra8PZ\n"
            + "5CNCSv9sqkG2Ipig2Srrw8zvrMZtJO+veymDDsc5edpjPbSY0A8XhYbhepjdid2V\n"
            + "kisp/RtyAq+CiWea3tH9y5BK0mDoiMhlJ59E52TsuHgZ7TpzoCnEsCWVPctj4e5N\n"
            + "uSs75oT1ap3wMHCvObincNovMaqtX8zy/oRr+NSoDQY2ZQ06viCHAKpudpUg4lLF\n"
            + "RKqW+UTCWDML6PriZsjXf6aSce4lK+UdmmL4VDh1Nf9y7FpSsNIaZfFrATrTfS1o\n"
            + "P8y0ZjapxUVVpiH11vIr8mqk6QT0WelbI4r4s45ZFjOeT69xSpsEIIcxYI1GpIbw\n"
            + "CMDvdMAmwzr2pur5PIKKVH1WkLtaf1aSruBTP/aeavS2P8NMy7tI1LKoNY8XvMFO\n"
            + "+E3jECsl+ke/Q3dfswLjNl+as1U10Ltgx7/IJanbDEQzVr6Sqgs=\n"
            + "-----END CERTIFICATE-----\n";

    @Test
    public void attemptToReachApacheDotOrg() throws IOException
    {
        byte[] certificateBytes =
                APACHE_INTERMEDIATE_PEM_2018.getBytes( StandardCharsets.US_ASCII );
        InputStream certificateStream =
                new ByteArrayInputStream( certificateBytes );

        SSLStuffThatTrustsOneCertificate sslStuff =
                new SSLStuffThatTrustsOneCertificate( certificateStream );

        SSLConnectionSocketFactory connectionSocketFactory =
                new SSLConnectionSocketFactory( sslStuff.getSSLContext(),
                                                new String[] { "TLSv1.2" },
                                                null,
                                                SSLConnectionSocketFactory.getDefaultHostnameVerifier() );
        HttpResponse response;

        try ( CloseableHttpClient httpClient =
                      HttpClients.custom()
                                 .setSSLSocketFactory( connectionSocketFactory )
                                 .build() )
        {
            HttpGet httpGet = new HttpGet( "https://hc.apache.org" );
            response = httpClient.execute( httpGet );
        }

        assertNotNull( response );
    }


    @Test
    public void attemptToReachGoogleWithApachesCertificate() throws IOException
    {
        byte[] certificateBytes =
                APACHE_INTERMEDIATE_PEM_2018.getBytes( StandardCharsets.US_ASCII );
        InputStream certificateStream =
                new ByteArrayInputStream( certificateBytes );

        SSLStuffThatTrustsOneCertificate sslStuff =
                new SSLStuffThatTrustsOneCertificate( certificateStream );

        SSLConnectionSocketFactory connectionSocketFactory =
                new SSLConnectionSocketFactory( sslStuff.getSSLContext(),
                                                new String[] { "TLSv1.2" },
                                                null,
                                                SSLConnectionSocketFactory.getDefaultHostnameVerifier() );
        HttpResponse response;

        try ( CloseableHttpClient httpClient =
                      HttpClients.custom()
                                 .setSSLSocketFactory( connectionSocketFactory )
                                 .build() )
        {
            HttpGet httpGet = new HttpGet( "https://www.google.com" );

            // Because the certificate is for apache's server, not google's,
            // we should expect a handshake failure.
            exception.expect( SSLHandshakeException.class );
            response = httpClient.execute( httpGet );
        }

        assertNull( "Response should not have been set because an exception should have occurred.", response );
    }
}