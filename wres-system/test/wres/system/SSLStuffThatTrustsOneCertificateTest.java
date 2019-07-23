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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockserver.integration.ClientAndServer;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertThrows;

public class SSLStuffThatTrustsOneCertificateTest
{

    private static ClientAndServer mockServer;

    /**
     * From https://github.com/jamesdbloom/mockserver/blob/master/mockserver-core/src/main/resources/org/mockserver/socket/CertificateAuthorityCertificate.pem
     * which was mentioned in https://www.mock-server.com/ as automatically
     * being set up for any SSL connections to the server.
     */

    private static final String MOCK_SERVER_CA_PEM_2019 =
            "-----BEGIN CERTIFICATE-----\n"
            + "MIIDqDCCApCgAwIBAgIEPhwe6TANBgkqhkiG9w0BAQsFADBiMRswGQYDVQQDDBJ3\n"
            + "d3cubW9ja3NlcnZlci5jb20xEzARBgNVBAoMCk1vY2tTZXJ2ZXIxDzANBgNVBAcM\n"
            + "BkxvbmRvbjEQMA4GA1UECAwHRW5nbGFuZDELMAkGA1UEBhMCVUswIBcNMTYwNjIw\n"
            + "MTYzNDE0WhgPMjExNzA1MjcxNjM0MTRaMGIxGzAZBgNVBAMMEnd3dy5tb2Nrc2Vy\n"
            + "dmVyLmNvbTETMBEGA1UECgwKTW9ja1NlcnZlcjEPMA0GA1UEBwwGTG9uZG9uMRAw\n"
            + "DgYDVQQIDAdFbmdsYW5kMQswCQYDVQQGEwJVSzCCASIwDQYJKoZIhvcNAQEBBQAD\n"
            + "ggEPADCCAQoCggEBAPGORrdkwTY1H1dvQPYaA+RpD+pSbsvHTtUSU6H7NQS2qu1p\n"
            + "sE6TEG2fE+Vb0QIXkeH+jjKzcfzHGCpIU/0qQCu4RVycrIW4CCdXjl+T3L4C0I3R\n"
            + "mIMciTig5qcAvY9P5bQAdWDkU36YGrCjGaX3QlndGxD9M974JdpVK4cqFyc6N4gA\n"
            + "Onys3uS8MMmSHTjTFAgR/WFeJiciQnal+Zy4ZF2x66CdjN+hP8ch2yH/CBwrSBc0\n"
            + "ZeH2flbYGgkh3PwKEqATqhVa+mft4dCrvqBwGhBTnzEGWK/qrl9xB4mTs4GQ/Z5E\n"
            + "8rXzlvpKzVJbfDHfqVzgFw4fQFGV0XMLTKyvOX0CAwEAAaNkMGIwHQYDVR0OBBYE\n"
            + "FH3W3sL4XRDM/VnRayaSamVLISndMA8GA1UdEwEB/wQFMAMBAf8wCwYDVR0PBAQD\n"
            + "AgG2MCMGA1UdJQQcMBoGCCsGAQUFBwMBBggrBgEFBQcDAgYEVR0lADANBgkqhkiG\n"
            + "9w0BAQsFAAOCAQEAecfgKuMxCBe/NxVqoc4kzacf9rjgz2houvXdZU2UDBY3hCs4\n"
            + "MBbM7U9Oi/3nAoU1zsA8Rg2nBwc76T8kSsfG1TK3iJkfGIOVjcwOoIjy3Z8zLM2V\n"
            + "YjYbOUyAQdO/s2uShAmzzjh9SV2NKtcNNdoE9e6udvwDV8s3NGMTUpY5d7BHYQqV\n"
            + "sqaPGlsKi8dN+gdLcRbtQo29bY8EYR5QJm7QJFDI1njODEnrUjjMvWw2yjFlje59\n"
            + "j/7LBRe2wfNmjXFYm5GqWft10UJ7Ypb3XYoGwcDac+IUvrgmgTHD+E3klV3SUi8i\n"
            + "Gm5MBedhPkXrLWmwuoMJd7tzARRHHT6PBH/ZGw==\n"
            + "-----END CERTIFICATE-----";

    /**
     * The intermediate certificate found that signed hc.apache.org certificate
     * as of 2018-11-15, will expire 2024-07-03. In 2024 it would be wise to
     * auto-generate a certificate here instead of using this old apache cert
     * otherwise the test will not be testing what is intended, instead it will
     * test that an expired certificate throws an SSLHandShakeException which
     * should always be true. What we are looking for is that exactly one cert
     * (intermediate or CA) is trusted, the exact one specified.
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

    @BeforeClass
    public static void beforeAllTests()
    {
        SSLStuffThatTrustsOneCertificateTest.mockServer = ClientAndServer.startClientAndServer( 0 );
    }

    @Test
    public void connectToServerWithCorrectCertificateTrustedSucceeds() throws IOException
    {
        byte[] certificateBytes =
                MOCK_SERVER_CA_PEM_2019.getBytes( StandardCharsets.US_ASCII );
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
            HttpGet httpGet = new HttpGet( "https://localhost:"
                                           + SSLStuffThatTrustsOneCertificateTest.mockServer.getLocalPort() );
            response = httpClient.execute( httpGet );
        }

        assertNotNull( response );
    }


    @Test
    public void connectToServerWithIncorrectCertificateTrustedThrowsHandshakeException() throws IOException
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

        try ( CloseableHttpClient httpClient =
                      HttpClients.custom()
                                 .setSSLSocketFactory( connectionSocketFactory )
                                 .build() )
        {
            HttpGet httpGet = new HttpGet( "https://localhost:"
                                           + SSLStuffThatTrustsOneCertificateTest.mockServer.getLocalPort() );

            // Because the certificate is for mockserver, not an old apache one,
            // we should expect a handshake failure.
            assertThrows( SSLHandshakeException.class, () -> { httpClient.execute( httpGet ); } );
        }
    }


    @AfterClass
    public static void afterAllTests()
    {
        SSLStuffThatTrustsOneCertificateTest.mockServer.stop();
    }
}
