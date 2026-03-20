package wres.system;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import javax.net.ssl.SSLHandshakeException;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.HttpsSupport;
import org.apache.hc.client5.http.ssl.TlsSocketStrategy;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.ssl.TLS;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockserver.integration.ClientAndServer;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SSLStuffThatTrustsOneCertificateTest
{

    private static ClientAndServer mockServer;

    /**
     * From <a href="https://github.com/jamesdbloom/mockserver/blob/master/mockserver-core/src/main/resources/org/mockserver/socket/CertificateAuthorityCertificate.pem">...</a>
     * which was mentioned in <a href="https://www.mock-server.com/">...</a> as automatically
     * being set up for any SSL connections to the server.
     */
    private static final String MOCK_SERVER_CA_PEM_2019 =
            """
                    -----BEGIN CERTIFICATE-----
                    MIIDqDCCApCgAwIBAgIEPhwe6TANBgkqhkiG9w0BAQsFADBiMRswGQYDVQQDDBJ3
                    d3cubW9ja3NlcnZlci5jb20xEzARBgNVBAoMCk1vY2tTZXJ2ZXIxDzANBgNVBAcM
                    BkxvbmRvbjEQMA4GA1UECAwHRW5nbGFuZDELMAkGA1UEBhMCVUswIBcNMTYwNjIw
                    MTYzNDE0WhgPMjExNzA1MjcxNjM0MTRaMGIxGzAZBgNVBAMMEnd3dy5tb2Nrc2Vy
                    dmVyLmNvbTETMBEGA1UECgwKTW9ja1NlcnZlcjEPMA0GA1UEBwwGTG9uZG9uMRAw
                    DgYDVQQIDAdFbmdsYW5kMQswCQYDVQQGEwJVSzCCASIwDQYJKoZIhvcNAQEBBQAD
                    ggEPADCCAQoCggEBAPGORrdkwTY1H1dvQPYaA+RpD+pSbsvHTtUSU6H7NQS2qu1p
                    sE6TEG2fE+Vb0QIXkeH+jjKzcfzHGCpIU/0qQCu4RVycrIW4CCdXjl+T3L4C0I3R
                    mIMciTig5qcAvY9P5bQAdWDkU36YGrCjGaX3QlndGxD9M974JdpVK4cqFyc6N4gA
                    Onys3uS8MMmSHTjTFAgR/WFeJiciQnal+Zy4ZF2x66CdjN+hP8ch2yH/CBwrSBc0
                    ZeH2flbYGgkh3PwKEqATqhVa+mft4dCrvqBwGhBTnzEGWK/qrl9xB4mTs4GQ/Z5E
                    8rXzlvpKzVJbfDHfqVzgFw4fQFGV0XMLTKyvOX0CAwEAAaNkMGIwHQYDVR0OBBYE
                    FH3W3sL4XRDM/VnRayaSamVLISndMA8GA1UdEwEB/wQFMAMBAf8wCwYDVR0PBAQD
                    AgG2MCMGA1UdJQQcMBoGCCsGAQUFBwMBBggrBgEFBQcDAgYEVR0lADANBgkqhkiG
                    9w0BAQsFAAOCAQEAecfgKuMxCBe/NxVqoc4kzacf9rjgz2houvXdZU2UDBY3hCs4
                    MBbM7U9Oi/3nAoU1zsA8Rg2nBwc76T8kSsfG1TK3iJkfGIOVjcwOoIjy3Z8zLM2V
                    YjYbOUyAQdO/s2uShAmzzjh9SV2NKtcNNdoE9e6udvwDV8s3NGMTUpY5d7BHYQqV
                    sqaPGlsKi8dN+gdLcRbtQo29bY8EYR5QJm7QJFDI1njODEnrUjjMvWw2yjFlje59
                    j/7LBRe2wfNmjXFYm5GqWft10UJ7Ypb3XYoGwcDac+IUvrgmgTHD+E3klV3SUi8i
                    Gm5MBedhPkXrLWmwuoMJd7tzARRHHT6PBH/ZGw==
                    -----END CERTIFICATE-----
                    """;

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
            """
                    -----BEGIN CERTIFICATE-----
                    MIIF8jCCA9qgAwIBAgIQUWEdv45DMOu87QrYvTIjzTANBgkqhkiG9w0BAQwFADCB
                    iDELMAkGA1UEBhMCVVMxEzARBgNVBAgTCk5ldyBKZXJzZXkxFDASBgNVBAcTC0pl
                    cnNleSBDaXR5MR4wHAYDVQQKExVUaGUgVVNFUlRSVVNUIE5ldHdvcmsxLjAsBgNV
                    BAMTJVVTRVJUcnVzdCBSU0EgQ2VydGlmaWNhdGlvbiBBdXRob3JpdHkwHhcNMTQw
                    NzA0MDAwMDAwWhcNMjQwNzAzMjM1OTU5WjBZMQswCQYDVQQGEwJVUzEQMA4GA1UE
                    ChMHU1NMLmNvbTEUMBIGA1UECxMLd3d3LnNzbC5jb20xIjAgBgNVBAMTGVNTTC5j
                    b20gSGlnaCBBc3N1cmFuY2UgQ0EwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEK
                    AoIBAQClFxgJFdeMFUw1Y4GEqcVHDiXJp7ukYtO31KKU+j/llv/DHKhRNO4CtbI0
                    febjCO2gMzYz3cvj9zgwWiX/oaeI5HWBqN3XqULI+UA84sBKzdsD5oELyfqC6Yyy
                    yPeXrS7SCQYblUZqmcjksbJgL2XA9nZ+iqiched+z4MWRbiU+DEklXssTPyo1f4A
                    QT033VxgkF6lNw+Q+Rm4FVQV7FrYQYj6mxSTFXtIvepbsszU6iy7iVXPA4RNb3Iu
                    REz8tBnce7+fwZkq+NPUlaKDUp82WvGHMwAzBULBZkd2iujWzDawFPVeYONb+P70
                    e///GTDhDvMe4E27zXoSW6ujZGh5AgMBAAGjggGEMIIBgDAfBgNVHSMEGDAWgBRT
                    eb9aqitKz1SA4dibwJ3ysgNmyzAdBgNVHQ4EFgQUTFQjZyl09ELqmbtoPBafCC8Q
                    5kEwDgYDVR0PAQH/BAQDAgGGMBIGA1UdEwEB/wQIMAYBAf8CAQAwHQYDVR0lBBYw
                    FAYIKwYBBQUHAwEGCCsGAQUFBwMCMCEGA1UdIAQaMBgwDAYKKwYBBAGCqTABATAI
                    BgZngQwBAgIwVQYDVR0fBE4wTDBKoEigRoZEaHR0cDovL2NybC50cnVzdC1wcm92
                    aWRlci5jb20vVVNFUlRydXN0UlNBQ2VydGlmaWNhdGlvbkF1dGhvcml0eS5jcmww
                    gYAGCCsGAQUFBwEBBHQwcjBEBggrBgEFBQcwAoY4aHR0cDovL2NydC50cnVzdC1w
                    cm92aWRlci5jb20vVVNFUlRydXN0UlNBQWRkVHJ1c3RDQS5jcnQwKgYIKwYBBQUH
                    MAGGHmh0dHA6Ly9vY3NwLnRydXN0LXByb3ZpZGVyLmNvbTANBgkqhkiG9w0BAQwF
                    AAOCAgEAKBjIAycQLrBawRYWFoRJIo4tP7ZkyEVRgd491E3beayYpTs+wLzbBOve
                    +v6wGx4ohIOACncEDgVy4bkBgYulsix6xWeVG1ZmF9WXhhFdP+3ZipAttMSh5s7m
                    U/5hAg5NvZ5h5VN/3EOJD8+0FOfEaUcgQFGorYXeL+/2+E/ixNWtTFl4ipZ7AU2k
                    +lSPxnsc4+3H1+EEea9xTmanMx+gY/tXEnyQ7iVtry4uUjHF+8ts0vvAlesra8PZ
                    5CNCSv9sqkG2Ipig2Srrw8zvrMZtJO+veymDDsc5edpjPbSY0A8XhYbhepjdid2V
                    kisp/RtyAq+CiWea3tH9y5BK0mDoiMhlJ59E52TsuHgZ7TpzoCnEsCWVPctj4e5N
                    uSs75oT1ap3wMHCvObincNovMaqtX8zy/oRr+NSoDQY2ZQ06viCHAKpudpUg4lLF
                    RKqW+UTCWDML6PriZsjXf6aSce4lK+UdmmL4VDh1Nf9y7FpSsNIaZfFrATrTfS1o
                    P8y0ZjapxUVVpiH11vIr8mqk6QT0WelbI4r4s45ZFjOeT69xSpsEIIcxYI1GpIbw
                    CMDvdMAmwzr2pur5PIKKVH1WkLtaf1aSruBTP/aeavS2P8NMy7tI1LKoNY8XvMFO
                    +E3jECsl+ke/Q3dfswLjNl+as1U10Ltgx7/IJanbDEQzVr6Sqgs=
                    -----END CERTIFICATE-----
                    """;

    @BeforeAll
    static void beforeAllTests()
    {
        SSLStuffThatTrustsOneCertificateTest.mockServer = ClientAndServer.startClientAndServer( 0 );
    }

    @Test
    void connectToServerWithCorrectCertificateTrustedSucceeds() throws IOException
    {
        byte[] certificateBytes = MOCK_SERVER_CA_PEM_2019.getBytes( StandardCharsets.US_ASCII );
        InputStream certificateStream = new ByteArrayInputStream( certificateBytes );

        SSLStuffThatTrustsOneCertificate sslStuff =
                new SSLStuffThatTrustsOneCertificate( certificateStream, null );

        TlsSocketStrategy tlsStrategy = ClientTlsStrategyBuilder.create()
                                                                .setSslContext( sslStuff.getSSLContext() )
                                                                .setTlsVersions( TLS.V_1_2 )
                                                                .setHostnameVerifier( HttpsSupport.getDefaultHostnameVerifier() )
                                                                .buildClassic();

        HttpClientConnectionManager cm = PoolingHttpClientConnectionManagerBuilder.create()
                                                                                  .setTlsSocketStrategy( tlsStrategy )
                                                                                  .build();

        Integer statusCode;

        try ( CloseableHttpClient httpClient = HttpClients.custom()
                                                          .setConnectionManager( cm )
                                                          .build() )
        {
            ClassicHttpRequest httpGet = new HttpGet( "https://localhost:"
                                                      + SSLStuffThatTrustsOneCertificateTest.mockServer.getLocalPort() );

            statusCode = httpClient.execute( httpGet, HttpResponse::getCode );
        }

        assertNotNull( statusCode );
    }

    @Test
    void connectToServerWithIncorrectCertificateTrustedThrowsHandshakeException() throws IOException
    {
        byte[] certificateBytes =
                APACHE_INTERMEDIATE_PEM_2018.getBytes( StandardCharsets.US_ASCII );
        InputStream certificateStream =
                new ByteArrayInputStream( certificateBytes );

        SSLStuffThatTrustsOneCertificate sslStuff =
                new SSLStuffThatTrustsOneCertificate( certificateStream, null );

        TlsSocketStrategy tlsStrategy =
                ClientTlsStrategyBuilder.create()
                                        .setSslContext( sslStuff.getSSLContext() )
                                        .setTlsVersions( TLS.V_1_2 )
                                        .setHostnameVerifier( HttpsSupport.getDefaultHostnameVerifier() )
                                        .buildClassic();

        HttpClientConnectionManager cm = PoolingHttpClientConnectionManagerBuilder.create()
                                                                                  .setTlsSocketStrategy( tlsStrategy )
                                                                                  .build();

        try ( CloseableHttpClient httpClient = HttpClients.custom()
                                                          .setConnectionManager( cm )
                                                          .build() )
        {

            HttpGet httpGet = new HttpGet( "https://localhost:"
                                           + SSLStuffThatTrustsOneCertificateTest.mockServer.getLocalPort() );

            assertThrows( SSLHandshakeException.class, () -> httpClient.execute( httpGet, HttpResponse::getCode ) );
        }
    }

    @AfterAll
    static void afterAllTests()
    {
        SSLStuffThatTrustsOneCertificateTest.mockServer.stop();
    }
}
