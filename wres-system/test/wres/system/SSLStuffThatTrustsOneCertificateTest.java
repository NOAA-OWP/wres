package wres.system;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import javax.net.ssl.SSLHandshakeException;

import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.HttpsSupport;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.client5.http.ssl.TlsSocketStrategy;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.ssl.TLS;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SSLStuffThatTrustsOneCertificateTest
{
    @RegisterExtension
    private static final WireMockExtension WIREMOCK =
            WireMockExtension.newInstance()
                             .options( WireMockConfiguration.wireMockConfig()
                                                            .dynamicPort()
                                                            .dynamicHttpsPort()
                                                            .keystorePath( "testNonSrc/test.jks" )
                                                            .keystorePassword( "changeme" )
                                                            .keyManagerPassword( "changeme" ) )
                             .build();

    /**
     * The x509 public server certificate presented to the client at the keystore path in the wiremock server instance
     * above, but in base64 PEM format. This certificate is valid for 5024 days from 24 March 2026. It would be better
     * to generate this certificate at runtime to avoid a ticking bomb. When generating an equivalent certificate,
     * ensure that the Common Name (CN) is set to localhost, i.e., -subj 'CN=localhost' using openSSL.
     */

    private static final String WIREMOCK_CERTIFICATE =
            """
                    -----BEGIN CERTIFICATE-----
                    MIIDCTCCAfGgAwIBAgIUE3u7asbSgYZUyD07MefhMKqpYw4wDQYJKoZIhvcNAQEL
                    BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI2MDMyNDEyMDYwMVoXDTM5MTIy
                    NTEyMDYwMVowFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEF
                    AAOCAQ8AMIIBCgKCAQEAp//fMVf67MjjPwc8Jk9A+i5JvgjOOPiH6BVWUgYLhyCS
                    Q1goTlHrt1GjoD2R1kSYKDd45lPXsGFAlH/BRF9teram63UwJbbi52zukhx93drZ
                    zdFIOVpasD6TV0arO/qrL9PKs99YcJlO/z2SoMd0j+nUnMYfWNk3RTtAtMUFPSlg
                    mIYFmTtNFfpNfGFEVuoHvbs37cEQzEpsfXUPma33CmoVGKWerMKp9aFk6fKsiGKp
                    L1Jv+k5q1H/0YoGzdTW+/0PCUB9K5Q8wq4r7JzrRgMwQCdm0DUwZwwJAKHvnasKe
                    n+/+PPvs/30Iu2DAGl5ZpV0yvh033FNpBZTAIfHTOwIDAQABo1MwUTAdBgNVHQ4E
                    FgQULnHJCT6DQADwET50vZbv6EFql6EwHwYDVR0jBBgwFoAULnHJCT6DQADwET50
                    vZbv6EFql6EwDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAoJET
                    ayoG7LHWgOPZDwdf6RHlcDLa+vv4T8QYIUkaqnoNCpAdGpVdgXVWralFduuvnR8L
                    YH/gqi3sbg2Se0esYxlGOPwP3UkWaX31w2Yw35Vi031VCLClhLlgxAhqHUPKGXzC
                    0EByOjMmmJM3mkAKgzp4jv0c5FTn1hE5HOm7koIsshGQRzsb+jhF+ioBuOV2zThW
                    KolGFOZy6nUY99eEUiX+fv6EHfj5V8u3CTw7H1/2pQBUnOSMy1j8i835mfseFKiS
                    4UDowp5TxafvdSZghgeTvH4ynWC+tT/ih4KQjYFJ7GvhO4KLrbAjeo3IXuaRB6zQ
                    cWVO69C17BaekFoDxg==
                    -----END CERTIFICATE-----
                    """;

    /**
     * <p>A mockserver x509 certificate, which is not trusted by the wiremock instance. Obtained from:
     * <a href="https://github.com/mock-server/mockserver/blob/master/mockserver-core/src/main/resources/org/mockserver/socket/CertificateAuthorityCertificate.pem">Mock server certificate.</a>
     * <p>It would be significantly better to auto-generate a x509 certificate in code to avoid the sad path test
     * failing with the same handshake exception, but for an unintended reason, such as certificate expiry.
     */
    private static final String UNTRUSTED_MOCKSERVER_CERT =
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

    @Test
    void connectToServerWithCorrectCertificateTrustedSucceeds() throws IOException
    {
        byte[] certificateBytes = WIREMOCK_CERTIFICATE.getBytes( StandardCharsets.US_ASCII );
        InputStream certificateStream = new ByteArrayInputStream( certificateBytes );

        SSLStuffThatTrustsOneCertificate sslStuff =
                new SSLStuffThatTrustsOneCertificate( certificateStream, "changeme" );

        TlsSocketStrategy tlsStrategy = ClientTlsStrategyBuilder.create()
                                                                .setSslContext( sslStuff.getSSLContext() )
                                                                .setTlsVersions( TLS.V_1_2 )
                                                                .setHostnameVerifier( NoopHostnameVerifier.INSTANCE )
                                                                .buildClassic();

        HttpClientConnectionManager cm = PoolingHttpClientConnectionManagerBuilder.create()
                                                                                  .setTlsSocketStrategy( tlsStrategy )
                                                                                  .build();

        Integer statusCode;
        try ( CloseableHttpClient httpClient = HttpClients.custom()
                                                          .setConnectionManager( cm )
                                                          .build() )
        {
            ClassicHttpRequest httpGet = new HttpGet( "https://localhost:" + WIREMOCK.getHttpsPort() );
            statusCode = httpClient.execute( httpGet, HttpResponse::getCode );
        }

        assertNotNull( statusCode );
    }

    @Test
    void connectToServerWithIncorrectCertificateTrustedThrowsHandshakeException() throws IOException
    {
        // Use an untrusted certificate (Apache intermediate)
        byte[] certificateBytes = UNTRUSTED_MOCKSERVER_CERT.getBytes( StandardCharsets.US_ASCII );
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

        try ( CloseableHttpClient httpClient = HttpClients.custom()
                                                          .setConnectionManager( cm )
                                                          .build() )
        {

            HttpGet httpGet = new HttpGet( "https://localhost:" + WIREMOCK.getHttpsPort() );

            // Should fail because the trusted cert doesn't match WireMock's CA
            assertThrows( SSLHandshakeException.class, () -> httpClient.execute( httpGet, HttpResponse::getCode ) );
        }
    }
}
