package wres.tasker;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Date;
import java.util.StringJoiner;
import jakarta.ws.rs.core.Response;

import org.apache.qpid.server.configuration.CommonProperties;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.X500NameBuilder;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.asn1.x509.AuthorityKeyIdentifier;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.asn1.x509.SubjectKeyIdentifier;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class WresJobTest
{
    private static Path tempDir;
    private static final String P12_FILE_NAME = "wres-tasker_client_private_key_and_x509_cert.p12";
    private static final String SERVER_CERT_FILE_NAME = "wres-broker-localhost_server_x509_cert.pem";
    private static final String SERVER_KEY_FILE_NAME = "wres-broker-localhost_server_private_rsa_key.pem";
    private static final String TRUST_STORE_FILE_NAME = "trustedCertificates-localhost.jks";

    private static final String[] APPROVED_CIPHERS = {
            "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
            "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA",
            "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA",
            "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256",
            "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384",
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
            "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA",
            "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA",
            "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
            "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
            "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256",
            "TLS_DHE_RSA_WITH_AES_256_GCM_SHA384",
            "TLS_DHE_RSA_WITH_AES_128_CBC_SHA",
            "TLS_DHE_RSA_WITH_AES_256_CBC_SHA",
            "TLS_DHE_RSA_WITH_AES_128_CBC_SHA256",
            "TLS_DHE_RSA_WITH_AES_256_CBC_SHA256"
    };

    private static final String FAKE_DECLARATION = "<?xml version='1.0' encoding='UTF-8'?><project><inputs><left></left><right></right></inputs><pair></pair><metrics></metrics><outputs></outputs></project>";

    @BeforeClass
    public static void setup() throws IOException, NoSuchAlgorithmException,
            OperatorCreationException, CertificateException, KeyStoreException
    {
        WresJobTest.tempDir = Files.createTempDirectory( null );

        Provider bouncyCastleProvider = new BouncyCastleProvider();
        Security.addProvider( bouncyCastleProvider );

        // Create a key
        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance( "RSA", bouncyCastleProvider );
        // Would initialize with our own fixed random data, but the APIs make
        // it hard to do that, and with good cause.
        SecureRandom secureRandom = new SecureRandom();
        keyPairGenerator.initialize( 2048, secureRandom );
        KeyPair keyPair = keyPairGenerator.generateKeyPair();

        // Create a certificate
        X500Name subject = new X500NameBuilder( BCStyle.INSTANCE )
                .addRDN( BCStyle.CN, "localhost" )
                .addRDN( BCStyle.O, "WRES" )
                .build();

        Instant now = Instant.now();
        Instant aLittleWhileAgo = now.minus( 10_000, ChronoUnit.SECONDS );
        Instant aLittleWhileFromNow = now.plus( 10_000, ChronoUnit.SECONDS );

        Date earliest = Date.from( aLittleWhileAgo );
        Date latest = Date.from( aLittleWhileFromNow );

        BigInteger serial = BigInteger.valueOf( aLittleWhileAgo.toEpochMilli() );

        // Thanks https://stackoverflow.com/questions/29852290/self-signed-x509-certificate-with-bouncy-castle-in-java#29853068
        X509v3CertificateBuilder certificateBuilder =
                new JcaX509v3CertificateBuilder( subject,
                                                 serial,
                                                 earliest,
                                                 latest,
                                                 subject,
                                                 keyPair.getPublic() );
        JcaX509ExtensionUtils extensionUtils = new JcaX509ExtensionUtils();
        SubjectKeyIdentifier keyIdentifier = extensionUtils.createSubjectKeyIdentifier( keyPair.getPublic() );
        AuthorityKeyIdentifier authorityKeyIdentifier = extensionUtils.createAuthorityKeyIdentifier( keyPair.getPublic() );
        certificateBuilder.addExtension( Extension.subjectKeyIdentifier,
                                         false,
                                         keyIdentifier );
        certificateBuilder.addExtension( Extension.authorityKeyIdentifier,
                                         false,
                                         authorityKeyIdentifier );
        BasicConstraints basicConstraints = new BasicConstraints( true );
        certificateBuilder.addExtension( Extension.basicConstraints,
                                         true,
                                         basicConstraints );
        KeyUsage keyUsage = new KeyUsage( KeyUsage.keyCertSign | KeyUsage.digitalSignature );
        certificateBuilder.addExtension( Extension.keyUsage, false, keyUsage );
        ExtendedKeyUsage extendedKeyUsage = new ExtendedKeyUsage( new KeyPurposeId[] { KeyPurposeId.id_kp_serverAuth } );
        certificateBuilder.addExtension( Extension.extendedKeyUsage,
                                         false,
                                         extendedKeyUsage );

        ContentSigner contentSigner = new JcaContentSignerBuilder( "SHA256withRSA" )
                .build( keyPair.getPrivate() );
        X509CertificateHolder certificateHolder = certificateBuilder.build( contentSigner );

        JcaX509CertificateConverter certificateConverter = new JcaX509CertificateConverter();
        certificateConverter.setProvider( bouncyCastleProvider );
        X509Certificate wowACertificate = certificateConverter.getCertificate( certificateHolder );

        String passphrase = "wres-tasker-passphrase";

        // Create a .p12 key + x509 certificate for tasker to use during testing
        KeyStore p12KeyStore = KeyStore.getInstance( "PKCS12" );
        p12KeyStore.load( null, passphrase.toCharArray() );

        KeyStore.PrivateKeyEntry privateKeyEntry = new KeyStore.PrivateKeyEntry( keyPair.getPrivate(),
                                                                                 new Certificate[] { wowACertificate } );
        p12KeyStore.setEntry( "privateKeyAlias", privateKeyEntry, new KeyStore.PasswordProtection( passphrase.toCharArray() ) );

        Path p12Path = Paths.get( WresJobTest.tempDir.toString(), P12_FILE_NAME );
        File p12File = p12Path.toFile();

        try ( FileOutputStream outputStream = new FileOutputStream( p12File ) )
        {
            p12KeyStore.store( outputStream, passphrase.toCharArray() );
        }

        // Use exact same keypair and certificate for the testing broker server
        // in order to save test execution time and simplify.
        // Qpid broker server is configured for the typical .pem format rather
        // than .p12 or .jks format (just to appear more like how rabbit is).
        Path certPemPath = Paths.get( WresJobTest.tempDir.toString(),
                                      SERVER_CERT_FILE_NAME );
        File certPemFile = certPemPath.toFile();

        try ( FileWriter fileWriter = new FileWriter( certPemFile );
              JcaPEMWriter pemWriter = new JcaPEMWriter( fileWriter ) )
        {
            pemWriter.writeObject( wowACertificate );
        }

        Path serverKeyPemPath = Paths.get( WresJobTest.tempDir.toString(),
                                           SERVER_KEY_FILE_NAME );
        File serverKeyPemFile = serverKeyPemPath.toFile();

        try ( FileWriter fileWriter = new FileWriter( serverKeyPemFile );
              JcaPEMWriter pemWriter = new JcaPEMWriter( fileWriter ) )
        {
            pemWriter.writeObject( keyPair.getPrivate() );
        }

        // Use exact same certificate to create a trustfile for clients to trust
        KeyStore trustStore = KeyStore.getInstance( "JKS" );
        char[] trustStorePassphrase = "changeit".toCharArray();
        trustStore.load( null, trustStorePassphrase );
        trustStore.setCertificateEntry( "localhost_ca", wowACertificate );
        Path trustStorePath = Paths.get( WresJobTest.tempDir.toString(),
                                        TRUST_STORE_FILE_NAME );
        File trustStoreFile = trustStorePath.toFile();

        try ( FileOutputStream trustOutputStream = new FileOutputStream( trustStoreFile ) )
        {
            trustStore.store( trustOutputStream, trustStorePassphrase );
        }

        // Must at least set up the trustStore property before class because
        // we statically load the trust files (maybe need to change that?)
        System.setProperty( "java.io.tmpdir", WresJobTest.tempDir.toString() );
        Path trustPath = Paths.get( WresJobTest.tempDir.toString(), TRUST_STORE_FILE_NAME );
        System.setProperty( "wres.trustStore", trustPath.toString() );

        // Set approved cipher suites for Qpid broker in expected json format
        StringJoiner approvedCiphersJoiner = new StringJoiner( "\", \"",
                                                               "[ \"",
                                                               "\" ]" );
        for ( String cipherSuite : APPROVED_CIPHERS )
        {
            approvedCiphersJoiner.add( cipherSuite );
        }

        System.setProperty( CommonProperties.QPID_SECURITY_TLS_CIPHER_SUITE_ALLOW_LIST,
                            approvedCiphersJoiner.toString() );

        // This class does not test redis integration.
    }

    @Test
    public void testBadRequest()
    {
        System.setProperty( "wres.secrets_dir", WresJobTest.tempDir.toString() );
        WresJob wresJob = new WresJob();
        Response response = wresJob.postWresJob( "fake", "hank", null, "boogaflickle", false, false,
                                                 Collections.emptyList() );
        assertEquals( "Expected a 400 bad request.", 400, response.getStatus() );
    }

    @Test
    public void testBadRequestDueToShortDeclaration()
    {
        System.setProperty( "wres.secrets_dir", WresJobTest.tempDir.toString() );
        WresJob wresJob = new WresJob();
        Response response = wresJob.postWresJob( "too short a declaration", null, null, null, false, false, Collections.emptyList() );
        assertEquals( "Expected a 400 bad request.", 400, response.getStatus() );
    }

    @Test
    public void testInternalServerErrorDueToNoBrokerConnectivity()
    {
        System.setProperty( "wres.secrets_dir", WresJobTest.tempDir.toString() );
        WresJob wresJob = new WresJob();
        Response response = wresJob.postWresJob( FAKE_DECLARATION, null, null, null, false, false, Collections.emptyList() );
        assertEquals( "Expected a 500 Internal Server Error.", 500, response.getStatus() );
    }


/*    @Test
    public void testMessage() throws Exception
    {
        String[] additionalArguments = new String[] { "host", "8000", "name" };
        List<String> argsList = Arrays.asList ( additionalArguments );

        EmbeddedBroker embeddedBroker = new EmbeddedBroker();
        embeddedBroker.start();
        WresJob wresJob = new WresJob();
        Response response = wresJob.postWresJob( FAKE_DECLARATION, null, null, null, false, Collections.emptyList() );
        assertEquals( "Expected a 201 Created. Response: " + response.toString(), 201, response.getStatus() );
        response = wresJob.postWresJob( "fake", null, "admintoken", "cleandatabase", false, Collections.emptyList() );
        assertEquals( "Expected a 201 Created. Response: " + response.toString(), 201, response.getStatus() );
        response = wresJob.postWresJob( "fake", null, "admintoken", "switchdatabase", false, Collections.emptyList() );
        assertEquals( "Expected a 400 Created. Response: " + response.toString(), 400, response.getStatus() );
        response = wresJob.postWresJob( "fake", null, "admintoken", "switchdatabase", false, argsList );
        assertEquals( "Expected a 200 Created. Response: " + response.toString(), 200, response.getStatus() );
        WresJob.shutdownNow();
        embeddedBroker.shutdown();
    }
*/
    @AfterClass
    public static void cleanUp() throws IOException
    {
        Files.deleteIfExists( Paths.get( WresJobTest.tempDir.toString(),
                                         P12_FILE_NAME ) );
        Files.deleteIfExists( Paths.get( WresJobTest.tempDir.toString(),
                                         SERVER_CERT_FILE_NAME ) );
        Files.deleteIfExists( Paths.get( WresJobTest.tempDir.toString(),
                                         SERVER_KEY_FILE_NAME ) );
        Files.deleteIfExists( Paths.get( WresJobTest.tempDir.toString(),
                                         TRUST_STORE_FILE_NAME ) );
        Files.deleteIfExists( WresJobTest.tempDir );
    }
}
