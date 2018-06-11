package wres.tasker;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.FileSystemException;
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
import java.util.Date;
import javax.ws.rs.core.Response;

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
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static junit.framework.TestCase.assertEquals;

public class WresJobTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger( WresJobTest.class );

    private static Path tempDir;
    private static final String P12_FILE_NAME = "wres-tasker_client_private_key_and_x509_cert.p12";

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
                .addRDN( BCStyle.CN, "dummydomain.com" )
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
    }

    @Test
    public void testNotFound()
    {
        System.setProperty( "wres.secrets_dir", WresJobTest.tempDir.toString() );
        WresJob wresJob = new WresJob();
        Response response = wresJob.postWresJob( "fake", "fake" );
        assertEquals( "Expected a 404 not found.", 404, response.getStatus() );
    }

    @Test
    public void testInternalServerError()
    {
        System.setProperty( "wres.secrets_dir", WresJobTest.tempDir.toString() );
        WresJob wresJob = new WresJob();
        // This line could be brittle due to looking up a particular user:
        Response response = wresJob.postWresJob( "fake", "hank" );
        assertEquals( "Expected a 500 Internal Server Error.", 500, response.getStatus() );
    }

    /**
     * Could be as simple as two lines of Files.deleteIfExists but for antivirus
     * (so instead do several retries)
     * @throws IOException when deletion fails
     * @throws InterruptedException when interrupted while waiting for antivirus
     */

    @AfterClass
    public static void cleanUp() throws IOException, InterruptedException
    {
        long startDatetimeMillis = System.currentTimeMillis();
        long backoffMillis = 2000;
        long maxBackoffMillis = 33000;

        long now = startDatetimeMillis;

        while ( startDatetimeMillis + maxBackoffMillis > now )
        {
            now = System.currentTimeMillis();

            try
            {
                Files.deleteIfExists( Paths.get( WresJobTest.tempDir.toString(),
                                                 P12_FILE_NAME ) );
                Files.deleteIfExists( WresJobTest.tempDir );
                // Successful deletion, break out, should be majority case.
                break;
            }
            catch ( FileSystemException fse )
            {
                // Failure due to some other process holding file, retry.
                // We are making the assumption that FileSystemException will be
                // the same exception that James got in #51416
                if ( startDatetimeMillis + maxBackoffMillis > now )
                {
                    LOGGER.warn( "Attempt to remove a temp file failed, trying again.", fse );
                }
                else
                {
                    // Timed out, so translate exception.
                    throw new IOException( "Could not remove temp file after several attempts.", fse );
                }
            }

            // Give other process (antivirus?) a chance to release the temp file
            Thread.sleep( backoffMillis );

            // Wait longer next time.
            backoffMillis = backoffMillis * 2;
        }
    }
}
