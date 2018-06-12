package wres.messages;

import javax.net.ssl.SSLContext;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;

public class BrokerHelperTest
{
    @Rule
    public final ExpectedException exception = ExpectedException.none();


    @Test
    public void getBrokerHostWhenUnset()
    {
        System.clearProperty( BrokerHelper.BROKER_HOST_PROPERTY_NAME );
        String host = BrokerHelper.getBrokerHost();
        assertEquals( "Default host should match default when prop not set",
                      BrokerHelper.DEFAULT_BROKER_HOST, host );
    }


    @Test
    public void getBrokerHostWhenSystemPropertySet()
    {
        String expectedValue = "some.arbitrary.host.com";
        System.setProperty( BrokerHelper.BROKER_HOST_PROPERTY_NAME, expectedValue );
        String host = BrokerHelper.getBrokerHost();
        assertEquals( "Default host should match default when prop set",
                      expectedValue, host );
    }


    @Test
    public void getBrokerVhostWhenUnset()
    {
        System.clearProperty( BrokerHelper.BROKER_VHOST_PROPERTY_NAME );
        String vhost = BrokerHelper.getBrokerVhost();
        assertEquals( "Default vhost should match default when prop not set",
                      BrokerHelper.DEFAULT_BROKER_VHOST, vhost );
    }


    @Test
    public void getBrokerVostWhenSystemPropertySet()
    {
        String expectedValue = "some_arbitrary_vhost";
        System.setProperty( BrokerHelper.BROKER_VHOST_PROPERTY_NAME, expectedValue );
        String vhost = BrokerHelper.getBrokerVhost();
        assertEquals( "Default vhost should match default when prop set",
                      expectedValue, vhost );
    }


    @Test
    public void getSecretsDirWhenUnset()
    {
        System.clearProperty( BrokerHelper.SECRETS_DIR_PROPERTY_NAME );
        String secretsDir = BrokerHelper.getSecretsDir();
        assertEquals( "Default secrets dir should match default when prop set",
                      BrokerHelper.DEFAULT_SECRETS_DIR, secretsDir );
    }


    @Test
    public void getSecretsDirWhenSystemPropertySet()
    {
        String expectedValue = "/some/arbitrary/secrets";
        System.setProperty( BrokerHelper.SECRETS_DIR_PROPERTY_NAME, expectedValue );
        String secretsDir = BrokerHelper.getSecretsDir();
        assertEquals( "Default secrets dir should match default when prop set",
                      expectedValue, secretsDir );
    }


    @Test
    public void getSSLContextThrowsIllegalStateExceptionWhenNothingSpecified()
    {
        exception.expect( IllegalStateException.class );
        SSLContext sslContext = BrokerHelper.getSSLContextWithClientCertificate(
                BrokerHelper.Role.TASKER );
    }
}
