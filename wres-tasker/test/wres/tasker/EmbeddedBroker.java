package wres.tasker;

import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.server.SystemLauncher;

/**
 * A broker for testing purposes running at test-time for integration tests of
 * the messaging package.
 *
 * Based on https://cwiki.apache.org/confluence/display/qpid/How+to+embed+Qpid+Broker-J
 * which came from a link found at
 * https://mail-archives.apache.org/mod_mbox/qpid-users/201806.mbox/browser
 */

class EmbeddedBroker
{
    private static final String INITIAL_CONFIGURATION = "initial-config-test.json";
    private final SystemLauncher systemLauncher;
    private final Map<String,Object> launchOptions;

    EmbeddedBroker()
    {
        URL initialConfig = EmbeddedBroker.class.getClassLoader().getResource( INITIAL_CONFIGURATION );

        if ( initialConfig == null )
        {
            throw new IllegalStateException( "Expected a resource named '"
                                             + INITIAL_CONFIGURATION
                                             + "' on the classpath." );
        }

        Map<String,Object> options = new HashMap<>();
        options.put( "type", "Memory" );
        options.put( "initialConfigurationLocation",
                     initialConfig.toExternalForm() );
        options.put( "startupLoggedToSystemOut", true );
        this.launchOptions = Collections.unmodifiableMap( options );
        this.systemLauncher = new SystemLauncher();
    }

    void start() throws Exception
    {
        this.systemLauncher.startup( this.launchOptions );
    }

    void shutdown()
    {
        this.systemLauncher.shutdown();
    }
}
