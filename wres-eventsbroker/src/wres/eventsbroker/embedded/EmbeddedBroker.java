package wres.eventsbroker.embedded;

import java.io.Closeable;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.qpid.server.SystemLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An embedded broker for publishing and subscribing to evaluation messages.
 *
 * Based on https://cwiki.apache.org/confluence/display/qpid/How+to+embed+Qpid+Broker-J
 * which came from a link found at
 * https://mail-archives.apache.org/mod_mbox/qpid-users/201806.mbox/browser
 */

public class EmbeddedBroker implements Closeable
{
    private static final Logger LOGGER = LoggerFactory.getLogger( EmbeddedBroker.class );

    /**
     * Default broker configuration on the classpath.
     */

    private static final String INITIAL_CONFIGURATION = "embedded-broker-config.json";

    /**
     * Broker launcher.
     */

    private final SystemLauncher systemLauncher;

    /**
     * Launcher options.
     */

    private final Map<String, Object> launchOptions;

    /**
     * Returns a broker instance with default launch options.
     * 
     * @return a broker instance with default options.
     */

    public static EmbeddedBroker of()
    {
        URL initialConfig = EmbeddedBroker.class.getClassLoader()
                                                .getResource( INITIAL_CONFIGURATION );

        if ( Objects.isNull( initialConfig ) )
        {
            throw new CouldNotStartEmbeddedBrokerException( "Expected a resource named '"
                                                            + INITIAL_CONFIGURATION
                                                            + "' on the classpath." );
        }
        Map<String, Object> options = new HashMap<>();
        options.put( "type", "Memory" );
        options.put( "initialConfigurationLocation",
                     initialConfig.toExternalForm() );
        options.put( "startupLoggedToSystemOut", true );

        LOGGER.debug( "Created new embedded broker with options {}.", options );

        return new EmbeddedBroker( options );
    }

    /**
     * Starts the broker.
     * 
     * @throws CouldNotStartEmbeddedBrokerException if the broker could not be started for any reason.
     */

    public void start()
    {
        LOGGER.debug( "Starting embedded broker with launch options {}.", this.launchOptions );

        try
        {
            this.systemLauncher.startup( this.launchOptions );
        }
        catch ( Exception e )
        {
            throw new CouldNotStartEmbeddedBrokerException( "Failed to start an embedded broker using the initial "
                                                            + "configuration in '"
                                                            + INITIAL_CONFIGURATION
                                                            + "'.",
                                                            e );
        }

        LOGGER.debug( "Finished starting embedded broker with launch options {}.", this.launchOptions );
    }

    @Override
    public void close() throws IOException
    {
        LOGGER.debug( "Closing embedded broker." );

        this.shutdown();

        LOGGER.debug( "Embedded broker closed." );
    }

    /**
     * Kills the broker.
     */

    private void shutdown()
    {
        this.systemLauncher.shutdown();
    }

    /**
     * Hidden constructor.
     * 
     * @param options the launch options
     * @throws NullPointerException if the launch options are null.
     * @throws CouldNotStartEmbeddedBrokerException if the broker configuration could not be found.
     */

    private EmbeddedBroker( Map<String, Object> options )
    {
        Objects.requireNonNull( options );

        this.launchOptions = Collections.unmodifiableMap( options );
        this.systemLauncher = new SystemLauncher();
    }
}
