package wres.eventsbroker.embedded;

import java.io.Closeable;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.qpid.server.SystemLauncher;
import org.apache.qpid.server.SystemLauncherListener;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An embedded broker for publishing and subscribing to evaluation messages.
 *
 * Based on https://cwiki.apache.org/confluence/display/qpid/How+to+embed+Qpid+Broker-J
 * which came from a link found at
 * https://mail-archives.apache.org/mod_mbox/qpid-users/201806.mbox/browser
 * Configuration of dead letter queues based on: 
 * <p><a href = "https://qpid.apache.org/releases/qpid-broker-j-8.0.0/book/Java-Broker-Runtime-Handling-Undeliverable-Messages.html">Undeliverable messages</a>
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
     * Port listener.
     */

    private final PortExtractingLauncherListener listener;

    /**
     * Launcher options.
     */

    private final Map<String, Object> launchOptions;

    /**
     * Is <code>true</code> if the broker is started, <code>false</code> otherwise.
     */

    private final AtomicBoolean isStarted;

    /**
     * Returns a broker instance with default launch options.
     * 
     * @return a broker instance with default options.
     */

    public static EmbeddedBroker of()
    {
        return EmbeddedBroker.of( 0 );
    }

    /**
     * Returns a broker instance with default launch options.
     * 
     * @param port an explicit port on which to start the broker
     * @return a broker instance with default options.
     */

    public static EmbeddedBroker of( int port )
    {
        URL initialConfig = EmbeddedBroker.class.getClassLoader()
                                                .getResource( INITIAL_CONFIGURATION );
        if ( Objects.isNull( initialConfig ) )
        {
            throw new CouldNotStartEmbeddedBrokerException( "Expected a resource named '"
                                                            + INITIAL_CONFIGURATION
                                                            + "' on the classpath." );
        }

        String initialConfigLocation = initialConfig.toExternalForm();
        
        LOGGER.debug( "The embedded broker initial configuration will be read from {}.", initialConfigLocation );

        Map<String, Object> options = new HashMap<>();
        options.put( ConfiguredObject.TYPE, "Memory" );
        options.put( SystemConfig.INITIAL_CONFIGURATION_LOCATION,
                     initialConfigLocation );
        options.put( SystemConfig.STARTUP_LOGGED_TO_SYSTEM_OUT, true );

        if ( port == 0 )
        {
            LOGGER.debug( "On creating an embedded broker, setting the value of the qpid.amqp_port to TCP "
                          + "reserved port 0, which instructs the broker to choose a port dynamically (assuming that "
                          + "the initial configuration at {} binds to amqp port '${qpid.amqp_port}').",
                          initialConfigLocation );
        }
        else
        {
            LOGGER.debug( "On creating an embedded broker, setting the value of the qpid.amqp_port to {}, which will "
                          + "be respected if the the initial configuration at {} binds to amqp port "
                          + "'${qpid.amqp_port}'.",
                          initialConfigLocation );
        }

        options.put( ConfiguredObject.CONTEXT, Collections.singletonMap( "qpid.amqp_port", port ) );

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
        if ( !isStarted.get() )
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

            // Port available?
            if ( this.getBoundPorts().values().contains( -1 ) )
            {
                throw new CouldNotStartEmbeddedBrokerException( "Failed to start an embedded broker using the initial "
                                                                + "configuration in '"
                                                                + INITIAL_CONFIGURATION
                                                                + "'. Could not discover a bound port." );
            }

            LOGGER.info( "Started an embedded broker with the following bound ports {}.",
                         this.getBoundPorts() );

            LOGGER.debug( "Finished starting embedded broker with launch options {}.", this.launchOptions );

            this.isStarted.set( true );
        }
    }

    /**
     * Returns a mapping of TCP ports bound by the embedded broker. Typical ports are for protocols AMQP and HTTP. This
     * will only return a non-empty mapping after {@link #start()} has been called.
     * 
     * @return the port mapping 
     */

    public Map<String, Integer> getBoundPorts()
    {
        Map<String, Integer> ports = this.listener.getPorts();

        if ( Objects.isNull( ports ) )
        {
            return Collections.emptyMap();
        }

        return ports; // Immutable on construction
    }

    @Override
    public void close() throws IOException
    {
        LOGGER.debug( "Closing the embedded broker." );

        this.shutdown();

        LOGGER.info( "The embedded broker has been closed." );
    }

    /**
     * Returns the maximum number of retries or null if not configured.
     * 
     * @return the number of retries or null
     */

    public Integer getMaximumRetries()
    {
        Integer returnMe = null;

        // Witness the horror of this nested configuration.
        Object vhn = this.listener.getBrokerConfiguration().get( "virtualhostnodes" );
        if ( Objects.nonNull( vhn ) && vhn instanceof List )
        {
            List<?> virtualHosts = (List<?>) vhn;
            if ( !virtualHosts.isEmpty() && virtualHosts.get( 0 ) instanceof Map )
            {
                Map<?, ?> firstHost = (Map<?, ?>) virtualHosts.get( 0 );
                Object context = firstHost.get( "context" );

                if ( Objects.nonNull( context ) && context instanceof Map )
                {
                    Map<?, ?> contextMap = (Map<?, ?>) context;

                    Object deliveryAttempts = contextMap.get( "queue.maximumDeliveryAttempts" );

                    try
                    {
                        returnMe = Integer.valueOf( deliveryAttempts + "" );

                        LOGGER.debug( "Discovered configuration for the maximum number of retries, which is {}. ",
                                      returnMe );
                    }
                    catch ( NumberFormatException e )
                    {
                        LOGGER.debug( "Unrecognized data type associated with queue.maximumDeliveryAttempts. "
                                      + "Expected an integer, but got {}.",
                                      deliveryAttempts );
                    }
                }
            }
        }

        return returnMe;
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

        this.listener = new PortExtractingLauncherListener();

        this.systemLauncher = new SystemLauncher( this.listener );

        this.isStarted = new AtomicBoolean();
    }

    /**
     * Listener that extracts port information on broker startup, allowing for dynamic port assignment by the broker to
     * be respected by the application.
     * 
     * @author james.brown@hydrosolved.com
     */

    private static class PortExtractingLauncherListener implements SystemLauncherListener
    {
        private SystemConfig<?> systemConfig;

        private Map<String, Integer> ports;

        private Map<String, Object> configuration;

        @Override
        public void beforeStartup()
        {
            // Not needed
        }

        @Override
        public void errorOnStartup( final RuntimeException e )
        {
            // Not needed
        }

        @Override
        public void afterStartup()
        {
            if ( Objects.isNull( this.systemConfig ) )
            {
                throw new IllegalStateException( "Cannot extract the port bound by the embedded broker without "
                                                 + "system configuration, which is missing." );
            }

            Broker<?> broker = (Broker<?>) this.systemConfig.getContainer();

            this.ports = broker.getChildren( Port.class )
                               .stream()
                               .collect( Collectors.toUnmodifiableMap( Port::getName,
                                                                       Port::getBoundPort ) );

            this.configuration = Collections.unmodifiableMap( broker.extractConfig( false ) );
        }

        @Override
        public void onContainerResolve( final SystemConfig<?> systemConfig )
        {
            LOGGER.debug( "Discovered the system configuration on resolving the embedded broker container. The system "
                          + "configuration is {}.",
                          systemConfig );

            this.systemConfig = systemConfig;
        }

        @Override
        public void onContainerClose( final SystemConfig<?> systemConfig )
        {
            // Not needed
        }

        @Override
        public void onShutdown( final int exitCode )
        {
            // Not needed
        }

        @Override
        public void exceptionOnShutdown( final Exception e )
        {
            // Not needed
        }

        /**
         * @return the ports.
         */

        private Map<String, Integer> getPorts()
        {
            return this.ports;
        }

        /**
         * @return the broker configuration.
         */

        private Map<String, Object> getBrokerConfiguration()
        {
            return this.configuration;
        }
    }

}
