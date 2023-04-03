package wres.eventsbroker.embedded;

import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Objects;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An embedded broker for publishing and subscribing to evaluation messages.
 */

public class EmbeddedBroker implements Closeable
{
    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( EmbeddedBroker.class );

    /**
     * The default protocol for which an acceptor and associated binding url should be registered.
     */

    private static final String DEFAULT_PROTOCOL = "amqp";

    /**
     * Is <code>true</code> if the broker is started, <code>false</code> otherwise.
     */

    private final AtomicBoolean isStarted;

    /**
     * The broker instance.
     */

    private final ActiveMQServer broker;

    /**
     * The binding url, including the host (localhost) and port.
     */

    private final String burl;

    /**
     * The port.
     */

    private final int port;

    /**
     * An exception encountered on starting the broker that is not fatal as judged by Artemis, but is fatal as judged
     * by this application. See: <a href="https://www.mail-archive.com/issues@activemq.apache.org/msg50741.html">https://www.mail-archive.com/issues@activemq.apache.org/msg50741.html</a>
     */

    private final AtomicReference<Exception> exceptionOnStartup;

    /**
     * <p>Attempts to create an embedded broker in two stages:
     * 
     * <ol>
     * <li>First, attempts to bind a broker on the configured port. If that fails, move the the second stage.</li>
     * <li>Second, if dynamic binding is allowed, attempts to bind a broker to a broker-chosen (free) port, which may 
     * be discovered from the embedded broker instance after startup.<li>
     * </ol>
     * 
     * <p>If a free port is selected, the supplied properties are updated to reflect that port.
     * 
     * @param properties the connection properties, not null                                                                     
     * @param dynamicBindingAllowed is true to bind any port, false to throw an exception if the requested port is bound
     * @return an embedded broker instance
     * @throws NullPointerException if the properties are null
     * @throws IllegalArgumentException if the connection property cannot be found
     */

    public static EmbeddedBroker of( Properties properties,
                                     boolean dynamicBindingAllowed )
    {
        Objects.requireNonNull( properties );
        String connectionPropertyName = EmbeddedBroker.getConnectionPropertyName( properties );

        if ( !properties.containsKey( connectionPropertyName ) )
        {
            throw new IllegalArgumentException( "Could not locate a connection property '"
                                                + connectionPropertyName
                                                + "' in the map of properties: "
                                                + properties
                                                + "." );
        }

        String bindingUrl = properties.getProperty( connectionPropertyName );

        LOGGER.debug( "Attempting to extract the desired port from the binding URL {}.", bindingUrl );

        // Discover the port to which a broker should be bound
        String regex = ":(?<port>[\\d]+)";

        Pattern p = Pattern.compile( regex );
        Matcher m = p.matcher( bindingUrl );
        int port = -1;

        // Port pattern found?
        if ( m.find() )
        {
            String portString = m.group().replace( ":", "" );

            LOGGER.debug( "While attempting to create an embedded broker, discovered the following port string to "
                          + "parse: {}.",
                          portString );

            try
            {
                port = Integer.parseInt( portString );

                LOGGER.debug( "While attempting to create an embedded broker, discovered a port to bind of: {}.",
                              port );
            }
            catch ( NumberFormatException e )
            {
                LOGGER.debug( "Failed to parse the port string into an integer port number.", e );
            }
        }

        // No port pattern: either absent or could not be parsed from string
        if ( port == -1 )
        {
            throw new CouldNotStartEmbeddedBrokerException( "Failed to identify a port number in the binding URL. "
                                                            + "Check that the binding URL contains a port number. The "
                                                            + "binding URL was: "
                                                            + bindingUrl );
        }

        EmbeddedBroker returnMe = null;

        try
        {
            returnMe = EmbeddedBroker.of( port );

            // Attempt to bind, which may fail
            returnMe.start();
        }
        catch ( CouldNotStartEmbeddedBrokerException e )
        {
            LOGGER.debug( "Unable to bind an embedded broker to the configured port of {}.", port );

            // Close now
            if ( Objects.nonNull( returnMe ) )
            {
                try
                {
                    returnMe.close();
                }
                catch ( IOException f )
                {
                    LOGGER.warn( "Unable to close an embedded broker instance.", f );
                }
            }

            if ( !dynamicBindingAllowed )
            {
                throw new CouldNotStartEmbeddedBrokerException( "Could not bind an embedded amqp broker to port "
                                                                + port
                                                                + " on the loopback network interface. If this port is "
                                                                + "already bound, change the configured port, "
                                                                + "free the configured port, configure a dynamic port "
                                                                + "using TCP reserved port 0 or request an "
                                                                + "embedded broker with dynamic binding to override a "
                                                                + "configured port that is already bound.",
                                                                e );
            }

            returnMe = EmbeddedBroker.of( 0 );

            LOGGER.info( "Unable to bind an embedded broker to the configured port of {}. Since dynamic binding was "
                         + "allowed, the broker chose another port, which is {}.",
                         port,
                         returnMe.getMessagingPort() );
        }

        // Update the properties with any automatically selected port
        EmbeddedBroker.updateConnectionStringWithDynamicPortIfConfigured( connectionPropertyName,
                                                                          properties,
                                                                          returnMe.getMessagingPort() );

        return returnMe;
    }

    /**
     * Starts the broker.
     * 
     * @throws CouldNotStartEmbeddedBrokerException if the broker could not be started for any reason.
     */

    public void start()
    {
        if ( !this.isStarted.get() )
        {
            LOGGER.debug( "Starting embedded broker." );

            // Record any non-fatal start-up exception identified by the callback that is considered fatal here
            Exception startupException = null;

            try
            {
                this.broker.start();
                startupException = this.exceptionOnStartup.get();
            }
            // Yuck
            catch ( Exception e )
            {
                throw new CouldNotStartEmbeddedBrokerException( "Failed to start an embedded broker.", e );
            }

            // Exception on callback?
            if ( Objects.nonNull( startupException ) )
            {
                throw new CouldNotStartEmbeddedBrokerException( "Failed to start an embedded broker.",
                                                                this.exceptionOnStartup.get() );
            }

            if ( LOGGER.isInfoEnabled() )
            {
                LOGGER.info( "Started an embedded broker with {} transport on port: {}. The full binding URL is: {}.",
                             DEFAULT_PROTOCOL.toUpperCase(),
                             this.getMessagingPort(),
                             this.burl );
            }

            this.isStarted.set( true );
        }
    }

    /**
     * Returns the port for messaging traffic
     * 
     * @return the port 
     */

    public int getMessagingPort()
    {
        return this.port;
    }

    @Override
    public void close() throws IOException
    {
        LOGGER.debug( "Closing the embedded broker." );

        this.shutdown();

        LOGGER.info( "The embedded broker has been closed." );
    }

    /**
     * Kills the broker.
     * @throws IOException if the broker could not be stopped
     */

    private void shutdown() throws IOException
    {
        if ( this.isStarted.getAndSet( false ) )
        {
            try
            {
                this.broker.stop();
            }
            // Yuck
            catch ( Exception e )
            {
                throw new IOException( "Could not stop the embedded broker.", e );
            }
        }
    }

    /**
     * @return an ephemeral open port
     * @throws IOException if a port could not be found
     */

    private static int getEphemeralPort() throws IOException
    {
        try ( ServerSocket socket = new ServerSocket( 0 ) )
        {
            return socket.getLocalPort();
        }
    }

    /**
     * Returns the connection property name from the map of properties.
     * 
     * @param properties the properties
     * @return the connection property name or null if none could be found
     * @throws NullPointerException if the properties is null
     */

    private static String getConnectionPropertyName( Properties properties )
    {
        Objects.requireNonNull( properties );

        for ( Entry<Object, Object> nextEntry : properties.entrySet() )
        {
            Object key = nextEntry.getKey();

            if ( Objects.nonNull( key ) && key.toString().contains( "connectionFactory" ) )
            {
                String name = key.toString();

                if ( LOGGER.isDebugEnabled() )
                {
                    LOGGER.debug( "Discovered a connection property name of {} with value {} among the broker "
                                  + "connection properties.",
                                  name,
                                  properties.get( key ) );
                }

                return name;
            }
        }

        LOGGER.warn( "Unable to locate a connection property name among the properties: {}.", properties );

        // None found
        return null;
    }

    /**
     * Returns a broker instance with default launch options on a prescribed port.
     * 
     * @param port an explicit port on which to bind the transport
     * @return a broker instance with default options
     */

    private static EmbeddedBroker of( int port )
    {
        return new EmbeddedBroker( port );
    }

    /**
     * If the connection string contains a different port than the port actually used, then update the port inline to 
     * the properties map with the relevant AMQP port from the list of broker ports for which bindings were found. 
     * 
     * @param connectionPropertyName the connection property name
     * @param properties the properties whose named value should be replaced
     * @param amqpPort the discovered amqp port
     * @throws NullPointerException if any nullable input is null
     */

    private static void updateConnectionStringWithDynamicPortIfConfigured( String connectionPropertyName,
                                                                           Properties properties,
                                                                           int amqpPort )
    {
        Objects.requireNonNull( connectionPropertyName );
        Objects.requireNonNull( properties );

        String propertyValue = properties.getProperty( connectionPropertyName );

        String updated = propertyValue.replaceAll( "localhost:\\d+", "localhost:" + amqpPort )
                                      .replaceAll( "127.0.0.1:\\d+", "127.0.0.1:" + amqpPort )
                                      .replaceAll( "0.0.0.0:\\d+", "0.0.0.0:" + amqpPort );

        properties.setProperty( connectionPropertyName, updated );

        LOGGER.debug( "The embedded broker was configured with a binding of {} for AMQP traffic "
                      + "but is actually bound to TCP port {}. Updated the configured TCP port to reflect "
                      + "the bound port. The configured property is {}={}. The updated property is "
                      + "{}={}.",
                      propertyValue,
                      amqpPort,
                      connectionPropertyName,
                      propertyValue,
                      connectionPropertyName,
                      updated );
    }

    /**
     * Hidden constructor.
     * 
     * @param amqpPort the port
     * @throws CouldNotStartEmbeddedBrokerException if the broker could not be started for any reason
     */

    private EmbeddedBroker( int amqpPort )
    {
        Configuration configuration;

        String localBurl = "";

        try
        {
            int finalPort = amqpPort;

            // Reserved TCP port 0? Find a free port.
            // TODO: if possible ask the broker to bind an ephemeral port and then query for that port. This was 
            // possible but difficult with Qpid, but seems harder with ActiveMQ. The (small) risk of letting the 
            // application decide is that the port discovered here is already bound when the broker attempts to bind it.
            if ( finalPort == 0 )
            {
                LOGGER.debug( "When instantiating an embedded broker, noticed that the requested TCP port is reserved "
                              + "port 0. Attempting to discover an ephemeral port that is unbound..." );

                finalPort = EmbeddedBroker.getEphemeralPort();

                LOGGER.debug( "Discovered an unbound port of {}, which will be used to bind the TCP transport on the "
                              + "embedded broker.",
                              finalPort );
            }

            this.burl = "tcp://127.0.0.1?port="
                        + finalPort;
            localBurl = this.burl;

            LOGGER.debug( "Created a binding URL for the embedded broker: {}.", this.burl );

            configuration = new ConfigurationImpl().setPersistenceEnabled( false )
                                                   .setJournalDirectory( "target/data/journal" )
                                                   .setSecurityEnabled( false )
                                                   .addAcceptorConfiguration( DEFAULT_PROTOCOL, this.burl );

            this.broker = ActiveMQServers.newActiveMQServer( configuration );
            this.port = finalPort;

            // Register any exception on start-up that is considered fatal by this application 
            this.exceptionOnStartup = new AtomicReference<>();
            this.broker.registerActivationFailureListener( this.exceptionOnStartup::set );
        }
        // Yuck
        catch ( Exception e )
        {
            throw new CouldNotStartEmbeddedBrokerException( "Could not configure the embedded broker at binding "
                                                            + "url "
                                                            + localBurl
                                                            + ".",
                                                            e );
        }


        this.isStarted = new AtomicBoolean();
    }

}
