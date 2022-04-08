package wres.eventsbroker.embedded;

import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.atomic.AtomicBoolean;

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
     * Returns a broker instance with default launch options
     * 
     * @return a broker instance with default options.
     */

    public static EmbeddedBroker of()
    {
        return EmbeddedBroker.of( 0 );
    }

    /**
     * Returns a broker instance with default launch options on a prescribed port.
     * 
     * @param port an explicit port on which to bind the transport
     * @return a broker instance with default options
     */

    public static EmbeddedBroker of( int port )
    {
        return new EmbeddedBroker( port );
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

            try
            {
                this.broker.start();
            }
            // Yuck
            catch ( Exception e )
            {
                throw new CouldNotStartEmbeddedBrokerException( "Failed to start an embedded broker.", e );
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
