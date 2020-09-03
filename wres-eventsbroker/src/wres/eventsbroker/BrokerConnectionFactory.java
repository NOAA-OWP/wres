package wres.eventsbroker;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Supplier;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.eventsbroker.embedded.CouldNotLoadBrokerConfigurationException;
import wres.eventsbroker.embedded.CouldNotStartEmbeddedBrokerException;
import wres.eventsbroker.embedded.EmbeddedBroker;

/**
 * <p>Manages connections to an AMQP broker. The broker configuration is contained in a jndi properties file on the 
 * classpath. If the configuration contains the loopback address (localhost, 127.0.0.1), then the factory creates an 
 * instance of an {@link EmbeddedBroker} unless a broker is already active on that address and port. If the configured 
 * port is free, then the embedded broker will attempt to bind to this port first. If the configured port is TCP 
 * reserved port zero or the configured port is not free, the embedded broker will choose a port and report this 
 * via the broker instance, but only after {@link EmbeddedBroker#start()} has been called.
 * 
 * <p>Any embedded broker instance is managed by this class and must be closed when the application exits. For this 
 * reason, the class implements {@link Closeable} and it is recommended to instantiate using a try-with-resources. 
 * For example:
 * 
 * <pre>
 * {@code
 *     try( BrokerConnectionFactory factory = BrokerConnectionFactory.of() )
 *     {
 *         // Acquire connections
 *     }
 * }
 * </pre>
 * 
 * @author james.brown@hydrosolved.com
 */

public class BrokerConnectionFactory implements Closeable, Supplier<ConnectionFactory>
{

    private static final String LOCALHOST_127_0_0_1_0 = "127.0.0.1:0";

    private static final String LOCALHOST_0 = "localhost:0";

    private static final Logger LOGGER = LoggerFactory.getLogger( BrokerConnectionFactory.class );

    /**
     * Default maximum number of retries. A small risk remains that this may disagree with broker configuration that
     * could not be found (and is otherwise canonical). If this number exceeds the number in the broker configuration, 
     * a race condition could emerge in marking a subscriber as complete. See #81735-145. This is considered very low
     * risk. It would be worse to default to zero retries, which would nevertheless guarantee to eliminate the small 
     * risk.
     */

    private static final int DEFAULT_MAXIMUM_RETRIES = 2;

    /**
     * Default jndi properties file on the classpath.
     */

    private static final String DEFAULT_PROPERTIES = "eventbroker.properties";

    /**
     * The maximum number of retries supported by the broker whose connections are exposed by this factory.
     */

    private final Integer maximumRetries;

    /**
     * Instance of an embedded broker managed by this factory instance, created as needed. There should be one instance 
     * of a {@link BrokerConnectionFactory} with an embedded broker instance per application instance.
     */

    private final EmbeddedBroker broker;

    /**
     * Context that maps JMS objects to names.
     */

    private final Context context;

    /**
     * A connection factory.
     */

    private final ConnectionFactory connectionFactory;

    /**
     * Returns an instance of a factory, which is created using {@link DEFAULT_PROPERTIES}.
     * 
     * @return an instance
     * @throws CouldNotLoadBrokerConfigurationException if the broker configuration could not be found
     * @throws CouldNotStartEmbeddedBrokerException if an embedded broker was requested and could not be started
     */

    public static BrokerConnectionFactory of()
    {
        return new BrokerConnectionFactory( BrokerConnectionFactory.DEFAULT_PROPERTIES );
    }

    @Override
    public ConnectionFactory get()
    {
        return this.connectionFactory;
    }

    /**
     * Returns a destination from the present context.
     * 
     * @param name the destination name
     * @return the destination
     * @throws NamingException if the destination does not exist
     * @throws NullPointerException if the name is null
     */

    public Destination getDestination( String name ) throws NamingException
    {
        Objects.requireNonNull( name );

        return (Destination) this.context.lookup( name );
    }

    @Override
    public void close() throws IOException
    {
        if ( Objects.nonNull( this.broker ) )
        {
            this.broker.close();
        }
    }

    /**
     * Returns <code>true</code> if this factory is managing an embedded broker instance whose resources must be closed
     * on completion, otherwise <code>false</code>.
     * 
     * @return true if this factory is managing an embedded broker
     */

    public boolean hasEmbeddedBroker()
    {
        return Objects.nonNull( this.broker );
    }

    /**
     * Returns the maximum number of retries allowed by the broker whose connections are exposed by this factory, else 
     * the default maximum of {@link BrokerConnectionFactory#DEFAULT_MAXIMUM_RETRIES}.
     * 
     * @return the maxcimum retry count
     */

    public int getMaximumRetries()
    {
        return this.maximumRetries;
    }

    /**
     * Constructs a new instances and creates an embedded broker as necessary.
     * 
     * @param jndiProperties the name of a jndi properties file on the classpath
     * @throws CouldNotLoadBrokerConfigurationException if the broker configuration could not be found
     * @throws CouldNotStartEmbeddedBrokerException if an embedded broker was requested and could not be started
     * @throws NullPointerException if the jndiProperties is null
     * @throws CouldNotLoadBrokerConfigurationException if the properties could not be read
     */

    private BrokerConnectionFactory( String jndiProperties )
    {
        Objects.requireNonNull( jndiProperties );

        Properties properties = new Properties();

        // Load the jndi.properties
        URL config = BrokerConnectionFactory.class.getClassLoader().getResource( jndiProperties );

        if ( Objects.isNull( config ) )
        {
            throw new CouldNotLoadBrokerConfigurationException( "Could not find the " + jndiProperties
                                                                + " file on the class path." );
        }

        try ( InputStream stream = config.openStream() )
        {
            properties.load( stream );

            LOGGER.debug( "Upon reading {}, discovered the following broker connection properties: {}",
                          jndiProperties,
                          properties );

            this.broker = this.createEmbeddedBrokerFromPropertiesIfRequired( properties );
        }
        catch ( IOException | NamingException e )
        {
            throw new CouldNotLoadBrokerConfigurationException( "Unable to load the expected broker configuration from "
                                                                + "a jndi.properties file on the application classpath.",
                                                                e );
        }

        // Start the embedded broker if one exists
        // This may adjust the properties if the broker is bound to a dynamic port
        if ( Objects.nonNull( this.broker ) )
        {
            this.broker.start();

            Map.Entry<String, String> connection = this.getConnectionProperty( properties );

            // If the port was configured dynamically by the broker, override it here
            this.updateConnectionStringWithDynamicPortIfConfigured( connection.getKey(),
                                                                    connection.getValue(),
                                                                    properties,
                                                                    this.broker.getBoundPorts() );

            Integer retries = this.broker.getMaximumRetries();

            // Retries?
            if ( Objects.nonNull( retries ) )
            {
                this.maximumRetries = retries;
            }
            else
            {
                this.maximumRetries = BrokerConnectionFactory.DEFAULT_MAXIMUM_RETRIES;
            }
        }
        else
        {
            this.maximumRetries = BrokerConnectionFactory.DEFAULT_MAXIMUM_RETRIES;
        }

        // Set any variables that depend on the (possibly adjusted) properties
        try
        {
            this.context = new InitialContext( properties );
            this.connectionFactory = this.createConnectionFactory( this.context, properties );
        }
        catch ( NamingException e )
        {
            throw new CouldNotLoadBrokerConfigurationException( "Encountered an error on instantiating the broker.",
                                                                e );
        }
    }

    /**
     * Creates an instance of an embedded broker if required.
     * 
     * @param properties the broker configuration properties
     * @return an embedded broker or null
     * @throws NamingException if the connection factory could not be located
     * @throws CouldNotStartEmbeddedBrokerException if an embedded broker was requested and could not be started.
     */

    private EmbeddedBroker createEmbeddedBrokerFromPropertiesIfRequired( Properties properties )
            throws NamingException
    {
        EmbeddedBroker returnMe = null;

        Context localContext = new InitialContext( properties );
        ConnectionFactory factory = this.createConnectionFactory( localContext, properties );

        Map.Entry<String, String> connectionProperty = this.getConnectionProperty( properties );

        String key = connectionProperty.getKey();
        String value = connectionProperty.getValue();

        if ( value.contains( "localhost" ) || value.contains( "127.0.0.1" ) )
        {
            LOGGER.debug( "Discovered the connection property {} with value {}, which "
                          + "indicates that a broker should be listening on localhost.",
                          key,
                          value );

            // Embedded broker with dynamic port assignment?
            if ( value.contains( LOCALHOST_0 ) || value.contains( LOCALHOST_127_0_0_1_0 ) )
            {
                LOGGER.debug( "Discovered the connection property {} with value {}, which indicates that an embedded "
                              + "broker should be started and bound to a port assigned dynamically by the broker.",
                              key,
                              value );

                returnMe = this.createEmbeddedBroker( properties );
            }
            // Look for an active broker, fall back on an embedded one
            else
            {
                // If retries are configured, then expect retries here, even if the connection ultimately fails
                LOGGER.debug( "Probing to establish whether an active broker is accepting connections at {}. This "
                              + "may fail!",
                              value );

                try ( Connection connection = factory.createConnection() )
                {
                    LOGGER.debug( "Discovered an active AMQP broker at {}", value );
                }
                catch ( JMSException e )
                {
                    LOGGER.debug( "Could not connect to an active AMQP broker at {}. Starting an embedded broker "
                                  + "instead.",
                                  value );

                    returnMe = this.createEmbeddedBroker( properties );
                }
            }
        }

        return returnMe;
    }

    /**
     * Returns the connection string from the map of properties.
     * 
     * @param properties the properties
     * @return the connection string
     * @throws CouldNotStartEmbeddedBrokerException if the property could not be found
     */

    private Map.Entry<String, String> getConnectionProperty( Properties properties )
    {
        Map.Entry<String, String> returnMe = null;

        for ( Entry<Object, Object> nextEntry : properties.entrySet() )
        {
            Object key = nextEntry.getKey();

            if ( Objects.nonNull( key ) && key.toString().contains( "connectionfactory" ) )
            {
                Object value = nextEntry.getValue();

                if ( Objects.nonNull( value ) )
                {
                    returnMe = new AbstractMap.SimpleEntry<>( key.toString(), value.toString() );
                }

                break;
            }
        }

        if ( Objects.isNull( returnMe ) )
        {
            throw new CouldNotStartEmbeddedBrokerException( "Could not locate a connection string in the properties "
                                                            + properties );
        }

        return returnMe;
    }

    /**
     * If the connection string contains a different port than the port actually used, then update the port inline to 
     * the properties map with the relevant AMQP port from the list of broker ports for which bindings were found. 
     * 
     * @param propertyName
     * @param propertyValue
     * @param properties
     */

    private void updateConnectionStringWithDynamicPortIfConfigured( String propertyName,
                                                                    String propertyValue,
                                                                    Properties properties,
                                                                    Map<String, Integer> ports )
    {
        if ( !ports.isEmpty() )
        {
            for ( Map.Entry<String, Integer> next : ports.entrySet() )
            {
                if ( next.getKey().contains( "AMQP" ) )
                {
                    Integer port = next.getValue();

                    String updated = propertyValue.replaceAll( "localhost:\\d+", "localhost:" + port )
                                                  .replaceAll( "127.0.0.1:\\d+", "127.0.0.1:" + port );

                    properties.setProperty( propertyName, updated );

                    LOGGER.debug( "The embedded broker was configured with a binding of {} for AMQP traffic "
                                  + "but is actually bound to TCP port {}. Updated the configured TCP port to reflect "
                                  + "the bound port. The configured property is {}={}. The updated property is "
                                  + "{}={}.",
                                  propertyValue,
                                  port,
                                  propertyName,
                                  propertyValue,
                                  propertyName,
                                  updated );
                }
            }
        }
    }

    /**
     * Attempts to create an embedded broker in two stages:
     * 
     * <ol>
     * <li>First, attempts to bind a broker on the configured port. If that fails, move the the second stage.</ol>
     * <li>Second, attempts to bind a broker to a broker-chosen (free) port, which may be discovered from the embedded
     * broker instance after startup.<li>
     * </ol>
     * 
     * @param properties the properties
     * @return an embedded broker instance
     */

    private EmbeddedBroker createEmbeddedBroker( Properties properties )
    {
        Map.Entry<String, String> connectionProperty = this.getConnectionProperty( properties );
        String connectionUrl = connectionProperty.getValue();

        String replace = connectionUrl.replaceAll( "[^\\d]+", "" );

        int port = Integer.parseInt( replace );

        EmbeddedBroker returnMe = null;

        try
        {
            returnMe = EmbeddedBroker.of( port );

            // Attempt to bind, which may fail
            returnMe.start();
        }
        catch ( CouldNotStartEmbeddedBrokerException e )
        {
            LOGGER.debug( "Unable to bind an embedded broker to the configured port of {}. Choosing another port, "
                          + "which will be available from the broker instance after startup." );

            returnMe = EmbeddedBroker.of();
        }

        return returnMe;
    }

    /**
     * Creates a connection factory.
     * 
     * @param context the context
     * @param properties the broker configuration properties.
     * @return a connection factory
     * @throws NamingException if the connection factory could not be located
     * @throws NullPointerException if any input is null
     */

    private ConnectionFactory createConnectionFactory( Context context, Properties properties )
            throws NamingException
    {
        Objects.requireNonNull( context );
        Objects.requireNonNull( properties );

        Map.Entry<String, String> connectionProperty = this.getConnectionProperty( properties );
        String factoryName = connectionProperty.getKey().replace( "connectionfactory.", "" );
        String connectionUrl = connectionProperty.getValue();

        ConnectionFactory factory = (ConnectionFactory) context.lookup( factoryName );

        LOGGER.debug( "Created a connection factory with name {} and URL connection string {}.",
                      factoryName,
                      connectionUrl );

        return factory;
    }

}
