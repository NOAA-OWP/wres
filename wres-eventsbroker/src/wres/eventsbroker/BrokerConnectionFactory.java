package wres.eventsbroker;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
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

/**
 * <p>Manages connections to an AMQP broker. The broker configuration is contained in a jndi properties file on the 
 * classpath. If the configuration contains the loopback address (localhost, 127.0.0.1), then the factory creates an 
 * instance of an {@link EmbeddedBroker} unless a broker is already active on that address and port. Any embedded 
 * broker instance is managed by this class and must be closed when the application exits. For this reason, the class 
 * implements {@link Closeable} and it is recommended to instantiate using a try-with-resources. For example:
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

    private static final Logger LOGGER = LoggerFactory.getLogger( BrokerConnectionFactory.class );

    /**
     * Default jndi properties file on the classpath.
     */
    
    private static final String DEFAULT_PROPERTIES = "jndi.properties";
    
    /**
     * Instance of an embedded broker managed by this factory instance, created as needed. There should be one instance 
     * of a {@link BrokerConnectionFactory} with an embedded broker instance per application instance.
     */

    private final EmbeddedBroker broker;

    /**
     * Context from which connections may be requested.
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
    
    public Destination get( String name ) throws NamingException
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
     * Constructs a new instances and creates an embedded broker as necessary.
     * 
     * @param jndiProperties the name of a jndi properties file on the classpath
     * @throws CouldNotLoadBrokerConfigurationException if the broker configuration could not be found
     * @throws CouldNotStartEmbeddedBrokerException if an embedded broker was requested and could not be started
     * @throws NullPointerException if the jndiProperties is null
     */

    private BrokerConnectionFactory( String jndiProperties )
    {
        Objects.requireNonNull( jndiProperties );

        Properties properties = new Properties();

        // Load the jndi.properties        
        URL config = BrokerConnectionFactory.class.getClassLoader().getResource( jndiProperties );
        try ( InputStream stream = config.openStream() )
        {
            properties.load( stream );

            this.context = new InitialContext( properties );

            LOGGER.debug( "Upon reading {}, discovered the following broker connection properties: {}",
                          jndiProperties,
                          properties );

            this.broker = this.createEmbeddedBrokerIfRequired( properties );
            this.connectionFactory = this.createConnectionFactory( properties );
        }
        catch ( IOException | NamingException e )
        {
            throw new CouldNotLoadBrokerConfigurationException( "Unable to load the expected broker configuration from "
                                                                + "a jndi.properties file on the application classpath.",
                                                                e );
        }

        // Start the embedded broker if one exists
        if ( Objects.nonNull( this.broker ) )
        {
            broker.start();
        }
    }

    /**
     * Creates an instance of an embedded broker if required.
     * 
     * @param properties the broker configuration properties
     * @return an embedded broker or null
     * @throws NamingException if the jndi property could not be found
     * @throws CouldNotStartEmbeddedBrokerException if an embedded broker was requested and could not be started.
     */

    private EmbeddedBroker createEmbeddedBrokerIfRequired( Properties properties ) throws NamingException
    {
        EmbeddedBroker returnMe = null;

        String factoryName = null;
        ConnectionFactory factory = null;

        for ( Entry<Object, Object> nextEntry : properties.entrySet() )
        {
            Object key = nextEntry.getKey();

            if ( Objects.nonNull( key ) && key.toString().contains( "connectionfactory" ) )
            {
                factoryName = key.toString().replace( "connectionfactory.", "" );

                Object value = nextEntry.getValue();

                if ( Objects.nonNull( value ) && ( value.toString().contains( "localhost" )
                                                   || value.toString().contains( "127.0.0.1" ) ) )
                {

                    LOGGER.debug( "Discovered the connection property {} with value {}, which "
                                  + "indicates that a broker should be listening on localhost.",
                                  key,
                                  value );

                    factory = (ConnectionFactory) this.context.lookup( factoryName );

                    // If retries are configured, then expect retries here, even if the connection ultimately fails
                    try ( Connection connection = factory.createConnection() )
                    {
                        LOGGER.info( "Discovered an active AMQP broker at {}", value );
                    }
                    catch ( JMSException e )
                    {
                        LOGGER.info( "Could not connect to an active AMQP broker at {}. Starting an embedded broker "
                                     + "instead.",
                                     value );
                        returnMe = EmbeddedBroker.of();
                    }
                }

                // Only need the connection factory property
                break;
            }
        }

        return returnMe;
    }

    /**
     * Creates a connection factory.
     * 
     * @param properties the broker configuration properties.
     * @return a connection factory
     * @throws NamingException if the jndi property could not be found
     */

    private ConnectionFactory createConnectionFactory( Properties properties ) throws NamingException
    {
        String factoryName = null;

        for ( Object nextKey : properties.keySet() )
        {
            if ( Objects.nonNull( nextKey ) && nextKey.toString().contains( "connectionfactory" ) )
            {
                factoryName = nextKey.toString().replace( "connectionfactory.", "" );
            }
        }

        return (ConnectionFactory) this.context.lookup( factoryName );
    }
}
