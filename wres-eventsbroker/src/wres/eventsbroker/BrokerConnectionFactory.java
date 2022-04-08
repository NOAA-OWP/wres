package wres.eventsbroker;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jcip.annotations.GuardedBy;
import wres.eventsbroker.embedded.CouldNotLoadBrokerConfigurationException;
import wres.eventsbroker.embedded.CouldNotStartEmbeddedBrokerException;
import wres.eventsbroker.embedded.EmbeddedBroker;

/**
 * <p>Manages connections to an AMQP broker. The basic configuration is contained in a jndi properties file on the 
 * classpath. If the configuration contains the loopback address (localhost, 127.0.0.1), then the factory creates an 
 * instance of an {@link EmbeddedBroker} unless a broker is already active on that address and port. If the configured 
 * port is free, then the embedded broker will attempt to bind to this port first. If the configured port is TCP 
 * reserved port zero or the configured port is not free, then the embedded broker will choose a port and report this 
 * via the broker instance.
 * 
 * <p>The configured address and port of the broker may be overridden at runtime with the system properties 
 * <code>wres.eventsBrokerPort</code> and <code>wres.eventsBrokerAddress</code>, respectively.
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
 * TODO: abstract the creation of an embedded broker away from this connection factory and move the connection factory
 * to wres-events, since it is a client-side utility and it should be engaged in the creation of connections only, not
 * brokers.
 * 
 * @author James Brown
 */

public class BrokerConnectionFactory implements Closeable, Supplier<Connection>
{
    private static final String IN_THE_MAP_OF_PROPERTIES = " in the map of properties.";


    private static final String COULD_NOT_FIND_THE_NAMED_CONNECTION_PROPERTY =
            "Could not find the named connection property ";

    private static final String LOCALHOST_127_0_0_1_0 = "127.0.0.1:0";

    private static final String LOCALHOST_0 = "localhost:0";

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( BrokerConnectionFactory.class );

    /**
     * Maximum number of connection retries on initially connecting to the broker.
     */

    private static final int MAXIMUM_CONNECTION_RETRIES = 5;

    /**
     * Default maximum number of times a message will be resent.
     */

    private static final int DEFAULT_MAXIMUM_MESSAGE_RETRIES = 4;

    /**
     * Default jndi properties file on the classpath.
     */

    private static final String DEFAULT_PROPERTIES = "eventbroker.properties";

    /**
     * The maximum number of times a message can be resent on failure.
     */

    private final Integer maximumMessageRetries;

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
     * The pool of all connections issued by this instance to be closed.
     */

    @GuardedBy( "ConnectionPoolLock" )
    private final List<Connection> connectionPool;

    /**
     * A lock for the {@link connectionPool}.
     */

    private final Object connectionPoolLock;

    /**
     * Is {@code true} if the broker has been closed, otherwise {@code false}.
     */

    private boolean isClosed;

    /**
     * <p>Returns an instance of a factory, which is created using {@link #DEFAULT_PROPERTIES}. If an embedded broker is
     * required and the broker configuration requests a specific TCP port (not reserved TCP port 0), then the embedded
     * broker is lenient when the configured port is already bound, instead choosing an available port.
     * 
     * See also {@link #of(boolean)}
     * 
     * @return an instance
     * @throws CouldNotLoadBrokerConfigurationException if the broker configuration could not be found
     * @throws CouldNotStartEmbeddedBrokerException if an embedded broker was requested and could not be started
     */

    public static BrokerConnectionFactory of()
    {
        return BrokerConnectionFactory.of( true );
    }

    /**
     * <p>Returns an instance of a factory, which is created using {@link #DEFAULT_PROPERTIES}. If an embedded broker is
     * required and the broker configuration requests a specific TCP port (not reserved TCP port 0), then the embedded
     * broker throws an exception when the configured port is already bound, unless dynamic binding is explicitly 
     * allowed.
     * 
     * See also {@link #of()}.
     * 
     * @param dynamicBindingAllowed is true to bind any port, false to throw an exception if the requested port is bound
     * @return an instance
     * @throws CouldNotLoadBrokerConfigurationException if the broker configuration could not be found
     * @throws CouldNotStartEmbeddedBrokerException if an embedded broker was requested and could not be started
     */

    public static BrokerConnectionFactory of( boolean dynamicBindingAllowed )
    {
        return new BrokerConnectionFactory( BrokerConnectionFactory.DEFAULT_PROPERTIES,
                                            BrokerConnectionFactory.DEFAULT_MAXIMUM_MESSAGE_RETRIES,
                                            dynamicBindingAllowed );
    }

    /**
     * <p>Returns an instance of a factory, which is created using {@link #DEFAULT_PROPERTIES}. If an embedded broker is
     * required and the broker configuration requests a specific TCP port (not reserved TCP port 0), then the embedded
     * broker throws an exception when the configured port is already bound, unless dynamic binding is explicitly 
     * allowed.
     * 
     * See also {@link #of()}.
     * 
     * @param dynamicBindingAllowed is true to bind any port, false to throw an exception if the requested port is bound
     * @param maximumMessageRetries the maximum number of message retries, must be greater than or equal to zero
     * @return an instance
     * @throws CouldNotLoadBrokerConfigurationException if the broker configuration could not be found
     * @throws CouldNotStartEmbeddedBrokerException if an embedded broker was requested and could not be started
     * @throws IllegalArgumentException if the maximumMessageRetries is less than zero
     */

    public static BrokerConnectionFactory of( boolean dynamicBindingAllowed, int maximumMessageRetries )
    {
        return new BrokerConnectionFactory( BrokerConnectionFactory.DEFAULT_PROPERTIES,
                                            maximumMessageRetries,
                                            dynamicBindingAllowed );
    }

    /**
     * Returns a {@link Connection}, which must be closed on completion. Calling {@link #close()} will close all 
     * connections issued by this factory that have not been closed already.
     * 
     * @throws FailedToAcquireConnectionException if a connection could not be acquired for any reason
     */

    @Override
    public Connection get()
    {
        if ( this.isClosed() )
        {
            throw new FailedToAcquireConnectionException( "Cannot acquire a connection: the broker connection factory "
                                                          + "has been closed." );
        }

        try
        {
            Connection connection = this.connectionFactory.createConnection();
            this.addConnectionToPool( connection );

            return connection;
        }
        catch ( JMSException e )
        {
            throw new FailedToAcquireConnectionException( "While attempting to acquire a broker connection.", e );
        }
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

    /**
     * Closes all connections supplied by this instance and any embedded broker if created.
     * @throws IOException if the resource could not be closed for any reason
     */

    @Override
    public void close() throws IOException
    {
        LOGGER.info( "Closing broker connection factory {} and all associated broker connections.", this );

        // Flag closed
        this.isClosed = true;

        // Close connections
        synchronized ( this.connectionPoolLock )
        {
            for( Connection connectionToClose : this.connectionPool )
            {
                try
                {
                    connectionToClose.close();

                    LOGGER.debug( "Successfully called close on connection {}. This may happen more than once.",
                                  connectionToClose );
                }
                catch ( JMSException e )
                {
                    LOGGER.warn( "Failed to close a broker connection. This message may be repeated for other "
                                 + "connections." );
                }
            }
        }

        if ( this.hasEmbeddedBroker() )
        {
            try
            {
                this.broker.close();
            }
            catch ( IOException e )
            {
                throw new IOException( "While attempting to close an embedded broker.", e );
            }
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
     * Returns the maximum number of times a message will be resent on consumption failure.
     * 
     * @return the maximum retry count for resending messages
     */

    public int getMaximumMessageRetries()
    {
        return this.maximumMessageRetries;
    }

    /**
     * @return true if closed.
     */

    public boolean isClosed()
    {
        return this.isClosed;
    }

    /**
     * Adds a new connection to the pool.
     * @param connection the wrapped connection
     */

    private void addConnectionToPool( Connection connection )
    {
        Objects.requireNonNull( connection );

        synchronized ( this.connectionPoolLock )
        {
            this.connectionPool.add( connection );

            LOGGER.debug( "Added connection {} to the connection pool in {}, which now contains {} connections.",
                          connection,
                          this,
                          this.connectionPool.size() );
        }
    }

    /**
     * <p>Creates an instance of an embedded broker if required. Begins by looking for a system property 
     * {@code wres.startBroker}. If {@code wres.startBroker=true}, then an embedded broker is created. If 
     * {@code wres.startBroker=false}, then an embedded broker is not created. If the system property is missing, then 
     * inspects the connection properties. If the connection properties declare a localhost, then first probes for an 
     * active broker, and finally falls back on an embedded one. 
     * 
     * <p>An embedded broker is only created if either {@code wres.startBroker=true} or the connection properties 
     * declare a localhost and there is no existing broker with those connection properties.
     * 
     * @param connectionProperty the connection property
     * @param properties the broker configuration properties
     * @param dynamicBindingAllowed is true to bind any port, false to throw an exception if the requested port is bound
     * @return an embedded broker or null
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the connection property cannot be found
     * @throws NamingException if the connection factory could not be located
     * @throws CouldNotStartEmbeddedBrokerException if an embedded broker was requested and could not be started.
     */

    private EmbeddedBroker createEmbeddedBrokerFromPropertiesIfRequired( String connectionPropertyName,
                                                                         Properties properties,
                                                                         boolean dynamicBindingAllowed )
            throws NamingException
    {
        Objects.requireNonNull( connectionPropertyName );
        Objects.requireNonNull( properties );

        if ( !properties.containsKey( connectionPropertyName ) )
        {
            throw new IllegalArgumentException( COULD_NOT_FIND_THE_NAMED_CONNECTION_PROPERTY + connectionPropertyName
                                                + IN_THE_MAP_OF_PROPERTIES );
        }

        String key = connectionPropertyName;
        String url = properties.getProperty( key );

        // Look for a system property that definitively says whether an embedded broker should be started
        String startBroker = System.getProperty( "wres.startBroker" );
        if ( "true".equalsIgnoreCase( startBroker ) )
        {
            LOGGER.info( "Discovered the WRES system property wres.startBroker=true. Starting an embedded broker "
                         + "at the binding URL {}...",
                         url );

            return this.createEmbeddedBroker( properties, connectionPropertyName, dynamicBindingAllowed );
        }
        else if ( "false".equalsIgnoreCase( startBroker ) )
        {
            LOGGER.warn( "Probing for an active AMQP broker at the binding URL {}. Discovered the WRES system property "
                         + "wres.startBroker=false, so the evaluation will fail if no active broker is discovered at "
                         + "the binding URL (after exhausting any failover options).",
                         url );

            return null;
        }

        EmbeddedBroker returnMe = null;

        // Loopback interface or all local interfaces? If so, an embedded broker may be required.
        if ( url.contains( "localhost" ) || url.contains( "127.0.0.1" ) || url.contains( "0.0.0.0" ) )
        {
            LOGGER.debug( "Discovered the connection property {} with value {}, which "
                          + "indicates that a broker should be listening on localhost.",
                          key,
                          url );

            // Does the url contain the tcp reserved port 0, i.e. dynamic binding required?
            if ( url.contains( LOCALHOST_0 ) || url.contains( LOCALHOST_127_0_0_1_0 ) )
            {
                LOGGER.info( "Discovered a binding URL of {}, which includes the reserved TCP port of 0. Starting an "
                             + "embedded broker at this URL and allowing the broker to assign a port dynamically. The "
                             + "assigned port will be identified after the embedded broker has launched successfully.",
                             url );

                returnMe = this.createEmbeddedBroker( properties, connectionPropertyName, dynamicBindingAllowed );
            }
            // Look for an active broker, fall back on an embedded one
            else
            {
                // If retries are configured, then expect retries here, even if the connection ultimately fails
                LOGGER.debug( "Probing to establish whether an active broker is accepting connections at {}. This "
                              + "may fail!",
                              url );

                Context localContext = new InitialContext( properties );
                ConnectionFactory factory = this.createConnectionFactory( localContext, connectionPropertyName );

                try
                {
                    LOGGER.warn( "Probing for an active AMQP broker at the binding URL {}. This may take some time if "
                                 + "no active broker exists and retries are configured. If no active broker is "
                                 + "discovered, an embedded broker will be started.",
                                 url );

                    this.testConnection( url, factory, 0 );

                    LOGGER.info( "Discovered an active AMQP broker at {}", url );
                }
                catch ( BrokerConnectionException e )
                {
                    LOGGER.info( "Could not connect to an active AMQP broker at {}. Starting an embedded broker "
                                 + "instead.",
                                 url );

                    returnMe = this.createEmbeddedBroker( properties, connectionPropertyName, dynamicBindingAllowed );
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

    private String findConnectionPropertyName( Properties properties )
    {
        String returnMe = null;

        for ( Entry<Object, Object> nextEntry : properties.entrySet() )
        {
            Object key = nextEntry.getKey();

            if ( Objects.nonNull( key ) && key.toString().contains( "connectionFactory" ) )
            {
                Object value = nextEntry.getValue();

                if ( Objects.nonNull( value ) )
                {
                    returnMe = key.toString();
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
     * Checks for a <code>wres.eventsBrokerPort</code> and/or <code>wres.eventsBrokerAddress</code> system property and 
     * substitutes these entries into the binding url contained in the supplied properties.
     * 
     * @param connectionPropertyName the connection property name
     * @param properties the properties to update
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the connection property cannot be found
     */

    private void updateConnectionStringWithSystemPropertiesIfConfigured( String connectionPropertyName,
                                                                         Properties properties )
    {
        Objects.requireNonNull( connectionPropertyName );
        Objects.requireNonNull( properties );

        if ( !properties.containsKey( connectionPropertyName ) )
        {
            throw new IllegalArgumentException( COULD_NOT_FIND_THE_NAMED_CONNECTION_PROPERTY + connectionPropertyName
                                                + IN_THE_MAP_OF_PROPERTIES );
        }

        String oldConnectionProperty = properties.getProperty( connectionPropertyName );
        String connectionProperty = oldConnectionProperty;

        Properties systemProperties = System.getProperties();

        boolean overriden = false;

        if ( systemProperties.containsKey( "wres.eventsBrokerAddress" ) )
        {
            Object address = systemProperties.get( "wres.eventsBrokerAddress" );
            connectionProperty = connectionProperty.replaceAll( "amqp://+[a-zA-Z0-9.]+",
                                                                "amqp://" + address );
            overriden = true;
        }

        if ( systemProperties.containsKey( "wres.eventsBrokerPort" ) )
        {
            Object port = systemProperties.get( "wres.eventsBrokerPort" );

            String innerConnectionProperty = connectionProperty;

            connectionProperty = connectionProperty.replaceAll( ":(?<port>[0-9]+)", ":" + port );

            LOGGER.debug( "Updated the port in the binding URL. The old binding URL was: {}. The new binding URL is: "
                          + "{}.",
                          innerConnectionProperty,
                          connectionProperty );

            overriden = true;
        }

        // Update the properties
        properties.setProperty( connectionPropertyName, connectionProperty );

        if ( LOGGER.isDebugEnabled() && overriden )
        {
            LOGGER.debug( "Updated the binding URL (BURL) using system properties discovered at runtime. The old "
                          + "BURL was {}. The new BURL is {}.",
                          oldConnectionProperty,
                          connectionProperty );
        }
    }

    /**
     * If the connection string contains a different port than the port actually used, then update the port inline to 
     * the properties map with the relevant AMQP port from the list of broker ports for which bindings were found. 
     * 
     * @param connectionPropertyName the connection property name
     * @param properties the properties whose named value should be replaced
     * @param amqpPort the discovered amqp port
     * @throws NullPointerException if any nullable input is null
     * @throws IllegalArgumentException if the connection property cannot be found
     */

    private void updateConnectionStringWithDynamicPortIfConfigured( String connectionPropertyName,
                                                                    Properties properties,
                                                                    int amqpPort )
    {
        Objects.requireNonNull( connectionPropertyName );
        Objects.requireNonNull( properties );

        if ( !properties.containsKey( connectionPropertyName ) )
        {
            throw new IllegalArgumentException( COULD_NOT_FIND_THE_NAMED_CONNECTION_PROPERTY + connectionPropertyName
                                                + IN_THE_MAP_OF_PROPERTIES );
        }

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
     * <p>Attempts to create an embedded broker in two stages:
     * 
     * <ol>
     * <li>First, attempts to bind a broker on the configured port. If that fails, move the the second stage.</ol>
     * <li>Second, if dynamic binding is allowed, attempts to bind a broker to a broker-chosen (free) port, which may 
     * be discovered from the embedded broker instance after startup.<li>
     * </ol>
     * 
     * @param properties the connection properties, not null
     * @param connectionPropertyName the name of the connection property to obtain from the properties                                                                        
     * @param dynamicBindingAllowed is true to bind any port, false to throw an exception if the requested port is bound
     * @return an embedded broker instance
     * @throws NullPointerException if the properties are null
     */

    private EmbeddedBroker createEmbeddedBroker( Properties properties,
                                                 String connectionPropertyName,
                                                 boolean dynamicBindingAllowed )
    {
        Objects.requireNonNull( properties );
        Objects.requireNonNull( connectionPropertyName );

        if ( !properties.containsKey( connectionPropertyName ) )
        {
            throw new IllegalArgumentException( COULD_NOT_FIND_THE_NAMED_CONNECTION_PROPERTY + connectionPropertyName
                                                + IN_THE_MAP_OF_PROPERTIES );
        }

        String bindingUrl = properties.getProperty( connectionPropertyName );

        LOGGER.debug( "Attempting to extract the desired port from the binding URL {}.", bindingUrl );

        // Discover the port to which a broker should be bound
        String regex = ":(?<port>[0-9]+)";

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
            LOGGER.debug( "Unable to bind an embedded broker to the configured port of {}. Choosing another port, "
                          + "which will be available from the broker instance after startup.",
                          port );

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

            returnMe = EmbeddedBroker.of();
        }

        return returnMe;
    }

    /**
     * Tests the connection with exponential back-off, up to the prescribed number of retries. If the properties 
     * contains a binding url that configures its own retries, then these retries will nest. Thus, to delegate retries 
     * to the broker (based on the declared burl), request zero retries in this context. 
     * 
     * @param connectionUrl the connection url string
     * @param conFactory the connection factory
     * @param retries the number of retries
     * @throws BrokerConnectionException if the connection finally fails after all retries
     * @throws NullPointerException if any input is null
     */

    private void testConnection( String connectionUrl, ConnectionFactory conFactory, int retries )
    {
        Objects.requireNonNull( connectionUrl );
        Objects.requireNonNull( conFactory );

        LOGGER.debug( "Testing the broker connection with exponential back-off up to the maximum retry count of {}.",
                      retries );

        long sleepMillis = 1000;
        for ( int i = 0; i <= retries; i++ )
        {
            Connection connection = null;
            try
            {
                if ( i > 0 )
                {
                    Thread.sleep( sleepMillis );

                    // Exponential back-off
                    sleepMillis *= 2;

                    LOGGER.info( "Retrying connection to {} following {} failed connection attempts. This is retry {} "
                                 + "of {}.",
                                 connectionUrl,
                                 i,
                                 i,
                                 retries );
                }

                connection = conFactory.createConnection();

                // Success
                break;
            }
            catch ( JMSException | RuntimeException e )
            {
                if ( i == retries )
                {
                    throw new BrokerConnectionException( "Unable to connect to the broker at "
                                                         + connectionUrl
                                                         + " after "
                                                         + retries
                                                         + " retries.",
                                                         e );
                }
            }
            // Propagate immediately
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();

                throw new BrokerConnectionException( "Unable to connect to the broker at "
                                                     + connectionUrl
                                                     + ".",
                                                     e );
            }
            finally
            {
                // Close any connection that succeeded
                if ( Objects.nonNull( connection ) )
                {
                    LOGGER.info( "Established a connection to an AMQP message broker at binding URL: {}.",
                                 connectionUrl );

                    try
                    {
                        connection.close();
                    }
                    catch ( JMSException e )
                    {
                        LOGGER.error( "Failed to close an attempted connection during the instantiation of a "
                                      + "connection factory." );
                    }
                }
            }
        }
    }

    /**
     * Creates a connection factory.
     * 
     * @param context the context
     * @param connectionPropertyName the connection property name
     * @return a connection factory
     * @throws NamingException if the connection factory could not be located
     * @throws NullPointerException if any input is null
     */

    private ConnectionFactory createConnectionFactory( Context context, String connectionPropertyName )
            throws NamingException
    {
        Objects.requireNonNull( context );
        Objects.requireNonNull( connectionPropertyName );

        String factoryName = connectionPropertyName.replaceAll( "(?i)connectionfactory.", "" );

        LOGGER.debug( "Looking up a connection factory with name {}.", factoryName );

        return (ConnectionFactory) context.lookup( factoryName );
    }

    /**
     * Constructs a new instances and creates an embedded broker as necessary.
     * 
     * @param dynamicBindingAllowed is true to bind any port, false to throw an exception if the requested port is bound
     * @param jndiProperties the name of a jndi properties file on the classpath
     * @throws CouldNotLoadBrokerConfigurationException if the broker configuration could not be found
     * @throws CouldNotStartEmbeddedBrokerException if an embedded broker was requested and could not be started
     * @throws NullPointerException if the jndiProperties is null
     * @throws CouldNotLoadBrokerConfigurationException if the properties could not be read
     * @throws BrokerConnectionException if the broker was not reachable
     * @throws IllegalArgumentException if the maximumMessageRetries is less than zero
     */

    private BrokerConnectionFactory( String jndiProperties, int maximumMessageRetries, boolean dynamicBindingAllowed )
    {
        Objects.requireNonNull( jndiProperties );

        if ( maximumMessageRetries < 0 )
        {
            throw new IllegalArgumentException( "The maximum number of message retries must be greater than zero: "
                                                + maximumMessageRetries
                                                + "." );
        }

        this.maximumMessageRetries = maximumMessageRetries;

        Properties properties = new Properties();

        // Load the jndi.properties
        URL config = BrokerConnectionFactory.class.getClassLoader().getResource( jndiProperties );

        if ( Objects.isNull( config ) )
        {
            throw new CouldNotLoadBrokerConfigurationException( "Could not find the " + jndiProperties
                                                                + " file on the class path." );
        }

        this.connectionPool = new ArrayList<>();
        this.connectionPoolLock = new Object();

        // An embedded broker instance to close if the construction fails after the embedded broker is created
        EmbeddedBroker brokerToClose = null;

        // The connection property name
        String connectionPropertyName = null;

        try ( InputStream stream = config.openStream() )
        {
            properties.load( stream );

            LOGGER.debug( "Upon reading {}, discovered the following broker connection properties: {}",
                          jndiProperties,
                          properties );

            // Find the connection property
            connectionPropertyName = this.findConnectionPropertyName( properties );

            // Adjust the connection property for any system property overrides, such as broker url and port
            this.updateConnectionStringWithSystemPropertiesIfConfigured( connectionPropertyName, properties );

            this.broker = this.createEmbeddedBrokerFromPropertiesIfRequired( connectionPropertyName,
                                                                             properties,
                                                                             dynamicBindingAllowed );
            brokerToClose = this.broker;
        }
        catch ( IOException | NamingException e )
        {
            throw new CouldNotLoadBrokerConfigurationException( "Unable to load the expected broker configuration from "
                                                                + "properties file "
                                                                + jndiProperties
                                                                + " on the application "
                                                                + "classpath.",
                                                                e );
        }

        // Start the embedded broker if one exists
        // This may adjust the properties if the broker is bound to a dynamic port
        if ( Objects.nonNull( this.broker ) )
        {
            this.broker.start();

            // If the port was configured dynamically by the broker, override it here
            this.updateConnectionStringWithDynamicPortIfConfigured( connectionPropertyName,
                                                                    properties,
                                                                    this.broker.getMessagingPort() );
        }

        // Set any variables that depend on the (possibly adjusted) properties
        try
        {
            LOGGER.debug( "Creating a connection factory with these properties: {}.", properties );

            this.context = new InitialContext( properties );
            this.connectionFactory =
                    this.createConnectionFactory( this.context, connectionPropertyName );

            // Test
            String connectionString = properties.getProperty( connectionPropertyName );

            LOGGER.debug( "Testing the connection property {} with corresponding connection string {}.",
                          connectionPropertyName,
                          connectionString );

            this.testConnection( connectionString,
                                 this.connectionFactory,
                                 BrokerConnectionFactory.MAXIMUM_CONNECTION_RETRIES );

            LOGGER.info( "Created a broker connection factory {} with name {} and binding URL {}.",
                         this,
                         connectionPropertyName,
                         properties.getProperty( connectionPropertyName ) );
        }
        catch ( NamingException e )
        {
            if ( Objects.nonNull( brokerToClose ) )
            {
                try
                {
                    brokerToClose.close();
                }
                catch ( IOException f )
                {
                    LOGGER.warn( "Encountered an error while attempting to close an embedded broker.", f );
                }
            }

            throw new CouldNotLoadBrokerConfigurationException( "Unable to load the expected broker configuration from "
                                                                + "properties file "
                                                                + jndiProperties
                                                                + " on the application "
                                                                + "classpath.",
                                                                e );
        }
    }

    /**
     * A runtime exception indicating a failure to load the configuration needed to connect to a broker.
     * 
     * @author James Brown
     */

    private static class FailedToAcquireConnectionException extends RuntimeException
    {

        /**
         * Serial version identifier.
         */

        private static final long serialVersionUID = -5224784772553896250L;

        /**
         * Constructs a {@link CouldNotLoadBrokerConfigurationException} with the specified message.
         * 
         * @param message the message.
         */

        private FailedToAcquireConnectionException( final String message )
        {
            super( message );
        }

        /**
         * Constructs a {@link CouldNotLoadBrokerConfigurationException} with the specified message and cause.
         * 
         * @param message the message.
         * @param cause the cause of the exception
         */

        private FailedToAcquireConnectionException( final String message, final Throwable cause )
        {
            super( message, cause );
        }
    }

}
