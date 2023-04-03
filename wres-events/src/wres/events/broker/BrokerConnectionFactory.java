package wres.events.broker;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serial;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Supplier;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jcip.annotations.GuardedBy;

/**
 * <p>Manages connections to an AMQP broker. The basic configuration is contained in a JNDI properties file on the 
 * classpath.
 * 
 * @author James Brown
 */

public class BrokerConnectionFactory implements Closeable, Supplier<Connection>
{
    /**
     * Default jndi properties file on the classpath.
     */

    public static final String DEFAULT_PROPERTIES = "eventbroker.properties";

    /**
     * Re-used string.
     */

    private static final String UNABLE_TO_CONNECT_TO_THE_BROKER_AT = "Unable to connect to the broker at ";

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
     * The maximum number of times a message can be resent on failure.
     */

    private final Integer maximumMessageRetries;

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
     * A lock for the {@link #connectionPool}.
     */

    private final Object connectionPoolLock;

    /**
     * Is {@code true} if the broker has been closed, otherwise {@code false}.
     */

    private boolean isClosed;

    /**
     * <p>Returns an instance of a factory, which is created with supplied properties and a default number of message 
     * retries, {@link #DEFAULT_MAXIMUM_MESSAGE_RETRIES}.
     * 
     * @param properties the broker connection properties, cannot be null
     * @return an instance
     * @throws CouldNotLoadBrokerConfigurationException if the broker configuration could not be found
     * @throws IllegalArgumentException if the maximumMessageRetries is less than zero
     */

    public static BrokerConnectionFactory of( Properties properties )
    {
        return new BrokerConnectionFactory( properties,
                                            BrokerConnectionFactory.DEFAULT_MAXIMUM_MESSAGE_RETRIES );
    }

    /**
     * <p>Returns an instance of a factory, which is created with supplied properties and a maximum number of 
     * message retries.
     * 
     * @param properties the broker connection properties, cannot be null
     * @param maximumMessageRetries the maximum number of message retries, must be greater than or equal to zero
     * @return an instance
     * @throws CouldNotLoadBrokerConfigurationException if the broker configuration could not be found
     * @throws IllegalArgumentException if the maximumMessageRetries is less than zero
     */

    public static BrokerConnectionFactory of( Properties properties,
                                              int maximumMessageRetries )
    {
        return new BrokerConnectionFactory( properties,
                                            maximumMessageRetries );
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
            for ( Connection connectionToClose : this.connectionPool )
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
     * Tests the connection with exponential back-off, up to the prescribed number of retries. If the properties 
     * contain a binding url that configures its own retries, then these retries will nest. Thus, to delegate retries
     * to the broker (based on the declared burl), request zero retries in this context. 
     * 
     * @param properties the connection properties
     * @param retries the number of retries
     * @throws BrokerConnectionException if the connection fails, possibly after retries
     * @throws NullPointerException if any input is null
     */

    static void testConnection( Properties properties, int retries )
    {
        Objects.requireNonNull( properties );

        String connectionPropertyName = BrokerUtilities.getConnectionPropertyName( properties );
        BrokerUtilities.testConnectionProperty( connectionPropertyName, properties );
        String connectionUrl = properties.getProperty( connectionPropertyName );

        Context localContext = BrokerConnectionFactory.getContextFromProperties( connectionUrl, properties );
        ConnectionFactory connFactory = BrokerConnectionFactory.getConnectionFactory( connectionUrl,
                                                                                      localContext,
                                                                                      connectionPropertyName );

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

                connection = connFactory.createConnection();

                // Success
                break;
            }
            catch ( JMSException | RuntimeException e )
            {
                if ( i == retries )
                {
                    throw new BrokerConnectionException( UNABLE_TO_CONNECT_TO_THE_BROKER_AT
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

                throw new BrokerConnectionException( UNABLE_TO_CONNECT_TO_THE_BROKER_AT
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
     * Returns the context from the properties.
     * @param connectionUrl the connection string to help with exception messaging
     * @param properties the properties
     * @return the context
     */

    private static Context getContextFromProperties( String connectionUrl, Properties properties )
    {
        Objects.requireNonNull( properties );

        try
        {
            return new InitialContext( properties );
        }
        catch ( NamingException e )
        {
            throw new BrokerConnectionException( UNABLE_TO_CONNECT_TO_THE_BROKER_AT
                                                 + connectionUrl
                                                 + ".",
                                                 e );
        }
    }

    /**
     * Gets a connection factory from the supplied context.
     * 
     * @param connectionUrl the connection string
     * @param context the context
     * @param connectionPropertyName the connection property name
     * @return a connection factory
     * @throws NullPointerException if any input is null
     */

    private static ConnectionFactory getConnectionFactory( String connectionUrl,
                                                           Context context,
                                                           String connectionPropertyName )
    {
        Objects.requireNonNull( context );
        Objects.requireNonNull( connectionPropertyName );

        String factoryName = connectionPropertyName.replaceAll( "(?i)connectionfactory.", "" );

        LOGGER.debug( "Looking up a connection factory with name {}.", factoryName );

        try
        {
            return (ConnectionFactory) context.lookup( factoryName );
        }
        catch ( NamingException e )
        {
            throw new BrokerConnectionException( UNABLE_TO_CONNECT_TO_THE_BROKER_AT
                                                 + connectionUrl
                                                 + ".",
                                                 e );
        }
    }

    /**
     * Constructs a new instances and creates an embedded broker as necessary.
     * 
     * @param properties the broker connection properties, cannot be null
     * @param maximumMessageRetries the maximum number of message retries
     * @throws CouldNotLoadBrokerConfigurationException if the broker configuration could not be found
     * @throws NullPointerException if the properties is null
     * @throws CouldNotLoadBrokerConfigurationException if the properties could not be read
     * @throws BrokerConnectionException if the broker was not reachable
     * @throws IllegalArgumentException if the maximumMessageRetries is less than zero
     */

    private BrokerConnectionFactory( Properties properties,
                                     int maximumMessageRetries )
    {
        if ( maximumMessageRetries < 0 )
        {
            throw new IllegalArgumentException( "The maximum number of message retries must be greater than zero: "
                                                + maximumMessageRetries
                                                + "." );
        }

        this.maximumMessageRetries = maximumMessageRetries;
        this.connectionPool = new ArrayList<>();
        this.connectionPoolLock = new Object();

        // The connection property name
        String connectionPropertyName = BrokerUtilities.getConnectionPropertyName( properties );

        // Set any variables that depend on the (possibly adjusted) properties
        try
        {
            LOGGER.debug( "Creating a connection factory with these properties: {}.", properties );

            // Test
            String connectionString = properties.getProperty( connectionPropertyName );
            this.context = new InitialContext( properties );
            this.connectionFactory = BrokerConnectionFactory.getConnectionFactory( connectionString,
                                                                                   this.context,
                                                                                   connectionPropertyName );

            LOGGER.debug( "Testing the connection property {} with corresponding connection string {}.",
                          connectionPropertyName,
                          connectionString );

            BrokerConnectionFactory.testConnection( properties,
                                                    BrokerConnectionFactory.MAXIMUM_CONNECTION_RETRIES );

            LOGGER.info( "Created a broker connection factory {} with name {} and binding URL {}.",
                         this,
                         connectionPropertyName,
                         properties.getProperty( connectionPropertyName ) );
        }
        catch ( NamingException e )
        {
            throw new CouldNotLoadBrokerConfigurationException( "Unable to load the expected broker configuration.",
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

        @Serial
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
