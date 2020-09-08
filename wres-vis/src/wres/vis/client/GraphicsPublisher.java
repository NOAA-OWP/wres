package wres.vis.client;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publishes evaluation status messages associated with the consumption of graphics. There is one {@link Connection} 
 * and one {@link Session} per instance.
 * 
 * @author james.brown@hydrosolved.com
 */

class GraphicsPublisher implements Closeable
{

    /**
     * Message property names.
     */

    enum MessageProperty
    {
        JMSX_GROUP_ID,

        JMS_CORRELATION_ID,

        JMS_MESSAGE_ID,

        CONSUMER_ID,
        
        OUTPUT_PATH;

        @Override
        public String toString()
        {
            switch ( this )
            {
                case JMSX_GROUP_ID:
                    return "JMSXGroupID";
                case JMS_CORRELATION_ID:
                    return "JMSCorrelationID";
                case JMS_MESSAGE_ID:
                    return "JMSMessageID";
                case CONSUMER_ID:
                    return "ConsumerID";
                case OUTPUT_PATH:
                    return "OutputPath";
                default:
                    throw new IllegalStateException( "Implement the string identifier for " + this );
            }
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger( GraphicsPublisher.class );

    /**
     * A connection to the broker.
     */

    private final Connection connection;

    /**
     * A session.
     */

    private final Session session;

    /**
     * A destination to which messages should be posted.
     */

    private final Destination destination;

    /**
     * A message producer.
     */

    private final MessageProducer producer;

    /**
     * Unique identifier for the producer.
     */

    private final String identifier;

    /**
     * Creates an instance with default settings.
     * 
     * @param connection a connection through which messages should be routed
     * @param destination the delivery destination
     * @param identifier the identifier for the publisher
     * @throws JMSException if the JMS provider fails to create the connection due to some internal error
     * @throws JMSSecurityException if client authentication fails
     * @throws NullPointerException if any input is null
     * @return an instance
     */

    static GraphicsPublisher of( Connection connection,
                                 Destination destination,
                                 String identifier )
            throws JMSException
    {
        return new GraphicsPublisher( connection, destination, identifier );
    }

    /**
     * Publishes a message to a destination.
     * 
     * @param messageBytes the message bytes to publish
     * @param messageId the message identifier
     * @param correlationId the correlation identifier
     * @param consumerId the consumer identifier
     * @throws JMSException - if the session fails to create a MessageProducerdue to some internal error
     * @throws NullPointerException if any input is null
     */

    void publish( ByteBuffer messageBytes, String messageId, String correlationId, String consumerId )
            throws JMSException
    {
        Objects.requireNonNull( messageBytes );
        Objects.requireNonNull( messageId );
        Objects.requireNonNull( correlationId );
        Objects.requireNonNull( consumerId );

        // Create the message and attach the properties
        BytesMessage message = this.session.createBytesMessage();
        // JMS properties
        message.setJMSMessageID( messageId );
        message.setJMSCorrelationID( correlationId );
        
        // Application properties
        message.setStringProperty( MessageProperty.CONSUMER_ID.toString(), consumerId );
        
        // At least until we can write from a buffer directly
        // For example: https://qpid.apache.org/releases/qpid-proton-j-0.33.4/api/index.html
        message.writeBytes( messageBytes.array() );

        // Send the message
        this.producer.send( message,
                            DeliveryMode.NON_PERSISTENT,
                            Message.DEFAULT_PRIORITY,
                            Message.DEFAULT_TIME_TO_LIVE );

        // Log the message
        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "From publisher {}, sent a message of {} bytes with messageId {} and correlationId {} to "
                          + "destination {}.",
                          this,
                          messageBytes.limit(),
                          messageId,
                          correlationId,
                          this.destination );
        }
    }

    @Override
    public void close() throws IOException
    {
        LOGGER.debug( "Closing message publisher {}.", this );

        try
        {
            this.producer.close();
        }
        catch ( JMSException e )
        {
            LOGGER.error( "Encountered an error while attempting to close a message publisher within client {}: {}",
                          this,
                          e.getMessage() );
        }

        try
        {
            this.session.close();
        }
        catch ( JMSException e )
        {
            LOGGER.error( "Encountered an error while attempting to close a broker session within client {}: {}",
                          this,
                          e.getMessage() );
        }

        // Do not close connection as it may be re-used.
    }

    @Override
    public String toString()
    {
        return this.identifier;
    }

    /**
     * Hidden constructor.
     * 
     * @param connection a connection through which messages should be routed
     * @param destination the delivery destination
     * @param identifier the identifier
     * @throws JMSException if the JMS provider fails to create the connection due to some internal error
     * @throws JMSSecurityException if client authentication fails
     * @throws NullPointerException if the connectionFactory or destination is null
     */

    private GraphicsPublisher( Connection connection,
                               Destination destination,
                               String identifier )
            throws JMSException
    {
        Objects.requireNonNull( connection );
        Objects.requireNonNull( destination );
        Objects.requireNonNull( identifier );

        this.connection = connection;
        this.destination = destination;
        this.identifier = identifier;

        // Register a listener for exceptions
        this.connection.setExceptionListener( new ConnectionExceptionListener( this.identifier ) );

        // Client acknowledges messages processed
        this.session = this.connection.createSession( false, Session.CLIENT_ACKNOWLEDGE );
        this.producer = this.session.createProducer( this.destination );

        this.connection.start();

        LOGGER.debug( "Created messager publisher {}, which is ready to receive messages to publish.", this );
    }

    /**
     * Listen for failures on a connection.
     */

    private static class ConnectionExceptionListener implements ExceptionListener
    {
        private static final Logger LOGGER = LoggerFactory.getLogger( ConnectionExceptionListener.class );

        /**
         * The client that encountered the exception.
         */

        private final String clientId;

        @Override
        public void onException( JMSException exception )
        {
            // Could consider promoting to WARN or ERROR. See #80267-109 for an example of the type of exception that 
            // might appear here. Could also rethrow, but that cannot be done until the embedded broker exits cleanly as
            // described in #80267-109.
            LOGGER.debug( "An exception listener uncovered an error in client {}. {}",
                          this.clientId,
                          exception.getMessage() );
        }

        /**
         * Creates an instance with an evaluation identifier and a message client identifier.
         * 
         * @param evaluationId the evaluation identifier
         * @param clientId the client identifier
         */

        ConnectionExceptionListener( String clientId )
        {
            Objects.requireNonNull( clientId );

            this.clientId = clientId;
        }

    }


}
