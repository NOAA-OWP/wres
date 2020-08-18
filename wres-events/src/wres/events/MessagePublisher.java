package wres.events;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publishes messages to a destination that is supplied on construction. There is one {@link Connection} per instance 
 * because connections are assumed to be expensive. Currently, there is also one {@link Session} per instance, but a 
 * pool of sessions might be better (to allow better message throughput, as a session is the work thread). Overall, it 
 * may be better to abstract connections and sessions away from specific helpers.
 * 
 * @author james.brown@hydrosolved.com
 */

class MessagePublisher implements Closeable
{

    private static final Logger LOGGER = LoggerFactory.getLogger( MessagePublisher.class );

    enum MessageProperty
    {

        JMSX_GROUP_ID,

        JMS_CORRELATION_ID,

        JMS_MESSAGE_ID,

        CONSUMER_ID;

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
                default:
                    throw new IllegalStateException( "Implement the string identifier for " + this );
            }
        }
    }

    /**
     * A connection to the broker.
     */

    private final Connection connection;

    /**
     * A session. TODO: this should probably be a pool of sessions.
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
     * The delivery mode.
     */

    private final int deliveryMode;

    /**
     * The message priority. See {@link Message}.
     */

    private final int messagePriority;

    /**
     * The message time to live.
     */

    private final long messageTimeToLive;

    /**
     * The unique identifier of this consumer.
     */

    private final String identifier;

    /**
     * Number of messages published so far to this publisher.
     */

    private int messageCount = 0;

    @Override
    public String toString()
    {
        return this.getIdentifier();
    }

    String getIdentifier()
    {
        return this.identifier;
    }

    /**
     * Creates an instance with default settings.
     * 
     * @param connection a connection through which messages should be routed
     * @param destination the delivery destination
     * @throws JMSException if the JMS provider fails to create the connection due to some internal error
     * @throws JMSSecurityException if client authentication fails
     * @throws NullPointerException if the connectionFactory or destination is null
     * @return an instance
     */

    static MessagePublisher of( Connection connection,
                                Destination destination )
            throws JMSException
    {
        return MessagePublisher.of( connection,
                                    destination,
                                    DeliveryMode.NON_PERSISTENT,
                                    Message.DEFAULT_PRIORITY,
                                    Message.DEFAULT_TIME_TO_LIVE );
    }

    /**
     * Creates an instance.
     * 
     * @param connection a connection through which messages should be routed
     * @param destination the delivery destination
     * @param deliveryMode the delivery mode
     * @param messagePriority the message priority
     * @param messageTimeToLive the message time to live
     * @throws JMSException if the JMS provider fails to create the connection due to some internal error
     * @throws JMSSecurityException if client authentication fails
     * @throws NullPointerException if the connectionFactory or destination is null
     * @return an instance
     */

    static MessagePublisher of( Connection connection,
                                Destination destination,
                                int deliveryMode,
                                int messagePriority,
                                long messageTimeToLive )
            throws JMSException
    {
        return new MessagePublisher( connection, destination, deliveryMode, messagePriority, messageTimeToLive );
    }

    /**
     * Publishes a message to a destination.
     * 
     * @param messageBytes the message bytes to publish
     * @param metadata the message metadata, minimally including a message identifier and correlation identifier
     * @throws JMSException - if the session fails to create a MessageProducerdue to some internal error
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if expected input is missing
     */

    void publish( ByteBuffer messageBytes, Map<MessageProperty, String> metadata ) throws JMSException
    {
        Objects.requireNonNull( messageBytes );
        Objects.requireNonNull( metadata );

        if ( !metadata.containsKey( MessageProperty.JMS_MESSAGE_ID ) )
        {
            throw new IllegalArgumentException( "Expected a " + MessageProperty.JMS_MESSAGE_ID + "." );
        }

        if ( !metadata.containsKey( MessageProperty.JMS_CORRELATION_ID ) )
        {
            throw new IllegalArgumentException( "Expected a " + MessageProperty.JMS_CORRELATION_ID + "." );
        }

        // Post
        BytesMessage message = this.session.createBytesMessage();

        // Set the message identifiers
        for ( Map.Entry<MessageProperty, String> next : metadata.entrySet() )
        {
            switch ( next.getKey() )
            {
                case JMS_MESSAGE_ID:
                    message.setJMSMessageID( next.getValue() );
                    break;
                case JMS_CORRELATION_ID:
                    message.setJMSCorrelationID( next.getValue() );
                    break;
                default:
                    message.setStringProperty( next.getKey().toString(), next.getValue() );
            }
        }

        // At least until we can write from a buffer directly
        // For example: https://qpid.apache.org/releases/qpid-proton-j-0.33.4/api/index.html
        message.writeBytes( messageBytes.array() );

        // Send the message
        this.producer.send( message,
                            this.deliveryMode,
                            this.messagePriority,
                            this.messageTimeToLive );

        // Log the message
        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "From publisher {}, sent a message of {} bytes with message properties {} to destination {}.",
                          this,
                          messageBytes.limit(),
                          metadata,
                          this.destination );
        }

        this.messageCount++;
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

    /**
     * @return the number of messages published so far.
     */

    int getMessageCount()
    {
        return this.messageCount;
    }

    /**
     * Hidden constructor.
     * 
     * @param connection a connection through which messages should be routed
     * @param destination the delivery destination
     * @param deliveryMode the delivery mode
     * @param messagePriority the message priority
     * @param messageTimeToLive the message time to live
     * @throws JMSException if the JMS provider fails to create the connection due to some internal error
     * @throws JMSSecurityException if client authentication fails
     * @throws NullPointerException if the connectionFactory or destination is null
     */

    private MessagePublisher( Connection connection,
                              Destination destination,
                              int deliveryMode,
                              int messagePriority,
                              long messageTimeToLive )
            throws JMSException
    {
        Objects.requireNonNull( connection );
        Objects.requireNonNull( destination );

        // Create a unique identifier for the publisher
        this.identifier = Evaluation.getUniqueId();

        this.connection = connection;
        this.destination = destination;

        // Register a listener for exceptions
        this.connection.setExceptionListener( new ConnectionExceptionListener( this.identifier ) );

        // Client acknowledges messages processed
        this.session = this.connection.createSession( false, Session.CLIENT_ACKNOWLEDGE );

        this.messagePriority = messagePriority;
        this.messageTimeToLive = messageTimeToLive;
        this.deliveryMode = deliveryMode;

        this.producer = this.session.createProducer( this.destination );

        this.connection.start();

        LOGGER.debug( "Created a messager publisher, {}, which is ready to receive messages to publish to destination "
                      + "{}. The messager publisher is configured with delivery mode {}, message priority {} and "
                      + "message time-to-live {}.",
                      this,
                      this.destination,
                      this.deliveryMode,
                      this.messagePriority,
                      this.messageTimeToLive );
    }

}
