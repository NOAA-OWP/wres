package wres.events.publish;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

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

import wres.events.Evaluation;
import wres.events.EvaluationEventException;
import wres.eventsbroker.BrokerConnectionFactory;

/**
 * Publishes messages to a destination that is supplied on construction. There is one {@link Connection} per instance 
 * because connections are assumed to be expensive. Currently, there is also one {@link Session} per instance, but a 
 * pool of sessions might be better (to allow better message throughput, as a session is the work thread). Overall, it 
 * may be better to abstract connections and sessions away from specific helpers.
 * 
 * @author james.brown@hydrosolved.com
 */

public class MessagePublisher implements Closeable
{
    private static final String TO_DESTINATION = " to destination ";

    private static final Logger LOGGER = LoggerFactory.getLogger( MessagePublisher.class );

    /**
     * An enumeration of message properties.
     */

    public enum MessageProperty
    {

        JMSX_GROUP_ID,

        JMS_CORRELATION_ID,

        JMS_MESSAGE_ID,

        CONSUMER_ID,

        EVALUATION_JOB_ID,

        PNG,

        SVG,

        PROTOBUF,

        NETCDF,

        CSV,

        PAIRS;

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
                case EVALUATION_JOB_ID:
                    return "EvaluationJobID";
                case PNG:
                    return "PNG";
                case SVG:
                    return "SVG";
                case PROTOBUF:
                    return "PROTOBUF";
                case NETCDF:
                    return "NETCDF";
                case CSV:
                    return "CSV";
                case PAIRS:
                    return "PAIRS";
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
     * The unique identifier of this publisher.
     */

    private final String identifier;

    /**
     * The number of retries that should be attempted before a failure propagates upwards.
     */

    private final int retryCount;

    /**
     * A lock to guard publication.
     */

    private final ReentrantLock publicationLock;

    /**
     * Indicates whether the publisher has been closed.
     */

    private final AtomicBoolean isClosed;

    @Override
    public String toString()
    {
        return this.identifier;
    }
    
    @Override
    public void close() throws IOException
    {
        if ( !this.isClosed.getAndSet( true ) )
        {
            LOGGER.debug( "Closing message publisher {}.", this );

            try
            {
                // No need to close session etc.
                this.connection.close();
            }
            catch ( JMSException e )
            {
                LOGGER.error( "Encountered an error while attempting to close a connection within message "
                              + "publisher {}: {}",
                              this,
                              e.getMessage() );
            }
        }
    }

    /**
     * Creates an instance with default settings.
     * 
     * @param connectionFactory a broker connection factory
     * @param destination the delivery destination
     * @throws JMSException if the JMS provider fails to create the connection due to some internal error
     * @throws JMSSecurityException if client authentication fails
     * @throws NullPointerException if the connectionFactory or destination is null
     * @return an instance
     */

    public static MessagePublisher of( BrokerConnectionFactory connectionFactory,
                                       Destination destination )
            throws JMSException
    {
        return MessagePublisher.of( connectionFactory,
                                    destination,
                                    DeliveryMode.NON_PERSISTENT,
                                    Message.DEFAULT_PRIORITY,
                                    Message.DEFAULT_TIME_TO_LIVE );
    }

    /**
     * Creates an instance.
     * 
     * @param connectionFactory a broker connection factory
     * @param destination the delivery destination
     * @param deliveryMode the delivery mode
     * @param messagePriority the message priority
     * @param messageTimeToLive the message time to live
     * @throws JMSException if the JMS provider fails to create the connection due to some internal error
     * @throws JMSSecurityException if client authentication fails
     * @throws NullPointerException if the connectionFactory or destination is null
     * @return an instance
     */

    public static MessagePublisher of( BrokerConnectionFactory connectionFactory,
                                       Destination destination,
                                       int deliveryMode,
                                       int messagePriority,
                                       long messageTimeToLive )
            throws JMSException
    {
        return new MessagePublisher( connectionFactory, destination, deliveryMode, messagePriority, messageTimeToLive );
    }

    /**
     * Publishes a message to a destination.
     * 
     * @param messageBytes the message bytes to publish
     * @param metadata the message metadata, minimally including a message identifier and correlation identifier
     * @throws EvaluationEventException - if the session fails to create or publish a message due to some internal error
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if expected input is missing
     */

    public void publish( ByteBuffer messageBytes, Map<MessageProperty, String> metadata )
    {
        Objects.requireNonNull( messageBytes );
        Objects.requireNonNull( metadata );

        // Still open?
        if ( this.isClosed() )
        {
            throw new IllegalArgumentException( "Message publisher " + this
                                                + " has been closed and cannot accept any further publication "
                                                + "requests." );
        }

        if ( !metadata.containsKey( MessageProperty.JMS_MESSAGE_ID ) )
        {
            throw new IllegalArgumentException( "Expected a " + MessageProperty.JMS_MESSAGE_ID + "." );
        }

        if ( !metadata.containsKey( MessageProperty.JMS_CORRELATION_ID ) )
        {
            throw new IllegalArgumentException( "Expected a " + MessageProperty.JMS_CORRELATION_ID + "." );
        }

        try
        {
            this.publicationLock.lock();
            this.internalPublishWithRetriesAndExponentialBackoff( messageBytes, metadata );
        }
        catch ( UnrecoverablePublisherException e )
        {
            throw new EvaluationEventException( "Failed to publish a message.", e );
        }
        finally
        {
            this.publicationLock.unlock();
        }
    }

    /**
     * @return the number of retries allowed when publishing a message.
     */

    private int getRetryCount()
    {
        return this.retryCount;
    }

    /**
     * @return true if this publisher has been closed, otherwise false.
     */

    private boolean isClosed()
    {
        return this.isClosed.get();
    }

    /**
     * Attempts to publish a message, up to the number of {@link #getRetryCount()}, using exponential back-off on each 
     * failed retry.
     * 
     * @param messageBytes the message bytes
     * @param metadata the message metadata
     * @throws UnrecoverablePublisherException if the publication fails after all retries
     */

    private void internalPublishWithRetriesAndExponentialBackoff( ByteBuffer messageBytes,
                                                                  Map<MessageProperty, String> metadata )
    {
        long sleepMillis = 1000;
        int retries = this.getRetryCount();
        for ( int i = 0; i <= retries; i++ )
        {
            // Still open?
            if( this.isClosed() )
            {
                LOGGER.debug( "Not attempting to publish message {} because the publisher has closed.",
                              metadata.get( MessageProperty.JMS_MESSAGE_ID ) );
                
                return;
            }
            
            try
            {
                if ( i > 0 )
                {
                    Thread.sleep( sleepMillis );

                    // Exponential back-off
                    sleepMillis *= 2;

                    LOGGER.debug( "Retrying the publication of message {}. This is retry {} of {}.",
                                  metadata.get( MessageProperty.JMS_MESSAGE_ID ),
                                  i,
                                  retries );
                }

                BytesMessage message = this.createMessage( messageBytes, metadata );

                // Send the message
                this.producer.send( message,
                                    this.deliveryMode,
                                    this.messagePriority,
                                    this.messageTimeToLive );

                // Log the message
                if ( LOGGER.isTraceEnabled() )
                {
                    LOGGER.trace( "From publisher {}, sent message {} with message properties {} to "
                                  + "destination {}.",
                                  this,
                                  metadata.get( MessageProperty.JMS_MESSAGE_ID ),
                                  metadata,
                                  this.destination );
                }

                // Success
                break;
            }
            catch ( JMSException e )
            {
                // Propagate if all retries exhausted
                if ( i == retries )
                {
                    throw new UnrecoverablePublisherException( "Publisher " + this
                                                               + " failed to send message "
                                                               + metadata.get( MessageProperty.JMS_MESSAGE_ID )
                                                               + TO_DESTINATION
                                                               + this.destination
                                                               + " after "
                                                               + retries
                                                               + " retries.",
                                                               e );
                }
                else if ( LOGGER.isDebugEnabled() )
                {
                    String message = "Failed to send message " + metadata.get( MessageProperty.JMS_MESSAGE_ID )
                                     + " with message properties "
                                     + metadata
                                     + TO_DESTINATION
                                     + this.destination
                                     + ".";

                    LOGGER.debug( message, e );
                }
            }
            // Propagate immediately
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();

                throw new UnrecoverablePublisherException( "Publisher " + this
                                                           + " was interrupted while attempting to send message "
                                                           + metadata.get( MessageProperty.JMS_MESSAGE_ID )
                                                           + TO_DESTINATION
                                                           + this.destination
                                                           + ".",
                                                           e );
            }
        }
    }

    /**
     * Creates a {@link BytesMessage} for publication from message bytes and associated metadata.
     * 
     * @param messageBytes the message bytes
     * @param metadata the metadata
     * @return the message
     * @throws JMSException if the message could not be created for any reason
     */

    private BytesMessage createMessage( ByteBuffer messageBytes, Map<MessageProperty, String> metadata )
            throws JMSException
    {
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

        return message;
    }

    /**
     * Hidden constructor.
     * 
     * @param connectionFactory a broker connection factory
     * @param destination the delivery destination
     * @param deliveryMode the delivery mode
     * @param messagePriority the message priority
     * @param messageTimeToLive the message time to live
     * @throws JMSException if the JMS provider fails to create the connection due to some internal error
     * @throws JMSSecurityException if client authentication fails
     * @throws NullPointerException if the connectionFactory or destination is null
     */

    private MessagePublisher( BrokerConnectionFactory connectionFactory,
                              Destination destination,
                              int deliveryMode,
                              int messagePriority,
                              long messageTimeToLive )
            throws JMSException
    {
        Objects.requireNonNull( connectionFactory );
        Objects.requireNonNull( destination );

        // Create a unique identifier for the publisher
        this.identifier = Evaluation.getUniqueId();

        this.connection = connectionFactory.get()
                                           .createConnection();

        this.retryCount = connectionFactory.getMaximumMessageRetries();

        this.destination = destination;

        // Register a listener for exceptions
        this.connection.setExceptionListener( new ConnectionExceptionListener( this.identifier ) );

        // Client acknowledges messages processed
        this.session = this.connection.createSession( false, Session.CLIENT_ACKNOWLEDGE );

        this.messagePriority = messagePriority;
        this.messageTimeToLive = messageTimeToLive;
        this.deliveryMode = deliveryMode;

        this.publicationLock = new ReentrantLock();

        this.producer = this.session.createProducer( this.destination );
        this.isClosed = new AtomicBoolean();

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

    /**
     * Listen for failures on a connection.
     */

    static class ConnectionExceptionListener implements ExceptionListener
    {
        private static final Logger LOGGER = LoggerFactory.getLogger( ConnectionExceptionListener.class );

        /**
         * The client that encountered the exception.
         */

        private final String clientId;

        @Override
        public void onException( JMSException exception )
        {
            // See #80267-109 for an example of the type of exception that might appear here.
            String message = "Encountered an error on a connection owned by messaging client "
                             + clientId
                             + ". If a failover policy was configured on the connection factory (e.g., connection "
                             + "retries), then that policy was exhausted before this error was thrown. As such, the "
                             + "error is not recoverable.";

            LOGGER.debug( message, exception );
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