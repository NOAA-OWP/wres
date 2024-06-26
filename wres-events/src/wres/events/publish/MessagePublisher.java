package wres.events.publish;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Destination;
import jakarta.jms.ExceptionListener;
import jakarta.jms.JMSException;
import jakarta.jms.JMSSecurityException;
import jakarta.jms.Message;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.events.EvaluationEventException;
import wres.events.EvaluationEventUtilities;
import wres.events.broker.BrokerConnectionFactory;

/**
 * Publishes messages to a destination that is supplied on construction. There is one {@link Connection} per instance 
 * because connections are assumed to be expensive. Currently, there is also one {@link Session} per instance, but a 
 * pool of sessions might be better (to allow better message throughput, as a session is the work thread). Overall, it 
 * may be better to abstract connections and sessions away from specific helpers.
 *
 * @author James Brown
 */

public class MessagePublisher implements Closeable
{
    /** String used repeatedly. */
    private static final String TO_DESTINATION = " to destination ";

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( MessagePublisher.class );

    /** An enumeration of message properties. */
    public enum MessageProperty
    {
        /** JMS message group ID. */
        JMSX_GROUP_ID,
        /** Message correlation ID. */
        JMS_CORRELATION_ID,
        /** Message ID. */
        JMS_MESSAGE_ID,
        /** Consumer ID. */
        CONSUMER_ID,
        /** EvaluationMessager job ID. */
        EVALUATION_JOB_ID,
        /** PNG format. */
        PNG,
        /** SVG format. */
        SVG,
        /** Protobuf format. */
        PROTOBUF,
        /** NetCDF format. */
        NETCDF,
        /** CSV format. */
        CSV,
        /** CSV2 format. */
        CSV2,
        /** Pairs format. */
        PAIRS;

        @Override
        public String toString()
        {
            return switch ( this )
            {
                case JMSX_GROUP_ID -> "JMSXGroupID";
                case JMS_CORRELATION_ID -> "JMSCorrelationID";
                case JMS_MESSAGE_ID -> "JMSMessageID";
                case CONSUMER_ID -> "ConsumerID";
                case EVALUATION_JOB_ID -> "EvaluationJobID";
                default -> super.toString();
            };
        }
    }

    /**
     * A connection to the broker.
     */

    private final Connection connection;

    /**
     * A destination to which messages should be posted.
     */

    private final Destination destination;

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

    /**
     * A session.
     */

    private Session session;

    /**
     * A message producer.
     */

    private MessageProducer producer;

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
                                    null,
                                    DeliveryMode.NON_PERSISTENT,
                                    Message.DEFAULT_PRIORITY,
                                    Message.DEFAULT_TIME_TO_LIVE );
    }


    /**
     * Creates an instance with default settings and a {@link ConnectionExceptionListener}.
     *
     * @param connectionFactory a broker connection factory
     * @param destination the delivery destination
     * @param listener a connection exception listener
     * @throws JMSException if the JMS provider fails to create the connection due to some internal error
     * @throws JMSSecurityException if client authentication fails
     * @throws NullPointerException if the connectionFactory or destination is null
     * @return an instance
     */

    public static MessagePublisher of( BrokerConnectionFactory connectionFactory,
                                       Destination destination,
                                       ExceptionListener listener )
            throws JMSException
    {
        return MessagePublisher.of( connectionFactory,
                                    destination,
                                    listener,
                                    DeliveryMode.NON_PERSISTENT,
                                    Message.DEFAULT_PRIORITY,
                                    Message.DEFAULT_TIME_TO_LIVE );
    }

    /**
     * Creates an instance.
     *
     * @param connectionFactory a broker connection factory
     * @param destination the delivery destination
     * @param listener an optional connection exception listener
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
                                       ExceptionListener listener,
                                       int deliveryMode,
                                       int messagePriority,
                                       long messageTimeToLive )
            throws JMSException
    {
        return new MessagePublisher( connectionFactory,
                                     destination,
                                     listener,
                                     deliveryMode,
                                     messagePriority,
                                     messageTimeToLive );
    }

    @Override
    public String toString()
    {
        return this.identifier;
    }

    /**
     * Starts the message publisher.
     */
    public void start()
    {
        try
        {
            // Client acknowledges messages processed
            this.session = this.connection.createSession( Session.CLIENT_ACKNOWLEDGE );
            this.producer = this.session.createProducer( this.destination );
            this.connection.start();
        }
        catch ( JMSException e )
        {
            throw new EvaluationEventException( "Failed to start message publisher.", e );
        }
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
                LOGGER.debug( "Closed connection {} in publisher {}.",
                              this.connection,
                              this );
            }
            catch ( JMSException e )
            {
                LOGGER.warn( "Encountered an error while attempting to close a connection within message "
                             + "publisher {}: {}",
                             this,
                             e.getMessage() );
            }
        }
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

        // Create an immutable copy
        Map<MessageProperty, String> immutableMeta = Collections.unmodifiableMap( metadata );

        // Still open?
        if ( this.isClosed() )
        {
            throw new IllegalArgumentException( "Message publisher "
                                                + this
                                                + " has been closed and cannot accept any further publication "
                                                + "requests." );
        }

        if ( !immutableMeta.containsKey( MessageProperty.JMS_MESSAGE_ID ) )
        {
            throw new IllegalArgumentException( "Expected a " + MessageProperty.JMS_MESSAGE_ID + "." );
        }

        if ( !immutableMeta.containsKey( MessageProperty.JMS_CORRELATION_ID ) )
        {
            throw new IllegalArgumentException( "Expected a " + MessageProperty.JMS_CORRELATION_ID + "." );
        }

        try
        {
            this.publicationLock.lock();
            this.internalPublishWithRetriesAndExponentialBackoff( messageBytes, immutableMeta );
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
        this.validateStarted();

        long sleepMillis = 1000;
        int retries = this.getRetryCount();
        String property = metadata.get( MessageProperty.JMS_MESSAGE_ID );
        for ( int i = 0; i <= retries; i++ )
        {
            // Still open?
            if ( this.isClosed() )
            {
                LOGGER.debug( "Not attempting to publish message {} because the publisher has closed.",
                              property );

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
                                  property,
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
                                  property,
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
                                                               + property
                                                               + TO_DESTINATION
                                                               + this.destination
                                                               + " after "
                                                               + retries
                                                               + " retries.",
                                                               e );
                }
                else if ( LOGGER.isDebugEnabled() )
                {
                    String message = "Failed to send message "
                                     + property
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
                Thread.currentThread()
                      .interrupt();

                throw new UnrecoverablePublisherException( "Publisher " + this
                                                           + " was interrupted while attempting to send message "
                                                           + property
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
        if ( Objects.isNull( this.session ) )
        {
            throw new IllegalStateException( "THe publisher must be started before messages are published." );
        }

        // Post
        BytesMessage message = this.session.createBytesMessage();

        // Set the message identifiers
        for ( Map.Entry<MessageProperty, String> next : metadata.entrySet() )
        {
            switch ( next.getKey() )
            {
                case JMS_MESSAGE_ID -> message.setJMSMessageID( next.getValue() );
                case JMS_CORRELATION_ID -> message.setJMSCorrelationID( next.getValue() );
                default -> message.setStringProperty( next.getKey().toString(), next.getValue() );
            }
        }

        // At least until we can write from a buffer directly
        // For example: https://qpid.apache.org/releases/qpid-proton-j-0.33.4/api/index.html
        message.writeBytes( messageBytes.array() );

        return message;
    }

    /**
     * Checks that the message publisher has been started.
     * @throws IllegalStateException if the message publisher has not been started
     */

    private void validateStarted()
    {
        if ( Objects.isNull( this.producer ) )
        {
            throw new IllegalStateException( "Cannot publish a message because the message publisher has not been "
                                             + "started." );
        }
    }

    /**
     * Hidden constructor.
     *
     * @param connectionFactory a broker connection factory
     * @param destination the delivery destination
     * @param listener an optional connection exception listener
     * @param deliveryMode the delivery mode
     * @param messagePriority the message priority
     * @param messageTimeToLive the message time to live
     * @throws JMSException if the JMS provider fails to create the connection due to some internal error
     * @throws JMSSecurityException if client authentication fails
     * @throws NullPointerException if the connectionFactory or destination is null
     */

    private MessagePublisher( BrokerConnectionFactory connectionFactory,
                              Destination destination,
                              ExceptionListener listener,
                              int deliveryMode,
                              int messagePriority,
                              long messageTimeToLive )
            throws JMSException
    {
        Objects.requireNonNull( connectionFactory );
        Objects.requireNonNull( destination );

        // Create a unique identifier for the publisher
        this.identifier = EvaluationEventUtilities.getId();

        // The connection factory is responsible for closing this. The connection owns all other resources to be closed.
        // According to the JMS specification, Connection::close will close all these resources.
        this.connection = connectionFactory.get();
        LOGGER.debug( "Created connection {} in publisher {}.", this.connection, this );

        this.retryCount = connectionFactory.getMaximumMessageRetries();

        this.destination = destination;

        // Register a listener for exceptions
        if ( Objects.nonNull( listener ) )
        {
            this.connection.setExceptionListener( listener );
        }
        else
        {
            this.connection.setExceptionListener( new ConnectionExceptionListener( this.identifier ) );
        }

        this.messagePriority = messagePriority;
        this.messageTimeToLive = messageTimeToLive;
        this.deliveryMode = deliveryMode;

        this.publicationLock = new ReentrantLock();

        this.isClosed = new AtomicBoolean();

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
         * @param clientId the client identifier
         */

        ConnectionExceptionListener( String clientId )
        {
            Objects.requireNonNull( clientId );

            this.clientId = clientId;
        }

    }
}