package wres.events;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
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
 * Publishes messages to a destination that is supplied on construction. There is one {@link Connection} per instance 
 * because connections are assumed to be expensive. Currently, there is also one {@link Session} per instance, but a 
 * pool of sessions might be better (to allow better message throughput, as a session is the work thread). Overall, it 
 * may be better to abstract connections and sessions away from specific helpers.
 * 
 * @author james.brown@hydrosolved.com
 */

public class MessagePublisher implements Closeable
{

    private static final Logger LOGGER = LoggerFactory.getLogger( MessagePublisher.class );

    /**
     * A connection to the broker.
     */

    private final Connection connection;

    /**
     * A session. TODO: this should probably be a pool of sessions.
     */

    private final Session session;

    /**
     * A topic to which messages should be posted.
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
     * Creates an instance with default settings.
     * 
     * @param connectionFactory the source of connections to a broker
     * @param destination the delivery destination
     * @throws JMSException if the JMS provider fails to create the connection due to some internal error
     * @throws JMSSecurityException if client authentication fails
     * @throws NullPointerException if the connectionFactory or destination is null
     * @return an instance
     */

    public static MessagePublisher of( ConnectionFactory connectionFactory,
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
     * @param connectionFactory the source of connections to a broker
     * @param destination the delivery destination
     * @param deliveryMode the delivery mode
     * @param messagePriority the message priority
     * @param messageTimeToLive the message time to live
     * @throws JMSException if the JMS provider fails to create the connection due to some internal error
     * @throws JMSSecurityException if client authentication fails
     * @throws NullPointerException if the connectionFactory or destination is null
     * @return an instance
     */

    public static MessagePublisher of( ConnectionFactory connectionFactory,
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
     * @param messageId the message identifier
     * @param correlationId an identifier to correlate statistics messages to an evaluation
     * @throws JMSException - if the session fails to create a MessageProducerdue to some internal error
     * @throws NullPointerException if any input is null
     */

    public void publish( ByteBuffer messageBytes, String messageId, String correlationId ) throws JMSException
    {
        Objects.requireNonNull( messageBytes );
        Objects.requireNonNull( messageId );
        Objects.requireNonNull( correlationId );

        // Post
        try ( MessageProducer messageProducer = this.session.createProducer( this.destination ); )
        {
            BytesMessage message = this.session.createBytesMessage();

            // Set the message identifiers
            message.setJMSMessageID( messageId );
            message.setJMSCorrelationID( correlationId );

            // At least until we can write from a buffer directly
            // For example: https://qpid.apache.org/releases/qpid-proton-j-0.33.4/api/index.html
            message.writeBytes( messageBytes.array() );

            // Send the message
            messageProducer.send( message,
                                  this.deliveryMode,
                                  this.messagePriority,
                                  this.messageTimeToLive );

            // Log the message
            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "From messager {}, sent a message of {} bytes with messageId {} and correlationId {} to "
                              + "destination {}.",
                              this,
                              messageBytes.limit(),
                              messageId,
                              correlationId,
                              this.destination );
            }
        }
    }

    @Override
    public void close() throws IOException
    {

        LOGGER.debug( "Closing the statistics messager, {}.", this );

        try
        {
            this.connection.close();
        }
        catch ( JMSException e )
        {
            throw new IOException( "Encountered an error while attempting to close a broker connection.", e );
        }

        try
        {
            this.session.close();
        }
        catch ( JMSException e )
        {
            throw new IOException( "Encountered an error while attempting to close a broker session.", e );
        }
    }

    /**
     * Listen for failures on a connection.
     */

    private static class EvaluationEventExceptionListener implements ExceptionListener
    {

        @Override
        public void onException( JMSException exception )
        {
            throw new EvaluationEventException( "Encountered an error while attempting to post an evaluation "
                                                + "message.",
                                                exception );
        }
    }

    /**
     * Hidden constructor.
     * 
     * @param connectionFactory the source of connections to a broker
     * @param destination the delivery destination
     * @param deliveryMode the delivery mode
     * @param messagePriority the message priority
     * @param messageTimeToLive the message time to live
     * @throws JMSException if the JMS provider fails to create the connection due to some internal error
     * @throws JMSSecurityException if client authentication fails
     * @throws NullPointerException if the connectionFactory or destination is null
     */

    private MessagePublisher( ConnectionFactory connectionFactory,
                              Destination destination,
                              int deliveryMode,
                              int messagePriority,
                              long messageTimeToLive )
            throws JMSException
    {
        Objects.requireNonNull( connectionFactory );
        Objects.requireNonNull( destination );

        this.connection = connectionFactory.createConnection();
        this.destination = destination;

        // Register a listener for exceptions
        this.connection.setExceptionListener( new EvaluationEventExceptionListener() );

        // Qpid broker requires this two-arg method sig to create a session, despite the advice to use a different one.
        // Auto-ack the messages
        this.session = connection.createSession( false, Session.AUTO_ACKNOWLEDGE );

        this.messagePriority = messagePriority;
        this.messageTimeToLive = messageTimeToLive;
        this.deliveryMode = deliveryMode;

        LOGGER.debug( "Created a messager publisher, {}, which is ready to receive messages to publish. "
                      + "The messager is configured with the following properties.",
                      this );
    }

}
