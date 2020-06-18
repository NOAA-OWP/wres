package wres.events;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Phaser;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registers a subscriber to a destination that is supplied on construction. There is one {@link Connection} per instance 
 * because connections are assumed to be expensive. Currently, there is also one {@link Session} per instance, but a 
 * pool of sessions might be better (to allow better message throughput, as a session is the work thread). Overall, it 
 * may be better to abstract connections and sessions away from specific helpers.
 * 
 * @author james.brown@hydrosolved.com
 */

class MessageSubscriber implements Closeable
{

    private static final Logger LOGGER = LoggerFactory.getLogger( MessageSubscriber.class );

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
     * A list of consumers.
     */

    private final List<MessageConsumer> consumers;

    /**
     * A countdown latch to count down consumption from an expected number of messages across N consumers.
     */

    private final Phaser phaser;

    /**
     * The message on which consumption failed, null if no failure.
     */

    private Message failure = null;

    /**
     * Returns the consumption status. Consumption is in progress until the latch reaches zero.
     * 
     * @return the consumption status
     */
    
    Phaser getStatus()
    {
        return this.phaser;
    }

    /**
     * Returns a message on which consumption failed, <code>null</code> if no failure occurred.
     * 
     * @return a message that was not consumed
     */

    Message getFailedOn()
    {
        return this.failure;
    }

    /**
     * Used to advance, by one, the expected number of messages to consume for each consumer attached to this 
     * subscriber. This provides a hint to the application to wait before closing subscriber resources.
     * 
     * See {@link #getStatus()} for the current status.
     */
    
    void advanceCountToAwaitOnClose()
    {
        this.phaser.bulkRegister( this.consumers.size() ); // Register one phase for each consumer    
    }
    
    /**
     * Builds an evaluation.
     * 
     * @author james.brown@hydrosolved.com
     */

    static class Builder<T>
    {
        /**
         * Broker connections.
         */

        private ConnectionFactory connectionFactory;

        /**
         * Destination.
         */

        private Destination destination;

        /**
         * List of subscriptions to evaluation events.
         */

        private List<Consumer<T>> subscribers = new ArrayList<>();

        /**
         * List of subscriptions to statistics events.
         */

        private Function<ByteBuffer, T> mapper;

        /**
         * An evaluation identifier.
         */

        private String evaluationId;

        /**
         * Sets the connection factory.
         * 
         * @param connectionFactory the connection factory
         * @return this builder
         */

        Builder<T> setConnectionFactory( ConnectionFactory connectionFactory )
        {
            this.connectionFactory = connectionFactory;

            return this;
        }

        /**
         * Sets the destination.
         * 
         * @param destination the destination
         * @return this builder
         */

        Builder<T> setDestination( Destination destination )
        {
            this.destination = destination;

            return this;
        }

        /**
         * Adds a list of subscribers.
         * 
         * @param subscribers the subscribers
         * @return this builder
         * @throws NullPointerException if the list is null
         */

        Builder<T> addSubscribers( List<Consumer<T>> subscribers )
        {
            Objects.requireNonNull( subscribers );

            this.subscribers.addAll( subscribers );

            return this;
        }

        /**
         * Adds a mapper from message bytes to messages.
         * 
         * @param mapper the byte mapper
         * @return this builder
         */

        Builder<T> setMapper( Function<ByteBuffer, T> mapper )
        {
            this.mapper = mapper;

            return this;
        }

        /**
         * Sets the evaluation identifier.
         * 
         * @param evaluationId the evaluation identifier
         * @return this builder
         */

        Builder<T> setEvaluationId( String evaluationId )
        {
            this.evaluationId = evaluationId;

            return this;
        }

        /**
         * Builds an evaluation.
         * 
         * @return an evaluation
         * @throws JMSException if the subscriber cannot be built for any reason
         */

        MessageSubscriber build() throws JMSException
        {
            return new MessageSubscriber( this );
        }
    }

    /**
     * Subscribes a list of consumers.
     * 
     * @param <T> the type of message
     * @param subscribers the consumers
     * @param mapper to map from message bytes to a typed message
     * @param evaluationId an evaluation identifier to help with exception messaging and logging
     * @return a list of subscriptions
     * @throws JMSException - if the session fails to create a MessageProducerdue to some internal error
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the list of subscribers is empty
     */

    private <T> List<MessageConsumer> subscribeAll( List<Consumer<T>> subscribers,
                                                    Function<ByteBuffer, T> mapper,
                                                    String evaluationId )
            throws JMSException
    {
        Objects.requireNonNull( subscribers );
        Objects.requireNonNull( mapper );
        Objects.requireNonNull( evaluationId );

        if ( subscribers.isEmpty() )
        {
            throw new IllegalArgumentException( "Specify one or more subscribers for evaluation " + evaluationId );
        }

        List<MessageConsumer> returnMe = new ArrayList<>();
        for ( Consumer<T> next : subscribers )
        {
            MessageConsumer consumer = this.subscribe( next, mapper, evaluationId );
            returnMe.add( consumer );
        }

        return Collections.unmodifiableList( returnMe );
    }

    /**
     * Subscribes a consumer.
     * 
     * @param <T> the type of message to consume
     * @param subscriber the consumer
     * @param mapper to map from message bytes to a typed message
     * @param evaluationId an evaluation identifier to help with exception messaging and logging
     * @return a subscription
     * @throws JMSException if the session fails to create a consumer due to some internal error
     * @throws NullPointerException if any input is null
     */

    private <T> MessageConsumer subscribe( Consumer<T> subscriber, Function<ByteBuffer, T> mapper, String evaluationId )
            throws JMSException
    {
        Objects.requireNonNull( subscriber );
        Objects.requireNonNull( mapper );
        Objects.requireNonNull( evaluationId );

        // A listener acknowledges the message when the consumption completes 
        MessageListener listener = message -> {

            BytesMessage receivedBytes = (BytesMessage) message;
            String messageId = "unknown";
            String correlationId = "unknown";

            try
            {
                messageId = message.getJMSMessageID();
                correlationId = message.getJMSCorrelationID();

                // Create the byte array to hold the message
                int messageLength = (int) receivedBytes.getBodyLength();

                byte[] messageContainer = new byte[messageLength];

                receivedBytes.readBytes( messageContainer );

                ByteBuffer bufferedMessage = ByteBuffer.wrap( messageContainer );

                T received = mapper.apply( bufferedMessage );

                subscriber.accept( received );

                // Acknowledge
                message.acknowledge();

                LOGGER.debug( "Consumer {} successfully consumed a message with identifier {} and correlation "
                              + "identifier {}.",
                              this,
                              messageId,
                              correlationId );
            }
            catch ( JMSException | EvaluationEventException e )
            {
                // Messages are on the DLQ, but signal locally too
                if ( Objects.isNull( this.failure ) )
                {
                    this.failure = receivedBytes;
                }

                LOGGER.error( "While attempting to consume a message with identifier {} and correlation identifier {}, "
                              + "encountered an error that prevented consumption: ",
                              messageId,
                              correlationId,
                              e.getMessage() );

                try
                {
                    // Attempt recovery in order to cycle the delivery attempts. When the maximum is reached, poison
                    // messages should hit the dead letter queue/DLQ
                    this.session.recover();
                }
                catch ( JMSException e1 )
                {
                    LOGGER.error( "While attempting to recover a session for evaluation {}, "
                                  + "encountered an error that prevented recovery: ",
                                  evaluationId,
                                  e.getMessage() );
                }
            }

            // Count down, whether success or failure
            this.phaser.arriveAndDeregister();
        };

        // Only consume messages for the current evaluation based on JMSCorrelationID
        String selector = "JMSCorrelationID='" + evaluationId + "'";

        // This resource needs to be kept open until consumption is done
        MessageConsumer consumer = this.getConsumer( selector );
        consumer.setMessageListener( listener );

        LOGGER.debug( "Successfully registered consumer {} for evaluation {}", this, evaluationId );

        return consumer;
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

        try
        {
            for ( MessageConsumer next : this.consumers )
            {
                next.close();
            }
        }
        catch ( JMSException e )
        {
            throw new IOException( "Encountered an error while attempting to close a broker session.", e );
        }
    }

    /**
     * Returns a consumer.
     * 
     * @param selector the message selector
     * @return a consumer
     * @throws JMSException if the consumer could not be created for any reason
     */

    private MessageConsumer getConsumer( String selector ) throws JMSException
    {
        return this.session.createConsumer( this.destination, selector );
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
     * @param <T> the type of message
     * @param builder the builder
     * @throws JMSException if the JMS provider fails to create the connection due to some internal error
     * @throws JMSSecurityException if client authentication fails
     * @throws NullPointerException if any input is null
     */

    private <T> MessageSubscriber( Builder<T> builder )
            throws JMSException
    {
        // Set then validate
        this.destination = builder.destination;
        ConnectionFactory localFactory = builder.connectionFactory;
        String evaluationId = builder.evaluationId;
        Function<ByteBuffer, T> mapper = builder.mapper;
        List<Consumer<T>> subscribers = builder.subscribers;

        Objects.requireNonNull( this.destination );
        Objects.requireNonNull( localFactory );
        Objects.requireNonNull( evaluationId );
        Objects.requireNonNull( mapper );
        Objects.requireNonNull( subscribers );

        this.connection = localFactory.createConnection();
        
        // Register a listener for exceptions
        this.connection.setExceptionListener( new EvaluationEventExceptionListener() );

        // Client acknowledges
        this.session = connection.createSession( false, Session.CLIENT_ACKNOWLEDGE );

        // Add subscriptions
        this.consumers = this.subscribeAll( subscribers, mapper, evaluationId );

        // Start the connection
        this.connection.start();

        // Hint awaiting before resources are closed
        this.phaser = new Phaser();
        
        LOGGER.debug( "Created message subscriber {}, which is ready to receive subscriptions.", this );
    }

}
