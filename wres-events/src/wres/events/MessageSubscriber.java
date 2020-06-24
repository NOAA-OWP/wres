package wres.events;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
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

import com.google.protobuf.InvalidProtocolBufferException;

import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.EvaluationStatus.CompletionStatus;

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
     * Unknown mesage identifier.
     */

    private static final String UNKNOWN = "unknown";

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
     * Destination for evaluation status messages, which are used to trigger consumption of statistics groups.
     */

    private final Destination statusDestination;

    /**
     * A list of consumers that do not filter by message group.
     */

    private final List<MessageConsumer> consumers;

    /**
     * A list of consumers that filter by message group.
     */

    private final List<MessageConsumer> groupedConsumers;

    /**
     * A list of grouped consumers whose consumption should be completed with closing.
     */

    private final List<OneGroupConsumer<?>> cleanUp;

    /**
     * Used to register consumptions for notification of completion against an expected number of consumptions.
     */

    private final CompletionTracker completionTracker;

    /**
     * The message on which consumption failed, null if no failure.
     */

    private Message failure = null;

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
     * Builds an evaluation.
     * 
     * @param <T> the type of message for which subscriptions are required
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
         * Destination for evaluation status messages.
         */

        private Destination statusDestination;

        /**
         * List of subscriptions to evaluation events.
         */

        private List<Consumer<T>> subscribers = new ArrayList<>();

        /**
         * List of subscriptions to groups of evaluation events.
         */

        private List<OneGroupConsumer<T>> groupSubscribers = new ArrayList<>();

        /**
         * List of subscriptions to statistics events.
         */

        private Function<ByteBuffer, T> mapper;

        /**
         * An evaluation identifier.
         */

        private String evaluationId;

        /**
         * Used to register consumptions for notification of completion against an expected number of consumptions.
         */

        private CompletionTracker completionTracker;

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
         * Sets the destination for evaluation status messages when listening to these for grouped consumption.
         * 
         * @param statusDestination the destination for evaluation status messages
         * @return this builder
         */

        Builder<T> setEvaluationStatusDestination( Destination statusDestination )
        {
            this.statusDestination = statusDestination;

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
         * Adds a list of subscribers for groups of evaluation events.
         * 
         * @param subscribers the subscribers for groups of evaluation events
         * @return this builder
         * @throws NullPointerException if the list is null
         */

        Builder<T> addGroupSubscribers( List<OneGroupConsumer<T>> subscribers )
        {
            Objects.requireNonNull( subscribers );

            this.groupSubscribers.addAll( subscribers );

            return this;
        }

        /**
         * Sets a notifier for monitoring the completion state of consumption.
         * 
         * @param completionTracker the notifier
         * @return this builder
         */

        Builder<T> setCompletionTracker( CompletionTracker completionTracker )
        {
            this.completionTracker = completionTracker;

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
     * @param groupId an optional group identifier to filter messages
     * @return a list of subscriptions
     * @throws JMSException - if the session fails to create a MessageProducerdue to some internal error
     * @throws NullPointerException if any input is null
     */

    private <T> List<MessageConsumer> subscribeAll( List<Consumer<T>> subscribers,
                                                    Function<ByteBuffer, T> mapper,
                                                    String evaluationId )
            throws JMSException
    {
        Objects.requireNonNull( subscribers );
        Objects.requireNonNull( mapper );
        Objects.requireNonNull( evaluationId );

        List<MessageConsumer> returnMe = new ArrayList<>();
        for ( Consumer<T> next : subscribers )
        {
            MessageConsumer consumer = this.subscribe( next, mapper, evaluationId, null );
            returnMe.add( consumer );
        }

        return Collections.unmodifiableList( returnMe );
    }

    /**
     * Subscribes a list of grouped consumers.
     * 
     * @param <T> the type of message
     * @param subscribers the consumers
     * @param mapper to map from message bytes to a typed message
     * @param evaluationId an evaluation identifier to help with exception messaging and logging
     * @return a list of subscriptions
     * @throws JMSException - if the session fails to create a MessageProducerdue to some internal error
     * @throws NullPointerException if any input is null
     */

    private <T> List<MessageConsumer> subscribeAllGrouped( List<OneGroupConsumer<T>> subscribers,
                                                           Function<ByteBuffer, T> mapper,
                                                           String evaluationId )
            throws JMSException
    {
        Objects.requireNonNull( subscribers );
        Objects.requireNonNull( mapper );
        Objects.requireNonNull( evaluationId );

        List<MessageConsumer> returnMe = new ArrayList<>();
        for ( OneGroupConsumer<T> next : subscribers )
        {
            MessageConsumer consumer = this.subscribe( next, mapper, evaluationId, next.getGroupId() );
            returnMe.add( consumer );
        }

        return Collections.unmodifiableList( returnMe );
    }

    /**
     * Subscribes a grouped consumer for grouped consumption.
     * 
     * @param <T> the type of message
     * @param subscriber the consumer
     * @param mapper to map from message bytes to a typed message
     * @param evaluationId an evaluation identifier to help with exception messaging and logging
     * @param groupId an optional group identifier by which to filter messages
     * @return a subscription
     * @throws JMSException if the session fails to create a consumer due to some internal error
     * @throws NullPointerException if any input is null
     */

    private <T> MessageConsumer subscribe( OneGroupConsumer<T> subscriber,
                                           Function<ByteBuffer, T> mapper,
                                           String evaluationId,
                                           String groupId )
            throws JMSException
    {
        Objects.requireNonNull( subscriber );
        Objects.requireNonNull( mapper );
        Objects.requireNonNull( evaluationId );
        Objects.requireNonNull( groupId );

        // Subscribe the message consumer and then subscribe a listener for status messages, which triggers 
        // the inner/grouped consumption
        MessageConsumer messageConsumer = this.subscribe( (Consumer<T>) subscriber, mapper, evaluationId, groupId );

        // Only consume messages for the current evaluation based on JMSCorrelationID and JMSXGroupID
        // In other words, the groupId must be in the status information too
        String selector = "JMSCorrelationID='" + evaluationId + "' AND JMSXGroupID='" + groupId + "'";

        MessageConsumer statusConsumer = this.getConsumer( this.statusDestination, selector );

        // A listener acknowledges the message when the consumption completes 
        MessageListener listener = message -> {

            BytesMessage receivedBytes = (BytesMessage) message;
            String messageId = UNKNOWN;
            String correlationId = UNKNOWN;

            try
            {
                messageId = message.getJMSMessageID();
                correlationId = message.getJMSCorrelationID();

                // Create the byte array to hold the message
                int messageLength = (int) receivedBytes.getBodyLength();

                byte[] messageContainer = new byte[messageLength];

                receivedBytes.readBytes( messageContainer );

                ByteBuffer bufferedMessage = ByteBuffer.wrap( messageContainer );

                EvaluationStatus status = EvaluationStatus.parseFrom( bufferedMessage.array() );

                // The status message signals the completion of a group and the group has received all expected
                // statistics messages
                if ( status.getCompletionStatus() == CompletionStatus.GROUP_COMPLETE_REPORTED_SUCCESS )
                {
                    // Set the expected number of messages per group
                    this.completionTracker.registerGroupComplete( status, groupId );

                    if ( status.getMessageCount() == subscriber.size() )
                    {
                        // Accept the group of messages
                        subscriber.acceptGroup();

                        // Close the consumer, as this group is done. According to the API docs, calling close on the
                        // parent consumer of a listener will allow the listener's onMessage method (this method) to 
                        // complete normally
                        messageConsumer.close();
                        statusConsumer.close();

                        LOGGER.debug( "While consuming message group {} with consumer {}, encountered an evaluation "
                                      + "status message with identifier {} and correlation identifier {} and "
                                      + "completion state {}, which successfully triggered the consumption of the "
                                      + "message group containing {} messages.",
                                      groupId,
                                      this,
                                      messageId,
                                      correlationId,
                                      status.getCompletionStatus(),
                                      status.getMessageCount() );
                    }
                    else
                    {
                        this.cleanUp.add( subscriber );

                        LOGGER.debug( "While consuming message group {} with consumer {}, encountered an evaluation "
                                      + "status message with identifier {} and correlation identifier {} and "
                                      + "completion state {}. The expected number of messages within the group is {} "
                                      + "but only {} messages have been consumed. Grouped consumption will happen when "
                                      + "this subscriber is closed.",
                                      groupId,
                                      this,
                                      messageId,
                                      correlationId,
                                      status.getCompletionStatus(),
                                      status.getMessageCount(),
                                      subscriber.size() );
                    }
                }

                LOGGER.debug( "While consuming message group {} with consumer {}, successfully consumed an evaluation "
                              + "status message with identifier {} and correlation "
                              + "identifier {} and completion state {}",
                              groupId,
                              this,
                              messageId,
                              correlationId,
                              status.getCompletionStatus() );

                // Acknowledge the message
                message.acknowledge();
            }
            catch ( JMSException | EvaluationEventException | InvalidProtocolBufferException e )
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
                catch ( JMSException f )
                {
                    LOGGER.error( "While attempting to recover a session for evaluation {}, "
                                  + "encountered an error that prevented recovery: ",
                                  evaluationId,
                                  f.getMessage() );
                }
            }
        };

        statusConsumer.setMessageListener( listener );

        LOGGER.debug( "Successfully registered a consumer {} for statistics messages in group {} associated with "
                      + "evaluation {}",
                      this,
                      groupId,
                      evaluationId );

        return messageConsumer;
    }

    /**
     * Subscribes a consumer.
     * 
     * @param <T> the type of message to consume
     * @param subscriber the consumer
     * @param mapper to map from message bytes to a typed message
     * @param evaluationId an evaluation identifier to help with exception messaging and logging
     * @param groupId an optional group identifier by which to filter messages
     * @return a subscription
     * @throws JMSException if the session fails to create a consumer due to some internal error
     * @throws NullPointerException if any input is null, except the optional groupId
     */

    private <T> MessageConsumer subscribe( Consumer<T> subscriber,
                                           Function<ByteBuffer, T> mapper,
                                           String evaluationId,
                                           String groupId )
            throws JMSException
    {
        Objects.requireNonNull( subscriber );
        Objects.requireNonNull( mapper );
        Objects.requireNonNull( evaluationId );

        // Only consume messages for the current evaluation based on JMSCorrelationID
        String selector = "JMSCorrelationID='" + evaluationId + "'";

        if ( Objects.nonNull( groupId ) )
        {
            selector = selector + " AND JMSXGroupID='" + groupId + "'";
        }

        // This resource needs to be kept open until consumption is done
        MessageConsumer consumer = this.getConsumer( this.destination, selector );

        // A listener acknowledges the message when the consumption completes 
        MessageListener listener = message -> {

            BytesMessage receivedBytes = (BytesMessage) message;
            String messageId = UNKNOWN;
            String correlationId = UNKNOWN;

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
                catch ( JMSException f )
                {
                    LOGGER.error( "While attempting to recover a session for evaluation {}, "
                                  + "encountered an error that prevented recovery: ",
                                  evaluationId,
                                  f.getMessage() );
                }
            }

            // Count down, whether success or failure
            LOGGER.debug( "Reduced the expected count associated with subscriber {} by 1.", this );

            this.completionTracker.register();

            // Check and close early if group consumption complete
            // A little ugly to check for instanceof, but it is a private implementation detail of this class
            if ( subscriber instanceof OneGroupConsumer )
            {
                this.checkAndCloseEarlyIfGroupConsumptionIsComplete( (OneGroupConsumer<?>) subscriber, consumer );
            }
        };

        consumer.setMessageListener( listener );

        LOGGER.debug( "Successfully registered consumer {} for evaluation {}.", this, evaluationId );

        return consumer;
    }

    /**
     * Allows for grouped consumption to complete earlier than the closure of this subscriber when a group complete 
     * status message is received before all grouped messages have been received.
     * 
     * @param group the grouped consumer whose consumption should be completed
     * @param consumer the message consumer whose resources should be closed
     */

    private void checkAndCloseEarlyIfGroupConsumptionIsComplete( OneGroupConsumer<?> group, MessageConsumer consumer )
    {
        Integer expected = this.completionTracker.getExpectedMessagesPerGroup( group.getGroupId() );

        if ( Objects.nonNull( expected ) && expected == group.size() && !group.hasBeenUsed() )
        {
            LOGGER.debug( "Discovered a consumer of message groups, {}, whose {} grouped messages have all been "
                          + "consumed. Triggering final consumption/aggregation of the group and clean-up.",
                          group,
                          group.size() );

            group.acceptGroup();

            try
            {
                consumer.close();
            }
            catch ( JMSException e )
            {
                throw new EvaluationEventException( "Failed to close a consumer early when completing group "
                                                    + "consumption." );
            }
        }
    }

    @Override
    public void close() throws IOException
    {
        LOGGER.debug( "Closing subscriber, {}.", this );

        // Check for group subscriptions that have not completed
        if ( !this.cleanUp.isEmpty() )
        {
            LOGGER.debug( "On closing subscriber {}, discovered {} consumers of message groups whose consumption had "
                          + "not yet completed. Cleaning up...",
                          this,
                          this.cleanUp.size() );

            for ( OneGroupConsumer<?> next : this.cleanUp )
            {
                Integer expectedCount = this.completionTracker.getExpectedMessagesPerGroup( next.getGroupId() );

                if ( Objects.nonNull( expectedCount ) && !next.hasBeenUsed() )
                {
                    if ( next.size() != expectedCount )
                    {
                        throw new EvaluationEventException( "While attempting to gracefully close subscriber " + this
                                                            + " , encountered an error. A consumer of grouped messages "
                                                            + "attached to this subscription expected to receive "
                                                            + expectedCount
                                                            + " but had only received "
                                                            + next.size()
                                                            + " messages on closing. A subscriber should not be closed "
                                                            + "until consumption is complete." );
                    }

                    next.acceptGroup();
                }
            }

            LOGGER.debug( "Completed clean-up of subscriber {}", this );
        }

        this.internalClose();
    }

    /**
     * Internally closes resources.
     * @throws IOException if any resource could not be closed for any reason.
     */

    private void internalClose() throws IOException
    {
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
            throw new IOException( "Encountered an error while attempting to close a consumer.", e );
        }

        try
        {
            for ( MessageConsumer next : this.groupedConsumers )
            {
                next.close();
            }
        }
        catch ( JMSException e )
        {
            throw new IOException( "Encountered an error while attempting to close a grouped consumer.", e );
        }
    }

    /**
     * Returns a consumer.
     * 
     * @param destination the destination
     * @param selector the message selector
     * @return a consumer
     * @throws JMSException if the consumer could not be created for any reason
     */

    private MessageConsumer getConsumer( Destination destination, String selector ) throws JMSException
    {
        return this.session.createConsumer( destination, selector );
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
     * @throws IllegalArgumentException if no subscribers are defined
     */

    private <T> MessageSubscriber( Builder<T> builder )
            throws JMSException
    {
        // Set then validate
        this.destination = builder.destination;
        this.statusDestination = builder.statusDestination;
        this.completionTracker = builder.completionTracker;
        ConnectionFactory localFactory = builder.connectionFactory;
        String evaluationId = builder.evaluationId;
        Function<ByteBuffer, T> mapper = builder.mapper;
        List<Consumer<T>> subscribers = builder.subscribers;
        List<OneGroupConsumer<T>> groupSubscribers = builder.groupSubscribers;

        Objects.requireNonNull( this.destination );
        Objects.requireNonNull( this.completionTracker );
        Objects.requireNonNull( localFactory );
        Objects.requireNonNull( evaluationId );
        Objects.requireNonNull( mapper );
        Objects.requireNonNull( subscribers );

        if ( subscribers.isEmpty() && groupSubscribers.isEmpty() )
        {
            throw new IllegalArgumentException( "Specify one or more subscribers for evaluation " + evaluationId );
        }

        this.connection = localFactory.createConnection();

        // Register a listener for exceptions
        this.connection.setExceptionListener( new EvaluationEventExceptionListener() );

        // Client acknowledges
        this.session = this.connection.createSession( false, Session.CLIENT_ACKNOWLEDGE );

        // Add subscriptions
        List<MessageConsumer> localConsumers = new ArrayList<>();
        List<MessageConsumer> localGroupedConsumers = new ArrayList<>();

        if ( !subscribers.isEmpty() )
        {
            List<MessageConsumer> someConsumers = this.subscribeAll( subscribers, mapper, evaluationId );
            localConsumers.addAll( someConsumers );
        }
        if ( !groupSubscribers.isEmpty() )
        {
            List<MessageConsumer> moreConsumers =
                    this.subscribeAllGrouped( groupSubscribers, mapper, evaluationId );
            localGroupedConsumers.addAll( moreConsumers );

            Objects.requireNonNull( this.statusDestination,
                                    "When setting grouped subscribers, the destination for evaluation status messages "
                                                            + "is also required because grouped subscriptions listen "
                                                            + "for status messages that identify when the group has "
                                                            + "completed." );
        }

        this.consumers = Collections.unmodifiableList( localConsumers );
        this.groupedConsumers = Collections.unmodifiableList( localGroupedConsumers );

        // Start the connection
        this.connection.start();

        this.cleanUp = new ArrayList<>();

        LOGGER.debug( "Created message subscriber {}, which is ready to receive subscriptions.", this );
    }

}
