package wres.events;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.Topic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

import wres.events.Evaluation.EvaluationInfo;
import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.EvaluationStatus.CompletionStatus;

/**
 * Registers a subscriber to a topic that is supplied on construction. There is one {@link Connection} per instance 
 * because connections are assumed to be expensive. Currently, there is also one {@link Session} per instance, but a 
 * pool of sessions might be better (to allow better message throughput, as a session is the work thread). Overall, it 
 * may be better to abstract connections and sessions away from specific helpers.
 * 
 * @param <T> the message type
 * @author james.brown@hydrosolved.com
 */

class MessageSubscriber<T> implements Closeable
{

    private static final Logger LOGGER = LoggerFactory.getLogger( MessageSubscriber.class );

    private static final String UNKNOWN = "unknown";

    private static final String ENCOUNTERED_AN_ERROR_THAT_PREVENTED_RECOVERY =
            "encountered an error that prevented recovery: ";

    private static final String WHILE_ATTEMPTING_TO_RECOVER_A_SESSION_FOR_EVALUATION =
            "While attempting to recover a session for evaluation {}, ";

    private static final String ENCOUNTERED_AN_ERROR_THAT_PREVENTED_CONSUMPTION =
            "encountered an error that prevented consumption: ";

    private static final String WHILE_ATTEMPTING_TO_CONSUME_A_MESSAGE_WITH_IDENTIFIER_AND_CORRELATION_IDENTIFIER =
            "While attempting to consume a message with identifier {} and correlation identifier {}, ";

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

    private final Topic topic;

    /**
     * Consumer for evaluation status messages, which are used to trigger consumption of statistics groups.
     */

    private final MessageConsumer statusConsumer;

    /**
     * A list of message consumers that do not filter by message group. These are resources to be closed.
     */

    private final List<MessageConsumer> consumers;

    /**
     * A map of group subscribers by group identifier.
     */

    private final Map<String, Queue<OneGroupConsumer<T>>> groupConsumers;

    /**
     * Evaluation information, including a tracker to register the number of consumptions for notification of completion 
     * against an expected number of consumptions.
     */

    private final EvaluationInfo evaluationInfo;

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

        private Topic topic;

        /**
         * Destination for evaluation status messages.
         */

        private Topic statusTopic;

        /**
         * List of subscriptions to evaluation events.
         */

        private List<Consumer<T>> subscribers = new ArrayList<>();

        /**
         * List of subscriptions to groups of evaluation events.
         */

        private List<Consumer<Collection<T>>> groupSubscribers = new ArrayList<>();

        /**
         * A mapper between message bytes and messages.
         */

        private Function<ByteBuffer, T> mapper;

        /**
         * The evaluation information, including a completion tracker used to register consumptions for notification of 
         * completion against an expected number of consumptions.
         */

        private EvaluationInfo evaluationInfo;

        /**
         * Optional context for naming durable queues.
         */

        private String context;

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
         * Sets the topic.
         * 
         * @param topic the topic
         * @return this builder
         */

        Builder<T> setTopic( Topic topic )
        {
            this.topic = topic;

            return this;
        }

        /**
         * Sets the topic for evaluation status messages when listening to these for grouped consumption.
         * 
         * @param statusTopic the topic for evaluation status messages
         * @return this builder
         */

        Builder<T> setEvaluationStatusTopic( Topic statusTopic )
        {
            this.statusTopic = statusTopic;

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
         * @param groupIds the group identifiers to which the subscriptions apply
         * @param subscribers the subscribers for groups of evaluation events
         * @return this builder
         * @throws NullPointerException if the list is null
         */

        Builder<T> addGroupSubscribers( List<Consumer<Collection<T>>> subscribers )
        {
            Objects.requireNonNull( subscribers );

            this.groupSubscribers.addAll( subscribers );

            return this;
        }

        /**
         * Sets a evaluation information, including an evaluation completion tracker.
         * 
         * @param evaluationInfo the evaluation information
         * @return this builder
         */

        Builder<T> setEvaluationInfo( EvaluationInfo evaluationInfo )
        {
            this.evaluationInfo = evaluationInfo;

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
         * Sets the context for naming a durable queue. This name will be prepended to each queue.
         * 
         * @param context the context
         * @return this builder 
         */

        Builder<T> setContext( String context )
        {
            this.context = context;

            return this;
        }

        /**
         * Builds an evaluation.
         * 
         * @return an evaluation
         * @throws JMSException if the subscriber cannot be built for any reason
         */

        MessageSubscriber<T> build() throws JMSException
        {
            return new MessageSubscriber<>( this );
        }
    }

    /**
     * Subscribes a list of consumers.
     * 
     * @param <T> the type of message
     * @param consumers the consumers
     * @param mapper to map from message bytes to a typed message
     * @param evaluationId an evaluation identifier to help with exception messaging and logging
     * @param context an optional context string to use when naming durable queues
     * @return a list of subscriptions
     * @throws JMSException - if the session fails to create a MessageProducerdue to some internal error
     * @throws NullPointerException if any input is null
     */

    private List<MessageConsumer> subscribeAllConsumers( List<Consumer<T>> consumers,
                                                         Function<ByteBuffer, T> mapper,
                                                         String evaluationId,
                                                         String context )
            throws JMSException
    {
        Objects.requireNonNull( consumers );
        Objects.requireNonNull( mapper );
        Objects.requireNonNull( evaluationId );

        List<MessageConsumer> returnMe = new ArrayList<>();
        for ( Consumer<T> next : consumers )
        {
            MessageConsumer consumer = this.subscribeOneConsumer( next, mapper, evaluationId, context );
            returnMe.add( consumer );
        }

        return Collections.unmodifiableList( returnMe );
    }

    /**
     * Subscribes a list of grouped consumers.
     * 
     * @param <T> the type of message
     * @param consumers the consumers
     * @param mapper to map from message bytes to a typed message
     * @param evaluationId an evaluation identifier to help with exception messaging and logging
     * @param context an optional context string to use when naming durable queues
     * @return a list of subscriptions
     * @throws JMSException - if the session fails to create a MessageProducerdue to some internal error
     * @throws NullPointerException if any input is null
     */

    private List<MessageConsumer> subscribeAllGroupedConsumers( List<Consumer<Collection<T>>> consumers,
                                                                Function<ByteBuffer, T> mapper,
                                                                String evaluationId,
                                                                String context )
            throws JMSException
    {
        Objects.requireNonNull( consumers );
        Objects.requireNonNull( mapper );
        Objects.requireNonNull( evaluationId );

        List<MessageConsumer> returnMe = new ArrayList<>();
        for ( Consumer<Collection<T>> next : consumers )
        {
            MessageConsumer consumer = this.subscribeOneGroupedConsumer( next, mapper, evaluationId, context );
            returnMe.add( consumer );
        }

        return Collections.unmodifiableList( returnMe );
    }

    /**
     * Subscribes a grouped consumer for grouped consumption.
     * 
     * @param <T> the type of message
     * @param innerSubscriber the inner consumer that should receive aggregated messages
     * @param mapper to map from message bytes to a typed message
     * @param evaluationId an evaluation identifier to help with exception messaging and logging
     * @param context an optional context string to use when naming durable queues
     * @return a group subscription
     * @throws JMSException if the session fails to create a consumer due to some internal error
     * @throws NullPointerException if any input is null
     */

    private MessageConsumer subscribeOneGroupedConsumer( Consumer<Collection<T>> innerSubscriber,
                                                         Function<ByteBuffer, T> mapper,
                                                         String evaluationId,
                                                         String context )
            throws JMSException
    {
        Objects.requireNonNull( innerSubscriber );
        Objects.requireNonNull( mapper );
        Objects.requireNonNull( evaluationId );

        // Create a consumer that accepts the small messages and then populates the list of group subscribers
        // as those messages arrive
        MessageConsumer messageConsumer = this.getConsumerForGroupedMessages( innerSubscriber,
                                                                              mapper,
                                                                              evaluationId,
                                                                              context );

        // Now listen for status messages when a group completes
        MessageListener listener = message -> {

            BytesMessage receivedBytes = (BytesMessage) message;
            String messageId = UNKNOWN;
            String correlationId = UNKNOWN;
            String messageGroupId = UNKNOWN;

            try
            {
                messageId = message.getJMSMessageID();
                correlationId = message.getJMSCorrelationID();
                messageGroupId = message.getStringProperty( MessagePublisher.JMSX_GROUP_ID );

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
                    this.evaluationInfo.getCompletionTracker().registerGroupComplete( status, messageGroupId );
                    int completed = this.checkAndCompleteGroup( messageGroupId );

                    if ( completed > 0 && LOGGER.isDebugEnabled() )
                    {
                        LOGGER.debug( "While consuming grouped messages for evaluation {}, encountered an evaluation "
                                      + "status message with identifier {} and correlation identifier {} and "
                                      + "completion state {}, which successfully triggered consumption across {} "
                                      + "consumers containing {} messages.",
                                      evaluationId,
                                      messageId,
                                      correlationId,
                                      status.getCompletionStatus(),
                                      completed,
                                      status.getMessageCount() );
                    }
                    else if ( LOGGER.isDebugEnabled() )
                    {
                        LOGGER.debug( "While consuming grouped messages for evaluation {}, encountered an evaluation "
                                      + "status message with identifier {} and correlation identifier {} and "
                                      + "completion state {}. The expected number of messages within the group is {} "
                                      + "but some of these messages are outstanding. Grouped consumption will happen "
                                      + "when this subscriber is closed.",
                                      evaluationId,
                                      messageId,
                                      correlationId,
                                      status.getCompletionStatus(),
                                      status.getMessageCount() );
                    }
                }

                LOGGER.debug( "While consuming message group {} with consumer {}, successfully consumed an evaluation "
                              + "status message with identifier {} and correlation "
                              + "identifier {} and completion state {}",
                              messageGroupId,
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

                LOGGER.error( WHILE_ATTEMPTING_TO_CONSUME_A_MESSAGE_WITH_IDENTIFIER_AND_CORRELATION_IDENTIFIER
                              + ENCOUNTERED_AN_ERROR_THAT_PREVENTED_CONSUMPTION,
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
                    LOGGER.error( WHILE_ATTEMPTING_TO_RECOVER_A_SESSION_FOR_EVALUATION
                                  + ENCOUNTERED_AN_ERROR_THAT_PREVENTED_RECOVERY,
                                  evaluationId,
                                  f.getMessage() );
                }
            }
        };

        this.statusConsumer.setMessageListener( listener );

        LOGGER.debug( "Successfully registered a subscriber {} for grouped statistics messages associated with "
                      + "evaluation {}",
                      this,
                      evaluationId );

        return messageConsumer;
    }

    /**
     * Returns a consumer for grouped messages.
     * 
     * @param <T> the type of message
     * @param innerSubscriber the inner subscriber that accepts aggregated messages
     * @param mapper to map from message bytes to a typed message
     * @param evaluationId an evaluation identifier to help with exception messaging and logging
     * @param context an optional context string to use when naming durable queues
     * @return a subscription
     * @throws JMSException if the session fails to create a consumer due to some internal error
     * @throws NullPointerException if any input is null
     */

    private MessageConsumer getConsumerForGroupedMessages( Consumer<Collection<T>> innerSubscriber,
                                                           Function<ByteBuffer, T> mapper,
                                                           String evaluationId,
                                                           String context )
            throws JMSException
    {
        Objects.requireNonNull( mapper );
        Objects.requireNonNull( evaluationId );

        // A listener acknowledges the message when the consumption completes 
        MessageListener listener = message -> {

            BytesMessage receivedBytes = (BytesMessage) message;
            String messageId = UNKNOWN;
            String correlationId = UNKNOWN;
            String groupId = UNKNOWN;

            try
            {
                messageId = message.getJMSMessageID();
                correlationId = message.getJMSCorrelationID();
                groupId = message.getStringProperty( MessagePublisher.JMSX_GROUP_ID );

                // Create the byte array to hold the message
                int messageLength = (int) receivedBytes.getBodyLength();

                byte[] messageContainer = new byte[messageLength];

                receivedBytes.readBytes( messageContainer );

                ByteBuffer bufferedMessage = ByteBuffer.wrap( messageContainer );

                T received = mapper.apply( bufferedMessage );

                Queue<OneGroupConsumer<T>> subscribers = this.getGroupSubscriber( innerSubscriber,
                                                                                  groupId );

                // Register the message with each grouped subscriber
                subscribers.forEach( next -> next.accept( received ) );

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

                LOGGER.error( WHILE_ATTEMPTING_TO_CONSUME_A_MESSAGE_WITH_IDENTIFIER_AND_CORRELATION_IDENTIFIER
                              + ENCOUNTERED_AN_ERROR_THAT_PREVENTED_CONSUMPTION,
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
                    LOGGER.error( WHILE_ATTEMPTING_TO_RECOVER_A_SESSION_FOR_EVALUATION
                                  + ENCOUNTERED_AN_ERROR_THAT_PREVENTED_RECOVERY,
                                  evaluationId,
                                  f.getMessage() );
                }
            }

            // Count down, whether success or failure
            LOGGER.debug( "Reduced the expected count associated with subscriber {} by 1.", this );

            this.evaluationInfo.getCompletionTracker().register();

            // Check and close early if group consumption is complete for one or more subscribers
            this.checkAndCompleteGroup( groupId );
        };

        // Only consume messages for the current evaluation based on JMSCorrelationID
        String selector = MessagePublisher.JMS_CORRELATION_ID + evaluationId + "'";
        MessageConsumer messageConsumer = this.getConsumer( this.topic, selector, context );
        messageConsumer.setMessageListener( listener );

        LOGGER.debug( "Successfully registered a subscriber {} for grouped statistics messages associated with "
                      + "evaluation {}",
                      this,
                      evaluationId );

        return messageConsumer;
    }

    /**
     * Creates a map of grouped subscriptions by group identifier. Returns a mutable queue, so do not expose more 
     * generally.
     * 
     * @param <T> the type of message
     * @param innerConsumer the inner consumer that accepts aggregate messages
     * @param groupId the group identifier
     * @return the group subscriber
     */

    private Queue<OneGroupConsumer<T>> getGroupSubscriber( Consumer<Collection<T>> innerConsumer,
                                                           String groupId )
    {
        Objects.requireNonNull( innerConsumer );
        Objects.requireNonNull( groupId );

        Queue<OneGroupConsumer<T>> add = new ConcurrentLinkedQueue<>();

        Queue<OneGroupConsumer<T>> existingConsumers = this.groupConsumers.putIfAbsent( groupId, add );

        // Already exists?
        if ( Objects.nonNull( existingConsumers ) )
        {
            for ( OneGroupConsumer<T> next : existingConsumers )
            {
                // Group subscriber already exists
                if ( next.getInnerConsumer() == innerConsumer )
                {
                    break;
                }

                // Does not exist, so add
                OneGroupConsumer<T> newConsumer = OneGroupConsumer.of( innerConsumer, groupId );

                existingConsumers.add( newConsumer );
            }

            return existingConsumers;
        }
        // No, so add it
        else
        {
            OneGroupConsumer<T> newConsumer = OneGroupConsumer.of( innerConsumer, groupId );
            add.add( newConsumer );

            return add;
        }
    }

    /**
     * Subscribes a consumer.
     * 
     * @param <T> the type of message to consume
     * @param subscriber the consumer
     * @param mapper to map from message bytes to a typed message
     * @param evaluationId an evaluation identifier to help with exception messaging and logging
     * @param context an optional context string to use when naming durable queues
     * @return a subscription
     * @throws JMSException if the session fails to create a consumer due to some internal error
     * @throws NullPointerException if any input is null, except the optional groupId
     */

    private MessageConsumer subscribeOneConsumer( Consumer<T> subscriber,
                                                  Function<ByteBuffer, T> mapper,
                                                  String evaluationId,
                                                  String context )
            throws JMSException
    {
        Objects.requireNonNull( subscriber );
        Objects.requireNonNull( mapper );
        Objects.requireNonNull( evaluationId );

        // Only consume messages for the current evaluation based on JMSCorrelationID
        String selector = MessagePublisher.JMS_CORRELATION_ID + evaluationId + "'";

        // This resource needs to be kept open until consumption is done
        MessageConsumer consumer = this.getConsumer( this.topic, selector, context );

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

                LOGGER.error( WHILE_ATTEMPTING_TO_CONSUME_A_MESSAGE_WITH_IDENTIFIER_AND_CORRELATION_IDENTIFIER
                              + ENCOUNTERED_AN_ERROR_THAT_PREVENTED_CONSUMPTION,
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
                    LOGGER.error( WHILE_ATTEMPTING_TO_RECOVER_A_SESSION_FOR_EVALUATION
                                  + ENCOUNTERED_AN_ERROR_THAT_PREVENTED_RECOVERY,
                                  evaluationId,
                                  f.getMessage() );
                }
            }

            // Count down, whether success or failure
            LOGGER.debug( "Reduced the expected count associated with subscriber {} by 1.", this );

            this.evaluationInfo.getCompletionTracker().register();
        };

        consumer.setMessageListener( listener );

        LOGGER.debug( "Successfully registered consumer {} for evaluation {}.", this, evaluationId );

        return consumer;
    }

    /**
     * Checks for complete groups and finalizes them.
     * 
     * @param group the grouped consumers whose consumption should be completed
     * @param consumer the message consumer whose resources should be closed
     * @return the number of groups completed
     */

    private int checkAndCompleteGroup( String groupId )
    {
        int completed = 0;

        if ( Objects.nonNull( this.groupConsumers ) && this.groupConsumers.containsKey( groupId ) )
        {
            Queue<OneGroupConsumer<T>> check = this.groupConsumers.get( groupId );

            if ( Objects.nonNull( check ) )
            {
                for ( OneGroupConsumer<T> next : check )
                {
                    Integer expected = this.evaluationInfo.getCompletionTracker()
                                                          .getExpectedMessagesPerGroup( next.getGroupId() );

                    if ( Objects.nonNull( expected ) && expected == next.size() && !next.hasBeenUsed() )
                    {
                        next.acceptGroup();
                        completed++;
                    }
                }
            }
        }

        return completed;
    }

    @Override
    public void close() throws IOException
    {
        LOGGER.debug( "Closing subscriber, {}.", this );

        // Check for group subscriptions that have not completed
        for ( Map.Entry<String, Queue<OneGroupConsumer<T>>> next : this.groupConsumers.entrySet() )
        {
            String groupId = next.getKey();
            Queue<OneGroupConsumer<T>> consumersToClose = next.getValue();

            Integer expectedCount = this.evaluationInfo.getCompletionTracker().getExpectedMessagesPerGroup( groupId );

            int total = 0;

            for ( OneGroupConsumer<T> consumer : consumersToClose )
            {
                if ( Objects.nonNull( expectedCount ) && !consumer.hasBeenUsed() )
                {
                    if ( consumer.size() != expectedCount )
                    {
                        throw new EvaluationEventException( "While attempting to gracefully close subscriber " + this
                                                            + " , encountered an error. A consumer of grouped messages "
                                                            + "attached to this subscription expected to receive "
                                                            + expectedCount
                                                            + " but had only received "
                                                            + consumer.size()
                                                            + " messages on closing. A subscriber should not be closed "
                                                            + "until consumption is complete." );
                    }

                    consumer.acceptGroup();
                    total++;
                }
            }

            LOGGER.trace( "On closing subscriber {}, discovered {} consumers associated with group {} whose "
                          + "consumption was ready to complete, but had not yet completed. These were completed.",
                          this,
                          total,
                          groupId );

        }

        LOGGER.debug( "Completed clean-up of subscriber {}", this );

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
            if ( Objects.nonNull( this.statusConsumer ) )
            {
                this.statusConsumer.close();
            }
        }
        catch ( JMSException e )
        {
            throw new IOException( "Encountered an error while attempting to close a evaluation status message "
                                   + "consumer.",
                                   e );
        }
    }

    /**
     * Returns a consumer.
     * 
     * @param topic the topic
     * @param selector the message selector
     * @param prepend a string to prepend to a durable queue name
     * @return a consumer
     * @throws JMSException if the consumer could not be created for any reason
     */

    private MessageConsumer getConsumer( Topic topic, String selector, String prepend ) throws JMSException
    {
        // For a non-durable subscriber, use: "return this.session.createConsumer( topic, selector );"
        // Get a unique subscriber name, using the method in the evaluation class
        String uniqueId = this.evaluationInfo.getNextDurableSubscriptionName();
        String name = prepend + "-" + uniqueId;
        return this.session.createDurableSubscriber( topic, name, selector, false );
    }

    /**
     * Listen for failures on a connection.
     */

    private static class EvaluationEventExceptionListener implements ExceptionListener
    {

        @Override
        public void onException( JMSException exception )
        {
            throw new EvaluationEventException( "Encountered an error while attempting to complete an evaluation "
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

    private MessageSubscriber( Builder<T> builder )
            throws JMSException
    {
        // Set then validate
        this.topic = builder.topic;
        this.evaluationInfo = builder.evaluationInfo;
        Topic statusTopic = builder.statusTopic;
        ConnectionFactory localFactory = builder.connectionFactory;
        String context = builder.context;
        Function<ByteBuffer, T> mapper = builder.mapper;
        List<Consumer<T>> subscribers = builder.subscribers;
        List<Consumer<Collection<T>>> groupSubscribers = builder.groupSubscribers;

        String evaluationId = this.evaluationInfo.getEvaluationId();
        
        Objects.requireNonNull( this.topic );
        Objects.requireNonNull( this.evaluationInfo );
        Objects.requireNonNull( localFactory );
        Objects.requireNonNull( mapper );
        Objects.requireNonNull( subscribers );

        this.connection = localFactory.createConnection();

        // Register a listener for exceptions
        this.connection.setExceptionListener( new EvaluationEventExceptionListener() );

        // Client acknowledges
        this.session = this.connection.createSession( false, Session.CLIENT_ACKNOWLEDGE );

        String selector = MessagePublisher.JMS_CORRELATION_ID + evaluationId + "'";

        if ( Objects.nonNull( statusTopic ) )
        {
            String groupContext = Evaluation.EVALUATION_STATUS_QUEUE + "-HOUSEKEEPING-group-complete";

            this.statusConsumer = this.getConsumer( statusTopic, selector, groupContext );
        }
        else
        {
            this.statusConsumer = null;
        }

        this.groupConsumers = new ConcurrentHashMap<>();

        // Add subscriptions
        List<MessageConsumer> localConsumers = new ArrayList<>();

        if ( !subscribers.isEmpty() )
        {
            List<MessageConsumer> someConsumers = this.subscribeAllConsumers( subscribers, 
                                                                              mapper, 
                                                                              evaluationId, 
                                                                              context );
            localConsumers.addAll( someConsumers );
        }
        if ( !groupSubscribers.isEmpty() )
        {
            LOGGER.debug( "Discovered {} consumers for message groups associated with evaluation {}.",
                          groupSubscribers.size(),
                          evaluationId );

            String groupContext = context + "-groups";

            List<MessageConsumer> moreConsumers = this.subscribeAllGroupedConsumers( groupSubscribers,
                                                                                     mapper,
                                                                                     evaluationId,
                                                                                     groupContext );
            localConsumers.addAll( moreConsumers );

            Objects.requireNonNull( statusTopic,
                                    "When setting grouped subscribers, the topic for evaluation status messages "
                                                 + "is also required because grouped subscriptions listen "
                                                 + "for status messages that identify when the group has "
                                                 + "completed." );
        }

        this.consumers = Collections.unmodifiableList( localConsumers );

        // Start the connection
        this.connection.start();

        LOGGER.debug( "Created message subscriber {}, which is ready to receive subscriptions.", this );
    }

}
