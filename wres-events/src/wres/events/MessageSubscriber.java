package wres.events;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;

import javax.jms.BytesMessage;
import javax.jms.Connection;
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
import wres.events.MessagePublisher.MessageProperty;
import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.EvaluationStatus.CompletionStatus;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent.StatusMessageType;

/**
 * Registers a subscriber to a topic that is supplied on construction. There is one {@link Connection} per instance 
 * because connections are assumed to be expensive. Currently, there is also one {@link Session} per instance, but a 
 * pool of sessions might be better (to allow better message throughput, as a session is the work thread).
 * 
 * @param <T> the message type
 * @author james.brown@hydrosolved.com
 */

class MessageSubscriber<T> implements Closeable
{

    private static final Logger LOGGER = LoggerFactory.getLogger( MessageSubscriber.class );

    private static final String UNKNOWN = "unknown";

    /**
     * Is true to use durable subscribers, false for temporary subscribers, which are auto-deleted.
     */

    private static final boolean DURABLE_SUBSCRIBERS = true;

    /**
     * Maximum number of retries allowed.
     */

    private final int maximumRetries;

    /**
     * Actual number of retries attempted.
     */

    private final AtomicInteger retriesAttempted;

    /**
     * A connection to the broker for consumption of messages.
     */

    private final Connection consumerConnection;

    /**
     * A connection to the broker for publishing consumption status messages.
     */

    private final Connection producerConnection;

    /**
     * A session.
     */

    private final Session session;

    /**
     * A topic from which messages should be consumed.
     */

    private final Topic topic;

    /**
     * Consumer for evaluation status messages, which are used to trigger consumption of statistics groups.
     */

    private final NamedMessageConsumer statusConsumer;

    /**
     * A publisher for publishing messages related to the status of a consumer. 
     */

    private final MessagePublisher statusPublisher;

    /**
     * A list of all message consumers. These are resources to be closed.
     */

    private final List<NamedMessageConsumer> consumers;

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
     * The unique identifier of this consumer.
     */

    private final String identifier;

    /**
     * The message on which consumption failed, null if no failure.
     */

    private Message failure = null;

    /**
     * Flags when this consumer has completed consumption.
     */

    private final AtomicBoolean isComplete;

    /**
     * Is <code>true</code> if the subscriber failed after all attempts to recover.
     */

    private final AtomicBoolean isFailedUnrecoverably;

    /**
     * A function that retrieves the expected message count from an {@link EvaluationStatus} message for the type of
     * message consumed by this subscription.
     */

    private final ToIntFunction<EvaluationStatus> expectedMessageCountSupplier;

    /**
     * The expected message count across all consumers attached to this subscriber.
     */

    private final AtomicInteger expectedMessageCount;

    /**
     * The actual message count, which is incremented on each completed consumption. This is the actual account across
     * all consumers attached to this subscriber.
     */

    private final AtomicInteger actualMessageCount;

    /**
     * Is true if consumer messages should be acknowledged, but not consumed.
     */

    private final boolean isIgnoreConsumerMessages;

    /**
     * A description of this consumer.
     */

    private final wres.statistics.generated.Consumer consumerDescription;

    /**
     * A message to publish when group consumption is complete.
     */

    private final EvaluationStatus groupConsumptionComplete;

    /**
     * Returns true if this consumer has completed consumption, otherwise false.
     * 
     * @return true if consumption is complete
     */

    boolean isComplete()
    {
        return this.isComplete.get();
    }

    @Override
    public String toString()
    {
        return this.getIdentifier();
    }

    @Override
    public void close() throws IOException
    {
        LOGGER.debug( "Closing subscriber, {}.", this );

        // Check for group subscriptions that have not completed
        try
        {
            for ( Map.Entry<String, Queue<OneGroupConsumer<T>>> next : this.groupConsumers.entrySet() )
            {
                String groupId = next.getKey();
                Queue<OneGroupConsumer<T>> consumersToClose = next.getValue();

                Integer expectedCount = this.getEvaluationInfo()
                                            .getCompletionTracker()
                                            .getExpectedMessagesPerGroup( groupId );

                int total = 0;

                for ( OneGroupConsumer<T> consumer : consumersToClose )
                {
                    if ( Objects.nonNull( expectedCount ) && !consumer.hasBeenUsed() )
                    {
                        // Should not close until complete, unless already flagged as failed
                        if ( consumer.size() != expectedCount && !this.failed() )
                        {
                            LOGGER.error( "While attempting to gracefully close subscriber {}"
                                          + " , encountered an error. A consumer of grouped "
                                          + "messages attached to this subscription expected to "
                                          + "receive {} but had only received {} messages on "
                                          + "closing. A subscriber should not be "
                                          + "closed until consumption is complete, unless the "
                                          + "overall evaluation has failed.",
                                          this,
                                          expectedCount,
                                          consumer.size() );
                        }
                        // Complete
                        else if ( consumer.size() == expectedCount )
                        {
                            consumer.acceptGroup();
                            total++;
                        }
                    }
                }

                LOGGER.trace( "On closing subscriber {}, discovered {} consumers associated with group {} whose "
                              + "consumption was ready to complete, but had not yet completed. These were completed.",
                              this,
                              total,
                              groupId );

            }
        }
        finally
        {
            this.internalClose();

            LOGGER.debug( "Completed closure of subscriber {}", this );
        }
    }

    /**
     * Returns a message on which consumption failed, <code>null</code> if no failure occurred.
     * 
     * @return a message that was not consumed
     */

    Message getFirstFailure()
    {
        return this.failure;
    }

    /**
     * @return the subscriber identifier.
     */

    String getIdentifier()
    {
        return this.identifier;
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
         * Connection for consumption.
         */

        private Connection consumerConnection;

        /**
         * Connection for production.
         */

        private Connection producerConnection;

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
         * A function that retrieves the expected message count from an {@link EvaluationStatus} message.
         */

        private ToIntFunction<EvaluationStatus> expectedMessageCountSupplier;

        /**
         * Is true to ignore messages about consumers, false to consume them.
         */

        private boolean isIgnoreConsumerMessages = true;

        /**
         * A unique identifier for the subscriber. If one is not set, it will be assigned.
         */

        private String identifier;

        /**
         * Maximum retries allowed.
         */

        private int maximumRetries = 0;

        /**
         * Sets the connection for consumption.
         * 
         * @param consumerConnection the consumer connection
         * @return this builder
         */

        Builder<T> setConsumerConnection( Connection consumerConnection )
        {
            this.consumerConnection = consumerConnection;

            return this;
        }

        /**
         * Sets the connection for publication of status messages.
         * 
         * @param producerConnection the producer connection
         * @return this builder
         */

        Builder<T> setProducerConnection( Connection producerConnection )
        {
            this.producerConnection = producerConnection;

            return this;
        }

        /**
         * Sets a function that retrieves the expected message count from an {@link EvaluationStatus} message.
         * 
         * @param expectedMessageCountSupplier the expected message count retriever
         * @return this builder
         */

        Builder<T> setExpectedMessageCountSupplier( ToIntFunction<EvaluationStatus> expectedMessageCountSupplier )
        {
            this.expectedMessageCountSupplier = expectedMessageCountSupplier;

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
         * Set <code>true</code> to ignore messages about consumers, <code>false</code> to consider them. Unless this
         * subscriber is explicity tracking messages about consumers, this should be <code>false</code>.
         * 
         * @param isIgnoreConsumerMessages is true to ignore consumer messages
         * @return this builder
         */

        Builder<T> setIgnoreConsumerMessages( boolean isIgnoreConsumerMessages )
        {
            this.isIgnoreConsumerMessages = isIgnoreConsumerMessages;

            return this;
        }

        /**
         * Sets a unique identifier for the subscriber. If one is not set, it will be assigned automatically.
         * 
         * @param identifier the identifier
         * @return this builder
         */

        Builder<T> setIdentifier( String identifier )
        {
            this.identifier = identifier;

            return this;
        }

        /**
         * Sets the maximum number of retries.
         * 
         * @param maximumRetries the maximum number of retries
         * @return this builder
         */

        Builder<T> setMaximumRetries( int maximumRetries )
        {
            this.maximumRetries = maximumRetries;

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

    private List<NamedMessageConsumer> subscribeAllConsumers( List<Consumer<T>> consumers,
                                                              Function<ByteBuffer, T> mapper,
                                                              String evaluationId,
                                                              String context )
            throws JMSException
    {
        Objects.requireNonNull( consumers );
        Objects.requireNonNull( mapper );
        Objects.requireNonNull( evaluationId );

        List<NamedMessageConsumer> returnMe = new ArrayList<>();
        for ( Consumer<T> next : consumers )
        {
            NamedMessageConsumer consumer = this.subscribeOneConsumer( next, mapper, evaluationId, context );
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

    private List<NamedMessageConsumer> subscribeAllGroupedConsumers( List<Consumer<Collection<T>>> consumers,
                                                                     Function<ByteBuffer, T> mapper,
                                                                     String evaluationId,
                                                                     String context )
            throws JMSException
    {
        Objects.requireNonNull( consumers );
        Objects.requireNonNull( mapper );
        Objects.requireNonNull( evaluationId );

        List<NamedMessageConsumer> returnMe = new ArrayList<>();
        for ( Consumer<Collection<T>> next : consumers )
        {
            NamedMessageConsumer consumer = this.subscribeOneGroupedConsumer( next, mapper, evaluationId, context );
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

    private NamedMessageConsumer subscribeOneGroupedConsumer( Consumer<Collection<T>> innerSubscriber,
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
        return this.getConsumerForGroupedMessages( innerSubscriber,
                                                   mapper,
                                                   evaluationId,
                                                   context );
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

    private NamedMessageConsumer getConsumerForGroupedMessages( Consumer<Collection<T>> innerSubscriber,
                                                                Function<ByteBuffer, T> mapper,
                                                                String evaluationId,
                                                                String context )
            throws JMSException
    {
        Objects.requireNonNull( mapper );
        Objects.requireNonNull( evaluationId );

        // A listener acknowledges the message when the consumption completes
        // According to Section 8.7 of the JMS 2 specification, it is considered a programming error for the onMessage 
        // method associated with a MessageListener to throw a runtime exception. It should handle all exceptions. This 
        // implies a catch-all exception, which we want to avoid.
        // Instead, look for a ConsumerException and document that consumers should throw these.        
        MessageListener listener = message -> {

            // Throwing exceptions on the MessageListener::onMessage is considered a bug, so need to track status and 
            // exit gracefully when problems occur. The message is either delivered, the session is recovering or the
            // subscriber is unrecoverable.
            AtomicBoolean success = new AtomicBoolean();
            AtomicBoolean recovering = new AtomicBoolean();

            BytesMessage receivedBytes = (BytesMessage) message;
            String messageId = UNKNOWN;
            String correlationId = UNKNOWN;
            String groupId = UNKNOWN;

            try
            {
                // Only consume if subscriber is live
                if ( !this.failed() )
                {
                    messageId = message.getJMSMessageID();
                    correlationId = message.getJMSCorrelationID();
                    groupId = message.getStringProperty( MessageProperty.JMSX_GROUP_ID.toString() );

                    // This is a grouped message
                    if ( !UNKNOWN.equals( groupId ) )
                    {
                        // Create the byte array to hold the message
                        int messageLength = (int) receivedBytes.getBodyLength();

                        byte[] messageContainer = new byte[messageLength];

                        receivedBytes.readBytes( messageContainer );

                        ByteBuffer bufferedMessage = ByteBuffer.wrap( messageContainer );

                        T received = mapper.apply( bufferedMessage );

                        Queue<OneGroupConsumer<T>> subscribers = this.getGroupSubscriber( innerSubscriber,
                                                                                          groupId );

                        // Register the message with each grouped subscriber
                        String messageIdFinal = messageId;
                        subscribers.forEach( next -> next.accept( messageIdFinal, received ) );

                        // Complete the group if possible
                        EvaluationStatus status = this.getGroupStatus( groupId );
                        this.checkAndCompleteGroup( status );
                    }

                    success.set( true );

                    // Increment the actual message count
                    this.actualMessageCount.incrementAndGet();

                    LOGGER.debug( "Consumer {} successfully consumed a message with identifier {} and correlation "
                                  + "identifier {}.",
                                  this,
                                  messageId,
                                  correlationId );
                }

                // Acknowledge
                message.acknowledge();
            }
            catch ( JMSException | RuntimeException e )
            {
                // Messages are on the DLQ, but signal locally too
                if ( Objects.isNull( this.failure ) )
                {
                    this.failure = receivedBytes;
                }

                // Attempt to recover and flag recovery locally
                this.recover( messageId, correlationId, e );
                recovering.set( true );
            }
            finally
            {
                // A consumption failed and a recovery is not underway. Unrecoverable.
                if ( !success.get() && !recovering.get() )
                {
                    this.markSubscriberFailed();
                }

                // Check completion
                this.checkAndCompleteSubscription();
            }
        };

        // Only consume messages for the current evaluation based on JMSCorrelationID
        String selector = MessageProperty.JMS_CORRELATION_ID + "='" + evaluationId + "'";

        // Name the subscriber
        String name = this.getNextSubscriptionName( context );
        MessageConsumer messageConsumer = this.getConsumer( this.topic, selector, name );
        messageConsumer.setMessageListener( listener );

        LOGGER.debug( "Successfully registered a subscriber {} for grouped statistics messages associated with "
                      + "evaluation {}",
                      this,
                      evaluationId );

        return new NamedMessageConsumer( this.getEvaluationId(),
                                         name,
                                         messageConsumer,
                                         this.session,
                                         MessageSubscriber.DURABLE_SUBSCRIBERS );
    }

    /**
     * @return the status of the specified group.
     */

    private EvaluationStatus getGroupStatus( String groupId )
    {
        EvaluationStatus.Builder builder = EvaluationStatus.newBuilder()
                                                           .setGroupId( groupId );

        Integer count = this.getEvaluationInfo()
                            .getCompletionTracker()
                            .getExpectedMessagesPerGroup( groupId );

        if ( Objects.nonNull( count ) )
        {
            builder.setMessageCount( count );
        }

        return builder.build();
    }

    /**
     * Creates a queue of grouped subscriptions. Returns a mutable queue, so do not expose more generally.
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

    private NamedMessageConsumer subscribeOneConsumer( Consumer<T> subscriber,
                                                       Function<ByteBuffer, T> mapper,
                                                       String evaluationId,
                                                       String context )
            throws JMSException
    {
        Objects.requireNonNull( subscriber );
        Objects.requireNonNull( mapper );
        Objects.requireNonNull( evaluationId );

        // Only consume messages for the current evaluation based on JMSCorrelationID
        String selector = MessageProperty.JMS_CORRELATION_ID + "='" + evaluationId + "'";

        // This resource needs to be kept open until consumption is done
        // Name the subscriber
        String name = this.getNextSubscriptionName( context );
        MessageConsumer consumer = this.getConsumer( this.topic, selector, name );

        // A listener acknowledges the message when the consumption completes       
        MessageListener listener = message -> {

            // Throwing exceptions on the MessageListener::onMessage is considered a bug, so need to track status and 
            // exit gracefully when problems occur. The message is either delivered, the session is recovering or the
            // subscriber is unrecoverable.
            AtomicBoolean success = new AtomicBoolean();
            AtomicBoolean recovering = new AtomicBoolean();

            BytesMessage receivedBytes = (BytesMessage) message;
            String messageId = UNKNOWN;
            String correlationId = UNKNOWN;
            String consumerId = UNKNOWN;

            try
            {
                // Only consume if subscriber is live
                if ( !this.failed() )
                {
                    messageId = message.getJMSMessageID();
                    correlationId = message.getJMSCorrelationID();
                    consumerId = message.getStringProperty( MessageProperty.CONSUMER_ID.toString() );

                    // Do not consume messages about subscribers unless this subscriber is explicitly tracking subscriber 
                    // messages
                    if ( Objects.isNull( consumerId ) || !this.isIgnoreConsumerMessages() )
                    {

                        // Create the byte array to hold the message
                        int messageLength = (int) receivedBytes.getBodyLength();

                        byte[] messageContainer = new byte[messageLength];

                        receivedBytes.readBytes( messageContainer );

                        ByteBuffer bufferedMessage = ByteBuffer.wrap( messageContainer );

                        T received = mapper.apply( bufferedMessage );

                        subscriber.accept( received );

                        // Increment the actual message count
                        this.actualMessageCount.incrementAndGet();

                        LOGGER.debug( "Consumer {} successfully consumed a message with identifier {} and correlation "
                                      + "identifier {}.",
                                      this,
                                      messageId,
                                      correlationId );
                    }

                    success.set( true );
                }

                // Acknowledge
                message.acknowledge();
            }
            catch ( JMSException | RuntimeException e )
            {
                // Messages are on the DLQ, but signal locally too
                if ( Objects.isNull( this.failure ) )
                {
                    this.failure = receivedBytes;
                }

                // Attempt to recover and flag recovery locally
                this.recover( messageId, correlationId, e );
                recovering.set( true );
            }
            finally
            {
                // A consumption failed and a recovery is not underway. Unrecoverable.
                if ( !success.get() && !recovering.get() )
                {
                    this.markSubscriberFailed();
                }

                // Check completion
                this.checkAndCompleteSubscription();
            }
        };

        consumer.setMessageListener( listener );

        LOGGER.debug( "Successfully registered consumer {} for evaluation {}.", this, evaluationId );

        return new NamedMessageConsumer( evaluationId,
                                         name,
                                         consumer,
                                         this.session,
                                         MessageSubscriber.DURABLE_SUBSCRIBERS );
    }

    /**
     * Checks for a complete group and finalizes it.
     * 
     * @param status the status message with the group count information
     * @return the number of consumers completed
     */

    private int checkAndCompleteGroup( EvaluationStatus status )
    {
        int completed = 0;

        String groupId = status.getGroupId();

        if ( Objects.nonNull( this.groupConsumers ) && this.groupConsumers.containsKey( groupId ) )
        {
            Queue<OneGroupConsumer<T>> check = this.groupConsumers.get( groupId );

            EvaluationStatus complete = this.groupConsumptionComplete.toBuilder()
                                                                     .setGroupId( groupId )
                                                                     .build();

            ByteBuffer buffer = ByteBuffer.wrap( complete.toByteArray() );

            int usedCount = 0;

            for ( OneGroupConsumer<T> next : check )
            {
                Integer expected = this.getExpectedGroupMessageCount( status );

                if ( Objects.nonNull( expected ) && expected == next.size() && !next.hasBeenUsed() )
                {
                    next.acceptGroup();
                    completed++;
                }

                if ( next.hasBeenUsed() )
                {
                    usedCount++;
                }
            }

            // Notify completion
            if ( usedCount == check.size() )
            {
                String messageId = "ID:" + this.getIdentifier() + groupId;
                this.publishStatusInternal( buffer,
                                            messageId,
                                            this.groupConsumptionComplete.getCompletionStatus() );
            }
        }

        return completed;
    }

    /**
     * @return the expected group message count.
     */

    private Integer getExpectedGroupMessageCount( EvaluationStatus status )
    {
        // Look inside the status tracker, which is updated out-of-band
        Integer count = this.evaluationInfo.getCompletionTracker()
                                           .getExpectedMessagesPerGroup( status.getGroupId() );

        // Look inside the message
        if ( Objects.isNull( count ) )
        {
            count = status.getMessageCount();
        }

        return count;
    }

    /**
     * Internally closes resources.
     */

    private void internalClose()
    {
        // Close and remove durable subscriptions, where applicable
        for ( NamedMessageConsumer next : this.consumers )
        {
            try
            {
                next.close();
            }
            catch ( IOException e )
            {
                LOGGER.warn( "Encountered an error while attempting to close registered consumer {} within "
                             + "subscriber {}: {}",
                             next.name,
                             this,
                             e.getMessage() );
            }
        }

        try
        {
            if ( Objects.nonNull( this.statusConsumer ) )
            {
                this.statusConsumer.close();
            }
        }
        catch ( IOException e )
        {
            LOGGER.warn( "Encountered an error while attempting to close a registered consumer of evaluation status "
                         + "messages within subscriber {}: {}",
                         this,
                         e.getMessage() );
        }

        try
        {
            if ( Objects.nonNull( this.statusPublisher ) )
            {
                this.statusPublisher.close();
            }
        }
        catch ( IOException e )
        {
            LOGGER.warn( "Encountered an error while attempting to close a registered publisher of evaluation status "
                         + "messages within subscriber {}: {}",
                         this,
                         e.getMessage() );
        }

        try
        {
            this.session.close();
        }
        catch ( JMSException e )
        {
            LOGGER.warn( "Encountered an error while attempting to close a broker session within subscriber {}: {}",
                         this,
                         e.getMessage() );
        }

        // Do not close connections as they may be re-used.
    }

    /**
     * Returns the evaluation information attached to this subscriber.
     * 
     * @return the evaluation information
     */

    private EvaluationInfo getEvaluationInfo()
    {
        return this.evaluationInfo;
    }

    /**
     * Returns the evaluation identifier to which this subscriber subscribes.
     * 
     * @return the evaluation identifier
     */

    private String getEvaluationId()
    {
        return this.getEvaluationInfo().getEvaluationId();
    }

    /**
     * Returns a consumer.
     * 
     * @param topic the topic
     * @param selector the message selector
     * @param name the name of the subscriber
     * @return a consumer
     * @throws JMSException if the consumer could not be created for any reason
     */

    private MessageConsumer getConsumer( Topic topic, String selector, String name ) throws JMSException
    {
        if ( MessageSubscriber.DURABLE_SUBSCRIBERS )
        {
            return this.session.createDurableSubscriber( topic, name, selector, false );
        }

        return this.session.createConsumer( topic, selector );
    }

    /**
     * Returns the next available subscription name.
     * 
     * @param prepend a string to prepend to the subscription name
     * @return the next available subscription name
     */

    private String getNextSubscriptionName( String prepend )
    {
        // For a non-durable subscriber, use: "return this.session.createConsumer( topic, selector );"
        // Get a unique subscriber name, using the method in the evaluation class
        String uniqueId = this.getEvaluationId() + "-"
                          + this.getIdentifier()
                          + "-c"
                          + this.getEvaluationInfo().getNextQueueNumber();

        return prepend + "-" + uniqueId;
    }

    /**
     * Registers the current consumer by publishing an evaluation status message indicating that the consumer is ready
     * to consume.
     */

    private void registerThisConsumer()
    {
        // Create a message identifier 
        String messageId = "ID:" + this.getIdentifier() + "-start";

        EvaluationStatus ready = EvaluationStatus.newBuilder()
                                                 .setCompletionStatus( CompletionStatus.READY_TO_CONSUME )
                                                 .setConsumer( wres.statistics.generated.Consumer.newBuilder()
                                                                                                 .setConsumerId( this.getIdentifier() ) )
                                                 .build();

        ByteBuffer message = ByteBuffer.wrap( ready.toByteArray() );

        this.publishStatusInternal( message, messageId, CompletionStatus.READY_TO_CONSUME );
    }

    /**
     * Publishes a message about the status of this subscriber.
     * 
     * @param message the message
     * @param messageId the message identifier
     * @param status the status to help with logging
     */

    private void publishStatusInternal( ByteBuffer message,
                                        String messageId,
                                        CompletionStatus status )
    {
        // Create the metadata
        String evaluationId = this.getEvaluationId();
        Map<MessageProperty, String> properties = new EnumMap<>( MessageProperty.class );
        properties.put( MessageProperty.JMS_MESSAGE_ID, messageId );
        properties.put( MessageProperty.JMS_CORRELATION_ID, evaluationId );
        properties.put( MessageProperty.CONSUMER_ID, this.getIdentifier() );

        try
        {
            this.statusPublisher.publish( message, Collections.unmodifiableMap( properties ) );

            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "Published an evaluation status message with metadata {} for "
                              + "evaluation {} with status {} to amq.topic/{}.",
                              properties,
                              evaluationId,
                              status,
                              Evaluation.EVALUATION_STATUS_QUEUE );
            }
        }
        catch ( JMSException e )
        {
            throw new EvaluationEventException( "Failed to publish an evaluation status message with metadata "
                                                + properties
                                                + " for evaluation "
                                                + evaluationId
                                                + " from subscriber "
                                                + this
                                                + " to amq.topic/"
                                                + Evaluation.EVALUATION_STATUS_QUEUE
                                                + ".",
                                                e );
        }
    }

    /**
     * Returns true if messages with consumer identifiers (i.e., updates about consumers) should be ignored. In general,
     * consumer messages are ignored unless this subscriber is intended to explicitly track them.
     * 
     * @return true if consumer messages should be ignored
     */

    private boolean isIgnoreConsumerMessages()
    {
        return this.isIgnoreConsumerMessages;
    }

    /**
     * <p>Checks whether this subscription is complete. The subscription is complete when the {@link expectedMessageCount} 
     * has been determined and the {@link actualMessageCount} equals the {@link expectedMessageCount}. If complete, 
     * sends a message indicating completion but does not close the subscription.
     * 
     * <p> For context, read {@link Evaluation#await()}. This method reports its completion status with limited scope. 
     * Specifically, it reports complete when all underlying consumers have received all messages. It makes no
     * guarantees that any work attached to that consumption, such as a delayed write, has completed. If this limited 
     * guarantee is insufficient for any inner consumer, then it should probably register as an "external subscriber".
     * An external subscriber is responsible for reporting its own lifecycle, including its completion status.
     */

    private void checkAndCompleteSubscription()
    {
        // Failed or not already completed and all messaging done? If so, close.
        if ( this.failed() || !this.isComplete() && this.expectedMessageCount.get() > -1
                              && this.expectedMessageCount.get() == this.actualMessageCount.get() )
        {
            // Send the status message only once
            if ( !this.isComplete() )
            {
                String messageId = "ID:" + this.getIdentifier() + "-stop";

                List<EvaluationStatusEvent> events = new ArrayList<>();
                CompletionStatus status = CompletionStatus.CONSUMPTION_COMPLETE_REPORTED_SUCCESS;

                // If any one (inner) consumer fails, the whole subscriber fails 
                if ( this.failed() )
                {
                    String message =
                            "While consuming a message for evaluation " + this.getEvaluationId()
                                     + " with subscriber "
                                     + this.getIdentifier()
                                     + " encountered an error. Failed to consume the following message: "
                                     + this.getFirstFailure()
                                     + ".";
                    status = CompletionStatus.CONSUMPTION_COMPLETE_REPORTED_FAILURE;
                    EvaluationStatusEvent event = EvaluationStatusEvent.newBuilder()
                                                                       .setEventType( StatusMessageType.ERROR )
                                                                       .setEventMessage( message )
                                                                       .build();
                    events.add( event );
                }

                EvaluationStatus ready = EvaluationStatus.newBuilder()
                                                         .setCompletionStatus( status )
                                                         .setConsumer( wres.statistics.generated.Consumer.newBuilder()
                                                                                                         .setConsumerId( this.getIdentifier() ) )
                                                         .addAllStatusEvents( events )
                                                         .build();

                ByteBuffer message = ByteBuffer.wrap( ready.toByteArray() );

                this.publishStatusInternal( message, messageId, status );
            }

            // Complete
            this.isComplete.set( true );
        }
    }

    /**
     * Returns <code>true</code> if an unrecoverable exception occurred during consumption, otherwise 
     * <code>false</code> . Note that {@link #getFirstFailure()} may contain an exception that was recovered.
     * 
     * @return true if an unrecoverable exception occurred.
     */

    boolean failed()
    {
        return this.isFailedUnrecoverably.get();
    }

    /**
     * Registers a listener for evaluation status messages.
     * 
     * @param statusConsumer the status consumer
     * @throws JMSException if the listener could not be registered for any reason
     */

    private void registerEvaluationStatusListener( MessageConsumer statusConsumer ) throws JMSException
    {
        String evaluationId = this.getEvaluationId();

        // Now listen for status messages when a group completes
        MessageListener listener = message -> {

            // Throwing exceptions on the MessageListener::onMessage is considered a bug, so need to track status and 
            // exit gracefully when problems occur. The message is either delivered, the session is recovering or the
            // subscriber is unrecoverable.
            AtomicBoolean success = new AtomicBoolean();
            AtomicBoolean recovering = new AtomicBoolean();

            BytesMessage receivedBytes = (BytesMessage) message;
            String messageId = UNKNOWN;
            String correlationId = UNKNOWN;

            try
            {
                // Only consume if the subscriber is live
                if ( !this.failed() )
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
                    switch ( status.getCompletionStatus() )
                    {
                        case GROUP_PUBLICATION_COMPLETE:
                            this.evaluationInfo.getCompletionTracker()
                                               .registerPublicationComplete( status );

                            this.checkAndCompleteGroup( status );
                            break;
                        case PUBLICATION_COMPLETE_REPORTED_SUCCESS:
                            this.setExpectedMessageCount( status,
                                                          evaluationId,
                                                          messageId,
                                                          correlationId );
                            break;
                        default:
                            break;
                    }

                    success.set( true );
                }

                // Acknowledge
                message.acknowledge();
            }
            catch ( JMSException | InvalidProtocolBufferException | RuntimeException e )
            {
                // Messages are on the DLQ, but signal locally too
                if ( Objects.isNull( this.failure ) )
                {
                    this.failure = receivedBytes;
                }

                // Attempt to recover and flag recovery locally
                this.recover( messageId, correlationId, e );
                recovering.set( true );
            }
            finally
            {
                // A consumption failed and a recovery is not underway. Unrecoverable.
                if ( !success.get() && !recovering.get() )
                {
                    this.markSubscriberFailed();
                }

                // Check completion
                this.checkAndCompleteSubscription();
            }
        };

        statusConsumer.setMessageListener( listener );

        LOGGER.debug( "Successfully registered a subscriber {} for grouped statistics messages associated with "
                      + "evaluation {}",
                      this,
                      evaluationId );
    }

    /**
     * <p>Attempts to recover the session up to the {@link #MAXIMUM_RETRIES}.
     * 
     * <p>TODO: Retries happen per message. Thus, for example, all graphics formats will be retried when any one format 
     * fails. This may in turn generate a different exception on attempting to overwrite. Thus, when the writing fails
     * for any one format, the consumer should be considered exceptional for all formats and the consumer should 
     * clean-up after itself (deleting paths written for all formats), ready for the next retry. Else, the consumer
     * should track what succeeded and failed and only retry the things that failed.
     * 
     * @param messageId the message identifier for the exceptional consumption
     * @param correlationId the correlation identifier for the exceptional consumption
     * @param exception the exception encountered
     */

    private void recover( String messageId, String correlationId, Exception e )
    {
        // Only try to recover if an evaluation hasn't already failed
        if ( !this.failed() )
        {
            try ( StringWriter sw = new StringWriter();
                  PrintWriter pw = new PrintWriter( sw ); )
            {
                // Create a stack trace to log
                e.printStackTrace( pw );
                String message = sw.toString();

                // Attempt recovery in order to cycle the delivery attempts. When the maximum is reached, poison
                // messages should hit the dead letter queue/DLQ
                LOGGER.error( "While attempting to consume a message with identifier {} and correlation identifier "
                              + "{} in subscriber {}, encountered an error. This is {} of {} allowed consumption "
                              + "failures before the subscriber will notify an unrecoverable failure for "
                              + "evaluation {}. The error is: {}",
                              messageId,
                              correlationId,
                              this.getIdentifier(),
                              this.getNumberOfRetriesAttempted().get() + 1, // Counter starts at zero
                              this.getMaximumRetries(),
                              correlationId,
                              message );

                this.session.recover();
            }
            catch ( JMSException f )
            {
                LOGGER.error( "While attempting to recover a session for evaluation {} in subscriber {}, encountered "
                              + "an error that prevented recovery: ",
                              correlationId,
                              this.getIdentifier(),
                              f.getMessage() );
            }
            catch ( IOException g )
            {
                LOGGER.error( "While attempting recovery in subscriber {}, failed to close an exception "
                              + "writer.",
                              this.getIdentifier() );
            }
        }

        // Stop if the maximum number of retries has been reached
        if ( this.getNumberOfRetriesAttempted()
                 .incrementAndGet() == this.getMaximumRetries() )
        {

            LOGGER.error( "Subscriber {} encountered a consumption failure for evaluation {}. Recovery failed "
                          + "after {} attempts.",
                          this.getIdentifier(),
                          correlationId,
                          this.getMaximumRetries() );

            // Register the subscriber as failed
            this.markSubscriberFailed();
        }
    }

    /**
     * @return the number of retries attempted so far.
     */

    private AtomicInteger getNumberOfRetriesAttempted()
    {
        return this.retriesAttempted;
    }

    /**
     * @return the number of retries allowed.
     */

    private int getMaximumRetries()
    {
        return this.maximumRetries;
    }

    /**
     * Marks the subscriber as failed after all recovery attempts.
     */

    private void markSubscriberFailed()
    {
        this.isFailedUnrecoverably.set( true );
    }

    /**
     * Sets the expected message count.
     * 
     * @param status the evaluation status message
     * @param evaluationId the evaluation identifier
     * @param messageId the message identifier
     * @param correlationId the correlation identifier
     */

    private void setExpectedMessageCount( EvaluationStatus status,
                                          String evaluationId,
                                          String messageId,
                                          String correlationId )
    {
        // Set the expected number of messages in total
        int expectedCount = this.expectedMessageCountSupplier.applyAsInt( status );

        // Multiply by the number of consumers attached to this subscriber
        int total = expectedCount * this.consumers.size();
        this.expectedMessageCount.set( total );

        LOGGER.debug( "While consuming evaluation status messages for evaluation {} and subscriber {}, encountered a "
                      + "message with identifier {} and correlation identifier {} and completion state {} that "
                      + "contained the expected message count of {}. This is the expected "
                      + "number of messages for each of {} consumers attached to this subscriber. The overall expected "
                      + "message count is, therefore, {}.",
                      evaluationId,
                      this,
                      messageId,
                      correlationId,
                      status.getCompletionStatus(),
                      expectedCount,
                      this.consumers.size(),
                      total );
    }

    /**
     * Hidden constructor.
     * 
     * @param <T> the type of message
     * @param builder the builder
     * @throws JMSException if the JMS provider fails to create the connection due to some internal error
     * @throws JMSSecurityException if client authentication fails
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if any input is invalid
     */

    private MessageSubscriber( Builder<T> builder )
            throws JMSException
    {
        // Set then validate
        String localIdentifier = builder.identifier;
        if ( Objects.isNull( localIdentifier ) )
        {
            localIdentifier = Evaluation.getUniqueId();
        }

        this.consumerDescription = wres.statistics.generated.Consumer.newBuilder()
                                                                     .setConsumerId( localIdentifier )
                                                                     .build();

        this.groupConsumptionComplete = EvaluationStatus.newBuilder()
                                                        .setCompletionStatus( CompletionStatus.GROUP_CONSUMPTION_COMPLETE )
                                                        .setConsumer( this.consumerDescription )
                                                        .build();

        this.identifier = localIdentifier;
        this.topic = builder.topic;
        this.evaluationInfo = builder.evaluationInfo;
        this.actualMessageCount = new AtomicInteger();
        this.retriesAttempted = new AtomicInteger();
        // Register the initial expectation as -1, because zero is a reasonable expectation too
        this.expectedMessageCount = new AtomicInteger( -1 );
        this.isComplete = new AtomicBoolean();
        this.isFailedUnrecoverably = new AtomicBoolean();
        this.expectedMessageCountSupplier = builder.expectedMessageCountSupplier;
        this.isIgnoreConsumerMessages = builder.isIgnoreConsumerMessages;
        this.consumerConnection = builder.consumerConnection;
        this.producerConnection = builder.producerConnection;
        this.maximumRetries = builder.maximumRetries;

        Topic statusTopic = builder.statusTopic;
        String context = builder.context;
        Function<ByteBuffer, T> mapper = builder.mapper;
        List<Consumer<T>> subscribers = builder.subscribers;
        List<Consumer<Collection<T>>> groupSubscribers = builder.groupSubscribers;

        String evaluationId = this.getEvaluationId();

        Objects.requireNonNull( this.topic, "Topic information required." );
        Objects.requireNonNull( this.evaluationInfo, "Evaluation information required." );
        Objects.requireNonNull( this.expectedMessageCountSupplier, "Expected message count supplier required." );
        Objects.requireNonNull( this.consumerConnection, "Connection for consumers required." );
        Objects.requireNonNull( this.producerConnection, "Connection for status messages requried." );

        Objects.requireNonNull( mapper );
        Objects.requireNonNull( subscribers );
        Objects.requireNonNull( statusTopic,
                                "Set the evaluation status topic for consumer " + this
                                             + ", which is "
                                             + "needed to report on consumption status." );

        if ( this.maximumRetries < 0 )
        {
            throw new IllegalArgumentException( "The maximum number of retries must be a positive integer: "
                                                + this.maximumRetries
                                                + "." );
        }

        // Register a listener on each connection for exceptions
        this.consumerConnection.setExceptionListener( new ConnectionExceptionListener( this.getIdentifier() ) );
        this.producerConnection.setExceptionListener( new ConnectionExceptionListener( this.getIdentifier() ) );

        // Client acknowledges
        this.session = this.consumerConnection.createSession( false, Session.CLIENT_ACKNOWLEDGE );

        String selector = MessageProperty.JMS_CORRELATION_ID + "='" + evaluationId + "'";
        String statusContext = Evaluation.EVALUATION_STATUS_QUEUE + "-HOUSEKEEPING-evaluation-status";

        // Name the subscriber
        String name = this.getNextSubscriptionName( statusContext );
        MessageConsumer localStatusConsumer = this.getConsumer( statusTopic, selector, name );
        this.registerEvaluationStatusListener( localStatusConsumer );

        this.statusConsumer = new NamedMessageConsumer( evaluationId,
                                                        name,
                                                        localStatusConsumer,
                                                        this.session,
                                                        MessageSubscriber.DURABLE_SUBSCRIBERS );

        this.groupConsumers = new ConcurrentHashMap<>();

        // Add subscriptions
        List<NamedMessageConsumer> localConsumers = new ArrayList<>();

        if ( !subscribers.isEmpty() )
        {
            List<NamedMessageConsumer> someConsumers = this.subscribeAllConsumers( subscribers,
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

            List<NamedMessageConsumer> moreConsumers = this.subscribeAllGroupedConsumers( groupSubscribers,
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

        // Create the publisher for status messages
        this.statusPublisher = MessagePublisher.of( this.producerConnection, statusTopic );

        // Start the connections
        this.consumerConnection.start();
        this.producerConnection.start();

        // Notify status as ready to consume if there are consumers attached to this subscriber, other than the
        // status consumer
        if ( !this.consumers.isEmpty() )
        {
            this.registerThisConsumer();
        }

        LOGGER.debug( "Created message subscriber {}, which is ready to receive subscriptions.", this );
    }

    /**
     * Collects a consumer and its name, in order to remove a subscription when complete.
     */

    private static class NamedMessageConsumer implements Closeable
    {

        /** The consumer.**/
        private final MessageConsumer consumer;

        /** The subscription name.**/
        private final String name;

        /** The evaluation identifier.**/
        private final String evaluationId;

        /** The session.**/
        private final Session session;

        /**Is true if the consumer refers to a durable subscriber.**/
        private final boolean isDurable;

        /**
         * Create an instance.
         * 
         * @param evaluationId the evaluation identifier
         * @param name the subscription name
         * @param consumer the consumer
         * @param session the session
         * @throws NullPointerException if any input is null
         */

        private NamedMessageConsumer( String evaluationId,
                                      String name,
                                      MessageConsumer consumer,
                                      Session session,
                                      boolean isDurable )
        {
            Objects.requireNonNull( name );
            Objects.requireNonNull( consumer );
            Objects.requireNonNull( session );
            Objects.requireNonNull( evaluationId );

            this.name = name;
            this.consumer = consumer;
            this.evaluationId = evaluationId;
            this.session = session;
            this.isDurable = isDurable;
        }

        @Override
        public void close() throws IOException
        {
            LOGGER.debug( "Closing and then unsubscribing consumer {} for evaluation {}.",
                          this.name,
                          this.evaluationId );

            try
            {
                // Close first according to docs
                this.consumer.close();

                // Then unsubscribe if the subscriber is durable
                if ( this.isDurable )
                {
                    this.session.unsubscribe( this.name );
                }
            }
            catch ( JMSException e )
            {
                throw new IOException( "Unable to close message consumer " + this.name
                                       + " for evaluation "
                                       + this.evaluationId,
                                       e );
            }
        }
    }

}
