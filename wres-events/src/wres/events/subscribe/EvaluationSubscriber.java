package wres.events.subscribe;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.Topic;
import javax.naming.NamingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

import wres.events.EvaluationEventException;
import wres.events.publish.MessagePublisher;
import wres.events.publish.MessagePublisher.MessageProperty;
import wres.eventsbroker.BrokerConnectionFactory;
import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.Statistics;
import wres.statistics.generated.Consumer;
import wres.statistics.generated.Consumer.Format;
import wres.statistics.generated.EvaluationStatus.CompletionStatus;
import wres.statistics.generated.Evaluation;

/**
 * <p>Abstracts a subscription to evaluation messages. A subscriber contains one {@link EvaluationConsumer} for each 
 * evaluation in progress, which is mapped against its unique evaluation identifier. The {@link EvaluationConsumer} 
 * consumes all of the messages related to one evaluation and the subscriber ensures that these messages are routed to 
 * the correct consumer. It also handles retries and other administrative tasks that satisfy the contract for a 
 * well-behaving subscriber. Specifically, a well-behaving subscriber:
 * 
 * <ol>
 * <li>Notifies any listening clients with an {@link EvaluationStatus} message that contains
 * {@link CompletionStatus#READY_TO_CONSUME} and the formats fulfilled by the subscriber.</li>
 * <li>Notifies the {@link EvaluationConsumer} when an evaluation fails unrecoverably. The consumer then reports on the 
 * failure to all listening clients with a {@link CompletionStatus#CONSUMPTION_COMPLETE_REPORTED_FAILURE}.</li>
 * <li>Notifies the {@link EvaluationConsumer} of every open evaluation when the subscriber fails unrecoverably. Each 
 * consumer then reports on the failure to all listening clients with a
 * {@link CompletionStatus#CONSUMPTION_COMPLETE_REPORTED_FAILURE}. This notification is attempted, but cannot be 
 * guaranteed as the subscriber may be in a state that prevents such notification.</li>
 * </ol>
 * 
 * <p>When an evaluation succeeds, the {@link EvaluationConsumer} reports on its success to all listening clients with 
 * a {@link CompletionStatus#CONSUMPTION_COMPLETE_REPORTED_SUCCESS}.  
 * 
 * @author james.brown@hydrosolved.com
 */

public class EvaluationSubscriber implements Closeable
{

    private static final String EVALUATION = "evaluation ";

    private static final String SUBSCRIBER_HAS_CLAIMED_OWNERSHIP_OF_MESSAGE_FOR_EVALUATION =
            "Subscriber {} has claimed ownership of message {} for evaluation {}.";

    private static final String ENCOUNTERED_AN_ERROR_WHILE_ATTEMPTING_TO_REMOVE_A_DURABLE_SUBSCRIPTION_FOR =
            "Encountered an error while attempting to remove a durable subscription for ";

    private static final Logger LOGGER = LoggerFactory.getLogger( EvaluationSubscriber.class );

    private static final String ACKNOWLEDGED_MESSAGE_WITH_CORRELATION_ID =
            "Acknowledged message {} with correlationId {}.";

    private static final String UNKNOWN = "unknown";

    /**
     * Is true to use durable subscribers, false for temporary subscribers, which are auto-deleted.
     */

    private static final boolean DURABLE_SUBSCRIBERS = true;

    /**
     * Default name for the queue on the amq.topic that accepts evaluation status messages.
     */

    private static final String EVALUATION_STATUS_QUEUE = "status";

    /**
     * Default name for the queue on the amq.topic that accepts evaluation status messages.
     */

    private static final String EVALUATION_QUEUE = "evaluation";

    /**
     * Default name for the queue on the amq.topic that accepts evaluation status messages.
     */

    private static final String STATISTICS_QUEUE = "statistics";

    /**
     * String representation of the {@link MessageProperty#CONSUMER_ID}.
     */

    private static final String CONSUMER_ID_STRING = MessageProperty.CONSUMER_ID.toString();

    /**
     * String representation of the {@link MessageProperty#GROUP_ID}.
     */

    private static final String GROUP_ID_STRING = MessageProperty.JMSX_GROUP_ID.toString();

    /**
     * Re-usable status message.
     */

    private final EvaluationStatus readyToConsume;

    /**
     * The formats supported as strings.
     */

    private final Collection<String> formatStrings;

    /**
     * A publisher for {@link EvaluationStatus} messages.
     */

    private final MessagePublisher evaluationStatusPublisher;

    /**
     * Status message consumer.
     */

    private final MessageConsumer evaluationStatusConsumer;

    /**
     * Evaluation message consumer.
     */

    private final MessageConsumer evaluationConsumer;

    /**
     * Statistics message consumer.
     */

    private final MessageConsumer statisticsConsumer;

    /**
     * An evaluation status topic.
     */

    private final Topic evaluationStatusTopic;

    /**
     * An evaluation topic.
     */

    private final Topic evaluationTopic;

    /**
     * A statistics topic.
     */

    private final Topic statisticsTopic;

    /**
     * A session for evaluation messages, other than statistics messages (see {@link #statisticsSession}).
     */

    private final Session session;

    /**
     * A session for statistics messages.
     */

    private final Session statisticsSession;

    /**
     * A consumer connection for all messages except statistics messages (see also 
     * {@link #statisticsConsumerConnection}).
     */

    private final Connection consumerConnection;

    /**
     * A statistics consumer connection.
     */

    private final Connection statisticsConsumerConnection;

    /**
     * A connection for publishing status messages.
     */

    private final Connection publisherConnection;

    /**
     * Broker connections.
     */

    private final BrokerConnectionFactory broker;

    /**
     * Client status.
     */

    private final SubscriberStatus status;

    /**
     * The evaluations by unique id.
     */

    private final Map<String, EvaluationConsumer> evaluations;

    /**
     * Actual number of retries attempted per evaluation.
     */

    private final Map<String, AtomicInteger> retriesAttempted;

    /**
     * An executor service for writing work.
     */

    private final ExecutorService executorService;

    /**
     * A lock that guards the evaluations. This is needed to sweep evaluations that have been completed, otherwise
     * the map will grow infinitely.
     */

    private final ReentrantLock evaluationsLock = new ReentrantLock();

    /**
     * Is <code>true</code> is the subscriber has failed unrecoverably.
     */

    private final AtomicBoolean isFailedUnrecoverably;

    /**
     * The factory that supplies consumers for evaluations.
     */

    private final ConsumerFactory consumerFactory;

    /**
     * Creates an instance.
     * 
     * @param consumerFactory the consumer factory
     * @param executorService the executor
     * @param broker the broker connection factory
     * @return a subscriber instance
     * @throws NullPointerException if any input is null
     * @throws UnrecoverableSubscriberException if the subscriber cannot be instantiated for any other reason
     */

    public static EvaluationSubscriber of( ConsumerFactory consumerFactory,
                                           ExecutorService executorService,
                                           BrokerConnectionFactory broker )
    {
        return new EvaluationSubscriber( consumerFactory, executorService, broker );
    }

    @Override
    public void close()
    {
        // Log an error if there are open evaluations
        if ( this.hasOpenEvaluations() )
        {
            Set<String> open = this.evaluations.entrySet()
                                               .stream()
                                               .filter( next -> !next.getValue().isComplete() )
                                               .map( Map.Entry::getKey )
                                               .collect( Collectors.toSet() );

            LOGGER.error( "While closing subscriber {}, {} open evaluations were discovered: {}. These "
                          + "evaluations will not be notified complete. They should be notified before a "
                          + "subscriber is closed.",
                          this.getSubscriberId(),
                          open.size(),
                          open );
        }

        // Durable subscriptions are removed if all evaluations succeeded
        this.closeSubscriptions();

        // No need to close any other pubs/subs or sessions (according to the JMS documentation of Connection::close).

        String message = "Encountered an error while attempting to close a broker connection within "
                         + "subscriber "
                         + this.getSubscriberId()
                         + ".";

        try
        {
            this.consumerConnection.close();
        }
        catch ( JMSException e )
        {
            LOGGER.error( message, e );
        }

        try
        {
            this.statisticsConsumerConnection.close();
        }
        catch ( JMSException e )
        {
            LOGGER.error( message, e );
        }

        try
        {
            this.publisherConnection.close();
        }
        catch ( JMSException e )
        {
            LOGGER.error( message, e );
        }

        // This subscriber is not responsible for closing the broker.
    }

    /**
     * Maintenance task that removes completed evaluations from the cache.
     */

    public void sweep()
    {
        // Lock for sweeping
        this.getEvaluationsLock()
            .lock();

        // Find the evaluations to sweep
        Set<String> completed = new HashSet<>();

        // Create an independent set to sweep as this is a mutating loop
        Set<String> toSweep = new HashSet<>( this.evaluations.keySet() );
        for ( String nextEvaluation : toSweep )
        {
            EvaluationConsumer nextValue = this.evaluations.get( nextEvaluation );
            if ( nextValue.isComplete() )
            {
                this.evaluations.remove( nextEvaluation );
                this.retriesAttempted.remove( nextEvaluation );
            }
        }

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "The sweeper for subscriber {} removed {} completed evaluations, including {}.",
                          this.getSubscriberId(),
                          completed.size(),
                          completed );
        }

        this.getEvaluationsLock()
            .unlock();
    }

    /**
     * Sends an evaluation status message with a status of {@link CompletionStatus#READY_TO_CONSUME} for each open
     * evaluation so that any listening listening client knows that the subscriber is still alive.
     * @throws EvaluationEventException if the notification failed
     */

    public void notifyAlive()
    {
        // Iterate the evaluations
        for ( Map.Entry<String, EvaluationConsumer> next : this.evaluations.entrySet() )
        {
            // Consider only open evaluations
            if ( !next.getValue().isComplete() )
            {
                this.publishReadyToConsume( next.getKey() );
            }
        }
    }

    /**
     * @return the subscriber identifier.
     */

    public String getSubscriberId()
    {
        return this.getConsumerDescription().getConsumerId();
    }

    /**
     * @return the status of the subscriber.
     */

    public SubscriberStatus getSubscriberStatus()
    {
        return this.status;
    }

    /**
     * Closes and, where necessary, removes the subscriptions.
     */

    private void closeSubscriptions()
    {
        LOGGER.debug( "Closing and then removing subscriptions for {}.",
                      this.getSubscriberId() );

        String errorMessage = "messages within subscriber " + this.getSubscriberId() + ".";

        // Remove durable subscriptions if there are no open evaluations
        if ( !this.hasOpenEvaluations() && EvaluationSubscriber.DURABLE_SUBSCRIBERS )
        {
            try
            {
                this.session.unsubscribe( this.getEvaluationStatusSubscriberName() );
            }
            catch ( JMSException e )
            {
                String message = ENCOUNTERED_AN_ERROR_WHILE_ATTEMPTING_TO_REMOVE_A_DURABLE_SUBSCRIPTION_FOR
                                 + "evaluation status "
                                 + errorMessage;

                LOGGER.error( message, e );
            }

            try
            {
                this.session.unsubscribe( this.getEvaluationSubscriberName() );
            }
            catch ( JMSException e )
            {
                String message = ENCOUNTERED_AN_ERROR_WHILE_ATTEMPTING_TO_REMOVE_A_DURABLE_SUBSCRIPTION_FOR
                                 + EVALUATION
                                 + errorMessage;

                LOGGER.error( message, e );
            }

            try
            {
                this.statisticsSession.unsubscribe( this.getStatisticsSubscriberName() );
            }
            catch ( JMSException e )
            {
                String message = ENCOUNTERED_AN_ERROR_WHILE_ATTEMPTING_TO_REMOVE_A_DURABLE_SUBSCRIPTION_FOR
                                 + "statistics "
                                 + errorMessage;

                LOGGER.error( message, e );
            }
        }

        try
        {
            if ( Objects.nonNull( this.evaluationStatusConsumer ) )
            {
                this.evaluationStatusConsumer.close();
            }
        }
        catch ( JMSException e )
        {
            String message = "Encountered an error while attempting to close a registered consumer of evaluation "
                             + "status "
                             + errorMessage;

            LOGGER.error( message, e );
        }

        try
        {
            if ( Objects.nonNull( this.evaluationConsumer ) )
            {
                this.evaluationConsumer.close();
            }
        }
        catch ( JMSException e )
        {
            String message = "Encountered an error while attempting to close a registered consumer of evaluation "
                             + errorMessage;

            LOGGER.error( message, e );
        }

        try
        {
            if ( Objects.nonNull( this.statisticsConsumer ) )
            {
                this.statisticsConsumer.close();
            }
        }
        catch ( JMSException e )
        {
            String message = "Encountered an error while attempting to close a registered consumer of statistics "
                             + errorMessage;

            LOGGER.error( message, e );
        }
    }

    /**
     * @return <code>true</code> if some evaluations remain incomplete, <code>false</code> otherwise.
     */

    private boolean hasOpenEvaluations()
    {
        // Iterate the evaluations
        for ( Map.Entry<String, EvaluationConsumer> next : this.evaluations.entrySet() )
        {
            if ( !next.getValue().isComplete() )
            {
                return true;
            }
        }

        return false;
    }

    /**
     * @return the name of the durable subscriber to the evaluation status message queue.
     */

    private String getEvaluationStatusSubscriberName()
    {
        return this.getSubscriberId() + "-EXTERNAL-status";
    }

    /**
     * @return the name of the durable subscriber to the evaluation message queue.
     */

    private String getEvaluationSubscriberName()
    {
        return this.getSubscriberId() + "-EXTERNAL-evaluation";
    }

    /**
     * @return the name of the durable subscriber to the statistics message queue.
     */

    private String getStatisticsSubscriberName()
    {
        return this.getSubscriberId() + "-EXTERNAL-statistics";
    }

    /**
     * Awaits evaluation status messages and then consumes them. 
     */

    private MessageListener getStatusListener()
    {
        return message -> {

            BytesMessage receivedBytes = (BytesMessage) message;
            String messageId = UNKNOWN;
            String correlationId = UNKNOWN;
            String consumerId = UNKNOWN;
            String groupId = null;

            try
            {
                messageId = message.getJMSMessageID();
                correlationId = message.getJMSCorrelationID();
                consumerId = message.getStringProperty( EvaluationSubscriber.CONSUMER_ID_STRING );
                groupId = message.getStringProperty( EvaluationSubscriber.GROUP_ID_STRING );

                // Ignore status messages about consumers, including this one
                if ( Objects.isNull( consumerId ) && !this.isSubscriberFailed() )
                {
                    int messageLength = (int) receivedBytes.getBodyLength();

                    // Create the byte array to hold the message
                    byte[] messageContainer = new byte[messageLength];

                    receivedBytes.readBytes( messageContainer );

                    ByteBuffer buffer = ByteBuffer.wrap( messageContainer );

                    EvaluationStatus statusEvent = EvaluationStatus.parseFrom( buffer );

                    // Request for a consumer?
                    if ( statusEvent.getCompletionStatus() == CompletionStatus.CONSUMER_REQUIRED )
                    {
                        // Offer services if services are required
                        this.offerServices( statusEvent, correlationId );
                    }
                    // Request for consumption, but only if the message is flagged for me
                    else if ( this.isThisMessageForMe( message ) )
                    {
                        LOGGER.debug( SUBSCRIBER_HAS_CLAIMED_OWNERSHIP_OF_MESSAGE_FOR_EVALUATION,
                                      this.getSubscriberId(),
                                      messageId,
                                      correlationId );

                        EvaluationConsumer consumer = this.getEvaluationConsumer( correlationId );

                        consumer.acceptStatusMessage( statusEvent, groupId, messageId );

                        // Complete?
                        if ( consumer.isComplete() )
                        {
                            // Yes, then close
                            consumer.close();
                            this.status.registerEvaluationCompleted( correlationId );
                        }
                    }
                }

                // Acknowledge and flag success locally
                message.acknowledge();

                LOGGER.debug( ACKNOWLEDGED_MESSAGE_WITH_CORRELATION_ID, messageId, correlationId );
            }
            // Do not attempt to recover
            catch ( UnrecoverableSubscriberException e )
            {
                this.markSubscriberFailed( e );
            }
            // Attempt to recover
            catch ( JMSException | InvalidProtocolBufferException | RuntimeException e )
            {
                this.recover( messageId, correlationId, this.session, e );
            }
        };
    }

    /**
     * Awaits evaluation messages and then consumes them. 
     */

    private MessageListener getStatisticsListener()
    {
        return message -> {

            BytesMessage receivedBytes = (BytesMessage) message;
            String messageId = UNKNOWN;
            String correlationId = UNKNOWN;
            String groupId = null;

            try
            {
                if ( !this.isSubscriberFailed() && this.isThisMessageForMe( message ) )
                {
                    messageId = message.getJMSMessageID();
                    correlationId = message.getJMSCorrelationID();
                    groupId = message.getStringProperty( EvaluationSubscriber.GROUP_ID_STRING );

                    LOGGER.debug( SUBSCRIBER_HAS_CLAIMED_OWNERSHIP_OF_MESSAGE_FOR_EVALUATION,
                                  this.getSubscriberId(),
                                  messageId,
                                  correlationId );

                    EvaluationConsumer consumer = this.getEvaluationConsumer( correlationId );

                    // Create the byte array to hold the message
                    int messageLength = (int) receivedBytes.getBodyLength();

                    byte[] messageContainer = new byte[messageLength];

                    receivedBytes.readBytes( messageContainer );

                    ByteBuffer buffer = ByteBuffer.wrap( messageContainer );

                    Statistics statistics = Statistics.parseFrom( buffer );

                    consumer.acceptStatisticsMessage( statistics, groupId, messageId );
                    this.status.registerStatistics( messageId );

                    // Complete?
                    if ( consumer.isComplete() )
                    {
                        // Yes, then close
                        consumer.close();
                        this.status.registerEvaluationCompleted( correlationId );
                    }
                }

                // Acknowledge and flag success locally
                message.acknowledge();

                LOGGER.debug( ACKNOWLEDGED_MESSAGE_WITH_CORRELATION_ID, messageId, correlationId );
            }
            // Do not attempt to recover
            catch ( UnrecoverableSubscriberException e )
            {
                this.markSubscriberFailed( e );
            }
            // Attempt to recover
            catch ( JMSException | InvalidProtocolBufferException | RuntimeException e )
            {
                this.recover( messageId, correlationId, this.statisticsSession, e );
            }
        };
    }

    /**
     * Awaits evaluation messages and then consumes them. 
     */

    private MessageListener getEvaluationListener()
    {
        return message -> {

            BytesMessage receivedBytes = (BytesMessage) message;
            String messageId = UNKNOWN;
            String correlationId = UNKNOWN;
            String jobId = null;

            try
            {
                if ( !this.isSubscriberFailed() && this.isThisMessageForMe( message ) )
                {
                    messageId = message.getJMSMessageID();
                    correlationId = message.getJMSCorrelationID();
                    jobId = message.getStringProperty( MessageProperty.EVALUATION_JOB_ID.toString() );

                    LOGGER.debug( SUBSCRIBER_HAS_CLAIMED_OWNERSHIP_OF_MESSAGE_FOR_EVALUATION,
                                  this.getSubscriberId(),
                                  messageId,
                                  correlationId );

                    EvaluationConsumer consumer = this.getEvaluationConsumer( correlationId );

                    // Create the byte array to hold the message
                    int messageLength = (int) receivedBytes.getBodyLength();

                    byte[] messageContainer = new byte[messageLength];

                    receivedBytes.readBytes( messageContainer );

                    ByteBuffer buffer = ByteBuffer.wrap( messageContainer );

                    Evaluation evaluation = Evaluation.parseFrom( buffer );

                    consumer.acceptEvaluationMessage( evaluation, messageId, jobId );

                    // Complete?
                    if ( consumer.isComplete() )
                    {
                        // Yes, then close
                        consumer.close();
                        this.status.registerEvaluationCompleted( correlationId );
                    }
                }

                // Acknowledge and flag success locally
                message.acknowledge();

                LOGGER.debug( ACKNOWLEDGED_MESSAGE_WITH_CORRELATION_ID, messageId, correlationId );
            }
            // Do not attempt to recover
            catch ( UnrecoverableSubscriberException e )
            {
                this.markSubscriberFailed( e );
            }
            // Attempt to recover
            catch ( JMSException | InvalidProtocolBufferException | RuntimeException e )
            {
                this.recover( messageId, correlationId, this.session, e );
            }
        };
    }

    /**
     * <p>Attempts to recover the session up to the {@link #MAXIMUM_RETRIES}. 
     * 
     * <p>A well-behaving consumer cleans up after itself. Thus, it is considered a consumer bug if the consumer reports
     * a failure to overwrite on attempting a retry when the consumer failed exceptionally on an earlier attempt. By way 
     * of example, if a consumer writes path A and then immediately fails, triggering this method, any failure of the
     * consumer to support the retry and overwrite A (because it already exists) is considered a bug in the consumer. A
     * consumer must be "retry friendly", which means that it must clean up before throwing an exception.
     * 
     * @param messageId the message identifier for the exceptional consumption
     * @param correlationId the correlation identifier for the exceptional consumption
     * @param session the session to recover
     * @param exception the exception encountered
     */

    private void recover( String messageId, String correlationId, Session session, Exception e )
    {
        // Only try to recover if an evaluation hasn't already failed
        if ( !this.getEvaluationConsumer( correlationId ).isFailed() )
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
                              + "{} in subscriber {}, encountered an error. This is {} of {} allowed "
                              + "consumption failures before the subscriber will notify an unrecoverable failure "
                              + "for evaluation {}. The error is: {}",
                              messageId,
                              correlationId,
                              this.getSubscriberId(),
                              this.getNumberOfRetriesAttempted( correlationId ).get() + 1, // Counter starts at zero
                              this.broker.getMaximumMessageRetries(),
                              correlationId,
                              message );

                session.recover();
            }
            catch ( JMSException f )
            {
                LOGGER.error( "While attempting to recover a session for evaluation {} in subscriber {}, encountered "
                              + "an error that prevented recovery: ",
                              correlationId,
                              this.getSubscriberId(),
                              f.getMessage() );
            }
            catch ( IOException g )
            {
                LOGGER.error( "While attempting recovery in subscriber {}, failed to close an exception writer.",
                              this.getSubscriberId() );
            }
        }

        // Stop if the maximum number of retries has been reached
        if ( this.getNumberOfRetriesAttempted( correlationId )
                 .incrementAndGet() == this.broker.getMaximumMessageRetries() )
        {
            LOGGER.error( "Subscriber {} encountered a consumption failure for evaluation {}. Recovery failed after {} "
                          + "attempts.",
                          this.getSubscriberId(),
                          correlationId,
                          this.broker.getMaximumMessageRetries() );

            // Register the evaluation as failed
            this.markEvaluationFailed( correlationId, e );
        }
    }

    /**
     * @param evaluationId the evaluation identifier
     * @return the number of retries attempted so far.
     */

    private AtomicInteger getNumberOfRetriesAttempted( String evaluationId )
    {
        // Add the retry count if none tried
        AtomicInteger retries = new AtomicInteger();
        AtomicInteger existingRetries = this.retriesAttempted.putIfAbsent( evaluationId, retries );

        if ( Objects.nonNull( existingRetries ) )
        {
            return existingRetries;
        }

        return retries;
    }

    /**
     * Marks writing as failed unrecoverably for a given evaluation.
     * @param evaluationId the evaluation identifier
     * @param an exception to notify
     * @throws UnrecoverableSubscriberException if the subscriber fails unrecoverably
     */

    private void markEvaluationFailed( String evaluationId, Exception exception )
    {
        this.status.registerFailedEvaluation( evaluationId );
        EvaluationConsumer consumer = this.getEvaluationConsumer( evaluationId );
        consumer.markEvaluationFailed( exception );

        try
        {
            consumer.close();
        }
        catch ( JMSException | UnrecoverableSubscriberException e )
        {
            String message = "Subscriber " + this.getSubscriberId()
                             + " encountered an error while marking "
                             + EVALUATION
                             + evaluationId
                             + " as failed.";

            LOGGER.error( message, e );
        }
    }

    /**
     * Marks the subscriber as failed unrecoverably and attempts to mark all open evaluations that depend on this 
     * subscriber as failed. Finally, rethrows the unrecoverable exception.
     * @param exception the source of the failure
     * @throws UnrecoverableSubscriberException always, after marking the subscriber and any open evaluations as failed
     */

    private void markSubscriberFailed( UnrecoverableSubscriberException exception )
    {
        LOGGER.error( "Message subscriber {} has been flagged as failed without the possibility of recovery.",
                      this.getSubscriberId() );

        // Attempt to mark all open evaluations as failed
        this.getEvaluationsLock().lock();

        for ( EvaluationConsumer nextEvaluation : this.evaluations.values() )
        {
            if ( !nextEvaluation.isComplete() )
            {
                nextEvaluation.markEvaluationFailed( exception );
            }
        }

        this.getEvaluationsLock().unlock();

        // Propagate upwards
        this.isFailedUnrecoverably.set( true );
        this.status.markFailedUnrecoverably( exception );

        throw exception;
    }

    /**
     * @return <code>true</code> if the subscriber failed unrecoverably, otherwise <code>false</code>.
     */

    private boolean isSubscriberFailed()
    {
        return this.isFailedUnrecoverably.get();
    }

    /**
     * Returns a consumer for a prescribed evaluation identifier.
     * @param evaluationId the evaluation identifier
     * @return the consumer
     */

    private EvaluationConsumer getEvaluationConsumer( String evaluationId )
    {
        Objects.requireNonNull( evaluationId,
                                "Cannot request an evaluation consumer for an evaluation with a "
                                              + "missing identifier." );

        // Lock to avoid sweeping
        this.getEvaluationsLock()
            .lock();

        // Check initially
        EvaluationConsumer consumer = this.evaluations.get( evaluationId );

        if ( Objects.isNull( consumer ) )
        {
            consumer = new EvaluationConsumer( evaluationId,
                                               this.getConsumerDescription(),
                                               this.consumerFactory,
                                               this.evaluationStatusPublisher,
                                               this.getExecutor() );
            this.status.registerEvaluationStarted( evaluationId );
        }

        // Check atomically
        EvaluationConsumer added = this.evaluations.putIfAbsent( evaluationId, consumer );
        if ( Objects.nonNull( added ) )
        {
            consumer = added;
        }

        // Unlock to allow sweeping
        this.getEvaluationsLock()
            .unlock();

        return consumer;
    }

    /**
     * @return the evaluations lock
     */

    private ReentrantLock getEvaluationsLock()
    {
        return this.evaluationsLock;
    }

    /**
     * @return the executor to do writing work.
     */

    private ExecutorService getExecutor()
    {
        return this.executorService;
    }

    /**
     * Offers services.
     * 
     * @param status the evaluation status message with the consumer request
     * @param evaluationId the identifier of the evaluation for which services should be offered
     */

    private void offerServices( EvaluationStatus status, String evaluationId )
    {
        // Only offer formats if the status message is non-specific or some required formats intersect the formats
        // offered
        Collection<Format> formatsOffered = this.getConsumerDescription()
                                                .getFormatsList();
        if ( status.getFormatsRequiredList().isEmpty()
             || status.getFormatsRequiredList().stream().anyMatch( formatsOffered::contains ) )
        {
            this.publishReadyToConsume( evaluationId );
        }
        else
        {
            LOGGER.debug( "Received a request from evaluation {} for a consumer, but subscriber {} could not fulfill "
                          + "the contract.",
                          evaluationId,
                          this.getSubscriberId() );
        }
    }

    /**
     * Returns <code>true</code> if the message is contracted by this subscriber, otherwise <code>false</code>. 
     * @param message the message
     * @return true if the message is contracted, false if not
     * @throws JMSException if the ownership cannot be determined for any reason
     */

    private boolean isThisMessageForMe( Message message ) throws JMSException
    {
        for ( String next : this.formatStrings )
        {
            String nextFormat = message.getStringProperty( next );

            if ( Objects.nonNull( nextFormat ) && this.getSubscriberId().equals( nextFormat ) )
            {
                return true;
            }
        }

        return false;
    }

    /**
     * Publishes the status message {@link #readyToConsume}.
     * @param evaluationId the evaluation to notify
     */

    private void publishReadyToConsume( String evaluationId )
    {
        String messageId = "ID:" + this.getSubscriberId();

        ByteBuffer buffer = ByteBuffer.wrap( this.readyToConsume.toByteArray() );

        try
        {
            Map<MessageProperty, String> properties = new EnumMap<>( MessageProperty.class );
            properties.put( MessageProperty.JMS_MESSAGE_ID, messageId );
            properties.put( MessageProperty.JMS_CORRELATION_ID, evaluationId );
            properties.put( MessageProperty.CONSUMER_ID, this.getSubscriberId() );

            this.evaluationStatusPublisher.publish( buffer, Collections.unmodifiableMap( properties ) );
        }
        catch ( JMSException e )
        {
            throw new EvaluationEventException( "Subscriber "
                                                + this.getSubscriberId()
                                                + " failed to publish an evaluation status message about "
                                                + EVALUATION
                                                + evaluationId
                                                + ".",
                                                e );
        }
    }

    /**
     * @return the consumer description
     */

    private Consumer getConsumerDescription()
    {
        return this.consumerFactory.getConsumerDescription();
    }

    /**
     * Builds a subscriber.
     * 
     * @param consumerFactory the consumer factory
     * @param executorService the executor
     * @param broker the broker connection factory
     * @throws NullPointerException if any input is null
     * @throws UnrecoverableSubscriberException if the subscriber cannot be instantiated for any other reason
     */

    private EvaluationSubscriber( ConsumerFactory consumerFactory,
                                  ExecutorService executorService,
                                  BrokerConnectionFactory broker )
    {
        Objects.requireNonNull( consumerFactory );
        Objects.requireNonNull( executorService );
        Objects.requireNonNull( broker );

        this.consumerFactory = consumerFactory;
        this.broker = broker;
        this.executorService = executorService;

        LOGGER.info( "Building a subscriber {} to listen for evaluation messages...",
                     this.getSubscriberId() );

        this.status = new SubscriberStatus( this.getSubscriberId() );

        try
        {

            this.evaluationStatusTopic = (Topic) this.broker.getDestination( EVALUATION_STATUS_QUEUE );
            this.evaluationTopic = (Topic) this.broker.getDestination( EVALUATION_QUEUE );
            this.statisticsTopic = (Topic) this.broker.getDestination( STATISTICS_QUEUE );

            this.consumerConnection = this.broker.get()
                                                 .createConnection();
            this.statisticsConsumerConnection = this.broker.get()
                                                           .createConnection();
            this.publisherConnection = this.broker.get()
                                                  .createConnection();

            this.evaluationStatusPublisher = MessagePublisher.of( this.publisherConnection,
                                                                  this.evaluationStatusTopic );

            // Register a listener for exceptions
            ConnectionExceptionListener exceptionListener = new ConnectionExceptionListener( this );
            this.consumerConnection.setExceptionListener( exceptionListener );
            this.statisticsConsumerConnection.setExceptionListener( exceptionListener );

            this.publisherConnection.setExceptionListener( exceptionListener );

            // Client acknowledges
            this.session = this.consumerConnection.createSession( false, Session.CLIENT_ACKNOWLEDGE );
            this.statisticsSession = this.statisticsConsumerConnection.createSession( false,
                                                                                      Session.CLIENT_ACKNOWLEDGE );

            this.evaluationStatusConsumer = this.getMessageConsumer( this.session,
                                                                     this.evaluationStatusTopic,
                                                                     this.getEvaluationStatusSubscriberName() );

            this.evaluationStatusConsumer.setMessageListener( this.getStatusListener() );

            this.evaluationConsumer = this.getMessageConsumer( this.session,
                                                               this.evaluationTopic,
                                                               this.getEvaluationSubscriberName() );

            this.evaluationConsumer.setMessageListener( this.getEvaluationListener() );

            this.statisticsConsumer = this.getMessageConsumer( this.statisticsSession,
                                                               this.statisticsTopic,
                                                               this.getStatisticsSubscriberName() );

            this.statisticsConsumer.setMessageListener( this.getStatisticsListener() );

            this.evaluations = new ConcurrentHashMap<>();
            this.retriesAttempted = new ConcurrentHashMap<>();

            this.readyToConsume = EvaluationStatus.newBuilder()
                                                  .setCompletionStatus( CompletionStatus.READY_TO_CONSUME )
                                                  .setConsumer( this.getConsumerDescription() )
                                                  .build();

            this.formatStrings = this.getConsumerDescription()
                                     .getFormatsList()
                                     .stream()
                                     .map( Format::toString )
                                     .collect( Collectors.toSet() );

            this.isFailedUnrecoverably = new AtomicBoolean();

            // Start the connections
            LOGGER.info( "Started the connections for subscriber {}...",
                         this.getSubscriberId() );
            this.consumerConnection.start();
            this.statisticsConsumerConnection.start();
            this.publisherConnection.start();
        }
        catch ( JMSException | NamingException e )
        {
            throw new UnrecoverableSubscriberException( "While attempting to create a subscriber with identifier "
                                                        + this.getSubscriberId()
                                                        + " to receive evaluation messages, encountered an error.",
                                                        e );
        }

        String tempDir = System.getProperty( "java.io.tmpdir" );

        LOGGER.info( "The subscriber will write outputs to the directory specified by the java.io.tmpdir "
                     + "system property, which is {}",
                     tempDir );

        LOGGER.info( "Created subscriber {}", this.getSubscriberId() );
    }

    /**
     * A mutable container that records the status of the subscriber and the jobs completed so far. All status 
     * information is updated atomically.
     * 
     * @author james.brown@hydrosolved.com
     */

    public static class SubscriberStatus
    {

        /** The client identifier.*/
        private final String clientId;

        /** The number of evaluations completed.*/
        private final AtomicInteger evaluationCount = new AtomicInteger();

        /** The number of statistics blobs completed.*/
        private final AtomicInteger statisticsCount = new AtomicInteger();

        /** The last evaluation started.*/
        private final AtomicReference<String> evaluationId = new AtomicReference<>();

        /** The last statistics message completed.*/
        private final AtomicReference<String> statisticsMessageId = new AtomicReference<>();

        /** The evaluations that failed.*/
        private final Set<String> evaluationFailed = ConcurrentHashMap.newKeySet();

        /** The evaluations that have completed.*/
        private final Set<String> evaluationComplete = ConcurrentHashMap.newKeySet();

        /** Is true if the subscriber has failed.*/
        private final AtomicBoolean isFailed = new AtomicBoolean();

        @Override
        public String toString()
        {
            String addSucceeded = "";
            String addFailed = "";
            String addComplete = "";

            if ( Objects.nonNull( this.evaluationId.get() ) && Objects.nonNull( this.statisticsMessageId.get() ) )
            {
                addSucceeded = " The most recent evaluation was "
                               + this.evaluationId.get()
                               + " and the most recent statistics were attached to message "
                               + this.statisticsMessageId.get()
                               + ".";
            }

            if ( !this.evaluationFailed.isEmpty() )
            {
                addFailed =
                        " Failed to consume one or more statistics messages for " + this.evaluationFailed.size()
                            + " evaluations. "
                            + "The failed evaluation are "
                            + this.evaluationFailed
                            + ".";
            }

            if ( !this.evaluationComplete.isEmpty() )
            {
                addComplete = " Evaluation subscriber "
                              + this.clientId
                              + " completed "
                              + this.evaluationComplete.size()
                              + " of the "
                              + this.evaluationCount.get()
                              + " evaluations that were started.";
            }

            return "Evaluation subscriber "
                   + this.clientId
                   + " is waiting for work. Until now, received "
                   + this.statisticsCount.get()
                   + " packets of statistics across "
                   + this.evaluationCount.get()
                   + " evaluations."
                   + addSucceeded
                   + addFailed
                   + addComplete;
        }

        /**
         * @return the evaluation count.
         */
        public int getEvaluationCount()
        {
            return this.evaluationCount.get();
        }

        /**
         * @return the statistics count.
         */
        public int getStatisticsCount()
        {
            return this.statisticsCount.get();
        }

        /**
         * Flags an unrecoverable failure in the subscriber.
         * @param exception the unrecoverable consumer exception that caused the failure
         */

        public void markFailedUnrecoverably( UnrecoverableSubscriberException exception )
        {
            String failure = "Evaluation subscriber " + this.clientId + " has failed unrecoverably and will now stop.";

            LOGGER.error( failure, exception );

            this.isFailed.set( true );
        }

        /**
         * Returns the failure state of the subscriber.
         * @return true if the subscriber has failed, otherwise false
         */

        public boolean isFailed()
        {
            return this.isFailed.get();
        }

        /**
         * Increment the evaluation count and last evaluation identifier.
         * @param evaluationId the evaluation identifier
         */

        private void registerEvaluationStarted( String evaluationId )
        {
            this.evaluationCount.incrementAndGet();
            this.evaluationId.set( evaluationId );
        }

        /**
         * Registers an evaluation completed.
         * @param evaluationId the evaluation identifier
         */

        private void registerEvaluationCompleted( String evaluationId )
        {
            this.evaluationComplete.add( evaluationId );
        }

        /**
         * Increment the failed evaluation count and last failed evaluation identifier.
         * @param evaluationId the evaluation identifier
         */

        private void registerFailedEvaluation( String evaluationId )
        {
            this.evaluationFailed.add( evaluationId );
        }

        /**
         * Increment the statistics count and last statistics message identifier.
         * @param messageId the identifier of the message that contained the statistics.
         */

        private void registerStatistics( String messageId )
        {
            this.statisticsCount.incrementAndGet();
            this.statisticsMessageId.set( messageId );
        }

        /**
         * Hidden constructor.
         * 
         * @param clientId the client identifier
         */

        private SubscriberStatus( String clientId )
        {
            Objects.requireNonNull( clientId );

            this.clientId = clientId;
        }
    }

    /**
     * Returns a consumer.
     * 
     * @param session the session
     * @param topic the topic
     * @param name the name of the subscriber
     * @return a consumer
     * @throws JMSException if the consumer could not be created for any reason
     */

    private MessageConsumer getMessageConsumer( Session session, Topic topic, String name )
            throws JMSException
    {
        if ( EvaluationSubscriber.DURABLE_SUBSCRIBERS )
        {
            return session.createDurableSubscriber( topic, name, null, false );
        }

        return session.createConsumer( topic, null );
    }

    /**
     * Listen for failures on a connection.
     */

    private static class ConnectionExceptionListener implements ExceptionListener
    {

        private final EvaluationSubscriber subscriber;

        @Override
        public void onException( JMSException exception )
        {
            String message = "Encountered an error on a connection owned by a subscriber. If a failover policy was "
                             + "configured on the connection factory (e.g., connection retries), then that policy was "
                             + "exhausted before this error was thrown. As such, the error is not recoverable and the "
                             + "subscriber will now stop.";

            UnrecoverableSubscriberException propagate = new UnrecoverableSubscriberException( message, exception );

            this.subscriber.markSubscriberFailed( propagate );
        }

        /**
         * Create an instance.
         * @param subscriber the subscriber
         */
        private ConnectionExceptionListener( EvaluationSubscriber subscriber )
        {
            Objects.requireNonNull( subscriber );

            this.subscriber = subscriber;
        }
    }
}
