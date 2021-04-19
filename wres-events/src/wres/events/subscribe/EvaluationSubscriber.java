package wres.events.subscribe;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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

import net.jcip.annotations.ThreadSafe;
import wres.events.EvaluationEventException;
import wres.events.EvaluationEventUtilities;
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

@ThreadSafe
public class EvaluationSubscriber implements Closeable
{

    private static final String ENCOUNTERED_AN_EXCEPTION_THAT_WILL_STOP_THE_SUBSCRIBER =
            "encountered an exception that will stop the subscriber.";

    private static final String AN_EVALUATION = "an evaluation";

    private static final String AN_EVALUATION_STATUS = "an evaluation status";

    private static final String A_STATISTICS = "a statistics";

    private static final String IN_SUBSCRIBER = " in subscriber ";

    private static final String EVALUATION = "evaluation ";

    private static final String SUBSCRIBER_HAS_CLAIMED_OWNERSHIP_OF_MESSAGE_FOR_EVALUATION =
            "Subscriber {} has claimed ownership of {} message with messageId {} for evaluation {}.";

    private static final String ENCOUNTERED_AN_ERROR_WHILE_ATTEMPTING_TO_REMOVE_A_DURABLE_SUBSCRIPTION_FOR =
            "Encountered an error while attempting to remove a durable subscription for ";

    private static final Logger LOGGER = LoggerFactory.getLogger( EvaluationSubscriber.class );

    private static final String ACKNOWLEDGED_MESSAGE_FOR_EVALUATION =
            "Subscriber {} has acknowledged (ACK-ed) a message from a {} queue with messageId {}, correlationId {} "
                                                                      + "and groupId {}.";

    /**
     * String representation of the {@link MessageProperty#CONSUMER_ID}.
     */

    private static final String CONSUMER_ID_STRING = MessageProperty.CONSUMER_ID.toString();

    /**
     * String representation of the {@link MessageProperty#GROUP_ID}.
     */

    private static final String GROUP_ID_STRING = MessageProperty.JMSX_GROUP_ID.toString();


    /**
     * The frequency with which to publish a subscriber-alive message in ms.
     */

    private static final long NOTIFY_ALIVE_MILLISECONDS = 100_000;

    /**
     * Is true to use durable subscribers, false for temporary subscribers, which are auto-deleted.
     */

    private final boolean durableSubscribers;

    /**
     * Re-usable status message indicating the consumer is alive with the completion status 
     * {@link CompletionStatus#CONSUMPTION_ONGOING}.
     */

    private final EvaluationStatus consumerIsAlive;

    /**
     * Re-usable status message that contains a service offer from this subscriber with the completion status 
     * {@link CompletionStatus#READY_TO_CONSUME}.
     */

    private final EvaluationStatus serviceOffer;

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
     * A session for evaluation description messages.
     */

    private final Session evaluationDescriptionSession;

    /**
     * A session for statistics messages.
     */

    private final Session statisticsSession;

    /**
     * A session for evaluation status messages.
     */

    private final Session statusSession;

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
     * Is <code>true</code> is the subscriber has failed unrecoverably.
     */

    private final AtomicBoolean isClosing;

    /**
     * The factory that supplies consumers for evaluations.
     */

    private final ConsumerFactory consumerFactory;

    /**
     * A timer task to publish information about the status of the subscriber.
     */

    private final Timer timer;

    /**
     * Number of retries allowed.
     */

    private final int maximumRetries;

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
    public void close() throws IOException
    {
        LOGGER.debug( "Closing subscriber {}.", this.getClientId() );

        this.isClosing.set( true );

        // Log an error if there are open evaluations
        if ( this.hasOpenEvaluations() )
        {
            Set<String> open = this.evaluations.entrySet()
                                               .stream()
                                               .filter( next -> !next.getValue().isComplete() )
                                               .map( Map.Entry::getKey )
                                               .collect( Collectors.toSet() );

            LOGGER.warn( "While closing subscriber {}, {} open evaluations were discovered: {}. These "
                         + "evaluations will not be notified complete. They should be notified before a "
                         + "subscriber is closed.",
                         this.getClientId(),
                         open.size(),
                         open );
        }

        // Durable subscriptions are removed if all evaluations succeeded
        this.closeSubscriptions();

        // Close connections; no need to close any other pubs/subs or sessions (according to the JMS documentation of 
        // Connection::close).
        String connectionmessage = "Encountered an error while attempting to close a broker connection within "
                                   + "subscriber "
                                   + this.getClientId()
                                   + ".";

        try
        {
            this.consumerConnection.close();
            LOGGER.debug( "Closed connection {} in subscriber {}.", this.consumerConnection, this );
        }
        catch ( JMSException e )
        {
            LOGGER.warn( connectionmessage, e );
        }

        try
        {
            this.statisticsConsumerConnection.close();
            LOGGER.debug( "Closed connection {} in subscriber {}.", this.statisticsConsumerConnection, this );
        }
        catch ( JMSException e )
        {
            LOGGER.warn( connectionmessage, e );
        }

        try
        {
            this.evaluationStatusPublisher.close();
        }
        catch ( IOException e )
        {
            String message =
                    "Failed to close a publisher of evaluation status messages in subscriber " + this.getClientId()
                             + ".";

            LOGGER.warn( message, e );
        }

        // Cancel notifications
        this.timer.cancel();

        LOGGER.debug( "Closed subscriber {}.", this.getClientId() );

        // This subscriber is not responsible for closing the executorService.

        // This subscriber is not responsible for closing the consumerFactory.
    }

    /**
     * Maintenance task that removes closed evaluations from the cache that succeeded and retains failed evaluations.
     */

    public void sweep()
    {
        // Lock for sweeping
        this.getEvaluationsLock()
            .lock();

        // Find the evaluations to sweep
        Set<String> completed = new HashSet<>();
        Set<String> failed = new HashSet<>();

        // Create an independent set to sweep as this is a mutating loop
        Set<String> toSweep = new HashSet<>( this.evaluations.keySet() );
        for ( String nextEvaluation : toSweep )
        {
            EvaluationConsumer nextValue = this.evaluations.get( nextEvaluation );

            if ( nextValue.isClosed() && !nextValue.isFailed() )
            {
                this.evaluations.remove( nextEvaluation );
                this.retriesAttempted.remove( nextEvaluation );

                completed.add( nextEvaluation );
            }
            else if ( nextValue.isFailed() )
            {
                failed.add( nextEvaluation );
            }
        }

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "The sweeper for subscriber {} swept away {} closed evaluations: {}. There are {} "
                          + "evaluations remaining of which {} are marked failed unrecoverably: {}.",
                          this.getClientId(),
                          completed.size(),
                          completed,
                          this.evaluations.size(),
                          failed.size(),
                          failed );
        }

        this.getEvaluationsLock()
            .unlock();
    }

    /**
     * @return the subscriber identifier.
     */

    public String getClientId()
    {
        return this.getConsumerDescription()
                   .getConsumerId();
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
                      this.getClientId() );

        String errorMessage = "messages within subscriber " + this.getClientId() + ".";

        // Remove durable subscriptions if there are no open evaluations
        if ( !this.hasOpenEvaluations() && this.areSubscribersDurable() )
        {
            try
            {
                this.statusSession.unsubscribe( this.getEvaluationStatusSubscriberName() );
            }
            catch ( JMSException e )
            {
                String message = ENCOUNTERED_AN_ERROR_WHILE_ATTEMPTING_TO_REMOVE_A_DURABLE_SUBSCRIPTION_FOR
                                 + "evaluation status "
                                 + errorMessage;

                LOGGER.warn( message, e );
            }

            try
            {
                this.evaluationDescriptionSession.unsubscribe( this.getEvaluationSubscriberName() );
            }
            catch ( JMSException e )
            {
                String message = ENCOUNTERED_AN_ERROR_WHILE_ATTEMPTING_TO_REMOVE_A_DURABLE_SUBSCRIPTION_FOR
                                 + EVALUATION
                                 + errorMessage;

                LOGGER.warn( message, e );
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

                LOGGER.warn( message, e );
            }
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
        return this.getClientId() + "-status";
    }

    /**
     * @return the name of the durable subscriber to the evaluation message queue.
     */

    private String getEvaluationSubscriberName()
    {
        return this.getClientId() + "-evaluation";
    }

    /**
     * @return the name of the durable subscriber to the statistics message queue.
     */

    private String getStatisticsSubscriberName()
    {
        return this.getClientId() + "-statistics";
    }


    Set<EvaluationStatus> pubComplete = new HashSet<>();

    /**
     * Awaits evaluation status messages and then consumes them. 
     */

    private MessageListener getStatusListener()
    {
        return message -> {

            BytesMessage receivedBytes = (BytesMessage) message;
            String messageId = null;
            String correlationId = null;
            String groupId = null;

            EvaluationConsumer consumer = null;

            try
            {
                messageId = message.getJMSMessageID();
                correlationId = message.getJMSCorrelationID();
                groupId = message.getStringProperty( EvaluationSubscriber.GROUP_ID_STRING );

                // Ignore status messages about consumers, including this one
                if ( this.shouldIForwardThisMessageForConsumption( message, QueueType.EVALUATION_STATUS_QUEUE ) )
                {
                    int messageLength = (int) receivedBytes.getBodyLength();

                    // Create the byte array to hold the message
                    byte[] messageContainer = new byte[messageLength];

                    receivedBytes.readBytes( messageContainer );

                    ByteBuffer buffer = ByteBuffer.wrap( messageContainer );

                    EvaluationStatus statusEvent = EvaluationStatus.parseFrom( buffer );

                    consumer = this.processStatusEvent( statusEvent,
                                                        correlationId,
                                                        message,
                                                        messageId,
                                                        groupId );
                }

                // Acknowledge and flag success locally
                message.acknowledge();

                LOGGER.debug( ACKNOWLEDGED_MESSAGE_FOR_EVALUATION,
                              this.getClientId(),
                              QueueType.EVALUATION_STATUS_QUEUE,
                              messageId,
                              correlationId,
                              groupId );

                // Register complete if complete
                this.registerEvaluationCompleteIfConsumptionComplete( consumer, correlationId );
            }
            // Attempt to recover
            catch ( JMSException | InvalidProtocolBufferException | ConsumerException e )
            {
                this.recover( messageId, correlationId, this.statusSession, e );
            }
            // Do not attempt to recover
            catch ( UnrecoverableEvaluationException e )
            {
                String failureMessage =
                        "While processing an evaluation status message, encountered an exception that will "
                                        + "stop evaluation "
                                        + correlationId
                                        + ".";

                LOGGER.error( failureMessage, e );

                this.markEvaluationFailed( correlationId, e );
            }
            // Do not attempt to recover and mark the subscriber failed also
            catch ( RuntimeException e )
            {
                LOGGER.error( "While processing an evaluation status message, "
                              + ENCOUNTERED_AN_EXCEPTION_THAT_WILL_STOP_THE_SUBSCRIBER,
                              e );

                this.markSubscriberFailed( e );
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
            String messageId = null;
            String correlationId = null;
            String groupId = null;

            try
            {
                messageId = message.getJMSMessageID();
                correlationId = message.getJMSCorrelationID();

                if ( this.shouldIForwardThisMessageForConsumption( message, QueueType.STATISTICS_QUEUE ) )
                {
                    groupId = message.getStringProperty( EvaluationSubscriber.GROUP_ID_STRING );

                    LOGGER.debug( SUBSCRIBER_HAS_CLAIMED_OWNERSHIP_OF_MESSAGE_FOR_EVALUATION,
                                  this.getClientId(),
                                  A_STATISTICS,
                                  messageId,
                                  correlationId );

                    EvaluationConsumer consumer = this.getOrCreateNewEvaluationConsumer( correlationId );

                    // Create the byte array to hold the message
                    int messageLength = (int) receivedBytes.getBodyLength();

                    byte[] messageContainer = new byte[messageLength];

                    receivedBytes.readBytes( messageContainer );

                    ByteBuffer buffer = ByteBuffer.wrap( messageContainer );

                    Statistics statistics = Statistics.parseFrom( buffer );

                    consumer.acceptStatisticsMessage( statistics, groupId, messageId );

                    // Register with the status monitor
                    this.status.registerStatistics( messageId );

                    // Register complete if complete
                    this.registerEvaluationCompleteIfConsumptionComplete( consumer, correlationId );
                }

                // Acknowledge and flag success locally
                message.acknowledge();

                LOGGER.debug( ACKNOWLEDGED_MESSAGE_FOR_EVALUATION,
                              this.getClientId(),
                              QueueType.STATISTICS_QUEUE,
                              messageId,
                              correlationId,
                              groupId );
            }
            // Attempt to recover
            catch ( JMSException | InvalidProtocolBufferException | ConsumerException e )
            {
                this.recover( messageId, correlationId, this.statisticsSession, e );
            }
            // Do not attempt to recover
            catch ( UnrecoverableEvaluationException e )
            {
                String failureMessage =
                        "While processing a statistics message, encountered an exception that will stop evaluation "
                                        + correlationId
                                        + ".";

                LOGGER.error( failureMessage, e );

                this.markEvaluationFailed( correlationId, e );
            }
            // Do not attempt to recover and mark the subscriber failed also
            catch ( RuntimeException e )
            {
                LOGGER.error( "While processing a statistics message, "
                              + ENCOUNTERED_AN_EXCEPTION_THAT_WILL_STOP_THE_SUBSCRIBER,
                              e );

                this.markSubscriberFailed( e );
            }
        };
    }

    /**
     * Consumes an {@link EvaluationStatus} message.
     * 
     * @param statusEvent the evaluation status message
     * @param evaluationId the evaluation identifier
     * @param message the originating message
     * @param messageId the identifier of the originating message
     * @param groupId the message group identifier
     * @return the evaluation consumer used, where applicable
     * @throws JMSException if the processing fails
     */

    private EvaluationConsumer processStatusEvent( EvaluationStatus statusEvent,
                                                   String evaluationId,
                                                   Message message,
                                                   String messageId,
                                                   String groupId )
            throws JMSException
    {
        EvaluationConsumer consumer = null;

        CompletionStatus completionStatus = statusEvent.getCompletionStatus();

        // Request for a consumer?
        if ( completionStatus == CompletionStatus.CONSUMER_REQUIRED )
        {
            // Offer services if services are required
            this.offerServices( statusEvent, evaluationId );
        }
        // Request for consumption, but only if the message is flagged for me
        else if ( this.isThisMessageForMe( message ) )
        {
            LOGGER.debug( SUBSCRIBER_HAS_CLAIMED_OWNERSHIP_OF_MESSAGE_FOR_EVALUATION,
                          this.getClientId(),
                          AN_EVALUATION_STATUS,
                          messageId,
                          evaluationId );

            consumer = this.getOrCreateNewEvaluationConsumer( evaluationId );

            consumer.acceptStatusMessage( statusEvent, groupId, messageId );
        }

        return consumer;
    }

    /**
     * Registers an evaluation complete when the consumption is complete.
     * 
     * @param consumer the consumer
     * @param evaluationId the evaluation identifier
     */

    private void registerEvaluationCompleteIfConsumptionComplete( EvaluationConsumer consumer, String evaluationId )
    {
        // Complete?
        if ( Objects.nonNull( consumer ) && consumer.isComplete() )
        {
            this.status.registerEvaluationCompleted( evaluationId );
        }
    }

    /**
     * Awaits evaluation messages and then consumes them. 
     */

    private MessageListener getEvaluationListener()
    {
        return message -> {

            BytesMessage receivedBytes = (BytesMessage) message;
            String messageId = null;
            String correlationId = null;
            String jobId = null;

            try
            {
                messageId = message.getJMSMessageID();
                correlationId = message.getJMSCorrelationID();

                if ( this.shouldIForwardThisMessageForConsumption( message, QueueType.EVALUATION_QUEUE ) )
                {
                    jobId = message.getStringProperty( MessageProperty.EVALUATION_JOB_ID.toString() );

                    LOGGER.debug( SUBSCRIBER_HAS_CLAIMED_OWNERSHIP_OF_MESSAGE_FOR_EVALUATION,
                                  this.getClientId(),
                                  AN_EVALUATION,
                                  messageId,
                                  correlationId );

                    EvaluationConsumer consumer = this.getOrCreateNewEvaluationConsumer( correlationId );

                    // Create the byte array to hold the message
                    int messageLength = (int) receivedBytes.getBodyLength();

                    byte[] messageContainer = new byte[messageLength];

                    receivedBytes.readBytes( messageContainer );

                    ByteBuffer buffer = ByteBuffer.wrap( messageContainer );

                    Evaluation evaluation = Evaluation.parseFrom( buffer );

                    consumer.acceptEvaluationMessage( evaluation, messageId, jobId );

                    // Register complete if complete
                    this.registerEvaluationCompleteIfConsumptionComplete( consumer, correlationId );
                }

                // Acknowledge and flag success locally
                message.acknowledge();

                LOGGER.debug( ACKNOWLEDGED_MESSAGE_FOR_EVALUATION,
                              this.getClientId(),
                              QueueType.EVALUATION_QUEUE,
                              messageId,
                              correlationId,
                              null );
            }
            // Attempt to recover
            catch ( JMSException | InvalidProtocolBufferException | ConsumerException e )
            {
                this.recover( messageId, correlationId, this.evaluationDescriptionSession, e );
            }
            // Do not attempt to recover
            catch ( UnrecoverableEvaluationException e )
            {
                String failureMessage =
                        "While processing an evaluation message, encountered an exception that will stop evaluation "
                                        + correlationId
                                        + ".";

                LOGGER.error( failureMessage, e );

                this.markEvaluationFailed( correlationId, e );
            }
            // Do not attempt to recover and mark the subscriber failed also
            catch ( RuntimeException e )
            {
                LOGGER.error( "While processing an evaluation message, "
                              + ENCOUNTERED_AN_EXCEPTION_THAT_WILL_STOP_THE_SUBSCRIBER,
                              e );

                this.markSubscriberFailed( e );
            }
        };
    }

    /**
     * Checks whether a message should be forwarded to a consumer attached to this subscriber. A message should be 
     * forwarded if all expected metadata is present and the evaluation is being handled by this subscriber and neither
     * the evaluation nor the subscriber has failed.
     * 
     * @return true if a message should be forward to a consumer, otherwise false
     * @throws JMSException if the status could not be determined
     * @throws IllegalArgumentException if the queue type is unrecognized
     */

    private boolean shouldIForwardThisMessageForConsumption( Message message, QueueType queueType ) throws JMSException
    {

        String messageId = message.getJMSMessageID();
        String correlationId = message.getJMSCorrelationID();
        String consumerId = message.getStringProperty( EvaluationSubscriber.CONSUMER_ID_STRING );
        String groupId = message.getStringProperty( EvaluationSubscriber.GROUP_ID_STRING );

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Subscriber {} has received a message from a {} queue with messageId {}, correlationId {}, "
                          + "groupId {} and consumerId {}. The correlationId is the unique evaluation identifier.",
                          this.getClientId(),
                          queueType,
                          messageId,
                          correlationId,
                          groupId,
                          consumerId );
        }

        // Should always have a messageId and correlationId, but see #87953
        if ( Objects.isNull( messageId ) || Objects.isNull( correlationId ) )
        {
            LOGGER.warn( "The messageId and correlationId are required AMQP metadata and cannot be null. "
                         + "The messageId is {} and the correlationId is {}. This message will be acknowledged but not "
                         + "forwarded for consumption. This may indicate a broker or messaging client in distress.",
                         messageId,
                         correlationId );

            return false;
        }

        // Subscriber failed?
        if ( this.isSubscriberFailed() )
        {
            LOGGER.debug( "A message was received from a {} queue, but subscriber {} has been marked failed. The "
                          + "messageId is {} and the correlationId is {}. This message will not be forwarded for "
                          + "consumption.",
                          queueType,
                          this.getClientId(),
                          messageId,
                          correlationId );

            return false;
        }

        // Evaluation failed?
        if ( this.isEvaluationFailed( correlationId ) )
        {
            LOGGER.debug( "A message was received from a {} queue, but evaluation {} has been marked failed. The "
                          + "messageId is {} and the correlationId is {}. This message will not be forwarded for "
                          + "consumption.",
                          queueType,
                          correlationId,
                          messageId,
                          correlationId );

            return false;
        }

        // Iterate through the queue types
        if ( queueType == QueueType.EVALUATION_QUEUE )
        {
            // Accept messages intended for this subscriber
            return this.isThisMessageForMe( message );
        }
        else if ( queueType == QueueType.STATISTICS_QUEUE )
        {
            // Accept messages intended for this subscriber
            return this.isThisMessageForMe( message );
        }
        else if ( queueType == QueueType.EVALUATION_STATUS_QUEUE )
        {
            // Ignore messages from subscribers/consumers, including this one. This is for status messages from 
            // publishers only. But accept messages that are not flagged for this subscriber because a subscriber must
            // be negotiated in the first instance.
            return Objects.isNull( consumerId );
        }
        else
        {
            throw new IllegalArgumentException( "Unrecognized queue type '" + queueType + "'." );
        }
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
     * @param evaluationId the correlation identifier for the exceptional consumption
     * @param session the session to recover
     * @param exception the exception encountered
     */

    private void recover( String messageId, String evaluationId, Session session, Exception exception )
    {
        LOGGER.debug( "Recovery triggered for message {} in evaluation {}.", messageId, evaluationId );

        // Only retry if the subscriber and evaluation are both in non-error states and there are retries remaining
        // for this evaluation
        int retryCount = this.getMaximumMessageRetries();

        // Get and increment the attempts atomically so that any retry-in-progress is transparent to other threads
        int attemptedRetries = this.getNumberOfRetriesAttempted( evaluationId )
                                   .getAndIncrement();

        // Check and retry if needed
        if ( !isEvaluationFailed( evaluationId ) && !this.isSubscriberFailed()
             && attemptedRetries < retryCount )
        {
            LOGGER.debug( "Attempting retry of message {} for evaluation {}.", messageId, evaluationId );

            this.attemptRetry( messageId, evaluationId, session, exception );

            // Last one? Then stop and mark failed because recovery won't be triggered again (recovery attempts are 
            // handled by broker configuration, which is used to set the internal retry count used here).
            if ( attemptedRetries + 1 == retryCount ) // Zero-based count
            {
                LOGGER.error( "Subscriber {} encountered a consumption failure for evaluation {}. Recovery failed "
                              + "after {} attempts.",
                              this.getClientId(),
                              evaluationId,
                              retryCount );

                // Register the evaluation as failed
                this.markEvaluationFailed( evaluationId, exception );
            }
        }
        // Evaluation has failed unrecoverably or all evaluations on this subscriber have failed unrecoverably
        else if ( this.isEvaluationFailed( evaluationId ) || this.isSubscriberFailed() && LOGGER.isDebugEnabled() )
        {
            String message = "While attempting to consume a message with identifier " + messageId
                             + " and correlation identifier "
                             + ""
                             + evaluationId
                             + IN_SUBSCRIBER
                             + this.getClientId()
                             + ", encountered an error. No further retries will be attempted "
                             + "because the evaluation has been marked failed unrecoverably. At the time of "
                             + "unrecoverable failure, "
                             + attemptedRetries
                             + " of "
                             + retryCount
                             + " retries had been attempted.";

            LOGGER.debug( message, exception );
        }
    }

    /**
     * Tests whether an evaluation has failed.
     * @param evaluationId the evaluation identifier
     * @return true if the evaluation has failed, otherwise false
     */

    private boolean isEvaluationFailed( String evaluationId )
    {
        // Wait for any mutation of the evaluations to complete
        try
        {
            this.getEvaluationsLock().lock();

            // Check the cache of failed evaluations as well as the cache of ongoing evaluations, which may contain
            // failed evaluations that have not yet been swept away.
            return this.evaluations.containsKey( evaluationId )
                   && this.evaluations.get( evaluationId )
                                      .isFailed();
        }
        finally
        {
            this.getEvaluationsLock().unlock();
        }
    }

    /**
     * Attempts to recover a session on failure.
     * 
     * @param messageId the message identifier for the exceptional consumption
     * @param evaluationId the correlation identifier for the exceptional consumption
     * @param session the session to recover
     * @param exception the exception encountered
     */

    private void attemptRetry( String messageId, String evaluationId, Session session, Exception exception )
    {
        // Incremented (starting from zero) in advance of this retry
        int retryCount = this.getNumberOfRetriesAttempted( evaluationId )
                             .get();

        try
        {
            // Exponential back-off, which includes a PT2S wait before the first attempt
            Thread.sleep( (long) Math.pow( 2, retryCount ) * 1000 );

            String errorMessage = "While attempting to consume a message with identifier " + messageId
                                  + " and correlation identifier "
                                  + evaluationId
                                  + IN_SUBSCRIBER
                                  + this.getClientId()
                                  + ", encountered an error. The session will now attempt to recover. This "
                                  + "is "
                                  + retryCount
                                  + " of "
                                  + this.getMaximumMessageRetries()
                                  + " allowed consumption failures before the subscriber will notify an "
                                  + "unrecoverable failure for evaluation "
                                  + evaluationId
                                  + ", unless the subscriber is otherwise marked "
                                  + "failed (in which case further retries may not occur).";

            LOGGER.error( errorMessage, exception );

            // Attempt recovery in order to cycle the delivery attempts. When the maximum is reached, poison
            // messages should hit the Dead Letter Queue (DLQ), assuming a DLQ is configured.
            session.recover();
        }
        catch ( JMSException f )
        {
            String message = "While attempting to recover a session for evaluation " + evaluationId
                             + IN_SUBSCRIBER
                             + this.getClientId()
                             + ", encountered "
                             + "an error that prevented recovery.";

            LOGGER.error( message, f );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();

            String message = "Interrupted while waiting to recover a session in evaluation " + evaluationId + ".";

            LOGGER.error( message, evaluationId );
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
     * Marks consumption as failed unrecoverably for a given evaluation.
     * @param evaluationId the evaluation identifier
     * @param an exception to notify
     * @throws UnrecoverableSubscriberException if the subscriber fails unrecoverably
     */

    private void markEvaluationFailed( String evaluationId, Exception exception )
    {
        // Close the consumer
        try
        {
            if ( this.evaluations.containsKey( evaluationId ) )
            {
                EvaluationConsumer consumer = this.evaluations.get( evaluationId );
                consumer.markEvaluationFailedOnConsumption( exception );
            }
        }
        catch ( JMSException | UnrecoverableSubscriberException e )
        {
            String message = "Subscriber " + this.getClientId()
                             + " encountered an error while marking "
                             + EVALUATION
                             + evaluationId
                             + " as failed.";

            LOGGER.error( message, e );
        }

        // Add to the list of failed evaluations
        this.status.registerEvaluationFailed( evaluationId );
    }

    /**
     * Marks the subscriber as failed unrecoverably and attempts to mark all open evaluations that depend on this 
     * subscriber as failed. Finally, rethrows the unrecoverable exception.
     * @param exception the source of the failure
     * @throws UnrecoverableSubscriberException always, after marking the subscriber and any open evaluations as failed
     */

    private void markSubscriberFailed( RuntimeException exception )
    {
        // Mark failed if not already failed
        if ( !this.isFailedUnrecoverably.getAndSet( true ) )
        {

            // Propagate to the caller
            this.status.markFailedUnrecoverably();

            String message = "Message subscriber " + this.getClientId()
                             + " has been flagged as failed without the possibility "
                             + "of recovery.";

            LOGGER.error( message, exception );

            // Attempt to mark all open evaluations as failed
            this.getEvaluationsLock().lock();

            try
            {
                for ( EvaluationConsumer nextEvaluation : this.evaluations.values() )
                {
                    if ( !nextEvaluation.isComplete() )
                    {
                        // Add to the list of failed evaluations
                        nextEvaluation.markEvaluationFailedOnConsumption( exception );
                    }
                }
            }
            catch ( JMSException e )
            {
                LOGGER.error( "While closing subscriber {}, failed to close some of the evaluations associated with "
                              + "it.",
                              this.getClientId() );
            }
            finally
            {
                this.getEvaluationsLock()
                    .unlock();
            }

            throw exception;
        }
    }

    /**
     * @return <code>true</code> if the subscriber failed unrecoverably, otherwise <code>false</code>.
     */

    private boolean isSubscriberFailed()
    {
        return this.isFailedUnrecoverably.get();
    }

    /**
     * Returns a consumer for a prescribed evaluation identifier, opening a new one if necessary.
     * @param evaluationId the evaluation identifier
     * @return the consumer
     */

    private EvaluationConsumer getOrCreateNewEvaluationConsumer( String evaluationId )
    {
        Objects.requireNonNull( evaluationId,
                                "Cannot request an evaluation consumer for an evaluation with a "
                                              + "missing identifier." );

        this.getEvaluationsLock()
            .lock();

        try
        {
            // Exists already?
            if ( !this.evaluations.containsKey( evaluationId ) )
            {
                EvaluationConsumer consumer = new EvaluationConsumer( evaluationId,
                                                                      this.getConsumerDescription(),
                                                                      this.consumerFactory,
                                                                      this.evaluationStatusPublisher,
                                                                      this.getExecutor(),
                                                                      this.status );
                this.evaluations.put( evaluationId, consumer );

                this.status.registerEvaluationStarted( evaluationId );
            }

            return this.evaluations.get( evaluationId );
        }
        finally
        {
            this.getEvaluationsLock()
                .unlock();
        }
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
            LOGGER.debug( "Subscriber {} is offering services for formats {}.", this.getClientId(), formatsOffered );

            this.publishStatusMessage( evaluationId, this.serviceOffer );
        }
        else
        {
            LOGGER.debug( "Received a request from evaluation {} for a consumer, but subscriber {} could not fulfill "
                          + "the contract.",
                          evaluationId,
                          this.getClientId() );
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
        for ( String nextFormat : this.formatStrings )
        {
            String subscriberId = message.getStringProperty( nextFormat );

            if ( Objects.nonNull( subscriberId ) && this.getClientId()
                                                        .equals( subscriberId ) )
            {
                return true;
            }
        }

        return false;
    }

    /**
     * Publishes a status message.
     * @param evaluationId the evaluation identifier to notify
     * @param status the status message to publish
     */

    private void publishStatusMessage( String evaluationId, EvaluationStatus status )
    {
        String messageId = "ID:" + this.getClientId() + "-m" + EvaluationEventUtilities.getUniqueId();

        ByteBuffer buffer = ByteBuffer.wrap( status.toByteArray() );

        try
        {
            Map<MessageProperty, String> properties = new EnumMap<>( MessageProperty.class );
            properties.put( MessageProperty.JMS_MESSAGE_ID, messageId );
            properties.put( MessageProperty.JMS_CORRELATION_ID, evaluationId );
            properties.put( MessageProperty.CONSUMER_ID, this.getClientId() );

            this.evaluationStatusPublisher.publish( buffer, Collections.unmodifiableMap( properties ) );
        }
        catch ( EvaluationEventException e )
        {
            throw new EvaluationEventException( "Subscriber "
                                                + this.getClientId()
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
     * Notifies all clients that depend on this subscriber that it is still alive. The notification happens at a fixed
     * time interval. Also closes a subscriber that is found to have failed.
     */

    private void checkAndNotifyStatusAtFixedInterval()
    {
        EvaluationSubscriber subscriber = this;

        // Create a timer task to update any listening clients that the subscriber is alive in case of long-running 
        // writing tasks
        TimerTask updater = new TimerTask()
        {
            @Override
            public void run()
            {
                // Notify alive
                subscriber.notifyAlive();

                // If failed, close the subscriber, which also ends this timer task
                if ( subscriber.isSubscriberFailed() )
                {
                    try
                    {
                        subscriber.close();
                    }
                    catch ( IOException e )
                    {
                        if ( LOGGER.isWarnEnabled() )
                        {
                            String message = "Failed to close subscriber " + subscriber.getClientId() + ".";
                            LOGGER.warn( message, e );
                        }
                    }
                }
            }
        };

        this.timer.schedule( updater, 0, EvaluationSubscriber.NOTIFY_ALIVE_MILLISECONDS );
    }

    /**
     * Sends an evaluation status message with a status of {@link CompletionStatus#CONSUMPTION_ONGOING} for each open
     * evaluation so that any listening client knows that the subscriber is still alive.
     * @throws EvaluationEventException if the notification failed
     */

    private void notifyAlive()
    {
        // Iterate the evaluations
        for ( Map.Entry<String, EvaluationConsumer> next : this.evaluations.entrySet() )
        {
            // Consider only open evaluations
            if ( !next.getValue().isComplete() )
            {
                this.publishStatusMessage( next.getKey(), this.consumerIsAlive );
            }
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
        if ( this.areSubscribersDurable() )
        {
            return session.createDurableSubscriber( topic, name, null, false );
        }

        return session.createConsumer( topic, null );
    }

    /**
     * @return true if the subscribers are durable, false if they are transient.
     */

    private boolean areSubscribersDurable()
    {
        return this.durableSubscribers;
    }

    /**
     * @return the maximum number of retries allowed.
     */

    private int getMaximumMessageRetries()
    {
        return this.maximumRetries;
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
        this.executorService = executorService;

        // Non-durable subscribers until we can properly recover from broker/client failures to warrant durable ones
        this.durableSubscribers = false;
        this.logSubscriberPolicy( this.durableSubscribers );

        LOGGER.info( "Building a subscriber {} to listen for evaluation messages...",
                     this.getClientId() );

        this.status = new SubscriberStatus( this.getClientId() );
        this.timer = new Timer( true );
        this.maximumRetries = broker.getMaximumMessageRetries();

        try
        {
            this.evaluationStatusTopic =
                    (Topic) broker.getDestination( QueueType.EVALUATION_STATUS_QUEUE.toString() );
            this.evaluationTopic = (Topic) broker.getDestination( QueueType.EVALUATION_QUEUE.toString() );
            this.statisticsTopic = (Topic) broker.getDestination( QueueType.STATISTICS_QUEUE.toString() );

            // The broker connection factory is responsible for closing these
            this.consumerConnection = broker.get();
            LOGGER.debug( "Created connection {} in subscriber {}.", this.consumerConnection, this );

            this.statisticsConsumerConnection = broker.get();
            LOGGER.debug( "Created connection {} in subscriber {}.", this.statisticsConsumerConnection, this );

            this.evaluationStatusPublisher = MessagePublisher.of( broker,
                                                                  this.evaluationStatusTopic );

            // Register an exception listener for each connection
            ConnectionExceptionListener consumerConnectionListener =
                    new ConnectionExceptionListener( this, EvaluationEventUtilities.getUniqueId() );
            this.consumerConnection.setExceptionListener( consumerConnectionListener );
            ConnectionExceptionListener statisticsConsumerConnectionListener =
                    new ConnectionExceptionListener( this, EvaluationEventUtilities.getUniqueId() );
            this.statisticsConsumerConnection.setExceptionListener( statisticsConsumerConnectionListener );

            // Client acknowledges
            this.evaluationDescriptionSession =
                    this.consumerConnection.createSession( false, Session.CLIENT_ACKNOWLEDGE );
            this.statusSession = this.consumerConnection.createSession( false, Session.CLIENT_ACKNOWLEDGE );
            this.statisticsSession = this.statisticsConsumerConnection.createSession( false,
                                                                                      Session.CLIENT_ACKNOWLEDGE );

            this.evaluationStatusConsumer = this.getMessageConsumer( this.statusSession,
                                                                     this.evaluationStatusTopic,
                                                                     this.getEvaluationStatusSubscriberName() );

            this.evaluationStatusConsumer.setMessageListener( this.getStatusListener() );

            this.evaluationConsumer = this.getMessageConsumer( this.evaluationDescriptionSession,
                                                               this.evaluationTopic,
                                                               this.getEvaluationSubscriberName() );

            this.evaluationConsumer.setMessageListener( this.getEvaluationListener() );

            this.statisticsConsumer = this.getMessageConsumer( this.statisticsSession,
                                                               this.statisticsTopic,
                                                               this.getStatisticsSubscriberName() );

            this.statisticsConsumer.setMessageListener( this.getStatisticsListener() );

            // An LRU cache that removes old evaluations that succeeded
            this.evaluations = new ConcurrentHashMap<>();
            this.retriesAttempted = new ConcurrentHashMap<>();

            this.consumerIsAlive = EvaluationStatus.newBuilder()
                                                   .setCompletionStatus( CompletionStatus.CONSUMPTION_ONGOING )
                                                   .setConsumer( this.getConsumerDescription() )
                                                   .setClientId( this.getClientId() )
                                                   .build();

            this.serviceOffer = EvaluationStatus.newBuilder()
                                                .setCompletionStatus( CompletionStatus.READY_TO_CONSUME )
                                                .setConsumer( this.getConsumerDescription() )
                                                .setClientId( this.getClientId() )
                                                .build();

            this.formatStrings = this.getConsumerDescription()
                                     .getFormatsList()
                                     .stream()
                                     .map( Format::toString )
                                     .collect( Collectors.toSet() );

            this.isFailedUnrecoverably = new AtomicBoolean();
            this.isClosing = new AtomicBoolean();

            // Start the connections
            LOGGER.info( "Started the connections for subscriber {}...",
                         this.getClientId() );
            this.consumerConnection.start();
            this.statisticsConsumerConnection.start();
        }
        catch ( JMSException | NamingException e )
        {
            throw new UnrecoverableSubscriberException( "While attempting to create a subscriber with identifier "
                                                        + this.getClientId()
                                                        + " to receive evaluation messages, encountered an error.",
                                                        e );
        }

        // Publish the status at a regular interval to producers that listen for it
        this.checkAndNotifyStatusAtFixedInterval();

        String tempDir = System.getProperty( "java.io.tmpdir" );

        LOGGER.info( "The subscriber will write outputs to the directory specified by the java.io.tmpdir "
                     + "system property, which is {}",
                     tempDir );

        LOGGER.info( "Created subscriber {}", this.getClientId() );
    }

    /**
     * Logs the subscriber policy.
     * 
     * @param durable is true to use durable subscribers, false for temporary subscribers
     */

    private void logSubscriberPolicy( boolean durableSubscribers )
    {
        if ( durableSubscribers && LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( "Subscriber {} is using durable queues. These queues are not auto-deleted and may be "
                         + "abandoned under some circumstances, which requires them to be deleted, otherwise they will "
                         + "continue to receive and enqueue messages.",
                         this.getClientId() );
        }
    }

    /**
     * Listen for failures on a connection.
     */

    private static class ConnectionExceptionListener implements ExceptionListener
    {

        private final EvaluationSubscriber subscriber;
        private final String connectionId;

        @Override
        public void onException( JMSException exception )
        {
            // Ignore errors on connections encountered during the shutdown sequence
            if ( !this.subscriber.isClosing.get() )
            {
                String message = "Encountered an error on connection " + this.connectionId
                                 + " owned by subscriber "
                                 + this.subscriber.getClientId()
                                 + ". If a failover policy was "
                                 + "configured on the connection factory (e.g., connection retries), then that policy "
                                 + "was exhausted before this error was thrown. As such, the error is not recoverable "
                                 + "and the subscriber will now stop.";

                UnrecoverableSubscriberException propagate = new UnrecoverableSubscriberException( message, exception );

                try
                {

                    this.subscriber.markSubscriberFailed( propagate );
                }
                catch ( UnrecoverableSubscriberException e )
                {
                    // Do nothing as the exception is rethrown.
                }
            }
        }

        /**
         * Create an instance.
         * @param subscriber the subscriber
         * @param connectionId the connection identifier
         */
        private ConnectionExceptionListener( EvaluationSubscriber subscriber, String connectionId )
        {
            Objects.requireNonNull( subscriber );

            this.subscriber = subscriber;
            this.connectionId = connectionId;
        }
    }

    /**
     * Enumeration of queue types on the broker.
     */

    private enum QueueType
    {
        /**
         * Default name for the queue on the amq.topic that accepts evaluation status messages.
         */

        EVALUATION_STATUS_QUEUE,

        /**
         * Default name for the queue on the amq.topic that accepts evaluation status messages.
         */

        EVALUATION_QUEUE,

        /**
         * Default name for the queue on the amq.topic that accepts evaluation status messages.
         */

        STATISTICS_QUEUE;

        /**
         * @return a string representation.
         */
        @Override
        public String toString()
        {
            switch ( this )
            {
                case EVALUATION_STATUS_QUEUE:
                    return "status";
                case EVALUATION_QUEUE:
                    return "evaluation";
                case STATISTICS_QUEUE:
                    return "statistics";
                default:
                    throw new IllegalArgumentException( "Unknown queue '" + this + "'." );
            }
        }
    }

}
