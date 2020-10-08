package wres.vis.client;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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

import wres.events.EvaluationEventException;
import wres.eventsbroker.BrokerConnectionFactory;
import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.Statistics;
import wres.statistics.generated.Consumer.Format;
import wres.statistics.generated.EvaluationStatus.CompletionStatus;
import wres.vis.client.GraphicsPublisher.MessageProperty;
import wres.vis.writing.GraphicsWriteException;
import wres.vis.client.GraphicsClient.ClientStatus;
import wres.statistics.generated.Evaluation;

/**
 * Subscribes to evaluation messages and serializes {@link Statistics} to graphics in a prescribed format.
 * 
 * @author james.brown@hydrosolved.com
 */

class GraphicsSubscriber implements Closeable
{

    private static final String SUBSCRIBER_HAS_CLAIMED_OWNERSHIP_OF_MESSAGE_FOR_EVALUATION =
            "Subscriber {} has claimed ownership of message {} for evaluation {}.";

    private static final String ENCOUNTERED_AN_ERROR_WHILE_ATTEMPTING_TO_REMOVE_A_DURABLE_SUBSCRIPTION_FOR =
            "Encountered an error while attempting to remove a durable subscription for ";

    private static final Logger LOGGER = LoggerFactory.getLogger( GraphicsSubscriber.class );

    private static final String ACKNOWLEDGED_MESSAGE_WITH_CORRELATION_ID =
            "Acknowledged message {} with correlationId {}.";

    private static final String UNKNOWN = "unknown";

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
     * String representation of the {@link MessageProperty#OUTPUT_PATH}.
     */

    private static final String OUTPUT_PATH_STRING = MessageProperty.OUTPUT_PATH.toString();


    /**
     * Description of this consumer.
     */

    private final wres.statistics.generated.Consumer consumerDescription;

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

    private final GraphicsPublisher evaluationStatusPublisher;

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
     * A session.
     */

    private final Session session;

    /**
     * A consumer connection.
     */

    private final Connection connection;

    /**
     * Broker connections.
     */

    private final BrokerConnectionFactory broker;

    /**
     * Client status.
     */

    private final ClientStatus status;

    /**
     * The evaluations by unique id.
     */

    private final Map<String, EvaluationConsumer> evaluations;

    /**
     * Actual number of retries attempted per evaluation.
     */

    private final Map<String, AtomicInteger> retriesAttempted;

    /**
     * An executor service for graphics writing work.
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

            LOGGER.error( "While closing graphics subscriber {}, {} open evaluations were discovered: {}. These "
                          + "evaluations will not be notified complete. They should be notified before a "
                          + "subscriber is closed.",
                          this.getSubscriberId(),
                          open.size(),
                          open );
        }

        // Durable subscriptions are removed if all evaluations succeeded
        this.closeSubscriptions();

        // No need to close any other pubs/subs or sessions (according to the JMS documentation of Connection::close).
        
        String errorMessage = "messages within graphics subscriber " + this.getSubscriberId() + ".";

        try
        {
            this.connection.close();
        }
        catch ( JMSException e )
        {
            String message = "Encountered an error while attempting to close a broker connection within graphics "
                             + "subscriber "
                             + errorMessage;

            LOGGER.warn( message, e );
        }

        // This subscriber is not responsible for closing the broker.
    }

    /**
     * Removes completed evaluations from the cache.
     */

    void sweep()
    {
        // Lock for sweeping
        this.getEvaluationsLock().lock();

        // Find the evaluations to sweep
        Set<String> completed = this.evaluations.entrySet()
                                                .stream()
                                                .filter( next -> next.getValue().isComplete() )
                                                .map( Map.Entry::getKey )
                                                .collect( Collectors.toSet() );

        // Do the actual sweeping
        for ( String next : completed )
        {
            this.evaluations.remove( next );
            this.retriesAttempted.remove( next );
        }

        LOGGER.debug( "The sweeper for subscriber {} removed {} completed evaluation, including {}.",
                      this.getSubscriberId(),
                      completed.size(),
                      completed );

        this.getEvaluationsLock().unlock();
    }

    /**
     * Sends an evaluation status message with a status of {@link CompletionStatus#READY_TO_CONSUME} for each open
     * evaluation so that any listening listening client knows that the subscriber is still alive.
     * @throws EvaluationEventException if the notification failed
     */

    void notifyAlive()
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

    String getSubscriberId()
    {
        return this.consumerDescription.getConsumerId();
    }

    /**
     * Closes and, where necessary, removes the subscriptions.
     */

    private void closeSubscriptions()
    {
        LOGGER.debug( "Closing and then removing subscriptions for {}.",
                      this.getSubscriberId() );
        
        String errorMessage = "messages within graphics subscriber " + this.getSubscriberId() + ".";

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

        // Remove durable subscriptions if there are no open evaluations
        if ( !this.hasOpenEvaluations() )
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
                                 + "evaluation status "
                                 + errorMessage;

                LOGGER.error( message, e );
            }

            try
            {
                this.session.unsubscribe( this.getStatisticsSubscriberName() );
            }
            catch ( JMSException e )
            {
                String message = ENCOUNTERED_AN_ERROR_WHILE_ATTEMPTING_TO_REMOVE_A_DURABLE_SUBSCRIPTION_FOR
                                 + "statistics "
                                 + errorMessage;

                LOGGER.error( message, e );
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
                consumerId = message.getStringProperty( GraphicsSubscriber.CONSUMER_ID_STRING );
                groupId = message.getStringProperty( GraphicsSubscriber.GROUP_ID_STRING );

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
            catch ( UnrecoverableConsumerException e )
            {
                this.markSubscriberFailed( e );
            }
            // Attempt to recover
            catch ( JMSException | InvalidProtocolBufferException | RuntimeException e )
            {
                this.recover( messageId, correlationId, e );
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
                    groupId = message.getStringProperty( GraphicsSubscriber.GROUP_ID_STRING );

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
            catch ( UnrecoverableConsumerException e )
            {
                this.markSubscriberFailed( e );
            }
            // Attempt to recover
            catch ( JMSException | InvalidProtocolBufferException | RuntimeException e )
            {
                this.recover( messageId, correlationId, e );
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
            String outputPath = null;

            try
            {
                if ( !this.isSubscriberFailed() && this.isThisMessageForMe( message ) )
                {
                    messageId = message.getJMSMessageID();
                    correlationId = message.getJMSCorrelationID();
                    outputPath = message.getStringProperty( GraphicsSubscriber.OUTPUT_PATH_STRING );

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

                    consumer.acceptEvaluationMessage( evaluation, outputPath, messageId );

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
            catch ( UnrecoverableConsumerException e )
            {
                this.markSubscriberFailed( e );
            }
            // Attempt to recover
            catch ( JMSException | InvalidProtocolBufferException | RuntimeException e )
            {
                this.recover( messageId, correlationId, e );
            }
        };
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
        if ( !this.getEvaluationConsumer( correlationId ).failed() )
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
                              + "{} in graphics subscriber {}, encountered an error. This is {} of {} allowed "
                              + "consumption failures before the subscriber will notify an unrecoverable failure "
                              + "for evaluation {}. The error is: {}",
                              messageId,
                              correlationId,
                              this.getSubscriberId(),
                              this.getNumberOfRetriesAttempted( correlationId ).get() + 1, // Counter starts at zero
                              this.broker.getMaximumMessageRetries(),
                              correlationId,
                              message );

                this.session.recover();
            }
            catch ( JMSException f )
            {
                LOGGER.error( "While attempting to recover a session for evaluation {} in graphics subscriber {}, "
                              + "encountered an error that prevented recovery: ",
                              correlationId,
                              this.getSubscriberId(),
                              f.getMessage() );
            }
            catch ( IOException g )
            {
                LOGGER.error( "While attempting recovery in graphics subscriber {}, failed to close an exception "
                              + "writer.",
                              this.getSubscriberId() );
            }
        }

        // Stop if the maximum number of retries has been reached
        if ( this.getNumberOfRetriesAttempted( correlationId )
                 .incrementAndGet() == this.broker.getMaximumMessageRetries() )
        {
            LOGGER.error( "Graphics subscriber {} encountered a consumption failure for evaluation {}. "
                          + "Recovery failed after {} attempts.",
                          this.getSubscriberId(),
                          correlationId,
                          this.broker.getMaximumMessageRetries() );

            // Register the evaluation as failed
            this.markEvaluationFailed( correlationId );
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
     * Marks graphics writing as failed unrecoverably for a given evaluation.
     * @param evaluationId the evaluation identifier
     * @throws UnrecoverableConsumerException 
     * @throws GraphicsWriteException if the subscriber fails unrecoverably
     */

    private void markEvaluationFailed( String evaluationId )
    {
        this.status.registerFailedEvaluation( evaluationId );
        EvaluationConsumer consumer = this.getEvaluationConsumer( evaluationId );
        consumer.markEvaluationFailed();

        try
        {
            consumer.close();
        }
        catch ( JMSException | UnrecoverableConsumerException e )
        {
            String message = "Graphics subscriber " + this.getSubscriberId()
                             + " encountered an error while marking "
                             + "evaluation "
                             + evaluationId
                             + " as failed.";

            LOGGER.error( message, e );
        }
    }

    /**
     * Marks the subscriber as failed unrecoverably and then rethrows the unrecoverable exception.
     * @param exception the source of the failure
     * @throws UnrecoverableConsumerException always, after marking the subscriber as failed
     */

    private void markSubscriberFailed( UnrecoverableConsumerException exception )
    {
        LOGGER.info( "Message subscriber {} has been flagged as failed without the possibility of recovery.",
                     this.getSubscriberId() );

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
                                               this.consumerDescription,
                                               this.evaluationStatusPublisher,
                                               this.getGraphicsExecutor() );
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
     * @return the executor to do graphics writing work.
     */

    private ExecutorService getGraphicsExecutor()
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
        Collection<Format> formatsOffered = this.consumerDescription.getFormatsList();
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
            this.evaluationStatusPublisher.publish( buffer, messageId, evaluationId, this.getSubscriberId() );
        }
        catch ( JMSException e )
        {
            throw new EvaluationEventException( "Subscriber "
                                                + this.getSubscriberId()
                                                + " failed to publish an evaluation status message about "
                                                + "evaluation "
                                                + evaluationId
                                                + ".",
                                                e );
        }
    }

    /**
     * Builds a subscriber.
     * 
     * @param subscriberId the subscriber identifier
     * @param status a graphics server whose status can be updated with subscriber progress
     * @param executorService the executor for completing graphics work
     * @param broker the broker
     * @throws JMSException if the subscriber components could not be constructed or started.
     * @throws NamingException if the expected broker destinations could not be discovered
     * @throws NullPointerException if any input is null
     */

    GraphicsSubscriber( String subscriberId,
                        ClientStatus status,
                        ExecutorService executorService,
                        BrokerConnectionFactory broker )
            throws JMSException, NamingException
    {
        Objects.requireNonNull( subscriberId );
        Objects.requireNonNull( status );
        Objects.requireNonNull( executorService );
        Objects.requireNonNull( broker );

        // Describe the consumer
        this.consumerDescription = wres.statistics.generated.Consumer.newBuilder()
                                                                     .setConsumerId( subscriberId )
                                                                     .addFormats( Format.PNG )
                                                                     .addFormats( Format.SVG )
                                                                     .build();

        LOGGER.info( "Building a long-running graphics subscriber {} to listen for evaluation messages...",
                     this.getSubscriberId() );

        this.status = status;
        this.broker = broker;

        this.evaluationStatusTopic = (Topic) this.broker.getDestination( EVALUATION_STATUS_QUEUE );
        this.evaluationTopic = (Topic) this.broker.getDestination( EVALUATION_QUEUE );
        this.statisticsTopic = (Topic) this.broker.getDestination( STATISTICS_QUEUE );

        this.connection = this.broker.get().createConnection();

        this.evaluationStatusPublisher = GraphicsPublisher.of( this.connection,
                                                               this.evaluationStatusTopic,
                                                               this.getSubscriberId() );

        // Create a connection for consumption and register a listener for exceptions
        this.connection.setExceptionListener( new ConnectionExceptionListener() );

        // Client acknowledges
        this.session = this.connection.createSession( false, Session.CLIENT_ACKNOWLEDGE );


        this.evaluationStatusConsumer = this.session.createDurableSubscriber( this.evaluationStatusTopic,
                                                                              this.getEvaluationStatusSubscriberName(),
                                                                              null,
                                                                              false );

        this.evaluationStatusConsumer.setMessageListener( this.getStatusListener() );

        this.evaluationConsumer = this.session.createDurableSubscriber( this.evaluationTopic,
                                                                        this.getEvaluationSubscriberName(),
                                                                        null,
                                                                        false );

        this.evaluationConsumer.setMessageListener( this.getEvaluationListener() );

        this.statisticsConsumer = this.session.createDurableSubscriber( this.statisticsTopic,
                                                                        this.getStatisticsSubscriberName(),
                                                                        null,
                                                                        false );

        this.statisticsConsumer.setMessageListener( this.getStatisticsListener() );

        this.evaluations = new ConcurrentHashMap<>();
        this.retriesAttempted = new ConcurrentHashMap<>();
        this.executorService = executorService;

        this.readyToConsume = EvaluationStatus.newBuilder()
                                              .setCompletionStatus( CompletionStatus.READY_TO_CONSUME )
                                              .setConsumer( this.consumerDescription )
                                              .build();

        this.formatStrings = this.consumerDescription.getFormatsList()
                                                     .stream()
                                                     .map( Format::toString )
                                                     .collect( Collectors.toSet() );

        this.isFailedUnrecoverably = new AtomicBoolean();

        // Start the consumer connection
        LOGGER.info( "Started the consumer connection for long-running subscriber {}...",
                     this.getSubscriberId() );
        this.connection.start();

        String tempDir = System.getProperty( "java.io.tmpdir" );

        LOGGER.info( "The graphics subscriber will write outputs to the directory specified by the java.io.tmpdir "
                     + "system property, which is {}",
                     tempDir );

        LOGGER.info( "Created long-running subscriber {}", this.getSubscriberId() );
    }

    /**
     * Listen for failures on a connection.
     */

    private static class ConnectionExceptionListener implements ExceptionListener
    {

        @Override
        public void onException( JMSException exception )
        {
            LOGGER.error( "Encountered an error on a connection owned by a graphics subscriber: {}.",
                          exception.getMessage() );
        }
    }
}
