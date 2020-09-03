package wres.vis.server;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
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
import wres.statistics.generated.EvaluationStatus.CompletionStatus;
import wres.vis.server.GraphicsPublisher.MessageProperty;
import wres.vis.server.GraphicsServer.ServerStatus;
import wres.vis.writing.GraphicsWriteException;
import wres.statistics.generated.Evaluation;

/**
 * Subscribes to evaluation messages and serializes {@link Statistics} to graphics in a prescribed format.
 * 
 * @author james.brown@hydrosolved.com
 */

class GraphicsSubscriber implements Closeable
{

    private static final Logger LOGGER = LoggerFactory.getLogger( GraphicsSubscriber.class );

    private static final String ACKNOWLEDGED_MESSAGE_WITH_CORRELATION_ID =
            "Acknowledged message {} with correlationId {}.";

    private static final String UNKNOWN = "unknown";

    private static final String MESSAGES_WITHIN_GRAPHICS_SUBSCRIBER = "messages within graphics subscriber {}: {}";

    /**
     * If <code>true</code>, do not allow the durable subscriptions to survive graphics server restarts - remove the 
     * subscriptions when closing this subscriber.
     */

    private static final boolean REMOVE_SUBSCRIPTIONS_ON_CLOSURE = false;

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
     * A unique identifier for the subscriber.
     */

    private final String uniqueId;

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
     * Server status.
     */

    private final ServerStatus status;

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

    @Override
    public void close()
    {
        LOGGER.debug( "Closing and then removing subscriptions for {}.",
                      this.getIdentifier() );

        this.closeSubscriptions();

        try
        {
            if ( Objects.nonNull( this.evaluationStatusPublisher ) )
            {
                this.evaluationStatusPublisher.close();
            }
        }
        catch ( IOException e )
        {
            LOGGER.error( "Encountered an error while attempting to close a registered publisher of evaluation status "
                          + MESSAGES_WITHIN_GRAPHICS_SUBSCRIBER,
                          this.getIdentifier(),
                          e.getMessage() );
        }

        try
        {
            this.session.close();
        }
        catch ( JMSException e )
        {
            LOGGER.error( "Encountered an error while attempting to close a broker session within graphics subscriber "
                          + "{}: {}",
                          this.getIdentifier(),
                          e.getMessage() );
        }

        try
        {
            this.connection.close();
        }
        catch ( JMSException e )
        {
            LOGGER.error( "Encountered an error while attempting to close a broker connection within graphics "
                          + "subscriber {}: {}",
                          this.getIdentifier(),
                          e.getMessage() );
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
                      this.getIdentifier(),
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
        // Create the status message to publish
        EvaluationStatus message = EvaluationStatus.newBuilder()
                                                   .setCompletionStatus( CompletionStatus.READY_TO_CONSUME )
                                                   .setConsumerId( this.getIdentifier() )
                                                   .build();

        // Iterate the evaluations
        for ( Map.Entry<String, EvaluationConsumer> next : this.evaluations.entrySet() )
        {
            // Consider only open evaluations
            if ( !next.getValue().isComplete() )
            {
                String messageId = "ID:" + this.getIdentifier();

                String evaluationId = next.getKey();

                ByteBuffer buffer = ByteBuffer.wrap( message.toByteArray() );

                try
                {
                    this.evaluationStatusPublisher.publish( buffer, messageId, evaluationId, this.getIdentifier() );
                }
                catch ( JMSException e )
                {
                    throw new EvaluationEventException( "Subscriber "
                                                        + this.getIdentifier()
                                                        + " failed to publish an evaluation status message about "
                                                        + "evaluation "
                                                        + evaluationId
                                                        + ".",
                                                        e );
                }
            }
        }
    }

    /**
     * Closes and, where necessary, removes the subscriptions.
     */

    private void closeSubscriptions()
    {
        try
        {
            if ( Objects.nonNull( this.evaluationStatusConsumer ) )
            {
                this.evaluationStatusConsumer.close();
            }
        }
        catch ( JMSException e )
        {
            LOGGER.error( "Encountered an error while attempting to close a registered consumer of evaluation status "
                          + MESSAGES_WITHIN_GRAPHICS_SUBSCRIBER,
                          this.getIdentifier(),
                          e.getMessage() );
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
            LOGGER.error( "Encountered an error while attempting to close a registered consumer of evaluation "
                          + MESSAGES_WITHIN_GRAPHICS_SUBSCRIBER,
                          this.getIdentifier(),
                          e.getMessage() );
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
            LOGGER.error( "Encountered an error while attempting to close a registered consumer of statistics "
                          + MESSAGES_WITHIN_GRAPHICS_SUBSCRIBER,
                          this.getIdentifier(),
                          e.getMessage() );
        }

        // Remove durable subscriptions: any messages that arrive when the subscriber is down will be lost
        if ( GraphicsSubscriber.REMOVE_SUBSCRIPTIONS_ON_CLOSURE )
        {
            try
            {
                this.session.unsubscribe( this.getEvaluationStatusSubscriberName() );
            }
            catch ( JMSException e )
            {
                LOGGER.error( "Encountered an error while attempting to remove a durable subscription for evaluation status "
                              + MESSAGES_WITHIN_GRAPHICS_SUBSCRIBER,
                              this.getIdentifier(),
                              e.getMessage() );
            }

            try
            {
                this.session.unsubscribe( this.getEvaluationSubscriberName() );
            }
            catch ( JMSException e )
            {
                LOGGER.error( "Encountered an error while attempting to remove a durable subscription for evaluation "
                              + MESSAGES_WITHIN_GRAPHICS_SUBSCRIBER,
                              this.getIdentifier(),
                              e.getMessage() );
            }

            try
            {
                this.session.unsubscribe( this.getStatisticsSubscriberName() );
            }
            catch ( JMSException e )
            {
                LOGGER.error( "Encountered an error while attempting to remove a durable subscription for statistics "
                              + MESSAGES_WITHIN_GRAPHICS_SUBSCRIBER,
                              this.getIdentifier(),
                              e.getMessage() );
            }
        }
    }

    /**
     * @return the subscriber identifier.
     */

    private String getIdentifier()
    {
        return this.uniqueId;
    }

    /**
     * @return the name of the durable subscriber to the evaluation status message queue.
     */

    private String getEvaluationStatusSubscriberName()
    {
        return this.getIdentifier() + "-EXTERNAL-status";
    }

    /**
     * @return the name of the durable subscriber to the evaluation message queue.
     */

    private String getEvaluationSubscriberName()
    {
        return this.getIdentifier() + "-EXTERNAL-evaluation";
    }

    /**
     * @return the name of the durable subscriber to the statistics message queue.
     */

    private String getStatisticsSubscriberName()
    {
        return this.getIdentifier() + "-EXTERNAL-statistics";
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
                if ( Objects.isNull( consumerId ) )
                {
                    int messageLength = (int) receivedBytes.getBodyLength();

                    // Create the byte array to hold the message
                    byte[] messageContainer = new byte[messageLength];

                    receivedBytes.readBytes( messageContainer );

                    ByteBuffer buffer = ByteBuffer.wrap( messageContainer );

                    EvaluationStatus statusEvent = EvaluationStatus.parseFrom( buffer );

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
                message.acknowledge();

                LOGGER.debug( ACKNOWLEDGED_MESSAGE_WITH_CORRELATION_ID, messageId, correlationId );
            }
            catch ( JMSException | EvaluationEventException | GraphicsWriteException
                    | InvalidProtocolBufferException e )
            {
                // Attempt to recover
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
                messageId = message.getJMSMessageID();
                correlationId = message.getJMSCorrelationID();
                groupId = message.getStringProperty( GraphicsSubscriber.GROUP_ID_STRING );

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

                message.acknowledge();

                LOGGER.debug( ACKNOWLEDGED_MESSAGE_WITH_CORRELATION_ID, messageId, correlationId );
            }
            catch ( JMSException | EvaluationEventException | GraphicsWriteException
                    | InvalidProtocolBufferException e )
            {
                // Attempt to recover
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
                messageId = message.getJMSMessageID();
                correlationId = message.getJMSCorrelationID();
                outputPath = message.getStringProperty( GraphicsSubscriber.OUTPUT_PATH_STRING );

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

                message.acknowledge();

                LOGGER.debug( ACKNOWLEDGED_MESSAGE_WITH_CORRELATION_ID, messageId, correlationId );
            }
            catch ( JMSException | EvaluationEventException | GraphicsWriteException
                    | InvalidProtocolBufferException e )
            {
                // Attempt to recover
                this.recover( messageId, correlationId, e );
            }
        };
    }

    /**
     * Attempts to recover the session up to the {@link #MAXIMUM_RETRIES}.
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
                              this.getIdentifier(),
                              this.getNumberOfRetriesAttempted( correlationId ).get() + 1, // Counter starts at zero
                              this.broker.getMaximumRetries(),
                              correlationId,
                              message );

                this.session.recover();
            }
            catch ( JMSException f )
            {
                LOGGER.error( "While attempting to recover a session for evaluation {} in graphics subscriber {}, "
                              + "encountered an error that prevented recovery: ",
                              correlationId,
                              this.getIdentifier(),
                              f.getMessage() );
            }
            catch ( IOException g )
            {
                LOGGER.error( "While attempting recovery in graphics subscriber {}, failed to close an exception "
                              + "writer.",
                              this.getIdentifier() );
            }
        }

        // Stop if the maximum number of retries has been reached
        if ( this.getNumberOfRetriesAttempted( correlationId )
                 .incrementAndGet() == this.broker.getMaximumRetries() )
        {
            LOGGER.error( "Graphics subscriber {} encountered a consumption failure for evaluation {}. "
                          + "Recovery failed after {} attempts.",
                          this.getIdentifier(),
                          correlationId,
                          this.broker.getMaximumRetries() );

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
     */

    private void markEvaluationFailed( String evaluationId )
    {
        this.status.registerFailedEvaluation( evaluationId );
        this.getEvaluationConsumer( evaluationId ).markEvaluationFailed();
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
        this.getEvaluationsLock().lock();

        // Check initially
        EvaluationConsumer consumer = this.evaluations.get( evaluationId );

        if ( Objects.isNull( consumer ) )
        {
            consumer = new EvaluationConsumer( evaluationId,
                                               this.getIdentifier(),
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
        this.getEvaluationsLock().unlock();

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
     * Builds a subscriber.
     * 
     * @param identifier a unique identifier for the subscriber
     * @param status a graphics server whose status can be updated with subscriber progress
     * @param executorService the executor for completing graphics work
     * @param broker the broker
     * @throws JMSException if the subscriber components could not be constructed or started.
     * @throws NamingException if the expected broker destinations could not be discovered
     * @throws NullPointerException if any input is null
     */

    GraphicsSubscriber( String uniqueId,
                        ServerStatus status,
                        ExecutorService executorService,
                        BrokerConnectionFactory broker )
            throws JMSException, NamingException
    {
        Objects.requireNonNull( uniqueId );
        Objects.requireNonNull( status );
        Objects.requireNonNull( executorService );
        Objects.requireNonNull( broker );

        LOGGER.info( "Building a long-running graphics subscriber {} to listen for evaluation messages...",
                     uniqueId );

        this.uniqueId = uniqueId;
        this.status = status;
        this.broker = broker;

        this.evaluationStatusTopic = (Topic) this.broker.getDestination( EVALUATION_STATUS_QUEUE );
        this.evaluationTopic = (Topic) this.broker.getDestination( EVALUATION_QUEUE );
        this.statisticsTopic = (Topic) this.broker.getDestination( STATISTICS_QUEUE );

        this.connection = this.broker.get().createConnection();

        this.evaluationStatusPublisher = GraphicsPublisher.of( this.connection,
                                                               this.evaluationStatusTopic,
                                                               this.getIdentifier() );

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

        // Start the consumer connection
        LOGGER.info( "Started the consumer connection for long-running subscriber {}...",
                     this.getIdentifier() );
        this.connection.start();

        LOGGER.info( "Created long-running subscriber {}", this.getIdentifier() );
    }

    /**
     * Listen for failures on a connection.
     */

    private static class ConnectionExceptionListener implements ExceptionListener
    {

        @Override
        public void onException( JMSException exception )
        {
            exception.printStackTrace();

            LOGGER.error( "Encountered an error on a connection owned by a graphics subscriber: {}.",
                          exception.getMessage() );
        }
    }
}
