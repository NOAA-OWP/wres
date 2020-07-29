package wres.events;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Topic;
import javax.naming.NamingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;

import net.jcip.annotations.ThreadSafe;
import wres.events.MessagePublisher.MessageProperty;
import wres.eventsbroker.BrokerConnectionFactory;
import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.EvaluationStatus.CompletionStatus;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent.StatusMessageType;
import wres.statistics.generated.Pairs;
import wres.statistics.generated.Statistics;

/**
 * <p>Manages the publication and consumption of messages associated with one evaluation, defined as a single 
 * computational instance of one project declaration. Manages the publication of, and subscription to, the following 
 * types of messages that map to evaluation events:
 * 
 * <ol>
 * <li>Evaluation messages contained in {@link wres.statistics.generated.Evaluation};</li>
 * <li>Evaluation status messages contained in {@link wres.statistics.generated.EvaluationStatus}; and</li>
 * <li>Statistics messages contained in {@link wres.statistics.generated.Statistics}.</li>
 * <li>Pairs messages contained in {@link wres.statistics.generated.Pairs}.</li>
 * </ol>
 * 
 * <p>An evaluation is assigned a unique identifier on construction. This identifier is used to correlate messages that
 * belong to the same evaluation.
 * 
 * <p>Currently, this is intended for internal use by java producers and consumers within the core of the wres. In 
 * future, it is envisaged that an "advanced" API will be exposed to external clients that can post evaluations and 
 * register consumers to consume all types of evaluation messages. This advanced API would provide developers of 
 * microservices an alternative route, alongside the RESTful API, to publish and subscribe to evaluations, adding more
 * advanced possibilities, such as asynchronous messaging and streaming. This API will probably leverage a 
 * request-response pattern, such as gRPC (www.grpc.io), in order to register an evaluation and obtain the evaluation 
 * identifier and connection details for brokered (i.e., non request-response) communication.
 * 
 * @author james.brown@hydrosolved.com
 */

@ThreadSafe
public class Evaluation implements Closeable
{

    private static final String ENCOUNTERED_AN_ERROR = ", encountered an error: ";

    private static final String WHILE_ATTEMPTING_TO_PUBLISH_A_MESSAGE_TO_EVALUATION = "While attempting to publish a "
                                                                                      + "message to evaluation ";

    private static final Logger LOGGER = LoggerFactory.getLogger( Evaluation.class );

    private static final String DISCOVERED_AN_EVALUATION_MESSAGE_WITH_MISSING_INFORMATION =
            " discovered an evaluation message with missing information. ";

    private static final String WHILE_PUBLISHING_TO_EVALUATION = "While publishing to evaluation ";

    private static final String CANNOT_BUILD_AN_EVALUATION_WITHOUT_ONE_OR_MORE_SUBSCRIBERS =
            "Cannot build an evaluation without one or more subscribers for ";

    private static final String MESSAGE_FROM_A_BYTEBUFFER = " message from a bytebuffer.";

    private static final String PUBLICATION_COMPLETE_ERROR = "Publication to this evaluation has been notified "
                                                             + "complete and no further messages may be published.";

    /**
     * Used to generate unique evaluation identifiers.
     */

    private static final RandomString ID_GENERATOR = new RandomString();

    /**
     * Default name for the queue on the amq.topic that accepts evaluation messages.
     */

    static final String EVALUATION_QUEUE = "evaluation";

    /**
     * Default name for the queue on the amq.topic that accepts evaluation status messages.
     */

    static final String EVALUATION_STATUS_QUEUE = "status";

    /**
     * Default name for the queue on the amq.topic that accepts statistics messages.
     */

    static final String STATISTICS_QUEUE = "statistics";

    /**
     * Default name for the queue on the amq.topic that accepts pairs messages.
     */

    static final String PAIRS_QUEUE = "pairs";

    /**
     * A publisher for {@link wres.statistics.generated.Evaluation} messages.
     */

    private final MessagePublisher evaluationPublisher;

    /**
     * A publisher for {@link EvaluationStatus} messages.
     */

    private final MessagePublisher evaluationStatusPublisher;

    /**
     * A publisher for {@link Statistics} messages.
     */

    private final MessagePublisher statisticsPublisher;

    /**
     * A publisher for {@link Pairs} messages.
     */

    private final MessagePublisher pairsPublisher;

    /**
     * A collection of subscribers for {@link wres.statistics.generated.Evaluation} messages.
     */

    private final MessageSubscriber<wres.statistics.generated.Evaluation> evaluationSubscribers;

    /**
     * A collection of subscribers for {@link wres.statistics.generated.Statistics} messages.
     */

    private final MessageSubscriber<Statistics> statisticsSubscribers;

    /**
     * A collection of subscribers for {@link EvaluationStatus} messages.
     */

    private final MessageSubscriber<EvaluationStatus> evaluationStatusSubscribers;

    /**
     * A subscriber that listens for an {@link EvaluationStatus} messaging indicating that the evaluation is complete.
     */

    private final MessageSubscriber<EvaluationStatus> statusTrackerSubscriber;

    /**
     * A collection of subscribers for {@link Pairs} messages.
     */

    private final MessageSubscriber<Pairs> pairsSubscribers;

    /**
     * A unique identifier for the evaluation.
     */

    private final String evaluationId;

    /**
     * Is <code>true</code> if this evaluation has one or more subscribers for message groups. If <code>true</code>,
     * then any attempt to publish a message must be accompanied by a message group identifier.
     */

    private final boolean hasGroupSubscriptions;

    /**
     * The total message count, excluding evaluation status messages. This is one more than the number of statistics
     * messages because an evaluation begins with an evaluation description message. This is mutable state.
     */

    private final AtomicInteger messageCount;

    /**
     * The total number of evaluation status messages. This is mutable state.
     */

    private final AtomicInteger statusMessageCount;

    /**
     * The total number of pairs messages. This is mutable state.
     */

    private final AtomicInteger pairsMessageCount;

    /**
     * A record of the message groups to which messages have been published.
     */

    private final Set<String> messageGroups;

    /**
     * Is <code>true</code> when publication is complete. Upon completion, an evaluation status message is sent to 
     * notify completion, which includes the expected shape of the evaluation, and no further messages can be published 
     * with the public methods of this instance.
     */

    private final AtomicBoolean publicationComplete;

    /**
     * Is <code>true</code> if the evaluation has been stopped.
     */

    private final AtomicBoolean isStopped;

    /**
     * An object that contains a completion status message and a latch that counts to zero when an evaluation has been 
     * notified complete.
     */

    private final EvaluationStatusTracker statusTracker;

    /**
     * Returns the unique evaluation identifier.
     * 
     * @return the evaluation identifier
     */

    public String getEvaluationId()
    {
        return this.evaluationId;
    }

    /**
     * Opens an evaluation.
     * 
     * @param evaluation the evaluation message
     * @param broker the broker
     * @param consumers the consumers to subscribe
     * @return an open evaluation
     * @throws NullPointerException if any input is null
     */

    public static Evaluation open( wres.statistics.generated.Evaluation evaluation,
                                   BrokerConnectionFactory broker,
                                   Consumers consumers )
    {
        Objects.requireNonNull( evaluation );
        Objects.requireNonNull( broker );
        Objects.requireNonNull( consumers );

        return new Builder().setBroker( broker )
                            .setEvaluation( evaluation )
                            .setConsumers( consumers )
                            .build();
    }

    /**
     * Publish an {@link wres.statistics.generated.EvaluationStatus} message for the current evaluation.
     * 
     * @param status the status message
     * @throws NullPointerException if the input is null
     * @throws IllegalStateException if the publication of messages to this evaluation has been notified complete
     */

    public void publish( EvaluationStatus status )
    {
        this.publish( status, null );
    }

    /**
     * Publish an {@link wres.statistics.generated.Statistics} message for the current evaluation.
     * 
     * @param statistics the statistics message
     * @throws NullPointerException if the input is null
     * @throws IllegalStateException if the publication of messages to this evaluation has been notified complete
     */

    public void publish( Statistics statistics )
    {
        this.publish( statistics, null );
    }

    /**
     * Publish an {@link wres.statistics.generated.Pairs} message for the current evaluation.
     * 
     * @param pairs the pairs message
     * @throws NullPointerException if the input is null
     * @throws IllegalStateException if the publication of messages to this evaluation has been notified complete
     */

    public void publish( Pairs pairs )
    {
        this.validateRequestToPublish();

        Objects.requireNonNull( pairs );

        ByteBuffer body = ByteBuffer.wrap( pairs.toByteArray() );

        this.internalPublish( body, this.pairsPublisher, Evaluation.PAIRS_QUEUE, null );

        this.pairsMessageCount.getAndIncrement();
    }

    /**
     * Publish an {@link wres.statistics.generated.EvaluationStatus} message for the current evaluation.
     * 
     * @param status the status message
     * @param groupId an optional group identifier to identify grouped status messages (required if group subscribers)
     * @throws NullPointerException if the message is null or the groupId is null when there are group subscriptions
     * @throws IllegalStateException if the publication of messages to this evaluation has been notified complete
     */

    public void publish( EvaluationStatus status, String groupId )
    {
        this.validateRequestToPublish();

        this.validateStatusMessage( status, groupId );

        ByteBuffer body = ByteBuffer.wrap( status.toByteArray() );

        this.internalPublish( body, this.evaluationStatusPublisher, Evaluation.EVALUATION_STATUS_QUEUE, groupId );

        this.statusMessageCount.getAndIncrement();

        // Record group
        if ( Objects.nonNull( groupId ) )
        {
            this.messageGroups.add( groupId );
        }
    }

    /**
     * Publish an {@link wres.statistics.generated.Statistics} message for the current evaluation.
     * 
     * @param statistics the statistics message
     * @param groupId an optional group identifier to identify grouped status messages (required if group subscribers)
     * @throws NullPointerException if the message is null or the groupId is null when there are group subscriptions
     * @throws IllegalStateException if the publication of messages to this evaluation has been notified complete
     */

    public void publish( Statistics statistics, String groupId )
    {
        this.validateRequestToPublish();

        Objects.requireNonNull( statistics );
        this.validateGroupId( groupId );

        ByteBuffer body = ByteBuffer.wrap( statistics.toByteArray() );

        this.internalPublish( body, this.statisticsPublisher, Evaluation.STATISTICS_QUEUE, groupId );

        this.messageCount.getAndIncrement();

        // Update the status
        String message = "Published a new statistics message for evaluation " + this.getEvaluationId()
                         + " with pool boundaries "
                         + statistics.getPool();

        EvaluationStatus ongoing = EvaluationStatus.newBuilder()
                                                   .setCompletionStatus( CompletionStatus.EVALUATION_ONGOING )
                                                   .addStatusEvents( EvaluationStatusEvent.newBuilder()
                                                                                          .setEventType( StatusMessageType.INFO )
                                                                                          .setEventMessage( message ) )
                                                   .build();

        this.publish( ongoing, groupId );

        // Record group
        if ( Objects.nonNull( groupId ) )
        {
            this.messageGroups.add( groupId );
        }
    }

    /**
     * <p>Marks complete the publication of messages by this instance, notwithstanding a final evaluation completion 
     * message after all consumers have finished consumption. Marking publication complete means that further messages
     * cannot be published using the public methods of this instance. However, other instances may continue to publish 
     * messages about other evaluations and consumers may continue to publish their own status with respect to this 
     * evaluation. 
     * 
     * <p>If the evaluation failed, then a failure message should be published so that consumers can learn about the 
     * failure and then evaluation should then be stopped promptly. This is achieved by 
     * {@link #stopOnException(Exception)}.
     * 
     * @throws IllegalStateException if publication was already marked complete
     * @see #stopOnException(Exception)
     */

    public void markPublicationCompleteReportedSuccess()
    {
        CompletionStatus status = CompletionStatus.PUBLICATION_COMPLETE_REPORTED_SUCCESS;

        EvaluationStatus complete = EvaluationStatus.newBuilder()
                                                    .setCompletionStatus( status )
                                                    .setMessageCount( this.getPublishedMessageCount() )
                                                    .setPairsMessageCount( this.getPublishedPairsMessageCount() )
                                                    .setGroupCount( this.getPublishedGroupCount() )
                                                    .setStatusMessageCount( this.getPublishedStatusMessageCount()
                                                                            + 1 ) // This one
                                                    .build();

        this.publish( complete );

        // No further publication allowed by public methods
        this.publicationComplete.set( true );
    }

    @Override
    public String toString()
    {
        return "Evaluation with unique identifier: " + evaluationId;
    }

    /**
     * <p>Forcibly terminates an evaluation without completing any outstanding publication or consumption. This may be
     * necessary when an out-of-band exception is encountered (e.g., when producing messages for publication by this 
     * evaluation), which requires the evaluation to terminate promptly, without attempting further progress. To 
     * terminate gracefully and await all outstanding publication and consumption, use {@link #close()}.
     * 
     * <p>Do not attempt to terminate an evaluation on encountering an {@link Error}, only an {@link Exception}. In 
     * general, errors are not recoverable and the application instance should be terminated without any attempt to 
     * recover.
     * 
     * <p>The provided exception is used to notify consumers of the failed evaluation, even when publication has been
     * marked complete for this instance.
     * 
     * @param exception an optional exception instance to propagate to consumers
     * @throws IOException if the evaluation could not be stopped. 
     * @see #close()
     */

    public void stopOnException( Exception exception ) throws IOException
    {
        LOGGER.debug( "Stopping evaluation {} on encountering an exception.", this.getEvaluationId() );

        CompletionStatus status = CompletionStatus.EVALUATION_COMPLETE_REPORTED_FAILURE;

        // Create a string representation of the exception stack
        StringWriter sw = new StringWriter();

        sw.append( "Evaluation " + this.getEvaluationId() + " failed." );

        if ( Objects.nonNull( exception ) )
        {
            sw.append( " The following exception was encountered: " );
            PrintWriter pw = new PrintWriter( sw );
            exception.printStackTrace( pw );
        }

        // Create an event to report on it
        EvaluationStatusEvent event = EvaluationStatusEvent.newBuilder()
                                                           .setEventType( StatusMessageType.ERROR )
                                                           .setEventMessage( sw.toString() )
                                                           .build();

        Instant now = Instant.now();
        long seconds = now.getEpochSecond();
        int nanos = now.getNano();

        EvaluationStatus complete = EvaluationStatus.newBuilder()
                                                    .setCompletionStatus( status )
                                                    .setTime( Timestamp.newBuilder()
                                                                       .setSeconds( seconds )
                                                                       .setNanos( nanos ) )
                                                    .addStatusEvents( event )
                                                    .build();

        // Internal publish in case publication has been completed for this instance
        ByteBuffer body = ByteBuffer.wrap( complete.toByteArray() );

        try
        {
            this.internalPublish( body, this.evaluationStatusPublisher, Evaluation.EVALUATION_STATUS_QUEUE, null );
        }
        catch ( EvaluationEventException e )
        {
            LOGGER.debug( "Unable to notify consumers about an exception that stopped  evalation {}.",
                          this.getEvaluationId() );
        }

        // Flag stopped, to allow for ungraceful close
        this.isStopped.set( true );

        // Now do the actual close ordinarily
        this.close();
    }

    @Override
    public void close() throws IOException
    {
        LOGGER.debug( "Closing evaluation {}.", this.getEvaluationId() );

        if ( !this.isStopped() )
        {
            LOGGER.debug( "Awaiting completion of evaluation {}...", this.getEvaluationId() );

            Instant then = Instant.now();

            try
            {
                this.statusTracker.await();

                Instant now = Instant.now();

                LOGGER.debug( "Completed publication and consumption for evaluation {}. {} elapsed between notification of "
                              + "completion and the end of consumption. Ready to validate and then close this evaluation.",
                              this.getEvaluationId(),
                              Duration.between( then, now ) );

                // Exceptions uncovered?
                // Perhaps failing on the delivery of evaluation status messages is too high a bar?
                boolean failed = Objects.nonNull( this.evaluationSubscribers.getFailedOn() )
                                 || Objects.nonNull( this.evaluationStatusSubscribers.getFailedOn() )
                                 || Objects.nonNull( this.statisticsSubscribers.getFailedOn() )
                                 || Objects.nonNull( this.pairsSubscribers.getFailedOn() )
                                 || Objects.nonNull( this.statusTrackerSubscriber.getFailedOn() );

                if ( failed )
                {
                    String separator = System.getProperty( "line.separator" );

                    LOGGER.debug( "While closing evaluation {}, discovered one or more undeliverable messages. The first "
                                  + "undeliverable evaluation message is:{}{}{}The first undeliverable evaluation status "
                                  + "message is:{}{}{}The first undeliverable statistics message is:{}{}{}The first "
                                  + "undeliverable pairs message is:{}{}{}The first undeliverable evaluation completion "
                                  + "message is:{}{}",
                                  this.getEvaluationId(),
                                  separator,
                                  this.evaluationSubscribers.getFailedOn(),
                                  separator,
                                  separator,
                                  this.evaluationStatusSubscribers.getFailedOn(),
                                  separator,
                                  separator,
                                  this.statisticsSubscribers.getFailedOn(),
                                  separator,
                                  separator,
                                  this.pairsSubscribers.getFailedOn(),
                                  separator,
                                  separator,
                                  this.statusTrackerSubscriber.getFailedOn() );

                    throw new EvaluationEventException( "While closing evaluation " + this.getEvaluationId()
                                                        + ", discovered undeliverable messages after repeated delivery "
                                                        + "attempts. These messages have been posted to their appropriate dead "
                                                        + "letter queue (DLQ) for further inspection and removal." );
                }
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();

                throw new EvaluationEventException( "Interrupted while waiting for completion status event for evaluuation "
                                                    + this.getEvaluationId() );
            }
            finally
            {
                this.evaluationPublisher.close();
                this.evaluationSubscribers.close();
                this.evaluationStatusPublisher.close();
                this.evaluationStatusSubscribers.close();
                this.statisticsPublisher.close();
                this.statisticsSubscribers.close();
                this.pairsSubscribers.close();
                this.statusTrackerSubscriber.close();
            }
        }

        LOGGER.debug( "Closed evaluation {}.", this.getEvaluationId() );
    }

    /**
     * Returns the expected number of messages, excluding evaluation status messages. This is one more than the number
     * of statistics messages because an evaluation starts with an evaluation description message.
     * 
     * @return the published message count
     */

    public int getPublishedMessageCount()
    {
        return this.messageCount.get();
    }

    /**
     * Returns the expected number of evaluation status messages.
     * 
     * @return the evaluation status message count
     */

    public int getPublishedStatusMessageCount()
    {
        return this.statusMessageCount.get();
    }

    /**
     * Returns the expected number of pairs messages.
     * 
     * @return the pairs message count
     */

    public int getPublishedPairsMessageCount()
    {
        return this.pairsMessageCount.get();
    }

    /**
     * Returns the  number of groups to which statistics messages were published.
     * 
     * @return the group  count
     */

    public int getPublishedGroupCount()
    {
        return this.messageGroups.size();
    }

    /**
     * Builds an evaluation.
     * 
     * @author james.brown@hydrosolved.com
     */

    public static class Builder
    {
        /**
         * Broker connections.
         */

        private BrokerConnectionFactory broker;

        /**
         * Evaluation message.
         */

        private wres.statistics.generated.Evaluation evaluation;

        /**
         * Consumers of evaluation events.
         */

        private Consumers consumers;

        /**
         * Sets the broker.
         * 
         * @param broker the broker
         * @return this builder
         */

        public Builder setBroker( BrokerConnectionFactory broker )
        {
            this.broker = broker;

            return this;
        }

        /**
         * Sets the evaluation message containing an overview of the evaluation.
         * 
         * @param evaluation the evaluation
         * @return this builder
         */

        public Builder setEvaluation( wres.statistics.generated.Evaluation evaluation )
        {
            this.evaluation = evaluation;

            return this;
        }

        /**
         * Adds a collection of consumers of evaluation events.
         * 
         * @param consumers the consumer subscription
         * @return this builder 
         */

        public Builder setConsumers( Consumers consumers )
        {
            this.consumers = consumers;

            return this;
        }

        /**
         * Builds an evaluation.
         * 
         * @return an evaluation
         */

        public Evaluation build()
        {
            return new Evaluation( this );
        }
    }

    /**
     * Small bag of state for sharing evaluation information.
     * 
     * @author james.brown@hydrosolved.com
     */

    static class EvaluationInfo
    {

        /**
         * Evaluation identifier.
         */

        private final String evaluationId;

        /**
         * Optional completion tracker for message groups.
         */

        private final GroupCompletionTracker completionTracker;

        /**
         * Number of queues constructed with respect to this evaluation, which assists in naming durable queues.
         */

        private final AtomicInteger queuesConstructed;

        /**
         * Returns the next available queue number, one more than the number of queues constructed before this call.
         * 
         * @return the next queue number.
         */

        int getNextQueueNumber()
        {
            return this.queuesConstructed.incrementAndGet();
        }

        /**
         * @return the evaluation identifier
         */
        String getEvaluationId()
        {
            return evaluationId;
        }

        /**
         * @return the completion tracker
         */
        GroupCompletionTracker getCompletionTracker()
        {
            return completionTracker;
        }

        /**
         * Return an instance.
         * 
         * @param evaluationId the evaluation identifier
         * @return an instance
         */

        static EvaluationInfo of( String evaluationId )
        {
            return new EvaluationInfo( evaluationId, null );
        }

        /**
         * Return an instance.
         * 
         * @param evaluationId the evaluation identifier
         * @param completionTracker the completion tracker
         * @return an instance
         */

        static EvaluationInfo of( String evaluationId, GroupCompletionTracker completionTracker )
        {
            return new EvaluationInfo( evaluationId, completionTracker );
        }

        /**
         * Build an instance.
         * 
         * @param evaluationId the evaluation identifier
         * @param completionTracker the optional completion tracker
         * @throws NullPointerException if any input is null
         */

        private EvaluationInfo( String evaluationId, GroupCompletionTracker completionTracker )
        {
            Objects.requireNonNull( evaluationId );

            this.evaluationId = evaluationId;
            this.completionTracker = completionTracker;
            this.queuesConstructed = new AtomicInteger();
        }
    }

    /**
     * Returns a unique identifier for identifying a component of an evaluations, such as the evaluation itself or a
     * consumer.
     * 
     * @return a unique identifier
     */

    static String getUniqueId()
    {
        return Evaluation.ID_GENERATOR.generate();
    }

    /**
     * Validates a request to publish.
     * 
     * @throws IllegalStateException if publication is already complete or the evaluation has been stopped
     */

    private void validateRequestToPublish()
    {
        if ( this.isStopped.get() )
        {
            throw new IllegalStateException( WHILE_ATTEMPTING_TO_PUBLISH_A_MESSAGE_TO_EVALUATION
                                             + this.getEvaluationId()
                                             + ENCOUNTERED_AN_ERROR
                                             + PUBLICATION_COMPLETE_ERROR );
        }

        if ( this.publicationComplete.get() )
        {
            throw new IllegalStateException( WHILE_ATTEMPTING_TO_PUBLISH_A_MESSAGE_TO_EVALUATION
                                             + this.getEvaluationId()
                                             + ENCOUNTERED_AN_ERROR
                                             + " this evaluation has been stopped and can no longer accept "
                                             + "publication requests." );
        }
    }

    /**
     * @return true if stopped, otherwise false.
     */

    private boolean isStopped()
    {
        return this.isStopped.get();
    }

    /**
     * Validates a status message for expected content and looks for the presence of a group identifier where required. 
     * 
     * @param message the message
     * @param groupId a group identifier
     * @throws NullPointerException if the group identifier is null
     */

    private void validateStatusMessage( EvaluationStatus message, String groupId )
    {
        Objects.requireNonNull( message );

        // Group identifier required in some cases
        if ( message.getCompletionStatus() == CompletionStatus.GROUP_COMPLETE_REPORTED_SUCCESS )
        {
            this.validateGroupId( groupId );

            if ( message.getMessageCount() == 0 )
            {
                throw new IllegalArgumentException( WHILE_PUBLISHING_TO_EVALUATION + this.getEvaluationId()
                                                    + DISCOVERED_AN_EVALUATION_MESSAGE_WITH_MISSING_INFORMATION
                                                    + "The expected message count is required when the completion "
                                                    + "status reports "
                                                    + CompletionStatus.GROUP_COMPLETE_REPORTED_SUCCESS );
            }
        }

        CompletionStatus status = message.getCompletionStatus();

        if ( status == CompletionStatus.EVALUATION_COMPLETE_REPORTED_SUCCESS
             || status == CompletionStatus.PUBLICATION_COMPLETE_REPORTED_SUCCESS )
        {

            if ( message.getMessageCount() == 0 )
            {
                throw new IllegalArgumentException( WHILE_PUBLISHING_TO_EVALUATION + this.getEvaluationId()
                                                    + DISCOVERED_AN_EVALUATION_MESSAGE_WITH_MISSING_INFORMATION
                                                    + "The expected message count is required when the completion "
                                                    + "status reports "
                                                    + status );
            }
            if ( message.getStatusMessageCount() == 0 )
            {
                throw new IllegalArgumentException( WHILE_PUBLISHING_TO_EVALUATION + this.getEvaluationId()
                                                    + DISCOVERED_AN_EVALUATION_MESSAGE_WITH_MISSING_INFORMATION
                                                    + "The expected count of evaluation status messages is required "
                                                    + "when the completion status reports "
                                                    + status );
            }
        }
    }

    /**
     * Looks for the presence of a group identifier and throws an exception when absent.
     * 
     * @param groupId a group identifier
     * @throws NullPointerException if the group identifier is null
     */

    private void validateGroupId( String groupId )
    {
        if ( this.hasGroupSubscriptions() )
        {
            Objects.requireNonNull( groupId,
                                    "Evaluation " + this.getEvaluationId()
                                             + " has subscriptions to message groups. When publishing to this "
                                             + "evaluation, a group identifier must be supplied." );
        }
    }

    /**
     * Returns <code>true</code> if the evaluation has subscriptions for message groups, otherwise <code>false</code>.
     * 
     * @return true if there are subscriptions for message groups
     */

    private boolean hasGroupSubscriptions()
    {
        return this.hasGroupSubscriptions;
    }

    /**
     * Builds an exception with the specified message.
     * 
     * @param builder the builder
     * @throws EvaluationEventException if the evaluation could not be constructed for any reason
     * @throws NullPointerException if the broker is null
     * @throws IllegalArgumentException if there are zero subscribers for any queue 
     * @throws IllegalStateException if the evaluation message fails to declare the expected number of pools
     */

    private Evaluation( Builder builder )
    {
        // Copy then validate
        BrokerConnectionFactory broker = builder.broker;
        List<Consumer<wres.statistics.generated.Evaluation>> evaluationSubs =
                builder.consumers.getEvaluationConsumers();
        List<Consumer<EvaluationStatus>> statusSubs = builder.consumers.getEvaluationStatusConsumers();
        List<Consumer<Statistics>> statisticsSubs = builder.consumers.getStatisticsConsumers();
        List<Consumer<Collection<Statistics>>> groupedStatisticsSubs =
                builder.consumers.getGroupedStatisticsConsumers();
        List<Consumer<Pairs>> pairsSubs = builder.consumers.getPairsConsumers();
        Set<String> externalSubs = builder.consumers.getExternalSubscribers();

        wres.statistics.generated.Evaluation evaluationMessage = builder.evaluation;

        Objects.requireNonNull( broker, "Cannot create an evaluation without a broker connection." );
        Objects.requireNonNull( evaluationMessage, "Cannot create an evaluation without an evaluation message." );

        if ( evaluationSubs.isEmpty() )
        {
            throw new IllegalArgumentException( CANNOT_BUILD_AN_EVALUATION_WITHOUT_ONE_OR_MORE_SUBSCRIBERS
                                                + "evaluation events." );
        }

        if ( statusSubs.isEmpty() )
        {
            throw new IllegalArgumentException( CANNOT_BUILD_AN_EVALUATION_WITHOUT_ONE_OR_MORE_SUBSCRIBERS
                                                + "evaluation status events." );
        }

        if ( statisticsSubs.isEmpty() && groupedStatisticsSubs.isEmpty() )
        {
            throw new IllegalArgumentException( CANNOT_BUILD_AN_EVALUATION_WITHOUT_ONE_OR_MORE_SUBSCRIBERS
                                                + "evaluation statistics events." );
        }

        this.publicationComplete = new AtomicBoolean();
        this.isStopped = new AtomicBoolean();
        this.evaluationId = Evaluation.getUniqueId();
        this.hasGroupSubscriptions = !groupedStatisticsSubs.isEmpty();

        EvaluationInfo evaluationInfo = EvaluationInfo.of( this.getEvaluationId(), GroupCompletionTracker.of() );

        // Register publishers and subscribers
        ConnectionFactory factory = broker.get();

        try
        {
            Topic status = (Topic) broker.getDestination( Evaluation.EVALUATION_STATUS_QUEUE );
            this.evaluationStatusPublisher = MessagePublisher.of( factory, status );
            this.evaluationStatusSubscribers =
                    new MessageSubscriber.Builder<EvaluationStatus>().setConnectionFactory( factory )
                                                                     .setTopic( status )
                                                                     .addSubscribers( statusSubs )
                                                                     .setEvaluationInfo( evaluationInfo )
                                                                     .setEvaluationStatusTopic( status )
                                                                     .setExpectedMessageCountSupplier( EvaluationStatus::getStatusMessageCount )
                                                                     .setMapper( this.getStatusMapper() )
                                                                     .setContext( Evaluation.EVALUATION_STATUS_QUEUE )
                                                                     .build();

            Topic evaluation = (Topic) broker.getDestination( Evaluation.EVALUATION_QUEUE );
            this.evaluationPublisher = MessagePublisher.of( factory, evaluation );
            this.evaluationSubscribers =
                    new MessageSubscriber.Builder<wres.statistics.generated.Evaluation>().setConnectionFactory( factory )
                                                                                         .setTopic( evaluation )
                                                                                         .addSubscribers( evaluationSubs )
                                                                                         .setEvaluationInfo( evaluationInfo )
                                                                                         .setExpectedMessageCountSupplier( message -> 1 )
                                                                                         .setMapper( this.getEvaluationMapper() )
                                                                                         .setEvaluationStatusTopic( status )
                                                                                         .setContext( Evaluation.EVALUATION_QUEUE )
                                                                                         .build();

            Topic statistics = (Topic) broker.getDestination( Evaluation.STATISTICS_QUEUE );
            this.statisticsPublisher = MessagePublisher.of( factory, statistics );
            this.statisticsSubscribers =
                    new MessageSubscriber.Builder<Statistics>().setConnectionFactory( factory )
                                                               .setTopic( statistics )
                                                               .addSubscribers( statisticsSubs )
                                                               .setEvaluationInfo( evaluationInfo )
                                                               .setExpectedMessageCountSupplier( message -> message.getMessageCount()
                                                                                                            - 1 )
                                                               .addGroupSubscribers( groupedStatisticsSubs )
                                                               .setEvaluationStatusTopic( status )
                                                               .setMapper( this.getStatisticsMapper() )
                                                               .setContext( Evaluation.STATISTICS_QUEUE )
                                                               .build();

            Topic pairs = (Topic) broker.getDestination( Evaluation.PAIRS_QUEUE );
            this.pairsPublisher = MessagePublisher.of( factory, pairs );
            this.pairsSubscribers =
                    new MessageSubscriber.Builder<Pairs>().setConnectionFactory( factory )
                                                          .setTopic( pairs )
                                                          .addSubscribers( pairsSubs )
                                                          .setEvaluationInfo( evaluationInfo )
                                                          .setExpectedMessageCountSupplier( EvaluationStatus::getPairsMessageCount )
                                                          .setEvaluationStatusTopic( status )
                                                          .setMapper( this.getPairsMapper() )
                                                          .setContext( Evaluation.PAIRS_QUEUE )
                                                          .build();
        }
        catch ( JMSException | NamingException e )
        {
            throw new EvaluationEventException( "Unable to construct an evaluation.", e );
        }

        LOGGER.info( "Created a new evaluation with id {}.", evaluationId );

        // Mutable state
        this.messageCount = new AtomicInteger();
        this.statusMessageCount = new AtomicInteger();
        this.pairsMessageCount = new AtomicInteger();
        this.messageGroups = ConcurrentHashMap.newKeySet();

        // Subscriber to evaluation status messages that allows this instance to track its own status
        Set<String> subscribers = new HashSet<>();
        subscribers.add( this.evaluationStatusSubscribers.getIdentifier() );
        subscribers.add( this.evaluationSubscribers.getIdentifier() );
        subscribers.add( this.statisticsSubscribers.getIdentifier() );
        // Add any external subscribers
        subscribers.addAll( externalSubs );

        // Optional pairs subscribers
        if ( !pairsSubs.isEmpty() )
        {
            subscribers.add( this.pairsSubscribers.getIdentifier() );
        }

        // Create the status tracker
        this.statusTracker = new EvaluationStatusTracker( this, Collections.unmodifiableSet( subscribers ) );
        String completionContext = Evaluation.EVALUATION_STATUS_QUEUE + "-HOUSEKEEPING-evaluation-complete";

        try
        {
            Topic status = (Topic) broker.getDestination( Evaluation.EVALUATION_STATUS_QUEUE );

            this.statusTrackerSubscriber =
                    new MessageSubscriber.Builder<EvaluationStatus>().setConnectionFactory( factory )
                                                                     .setTopic( status )
                                                                     .addSubscribers( List.of( this.statusTracker ) )
                                                                     .setEvaluationInfo( evaluationInfo )
                                                                     .setMapper( this.getStatusMapper() )
                                                                     // Status tracker has no precise expectation of count
                                                                     .setExpectedMessageCountSupplier( message -> 0 )
                                                                     .setEvaluationStatusTopic( status )
                                                                     .setContext( completionContext )
                                                                     .setIgnoreConsumerMessages( false ) // Track consumption too
                                                                     .build();
        }
        catch ( JMSException | NamingException e )
        {
            throw new EvaluationEventException( "Unable to construct an evaluation.", e );
        }

        // Publish the evaluation and update the evaluation status
        this.internalPublish( evaluationMessage );

        Instant now = Instant.now();
        long seconds = now.getEpochSecond();
        int nanos = now.getNano();
        EvaluationStatus started = EvaluationStatus.newBuilder()
                                                   .setCompletionStatus( CompletionStatus.EVALUATION_STARTED )
                                                   .setTime( Timestamp.newBuilder()
                                                                      .setSeconds( seconds )
                                                                      .setNanos( nanos ) )
                                                   .build();

        this.publish( started );
    }

    /**
     * Maps a message contained in a {@link ByteBuffer} to a {@link EvaluationStatus}.
     * @return a mapper
     */

    private Function<ByteBuffer, EvaluationStatus> getStatusMapper()
    {
        return buffer -> {
            try
            {
                return EvaluationStatus.parseFrom( buffer );
            }
            catch ( InvalidProtocolBufferException e )
            {
                throw new EvaluationEventException( "While processing an evaluation status event for evaluation "
                                                    + this.getEvaluationId()
                                                    + ", failed to create an evaluation status "
                                                    + MESSAGE_FROM_A_BYTEBUFFER,
                                                    e );
            }
        };
    }

    /**
     * Maps a message contained in a {@link ByteBuffer} to a {@link Pairs}.
     * @return a mapper
     */

    private Function<ByteBuffer, Pairs> getPairsMapper()
    {
        return buffer -> {
            try
            {
                return Pairs.parseFrom( buffer );
            }
            catch ( InvalidProtocolBufferException e )
            {
                throw new EvaluationEventException( "While processing a pairs event for evaluation "
                                                    + this.getEvaluationId()
                                                    + ", failed to create a pairs "
                                                    + MESSAGE_FROM_A_BYTEBUFFER,
                                                    e );
            }
        };
    }

    /**
     * Maps a message contained in a {@link ByteBuffer} to a {@link wres.statistics.generated.Evaluation}.
     * @return a mapper
     */

    private Function<ByteBuffer, wres.statistics.generated.Evaluation> getEvaluationMapper()
    {
        return buffer -> {
            try
            {
                return wres.statistics.generated.Evaluation.parseFrom( buffer );
            }
            catch ( InvalidProtocolBufferException e )
            {
                throw new EvaluationEventException( "While processing an evaluation event for evaluation "
                                                    + this.getEvaluationId()
                                                    + ", failed to create an evaluation "
                                                    + MESSAGE_FROM_A_BYTEBUFFER,
                                                    e );
            }
        };
    }

    /**
     * Maps a message contained in a {@link ByteBuffer} to a {@link Statistics}.
     * @return a mapper
     */

    private Function<ByteBuffer, Statistics> getStatisticsMapper()
    {
        return buffer -> {
            try
            {
                return Statistics.parseFrom( buffer );
            }
            catch ( InvalidProtocolBufferException e )
            {
                throw new EvaluationEventException( "While processing a statistics event for evaluation "
                                                    + this.getEvaluationId()
                                                    + ", failed to create a statistics "
                                                    + MESSAGE_FROM_A_BYTEBUFFER,
                                                    e );
            }
        };
    }

    /**
     * Internal publish, do not expose.
     * 
     * @param body the message body
     * @param publisher the publisher
     * @param queue the queue name on the amq.topic
     * @param groupId the optional message group identifier
     */

    private void internalPublish( ByteBuffer body, MessagePublisher publisher, String queue, String groupId )
    {
        // Published below, so increment by 1 here 
        String messageId = "ID:" + this.getEvaluationId() + "-m" + ( publisher.getMessageCount() + 1 );

        // Create the metadata
        Map<MessageProperty, String> properties = new EnumMap<>( MessageProperty.class );
        properties.put( MessageProperty.JMS_MESSAGE_ID, messageId );
        properties.put( MessageProperty.JMS_CORRELATION_ID, this.getEvaluationId() );

        if ( Objects.nonNull( groupId ) )
        {
            properties.put( MessageProperty.JMSX_GROUP_ID, groupId );
        }

        try
        {
            publisher.publish( body, Collections.unmodifiableMap( properties ) );

            LOGGER.info( "Published a message with identifier {} and correlation identifier {} and groupId {} for "
                         + "evaluation {} to amq.topic/{}.",
                         messageId,
                         this.getEvaluationId(),
                         groupId,
                         this.getEvaluationId(),
                         queue );
        }
        catch ( JMSException e )
        {
            throw new EvaluationEventException( "Failed to send a message for evaluation "
                                                + this.getEvaluationId()
                                                + " to amq.topic/"
                                                + queue,
                                                e );
        }
    }

    /**
     * Publish an {@link wres.statistics.generated.Evaluation} message for the current evaluation.
     * 
     * @param evaluation the evaluation message
     */

    private void internalPublish( wres.statistics.generated.Evaluation evaluation )
    {
        Objects.requireNonNull( evaluation );

        ByteBuffer body = ByteBuffer.wrap( evaluation.toByteArray() );

        this.internalPublish( body, this.evaluationPublisher, Evaluation.EVALUATION_QUEUE, null );

        this.messageCount.getAndIncrement();
    }

    /**
     * Generate a compact, unique, identifier for an evaluation. Thanks to: 
     * https://neilmadden.blog/2018/08/30/moving-away-from-uuids/
     * 
     * @author james.brown@hydrosolved.com
     */

    private static class RandomString
    {
        private static final SecureRandom random = new SecureRandom();
        private static final Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();

        private String generate()
        {
            byte[] buffer = new byte[20];
            random.nextBytes( buffer );
            return encoder.encodeToString( buffer );
        }
    }

    /**
     * Value object for storing a completion status message and a latch to wait until it is available.
     * 
     * @author james.brown@hydrosolved.com
     */

    private class EvaluationStatusTracker implements Consumer<EvaluationStatus>
    {

        /**
         * A latch that records the completion of publication. There is no timeout on publication.
         */

        private final CountDownLatch publicationLatch = new CountDownLatch( 1 );

        /**
         * A latch for each top-level subscriber by subscriber identifier. A top-level subscriber is an external 
         * subscriber or an internal subscriber that collates one or more consumers. For example if there are three 
         * consumers of evaluation status messages, one consumer of pairs messages, one consumer of statistics messages, 
         * one consumer of evaluation description messages and one external subscriber, then there are five top-level 
         * subscribers, one for each message type that has one or more consumers. Once set, this latch is counted down 
         * for each subscriber that reports completion. Each latch is initialized with a count of one. Consumption is
         * allowed to timeout when no progress is recorded within a prescribed duration.
         */

        private final Map<String, TimedCountDownLatch> subscriberLatches;

        /**
         * The evaluation.
         */

        private final Evaluation evaluation;

        /**
         * A set of subscriber identifiers that have been registered as ready for consumption.
         */

        private final Set<String> subscribersReady;

        /**
         * A set of subscriber identifiers for which subscriptions are expected.
         */

        private final Set<String> expectedSubscribers;

        /**
         * The timeout duration.
         */

        private final long timeout;

        /**
         * The timeout units.
         */

        private final TimeUnit timeoutUnits;

        @Override
        public void accept( EvaluationStatus message )
        {
            Objects.requireNonNull( message );

            CompletionStatus status = message.getCompletionStatus();

            switch ( status )
            {
                case PUBLICATION_COMPLETE_REPORTED_SUCCESS:
                case PUBLICATION_COMPLETE_REPORTED_FAILURE:
                    this.publicationLatch.countDown();
                    break;
                case READY_TO_CONSUME:
                    this.registerSubscriberReady( message );
                    break;
                case CONSUMPTION_COMPLETE_REPORTED_SUCCESS:
                case CONSUMPTION_COMPLETE_REPORTED_FAILURE:
                    this.registerSubscriberComplete( message );
                    break;
                case CONSUMPTION_ONGOING:
                    this.registerConsumptionOngoing( message );
                    break;
                default:
                    break;
            }
        }

        /**
         * Wait until the evaluation has completed.
         * 
         * @throws InterruptedException if the evaluation was interrupted.
         */

        private void await() throws InterruptedException
        {
            LOGGER.debug( "While processing evaluation {}, awaiting confirmation that publication has completed.",
                          this.evaluation.getEvaluationId() );

            this.publicationLatch.await();

            LOGGER.debug( "While processing evaluation {}, received confirmation that publication has completed.",
                          this.evaluation.getEvaluationId() );

            for ( Map.Entry<String, TimedCountDownLatch> nextEntry : this.subscriberLatches.entrySet() )
            {
                String consumerId = nextEntry.getKey();
                TimedCountDownLatch nextLatch = nextEntry.getValue();

                LOGGER.debug( "While processing evaluation {}, awaiting confirmation that consumption has completed "
                              + "for subscription {}.",
                              this.evaluation.getEvaluationId(),
                              consumerId );

                // Wait for a fixed period unless there is progress
                nextLatch.await( this.timeout, this.timeoutUnits );

                LOGGER.debug( "While processing evaluation {}, received confirmation that consumption has completed "
                              + "for subscription {}.",
                              this.evaluation.getEvaluationId(),
                              consumerId );
            }

            LOGGER.debug( "While processing evaluation {}, received confirmation that consumption has stopped "
                          + "across all {} registered subscriptions.",
                          this.evaluation.getEvaluationId(),
                          this.expectedSubscribers.size() );

            // Throw an exception if any consumers failed to complete consumption
            Set<String> identifiers = this.subscriberLatches.entrySet()
                                                            .stream()
                                                            .filter( next -> next.getValue().getCount() > 0 )
                                                            .map( Map.Entry::getKey )
                                                            .collect( Collectors.toUnmodifiableSet() );

            if ( !identifiers.isEmpty() )
            {
                throw new SubscriberTimedOutException( "While processing evaluation "
                                                       + this.evaluation.getEvaluationId()
                                                       + ", the following subscribers failed to show progress within a "
                                                       + "timeout period of "
                                                       + this.timeout
                                                       + " "
                                                       + this.timeoutUnits
                                                       + ": "
                                                       + identifiers
                                                       + ". Subscribers should report their status regularly, in "
                                                       + "order to reset the timeout period. " );
            }
        }

        /**
         * Registers a new consumer as ready to consume.
         * 
         * @param message the status message containing the subscriber event
         */

        private void registerSubscriberReady( EvaluationStatus message )
        {
            String consumerId = message.getConsumerId();
            this.validateConsumerId( consumerId );

            this.subscribersReady.add( consumerId );

            // Reset the countdown
            this.getSubscriberLatch( consumerId ).resetClock();

            LOGGER.debug( "Registered a message subscriber {} for evaluation {} as {}.",
                          consumerId,
                          this.evaluation.getEvaluationId(),
                          message.getCompletionStatus() );
        }

        /**
         * Registers a new consumer as ready to consume.
         * 
         * @param message the status message containing the subscriber event
         */

        private void registerSubscriberComplete( EvaluationStatus message )
        {
            String consumerId = message.getConsumerId();
            this.validateConsumerId( consumerId );

            if ( !this.expectedSubscribers.contains( consumerId ) )
            {
                throw new EvaluationEventException( "While completing a subscription for evaluation "
                                                    + this.evaluation.getEvaluationId()
                                                    + " received a message about a consumer event with a consumerId of "
                                                    + consumerId
                                                    + ", which is not registered with this evaluation." );
            }

            // Countdown the subscription as complete
            this.subscribersReady.remove( consumerId );
            this.getSubscriberLatch( consumerId ).countDown();

            LOGGER.debug( "Removed a message subscriber {} for evaluation {} as {}.",
                          consumerId,
                          this.evaluation.getEvaluationId(),
                          message.getCompletionStatus() );
        }


        /**
         * Registers consumption as ongoing for a given subscriber.
         * 
         * @param message the status message containing the subscriber event
         */

        private void registerConsumptionOngoing( EvaluationStatus message )
        {
            String consumerId = message.getConsumerId();
            this.validateConsumerId( consumerId );

            // Reset the countdown
            this.getSubscriberLatch( consumerId ).resetClock();

            LOGGER.debug( "Message subscriber {} for evaluation {} reports {}.",
                          consumerId,
                          this.evaluation.getEvaluationId(),
                          message.getCompletionStatus() );
        }

        /**
         * Throws an exception if the consumer identifier is blank.
         * @param consumerId the consumer identifier
         * @throws EvaluationEventException if the consumer identifier is blank
         */

        private void validateConsumerId( String consumerId )
        {
            if ( consumerId.isBlank() )
            {
                throw new EvaluationEventException( "While awaiting consumption for evaluation "
                                                    + this.evaluation.getEvaluationId()
                                                    + " received a message about a consumer event that did not "
                                                    + "contain the consumerId, which is not allowed." );
            }
        }

        /**
         * Returns a subscriber latch for a given subscriber identifier.
         * 
         * @param consumerId the subscriber identifier
         * @return the latch
         */

        private TimedCountDownLatch getSubscriberLatch( String consumerId )
        {
            this.validateConsumerId( consumerId );

            return this.subscriberLatches.get( consumerId );
        }

        /**
         * Create an instance with an evaluation and an expected list of subscriber identifiers.
         * @param evaluation the evaluation 
         * @param expectedSubscribers the list of expected subscriber identifiers
         * @throws NullPointerException if any input is null
         * @throws IllegalArgumentException if the list of subscribers is empty
         */

        private EvaluationStatusTracker( Evaluation evaluation, Set<String> expectedSubscribers )
        {
            Objects.requireNonNull( evaluation );
            Objects.requireNonNull( expectedSubscribers );

            if ( expectedSubscribers.isEmpty() )
            {
                throw new IllegalArgumentException( "Expected one or more subscribers when building evaluation "
                                                    + evaluation.getEvaluationId()
                                                    + " but found none." );
            }

            this.evaluation = evaluation;
            this.subscribersReady = new HashSet<>();
            this.expectedSubscribers = expectedSubscribers;

            // Default timeout for consumption from an individual consumer unless progress is reported
            // In practice, this is extremely lenient
            this.timeout = 120;
            this.timeoutUnits = TimeUnit.MINUTES;

            // Create the latches
            Map<String, TimedCountDownLatch> internalLatches = new HashMap<>( this.expectedSubscribers.size() );
            this.expectedSubscribers.forEach( next -> internalLatches.put( next, new TimedCountDownLatch( 1 ) ) );
            this.subscriberLatches = Collections.unmodifiableMap( internalLatches );
        }
    }

}
