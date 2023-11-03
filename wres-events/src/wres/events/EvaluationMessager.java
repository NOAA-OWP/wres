package wres.events;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import jakarta.jms.JMSException;
import jakarta.jms.Topic;

import javax.naming.NamingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Timestamp;

import net.jcip.annotations.ThreadSafe;

import wres.events.broker.BrokerConnectionFactory;
import wres.events.publish.MessagePublisher;
import wres.events.publish.MessagePublisher.MessageProperty;
import wres.events.subscribe.SubscriberApprover;
import wres.statistics.MessageFactory;
import wres.statistics.generated.Consumer.Format;
import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.EvaluationStatus.CompletionStatus;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Pairs;
import wres.statistics.generated.Statistics;

/**
 * <p>Manages the publication and consumption of messages associated with one evaluation, defined as a single 
 * computational instance of one project declaration. Manages the publication of, and subscription to, the following 
 * types of messages that map to evaluation events:
 *
 * <ol>
 * <li>EvaluationMessager messages contained in {@link wres.statistics.generated.Evaluation};</li>
 * <li>EvaluationMessager status messages contained in {@link wres.statistics.generated.EvaluationStatus}; and</li>
 * <li>Statistics messages contained in {@link wres.statistics.generated.Statistics}.</li>
 * <li>Pairs messages contained in {@link wres.statistics.generated.Pairs}.</li>
 * </ol>
 *
 * <p>An evaluation is assigned a unique identifier on construction. This identifier is used to correlate messages that
 * belong to the same evaluation.
 *
 * <p> The messaging lifecycle for an evaluation is composed of three parts:
 * <ol>
 * <li>Opening, which corresponds to 
 * {@link #of(wres.statistics.generated.Evaluation, BrokerConnectionFactory, String)} or an overloaded version;</li>
 * <li>Awaiting completion {@link #await()}; and</li>
 * <li>Closing, either forcibly ({@link #stop(Exception)}) or nominally {@link #close()}.</li>
 * </ol>
 *
 * <p>Currently, this is intended for internal use by java producers and consumers within the core of the wres. In 
 * future, it is envisaged that an "advanced" API will be exposed to external clients that can post evaluations and 
 * register consumers to consume all types of evaluation messages. This advanced API would provide developers of 
 * microservices an alternative route, alongside the RESTful API, to publish and subscribe to evaluations, adding more
 * advanced possibilities, such as asynchronous messaging and streaming. Service discovery could involve a
 * request-response pattern, such as gRPC (www.grpc.io), in order to register an evaluation and obtain the evaluation 
 * identifier and connection details for brokered (i.e., non request-response) communication. Alternatively, the broker
 * could broadcast its existence to listening consumers.
 *
 * @author James Brown
 */

@ThreadSafe
public class EvaluationMessager implements Closeable
{
    /** Default name for the queue on the amq.topic that accepts evaluation messages. */
    private static final String EVALUATION_QUEUE = QueueType.EVALUATION_QUEUE.toString();

    /** Default name for the queue on the amq.topic that accepts evaluation status messages. */
    private static final String EVALUATION_STATUS_QUEUE = QueueType.EVALUATION_STATUS_QUEUE.toString();

    /** Default name for the queue on the amq.topic that accepts statistics messages. */
    private static final String STATISTICS_QUEUE = QueueType.STATISTICS_QUEUE.toString();

    /** Default name for the queue on the amq.topic that accepts pairs messages. */
    private static final String PAIRS_QUEUE = QueueType.PAIRS_QUEUE.toString();

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( EvaluationMessager.class );

    /** Re-used string. */
    private static final String EVALUATION_STRING = "EvaluationMessager ";

    /** Re-used string. */
    private static final String ENCOUNTERED_AN_ERROR = ", encountered an error: ";

    /** Re-used string. */
    private static final String WHILE_ATTEMPTING_TO_PUBLISH_A_MESSAGE_TO_EVALUATION = "While attempting to publish a "
                                                                                      + "message to evaluation ";

    /** Re-used string. */
    private static final String DISCOVERED_AN_EVALUATION_MESSAGE_WITH_MISSING_INFORMATION =
            " discovered an evaluation message with missing information. ";

    /** Re-used string. */
    private static final String WHILE_PUBLISHING_TO_EVALUATION = "While publishing to evaluation ";

    /** Re-used string. */
    private static final String PUBLICATION_COMPLETE_ERROR = "Publication to this evaluation has been notified "
                                                             + "complete and no further messages may be published.";

    /** The frequency with which to publish an evaluation-alive message in ms. */
    private static final long NOTIFY_ALIVE_MILLISECONDS = 100_000;

    /** The evaluation description message. */
    private final wres.statistics.generated.Evaluation evaluationDescription;

    /** A status message indicating that the evaluation is ongoing. */
    private final EvaluationStatus statusOngoing;

    /** A publisher for {@link wres.statistics.generated.Evaluation} messages. */
    private final MessagePublisher evaluationPublisher;

    /** A publisher for {@link EvaluationStatus} messages. */
    private final MessagePublisher evaluationStatusPublisher;

    /** A publisher for {@link Statistics} messages. */
    private final MessagePublisher statisticsPublisher;

    /** A publisher for {@link Pairs} messages. */
    private final MessagePublisher pairsPublisher;

    /** A unique identifier for the evaluation. */
    private final String evaluationId;

    /** The identifier of the messaging client responsible for creating this evaluation. */
    private final String clientId;

    /** The total message count, excluding evaluation status messages. This is one more than the number of statistics
     * messages because an evaluation begins with an evaluation description message. This is mutable state. */
    private final AtomicInteger messageCount;

    /** The total number of evaluation status messages. This is mutable state. */
    private final AtomicInteger statusMessageCount;

    /** The total number of pairs messages. This is mutable state. */
    private final AtomicInteger pairsMessageCount;

    /** A record of the message groups to which messages have been published, together with the number of statistics
     * messages published against that identifier. When a group has been marked complete, the message count is set to
     * a negative number so that the completion state is transparent to publishers for validation purposes. */
    private final Map<String, AtomicInteger> messageGroups;

    /** Is <code>true</code> when publication is complete. Upon completion, an evaluation status message is sent to
     * notify completion, which includes the expected shape of the evaluation, and no further messages can be published 
     * with the public methods of this instance. */
    private final AtomicBoolean publicationComplete;

    /** Is <code>true</code> if the evaluation has been started. */
    private final AtomicBoolean isStarted;

    /** Is <code>true</code> if the evaluation has been stopped. */
    private final AtomicBoolean isStopped;

    /** Is <code>true</code> if the evaluation has been closed. */
    private final AtomicBoolean isClosed;

    /** An object that contains a completion status message and a latch that counts to zero when an evaluation has been
     * notified complete. */
    private final EvaluationStatusTracker statusTracker;

    /** The status of the evaluation on exit. A non-zero exit status corresponds to failure. Initialized with a
     * negative status. */
    private final AtomicInteger exitCode = new AtomicInteger( -1 );

    /** Producer flow controller. */
    private final ProducerFlowController flowController;

    /** A timer task to publish information about the status of the evaluation. */
    private final Timer timer;

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
     * @param evaluationDescription the evaluation description message
     * @param broker the broker
     * @param clientId the identifier of the messaging client requesting an evaluation
     * @return an open evaluation
     * @throws NullPointerException if any input is null
     * @throws EvaluationEventException if the evaluation could not be constructed
     * @throws IllegalArgumentException if any input is invalid
     */

    public static EvaluationMessager of( wres.statistics.generated.Evaluation evaluationDescription,
                                         BrokerConnectionFactory broker,
                                         String clientId )
    {
        return new Builder().setBroker( broker )
                            .setEvaluationDescription( evaluationDescription )
                            .setClientId( clientId )
                            .build();
    }

    /**
     * Opens an evaluation with a prescribed evaluation identifier.
     *
     * @param evaluationDescription the evaluation description message
     * @param broker the broker
     * @param clientId the identifier of the messaging client requesting an evaluation
     * @param evaluationId the evaluation identifier
     * @return an open evaluation
     * @throws NullPointerException if any input is null
     * @throws EvaluationEventException if the evaluation could not be constructed
     * @throws IllegalArgumentException if any input is invalid
     */

    public static EvaluationMessager of( wres.statistics.generated.Evaluation evaluationDescription,
                                         BrokerConnectionFactory broker,
                                         String clientId,
                                         String evaluationId )
    {
        return new Builder().setBroker( broker )
                            .setEvaluationDescription( evaluationDescription )
                            .setClientId( clientId )
                            .setEvaluationId( evaluationId )
                            .build();
    }

    /**
     * Opens an evaluation with a prescribed evaluation identifier and restrictions on approved subscribers.
     *
     * @param evaluationDescription the evaluation description message
     * @param broker the broker
     * @param clientId the identifier of the messaging client requesting an evaluation
     * @param evaluationId the evaluation identifier
     * @param subscriberApprover a collection of approved subscribers that deliver formats
     * @return an open evaluation
     * @throws NullPointerException if any input is null
     * @throws EvaluationEventException if the evaluation could not be constructed
     * @throws IllegalArgumentException if any input is invalid
     */

    public static EvaluationMessager of( wres.statistics.generated.Evaluation evaluationDescription,
                                         BrokerConnectionFactory broker,
                                         String clientId,
                                         String evaluationId,
                                         SubscriberApprover subscriberApprover )
    {
        return new Builder().setBroker( broker )
                            .setEvaluationDescription( evaluationDescription )
                            .setClientId( clientId )
                            .setEvaluationId( evaluationId )
                            .setSubscriberApprover( subscriberApprover )
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
        Objects.requireNonNull( pairs );

        this.validateRequestToPublish();

        ByteBuffer body = ByteBuffer.wrap( pairs.toByteArray() );

        this.internalPublish( body, this.pairsPublisher, EvaluationMessager.PAIRS_QUEUE, null );

        this.pairsMessageCount.getAndIncrement();
    }

    /**
     * Publish an {@link wres.statistics.generated.EvaluationStatus} message for the current evaluation.
     *
     * @param status the status message
     * @param groupId an optional group identifier to identify grouped messages
     * @throws NullPointerException if the message is null
     * @throws IllegalStateException if the publication of messages to this evaluation has been notified complete
     * @throws IllegalArgumentException if the group has already been marked complete
     */

    public void publish( EvaluationStatus status, String groupId )
    {
        Objects.requireNonNull( status );

        this.validateRequestToPublish();
        this.validateStatusMessage( status, groupId );

        ByteBuffer body = ByteBuffer.wrap( status.toByteArray() );

        this.internalPublish( body,
                              this.evaluationStatusPublisher,
                              EvaluationMessager.EVALUATION_STATUS_QUEUE,
                              groupId );

        this.statusMessageCount.getAndIncrement();

        // Record group
        if ( Objects.nonNull( groupId ) )
        {
            this.messageGroups.putIfAbsent( groupId, new AtomicInteger() );
        }
    }

    /**
     * Publish an {@link wres.statistics.generated.Statistics} message for the current evaluation.
     *
     * @param statistics the statistics message
     * @param groupId an optional group identifier to identify grouped messages
     * @throws NullPointerException if the message is null
     * @throws IllegalStateException if the publication of messages to this evaluation has been notified complete
     * @throws IllegalArgumentException if the group has already been marked complete
     */

    public void publish( Statistics statistics, String groupId )
    {
        Objects.requireNonNull( statistics );

        this.validateRequestToPublish();
        this.validateGroupId( groupId, EvaluationMessager.STATISTICS_QUEUE );

        // Acquire a publication lock when producer flow control is engaged
        if ( Objects.nonNull( groupId ) )
        {
            this.startFlowControl();
        }

        try
        {

            ByteBuffer body = ByteBuffer.wrap( statistics.toByteArray() );

            this.internalPublish( body, this.statisticsPublisher, EvaluationMessager.STATISTICS_QUEUE, groupId );

            this.messageCount.getAndIncrement();

            // Record group
            if ( Objects.nonNull( groupId ) )
            {
                // Add a new group or increment an existing one
                AtomicInteger group = new AtomicInteger( 1 );
                group = this.messageGroups.putIfAbsent( groupId, group );
                if ( Objects.nonNull( group ) )
                {
                    group.incrementAndGet();
                }
            }
        }
        // Release the flow control lock, if acquired by this thread
        finally
        {
            this.stopFlowControl();
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
     * {@link #stop(Exception)}.
     *
     * @see #stop(Exception)
     */

    public void markPublicationCompleteReportedSuccess()
    {
        this.validateStarted();

        if ( !this.isAlive() )
        {
            LOGGER.warn( "Cannot mark publication complete for evaluation {} because the evaluation itself has "
                         + "already been marked complete.",
                         this.getEvaluationId() );

            return;
        }

        this.completeAllMessageGroups();

        CompletionStatus status = CompletionStatus.PUBLICATION_COMPLETE_REPORTED_SUCCESS;

        EvaluationStatus complete = EvaluationStatus.newBuilder()
                                                    .setClientId( this.getClientId() )
                                                    .setCompletionStatus( status )
                                                    .setMessageCount( this.getPublishedMessageCount() )
                                                    .setPairsMessageCount( this.getPublishedPairsMessageCount() )
                                                    .setGroupCount( this.getPublishedGroupCount() )
                                                    .setStatusMessageCount( this.getPublishedStatusMessageCount()
                                                                            + 1 ) // This one
                                                    .build();

        ByteBuffer completeBuffer = ByteBuffer.wrap( complete.toByteArray() );
        this.internalPublish( completeBuffer,
                              this.evaluationStatusPublisher,
                              EvaluationMessager.EVALUATION_STATUS_QUEUE,
                              null );
        this.statusMessageCount.getAndIncrement();

        // No further publication allowed by public methods
        this.publicationComplete.set( true );

        // Information about groups is now redundant
        this.messageGroups.clear();

        LOGGER.info( "Publication of messages to evaluation {} has been marked complete. No further messages may be "
                     + "published to this evaluation. Upon completion, {} evaluation description message, {} "
                     + "statistics messages, {} pairs messages and {} evaluation status "
                     + "messages were published to this evaluation.",
                     this.getEvaluationId(),
                     1,
                     this.getPublishedMessageCount() - 1,
                     this.getPublishedPairsMessageCount(),
                     this.getPublishedStatusMessageCount() );
    }

    /**
     * <p>Marks complete the publication of statistics messages by this instance for the prescribed message group. 
     *
     * <p>If no statistics messages were published by this instance, then no tracking by group is needed and
     * {@link #markPublicationCompleteReportedSuccess()} should be used instead.
     *
     * @param groupId the group identifier
     * @see #markPublicationCompleteReportedSuccess()
     * @throws NullPointerException if the group identifier is null
     */

    public void markGroupPublicationCompleteReportedSuccess( String groupId )
    {
        Objects.requireNonNull( groupId );

        this.validateStarted();

        AtomicInteger groupCount = new AtomicInteger( 0 );

        // Some messages published?
        if ( this.messageGroups.containsKey( groupId ) )
        {
            groupCount = this.messageGroups.get( groupId );
        }
        // No. This is allowed in a no data scenario.
        else
        {
            LOGGER.warn( "Marking message group {} complete, but no statistics messages were published to this message "
                         + "group.",
                         groupId );
        }

        CompletionStatus status = CompletionStatus.GROUP_PUBLICATION_COMPLETE;

        EvaluationStatus complete = EvaluationStatus.newBuilder()
                                                    .setClientId( this.getClientId() )
                                                    .setCompletionStatus( status )
                                                    .setGroupId( groupId )
                                                    .setMessageCount( groupCount.get() )
                                                    .build();

        this.publish( complete, groupId );

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Publication of messages to group {} within evaluation {} has been marked complete. No "
                          + "further messages may be published to this group. Upon completion, {} statistics messages "
                          + "were published to this group.",
                          groupId,
                          this.getEvaluationId(),
                          groupCount.get() );
        }

        // Make completion of this group transparent to publishers by assigning a negative message count
        groupCount.set( -1 );
    }

    @Override
    public String toString()
    {
        return "EvaluationMessager with unique identifier: " + this.getEvaluationId();
    }

    /**
     * Starts an evaluation by negotiating subscriptions for all required formats.
     */

    public void start()
    {
        LOGGER.debug( "Starting the evaluation messager for evaluation {}.", this.getEvaluationId() );

        // Start the publishers
        this.evaluationStatusPublisher.start();
        this.evaluationPublisher.start();
        this.statisticsPublisher.start();
        this.pairsPublisher.start();

        // Now ready to publish messages
        this.isStarted.set( true );

        // Wait for all subscribers to be negotiated
        try
        {
            this.statusTracker.awaitNegotiatedSubscribers();
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread()
                  .interrupt();

            EvaluationEventException f = new EvaluationEventException( EVALUATION_STRING + this.evaluationId
                                                + " was Interrupted while waiting for "
                                                + "subscriptions to be negotiated.",
                                                e );

            EvaluationStatus error = this.getStatusOnException( f );
            this.statusTracker.stopOnFailure( error );
            throw f;
        }

        Instant now = Instant.now();
        long seconds = now.getEpochSecond();
        int nanos = now.getNano();
        EvaluationStatus started = EvaluationStatus.newBuilder()
                                                   .setCompletionStatus( CompletionStatus.EVALUATION_STARTED )
                                                   .setClientId( this.getClientId() )
                                                   .setTime( Timestamp.newBuilder()
                                                                      .setSeconds( seconds )
                                                                      .setNanos( nanos ) )
                                                   .build();

        this.publish( started );

        // Publish the evaluation description and update the evaluation status
        wres.statistics.generated.Evaluation description = this.getEvaluationDescription();
        this.internalPublish( description );

        // Notify that the evaluation is alive
        this.checkAndNotifyStatusAtFixedInterval( this, this.timer );

        LOGGER.info( "Started an evaluation messager for {}, which negotiated these subscribers by output format type: "
                     + "{}.",
                     this.evaluationId,
                     this.statusTracker.getNegotiatedSubscribers() );
    }

    /**
     * <p>Forcibly terminates an evaluation without completing any outstanding publication or consumption. This may be
     * necessary when an out-of-band exception is encountered (e.g., when producing messages for publication by this 
     * evaluation), which requires the evaluation to terminate promptly, without attempting further progress. To 
     * terminate gracefully and await all outstanding publication and consumption, use {@link #await()}.
     *
     * <p>The provided exception is used to notify consumers of the failed evaluation, even when publication has been
     * marked complete for this instance.
     *
     * <p>Calling this method multiple times has no effect.
     *
     * @param exception an optional exception instance to propagate to consumers
     * @see #close()
     */

    public void stop( Exception exception )
    {
        this.validateStarted();

        if ( !this.isStopped.getAndSet( true ) )
        {
            LOGGER.debug( "Stopping evaluation {} on encountering an exception.", this.getEvaluationId() );

            // Publish the completion status, if possible
            EvaluationStatus status = this.getStatusOnException( exception );
            this.publishEvaluationFailed( status );

            // Stop any flow control
            this.stopFlowControl();

            // Set a non-normal exit code
            LOGGER.debug( "Setting exit code to {}.", 1 );
            this.exitCode.set( 1 );

            // Stop tracking the evaluation
            this.statusTracker.stopOnFailure( status );
        }
    }

    /**
     * <p>Closes the evaluation.
     *
     * <p>This method does not wait for publication or consumption to complete. To await completion, call 
     * {@link #await()} before calling {@link #close()}. An exception may be notified with {@link #stop(Exception)}.
     *
     * <p>Calling this method multiple times has no effect (other than logging the additional attempts).
     */

    @Override
    public void close() throws IOException
    {
        this.validateStarted();

        if ( this.isClosed.getAndSet( true ) )
        {
            LOGGER.debug( "EvaluationMessager {} has already been closed.", this.getEvaluationId() );

            return;
        }

        LOGGER.debug( "Closing evaluation {}.", this.getEvaluationId() );

        // Close the resources gracefully
        this.closeGracefully( this.evaluationPublisher );
        this.closeGracefully( this.evaluationStatusPublisher );
        this.closeGracefully( this.statisticsPublisher );
        this.closeGracefully( this.pairsPublisher );
        this.closeGracefully( this.statusTracker );

        // Cancel otherwise the current instance will persist in cluster-server mode: #103066
        this.timer.cancel();

        if ( LOGGER.isInfoEnabled() )
        {
            // Log the completion status
            CompletionStatus onCompletion = CompletionStatus.EVALUATION_COMPLETE_REPORTED_SUCCESS;
            if ( this.getExitCode() != 0 )
            {
                onCompletion = CompletionStatus.EVALUATION_COMPLETE_REPORTED_FAILURE;
            }

            // Description message published?
            int descriptionCount = this.isStarted.get() ? 1 : 0;

            LOGGER.info( "Closed evaluation {} with status {}. This evaluation contained {} evaluation description "
                         + "message, {} statistics messages, {} pairs messages and {} evaluation status messages. The "
                         + "exit code was {}.",
                         descriptionCount,
                         this.getEvaluationId(),
                         onCompletion,
                         this.getPublishedMessageCount() - descriptionCount,
                         this.getPublishedPairsMessageCount(),
                         this.getPublishedStatusMessageCount(),
                         this.getExitCode() );
        }
    }

    /**
     * Uncovers all paths written by subscribers.
     *
     * @return the paths written
     */

    public Set<Path> getPathsWrittenBySubscribers()
    {
        Set<String> resourcesWritten = this.statusTracker.getResourcesWritten();

        // Attempt to create paths from the resource strings
        return resourcesWritten.stream()
                               .map( Paths::get )
                               .collect( Collectors.toUnmodifiableSet() );
    }

    /**
     * <p>Waits for the evaluation to complete. Complete in this context means two specific things:
     *
     * <ol>
     * <li>That publication has been marked completed; and</li>
     * <li>That every subscriber has received all of the messages it expected to receive.</li>
     * </ol>
     *
     * <p>It does *not* mean that every inner consumer within a subscriber has completed all of its work. For example,
     * consider the possibility of a consumer that implements a delayed write, as described in #81790-21. This method
     * may (probably will) complete before that delayed write has occurred. Thus, an evaluation should not be 
     * considered complete until all underlying consumers have reported complete.
     *
     * <p>In short, this method awaits with limited scope and makes only the guarantees described herein. 
     *
     * <p>To await with broader scope (e.g., to guarantee that consumers have finished their work), the inner consumers 
     * would need to implement a reporting contract. The simplest way to achieve this would be to promote each inner 
     * consumer to an external/outer subscriber that registers with an evaluation using a unique identifier. An outer 
     * subscriber is always responsible for messaging its own lifecycle and this evaluation will await those messages.
     *
     * <p>Calling this method multiple times has no effect (other than logging the additional attempts).
     *
     * @return the exit code on completion.
     * @throws EvaluationFailedToCompleteException if the evaluation failed to complete while waiting
     * @throws EvaluationEventException if the evaluation is asked to wait before publication is complete.
     */

    public int await()
    {
        this.validateStarted();

        if ( this.isStopped() || this.isClosed() )
        {
            LOGGER.debug( "EvaluationMessager {} has already completed.", this.getEvaluationId() );

            return this.getExitCode();
        }

        if ( !this.publicationComplete.get() )
        {
            throw new EvaluationEventException( "While attempting to wait for evaluation " + this.getEvaluationId()
                                                + ", encountered an error: this evaluation cannot be asked to wait "
                                                + "because publication has not been marked complete." );
        }

        LOGGER.debug( "Awaiting completion of evaluation {}...", this.getEvaluationId() );

        Instant then = Instant.now();

        try
        {
            // Await completion
            int exit = this.statusTracker.await();
            LOGGER.debug( "Setting exit code to {}.", exit );
            this.exitCode.set( exit );

            Instant now = Instant.now();

            LOGGER.debug( "Completed publication and consumption for evaluation {}. {} elapsed between notification to "
                          + "await completion and the end of consumption. Ready to validate and then close this "
                          + "evaluation.",
                          this.getEvaluationId(),
                          Duration.between( then, now ) );

            if ( exit != 0 )
            {
                EvaluationFailedToCompleteException exception =
                        new EvaluationFailedToCompleteException( "Failed to complete evaluation "
                                                                 + this.getEvaluationId()
                                                                 + " due to a subscriber error. The failing "
                                                                 + "subscribers are "
                                                                 + this.statusTracker.getFailedSubscribers()
                                                                 + "." );

                this.stop( exception );

                // Rethrow
                throw exception;
            }
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();

            throw new EvaluationEventException( "Interrupted while waiting for evaluation {} to complete."
                                                + this.getEvaluationId() );
        }

        // Do not publish success because no subscriber can leverage it for any purpose (subscribers must report 
        // success to reach this point). Equally, a short-running subscriber (one that dies with an evaluation) may 
        // receive it while shutting down, which can lead to an exception. See #90908.

        // Inspect internally rather than using the public api, as the evaluation has not stopped
        return this.exitCode.get();
    }

    /**
     * Checks whether the evaluation has failed. If the evaluation has not failed at the time of calling, it may yet
     * fail.
     *
     * @return true if the evaluation has failed, false if the evaluation has succeeded or is ongoing
     */

    public boolean isFailed()
    {
        return this.exitCode.get() > 0;
    }

    /**
     * Returns the status of the evaluation on exit.
     *
     * @return the exit status
     * @throws IllegalStateException if this message is called before the evaluation has been closed.
     */

    public int getExitCode()
    {
        int code = this.exitCode.get();

        if ( !( this.isStopped() || this.isClosed() ) )
        {
            throw new IllegalStateException( "Cannot acquire the exit status of a running evaluation." );
        }

        return code;
    }

    /**
     * Builds an evaluation.
     *
     * @author James Brown
     */

    public static class Builder
    {
        /**
         * Broker connections.
         */

        private BrokerConnectionFactory broker;

        /**
         * EvaluationMessager message.
         */

        private wres.statistics.generated.Evaluation evaluationDescription;

        /**
         * EvaluationMessager identifier.
         */

        private String evaluationId;

        /**
         * Messaging client identifier.
         */

        private String clientId;

        /**
         * Subscriber approver.
         */

        private SubscriberApprover subscriberApprover;

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
         * @param evaluationDescription the evaluation
         * @return this builder
         */

        public Builder setEvaluationDescription( wres.statistics.generated.Evaluation evaluationDescription )
        {
            this.evaluationDescription = evaluationDescription;

            return this;
        }

        /**
         * Sets the messaging client identifier.
         *
         * @param clientId the messaging client identifier
         * @return this builder 
         */

        public Builder setClientId( String clientId )
        {
            this.clientId = clientId;

            return this;
        }

        /**
         * Sets the evaluation identifier. See {@link EvaluationEventUtilities#getId()}.
         *
         * @param evaluationId the evaluation identifier
         * @return this builder 
         */

        public Builder setEvaluationId( String evaluationId )
        {
            this.evaluationId = evaluationId;

            return this;
        }

        /**
         * Sets the {@link SubscriberApprover}.
         *
         * @param subscriberApprover the subscriber approver
         * @return this builder 
         */

        public Builder setSubscriberApprover( SubscriberApprover subscriberApprover )
        {
            this.subscriberApprover = subscriberApprover;

            return this;
        }

        /**
         * Builds an evaluation.
         *
         * @return an evaluation
         */

        public EvaluationMessager build()
        {
            return new EvaluationMessager( this );
        }
    }

    /**
     * Returns the unique identifier of the client that is responsible for publishing the evaluation being tracked.
     *
     * @return the client identifier
     */

    String getClientId()
    {
        return this.clientId;
    }

    /**
     * Starts producer flow control when consumption lags behind production.
     */

    private void startFlowControl()
    {
        this.flowController.start();
    }

    /**
     * Stops producer flow control when consumption catches up with production. Should be stopped by the thread that 
     * started flow control.
     */

    private void stopFlowControl()
    {
        this.flowController.stop();
    }

    /**
     * Generates a status message on failure, adding the optional exception where available.
     * @param exception an optional exception message
     * @return the status message
     */
    private EvaluationStatus getStatusOnException( Exception exception )
    {
        Instant now = Instant.now();
        long seconds = now.getEpochSecond();
        int nanos = now.getNano();

        EvaluationStatus.Builder complete = EvaluationStatus.newBuilder()
                                                            .setCompletionStatus( CompletionStatus.EVALUATION_COMPLETE_REPORTED_FAILURE )
                                                            .setClientId( this.getClientId() )
                                                            .setTime( Timestamp.newBuilder()
                                                                               .setSeconds( seconds )
                                                                               .setNanos( nanos ) );

        if ( Objects.nonNull( exception ) )
        {
            EvaluationStatusEvent event = EvaluationEventUtilities.getStatusEventFromException( exception );
            complete.addStatusEvents( event );
        }

        return complete.build();
    }

    /**
     * Publishes the completion status of an evaluation as failed.
     * @param status the final status message
     */

    private void publishEvaluationFailed( EvaluationStatus status )
    {
        ByteBuffer body = ByteBuffer.wrap( status.toByteArray() );

        try
        {
            // Internal publish in case publication has already been completed for this instance      
            this.internalPublish( body,
                                  this.evaluationStatusPublisher,
                                  EvaluationMessager.EVALUATION_STATUS_QUEUE,
                                  null );
        }
        catch ( EvaluationEventException e )
        {
            LOGGER.warn( "Unable to publish the completion status of evaluation {}, which was {}.",
                         this.getEvaluationId(),
                         CompletionStatus.EVALUATION_COMPLETE_REPORTED_FAILURE );
        }
    }

    /**
     * Completes any outstanding message groups.
     */

    private void completeAllMessageGroups()
    {
        // Complete any message groups that have not been marked complete
        for ( Map.Entry<String, AtomicInteger> nextGroup : this.messageGroups.entrySet() )
        {
            // Unfinished group flagged as -1
            if ( nextGroup.getValue().get() != -1 )
            {
                LOGGER.debug( "Marked message group {} associated with evaluation {} as complete.",
                              nextGroup.getKey(),
                              this.getEvaluationId() );

                this.markGroupPublicationCompleteReportedSuccess( nextGroup.getKey() );
            }
        }
    }

    /**
     * Returns the expected number of messages, excluding evaluation status messages. This is one more than the number
     * of statistics messages because an evaluation starts with an evaluation description message.
     *
     * @return the published message count
     */

    private int getPublishedMessageCount()
    {
        return this.messageCount.get();
    }

    /**
     * Notifies an existing evaluation as {@link CompletionStatus#EVALUATION_ONGOING}.
     */

    private void notifyAlive()
    {
        ByteBuffer status = ByteBuffer.wrap( this.statusOngoing.toByteArray() );
        this.internalPublish( status,
                              this.evaluationStatusPublisher,
                              EvaluationMessager.EVALUATION_STATUS_QUEUE,
                              null );

        this.statusMessageCount.getAndIncrement();
    }

    /**
     * @return true if the evaluation is still alive, otherwise false
     */

    private boolean isAlive()
    {
        return !this.isStopped() && !this.isClosed();
    }

    /**
     * Returns the expected number of evaluation status messages.
     *
     * @return the evaluation status message count
     */

    private int getPublishedStatusMessageCount()
    {
        return this.statusMessageCount.get();
    }

    /**
     * Returns the expected number of pairs messages.
     *
     * @return the pairs message count
     */

    private int getPublishedPairsMessageCount()
    {
        return this.pairsMessageCount.get();
    }

    /**
     * Returns the  number of groups to which statistics messages were published.
     *
     * @return the group  count
     */

    private int getPublishedGroupCount()
    {
        return this.messageGroups.size();
    }

    /**
     * Validates a request to publish.
     *
     * @throws IllegalStateException if publication is already complete or the evaluation has been stopped
     */

    private void validateRequestToPublish()
    {
        this.validateStarted();

        if ( this.publicationComplete.get() )
        {
            throw new IllegalStateException( WHILE_ATTEMPTING_TO_PUBLISH_A_MESSAGE_TO_EVALUATION
                                             + this.getEvaluationId()
                                             + ENCOUNTERED_AN_ERROR
                                             + PUBLICATION_COMPLETE_ERROR );
        }

        if ( this.isClosed() || this.isStopped() )
        {
            throw new IllegalStateException( WHILE_ATTEMPTING_TO_PUBLISH_A_MESSAGE_TO_EVALUATION
                                             + this.getEvaluationId()
                                             + ENCOUNTERED_AN_ERROR
                                             + "this evaluation has been stopped and can no longer accept "
                                             + "publication requests." );
        }

        if ( this.statusTracker.hasFailedSubscribers() )
        {
            throw new IllegalStateException( WHILE_ATTEMPTING_TO_PUBLISH_A_MESSAGE_TO_EVALUATION
                                             + this.getEvaluationId()
                                             + ENCOUNTERED_AN_ERROR
                                             + "this evaluation has subscribers that are marked as failed." );
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
     * @return true if closed, otherwise false.
     */

    private boolean isClosed()
    {
        return this.isClosed.get();
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
        if ( message.getCompletionStatus() == CompletionStatus.GROUP_PUBLICATION_COMPLETE )
        {
            this.validateGroupId( groupId, EvaluationMessager.EVALUATION_STATUS_QUEUE );

            // Can happen in a no data scenario
            if ( message.getMessageCount() == 0 && LOGGER.isWarnEnabled() )
            {
                LOGGER.warn( WHILE_PUBLISHING_TO_EVALUATION + "{}"
                             + " discovered an evaluation status message with an expected message count of zero. This "
                             + "is unusual when reporting {} and may indicate an error.",
                             this.getEvaluationId(),
                             CompletionStatus.GROUP_PUBLICATION_COMPLETE );
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
     * Looks for the presence of a group identifier and throws an exception when the group has already been completed.
     *
     * @param groupId a group identifier
     * @param queue the queue name to use in any error message
     * @throws IllegalArgumentException if there are group subscriptions and the group has already been marked complete
     */

    private void validateGroupId( String groupId, String queue )
    {
        if ( Objects.nonNull( groupId ) )
        {
            AtomicInteger count = this.messageGroups.get( groupId );

            if ( Objects.nonNull( count ) && count.get() < 0 )
            {
                throw new IllegalArgumentException( "While attempting to publish a grouped message to the "
                                                    + queue
                                                    + " queue of evaluation "
                                                    + this.getEvaluationId()
                                                    + ", discovered that group "
                                                    + groupId
                                                    + " has already been marked complete and cannot accept further "
                                                    + "messages." );
            }
        }
    }

    /**
     * Builds an exception with the specified message.
     *
     * @param builder the builder
     * @throws EvaluationEventException if the evaluation could not be constructed for any reason
     * @throws NullPointerException if the broker is null
     * @throws IllegalArgumentException if there are zero subscribers for any queue 
     * @throws IllegalStateException if the evaluation message fails to declare the expected number of pools or contains
     *            no formats
     */

    private EvaluationMessager( Builder builder )
    {
        String internalId = builder.evaluationId;

        if ( Objects.isNull( internalId ) )
        {
            internalId = EvaluationEventUtilities.getId();
        }

        this.evaluationId = internalId;

        // Copy then validate
        BrokerConnectionFactory broker = builder.broker;

        this.evaluationDescription = builder.evaluationDescription;
        this.clientId = builder.clientId;
        SubscriberApprover subscriberApprover = builder.subscriberApprover;

        if ( Objects.isNull( subscriberApprover ) )
        {
            // Permissive approver: accepts any viable subscription
            subscriberApprover = new SubscriberApprover.Builder().build();
        }

        // Get the formats that are required
        Outputs outputs = evaluationDescription.getOutputs();
        Set<Format> formatsRequired = MessageFactory.getDeclaredFormats( outputs );

        Objects.requireNonNull( broker, "Cannot create an evaluation without a broker connection." );
        Objects.requireNonNull( evaluationDescription,
                                "Cannot create an evaluation without an evaluation description message." );
        Objects.requireNonNull( this.clientId,
                                "Cannot create an evaluation without the identifier of the messaging "
                                + "client that requested it." );

        // Must have one or more formats
        if ( formatsRequired.isEmpty() )
        {
            throw new IllegalArgumentException( "Encountered an error while building evaluation "
                                                + this.evaluationId
                                                + ": there are no formats to be delivered by format "
                                                + "subscribers. An evaluation must write statistics to at least one "
                                                + "format." );
        }

        this.publicationComplete = new AtomicBoolean();
        this.isStopped = new AtomicBoolean();
        this.isStarted = new AtomicBoolean();
        this.isClosed = new AtomicBoolean();
        this.flowController = new ProducerFlowController( this );
        this.statusOngoing = EvaluationStatus.newBuilder()
                                             .setClientId( this.getClientId() )
                                             .setCompletionStatus( CompletionStatus.EVALUATION_ONGOING )
                                             .build();

        // Timer running in a daemon thread
        this.timer = new Timer( true );

        try
        {
            // Create the status tracker first so that subscribers can register
            this.statusTracker = new EvaluationStatusTracker( this,
                                                              broker,
                                                              formatsRequired,
                                                              broker.getMaximumMessageRetries(),
                                                              subscriberApprover,
                                                              this.flowController );
            this.statusTracker.start();

            Topic status = ( Topic ) broker.getDestination( EvaluationMessager.EVALUATION_STATUS_QUEUE );

            this.evaluationStatusPublisher = MessagePublisher.of( broker, status );

            Topic evaluation = ( Topic ) broker.getDestination( EvaluationMessager.EVALUATION_QUEUE );
            this.evaluationPublisher = MessagePublisher.of( broker, evaluation );

            Topic statistics = ( Topic ) broker.getDestination( EvaluationMessager.STATISTICS_QUEUE );
            this.statisticsPublisher = MessagePublisher.of( broker, statistics );

            Topic pairs = ( Topic ) broker.getDestination( EvaluationMessager.PAIRS_QUEUE );
            this.pairsPublisher = MessagePublisher.of( broker, pairs );
        }
        catch ( JMSException | NamingException e )
        {
            throw new EvaluationEventException( "Unable to construct an evaluation.", e );
        }

        // Mutable state
        this.messageCount = new AtomicInteger();
        this.statusMessageCount = new AtomicInteger();
        this.pairsMessageCount = new AtomicInteger();
        this.messageGroups = new ConcurrentHashMap<>();

        LOGGER.info( "Created an evaluation messager for evaluation {}.", this.evaluationId );
    }

    /**
     * Internal publish, do not expose.
     *
     * @param body the message body
     * @param publisher the publisher
     * @param queue the queue name on the amq.topic
     * @param groupId the optional message group identifier
     * @throws EvaluationEventException if the message could not be published
     */

    private void internalPublish( ByteBuffer body,
                                  MessagePublisher publisher,
                                  String queue,
                                  String groupId )
    {
        this.validateStarted();

        // Published below, so increment by 1 here 
        String messageId = "ID:" + this.getEvaluationId() + "-m" + EvaluationEventUtilities.getId();

        // Create the metadata
        Map<MessageProperty, String> properties = new EnumMap<>( MessageProperty.class );
        properties.put( MessageProperty.JMS_MESSAGE_ID, messageId );
        properties.put( MessageProperty.JMS_CORRELATION_ID, this.getEvaluationId() );

        if ( Objects.nonNull( groupId ) )
        {
            properties.put( MessageProperty.JMSX_GROUP_ID, groupId );
        }

        // Add the evaluation job identifier if this has been configured as a system property. See #84942. This is not
        // present in most contexts but, at the time of writing, is present when running in cluster mode with a short-
        // running wres process. In that case, it is needed by client subscribers to qualify the output directory
        String wresJobId = System.getProperty( "wres.jobId" );
        if ( Objects.nonNull( wresJobId ) )
        {
            properties.put( MessageProperty.EVALUATION_JOB_ID, wresJobId );
        }

        // Add the formats delivered by external subscribers to allow for competing subscribers to identify the
        // messages that belong to them
        Map<Format, String> negotiatedSubscribers = this.statusTracker.getNegotiatedSubscribers();
        for ( Map.Entry<Format, String> nextEntry : negotiatedSubscribers.entrySet() )
        {
            properties.put( MessageProperty.valueOf( nextEntry.getKey().name() ), nextEntry.getValue() );
        }

        try
        {
            publisher.publish( body, Collections.unmodifiableMap( properties ) );

            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "Published a message with metadata {} for evaluation {} to amq.topic/{}.",
                              properties,
                              this.getEvaluationId(),
                              queue );
            }
        }
        catch ( EvaluationEventException e )
        {
            throw new EvaluationEventException( "Failed to publish a message with metadata " + properties
                                                + " for evaluation "
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

        this.validateStarted();

        ByteBuffer body = ByteBuffer.wrap( evaluation.toByteArray() );

        this.internalPublish( body, this.evaluationPublisher, EvaluationMessager.EVALUATION_QUEUE, null );

        this.messageCount.getAndIncrement();
    }

    /**
     * Notifies all clients that depend on this evaluation that it is still alive. The notification happens at a fixed
     * time interval of {@link EvaluationMessager#NOTIFY_ALIVE_MILLISECONDS}.
     *
     * @param evaluation the evaluation
     * @param timer the timer
     */

    private void checkAndNotifyStatusAtFixedInterval( EvaluationMessager evaluation, Timer timer )
    {
        // Create a timer task to update any listening clients that the evaluation is alive in case of long-running 
        // statistics tasks
        TimerTask updater = new TimerTask()
        {
            @Override
            public void run()
            {
                if ( evaluation.isAlive() )
                {
                    evaluation.notifyAlive();
                }
            }
        };

        timer.schedule( updater, 0, EvaluationMessager.NOTIFY_ALIVE_MILLISECONDS );
    }

    /**
     * Closes a closeable gracefully.
     *
     * @param closeable the closeable to close
     */

    private void closeGracefully( Closeable closeable )
    {
        try
        {
            closeable.close();
            LOGGER.debug( "Closed messaging component {} in evaluation {}.", closeable, this );
        }
        catch ( IOException e )
        {
            String message = "Encountered an error while attempting to close messaging component "
                             + closeable
                             + " for evaluation "
                             + this.getEvaluationId();

            LOGGER.error( message, e );
        }
    }

    /**
     * @return the evaluation description message
     */
    private wres.statistics.generated.Evaluation getEvaluationDescription()
    {
        return this.evaluationDescription;
    }

    /**
     * Validates that the messager has been started by calling {@link #start()}.
     *
     * @throws IllegalStateException if the messager has not been started
     */

    private void validateStarted()
    {
        if( ! this.isStarted.get() )
        {
            throw new IllegalStateException( "The evaluation messager for evaluation "
                                             + this.getEvaluationId()
                                             + " has not been started, but an operation was attempted that requires "
                                             + "the messager to have been started." );
        }
    }
}
