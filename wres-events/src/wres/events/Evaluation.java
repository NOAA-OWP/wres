package wres.events;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Topic;
import javax.naming.NamingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Timestamp;

import net.jcip.annotations.ThreadSafe;
import wres.events.publish.MessagePublisher;
import wres.events.publish.MessagePublisher.MessageProperty;
import wres.eventsbroker.BrokerConnectionFactory;
import wres.statistics.generated.Consumer.Format;
import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.EvaluationStatus.CompletionStatus;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent.StatusMessageType;
import wres.statistics.generated.Outputs;
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
 * <p> The lifecycle for an evaluation is composed of three parts:
 * <ol>
 * <li>Opening, which corresponds to 
 * {@link #of(wres.statistics.generated.Evaluation, BrokerConnectionFactory)} or an overloaded version;</li>
 * <li>Awaiting completion {@link #await()}; and</li>
 * <li>Closing, either forcibly ({@link #stop(Exception)}) or nominally {@link #close()}.</li>
 * </ol>
 * 
 * <p>Currently, this is intended for internal use by java producers and consumers within the core of the wres. In 
 * future, it is envisaged that an "advanced" API will be exposed to external clients that can post evaluations and 
 * register consumers to consume all types of evaluation messages. This advanced API would provide developers of 
 * microservices an alternative route, alongside the RESTful API, to publish and subscribe to evaluations, adding more
 * advanced possibilities, such as asynchronous messaging and streaming. This API will probably leverage a 
 * request-response pattern, such as gRPC (www.grpc.io), in order to register an evaluation and obtain the evaluation 
 * identifier and connection details for brokered (i.e., non request-response) communication. Alternatively, the broker
 * could broadcast its existence to listening consumers.
 * 
 * @author james.brown@hydrosolved.com
 */

@ThreadSafe
public class Evaluation implements Closeable
{

    private static final String EVALUATION_STRING = "Evaluation ";

    private static final String ENCOUNTERED_AN_ERROR = ", encountered an error: ";

    private static final String WHILE_ATTEMPTING_TO_PUBLISH_A_MESSAGE_TO_EVALUATION = "While attempting to publish a "
                                                                                      + "message to evaluation ";

    private static final Logger LOGGER = LoggerFactory.getLogger( Evaluation.class );

    private static final String DISCOVERED_AN_EVALUATION_MESSAGE_WITH_MISSING_INFORMATION =
            " discovered an evaluation message with missing information. ";

    private static final String WHILE_PUBLISHING_TO_EVALUATION = "While publishing to evaluation ";

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
     * A description of the evaluation.
     */

    private final wres.statistics.generated.Evaluation evaluationDescription;

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
     * A unique identifier for the evaluation.
     */

    private final String evaluationId;

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
     * A record of the message groups to which messages have been published, together with the number of statistics 
     * messages published against that identifier. When a group has been marked complete, the message count is set to
     * a negative number so that the completion state is transparent to publishers for validation purposes.
     */

    private final Map<String, AtomicInteger> messageGroups;

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
     * Is <code>true</code> if the evaluation has been closed.
     */

    private final AtomicBoolean isClosed;

    /**
     * An object that contains a completion status message and a latch that counts to zero when an evaluation has been 
     * notified complete.
     */

    private final EvaluationStatusTracker statusTracker;

    /**
     * The status of the evaluation on exit. A non-zero exit status corresponds to failure. Initialized with a 
     * negative status.
     */

    private final AtomicInteger exitCode = new AtomicInteger( -1 );

    /**
     * A publisher connection for re-use.
     */

    private final Connection producerConnection;

    /**
     * A subscriber connection for re-use.
     */

    private final Connection consumerConnection;

    /**
     * A lock to control the flow of producers and thereby avoid overwhelming the broker when consumers a much slower
     * than producers (a.k.a. producer flow control). TODO: delegate this to the broker when adopting a broker than 
     * supports flow control for topic exchanges via JMS (Qpid does not).
     */

    private final ReentrantLock flowControlLock;

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
     * @return an open evaluation
     * @throws NullPointerException if any input is null
     * @throws EvaluationEventException if the evaluation could not be constructed
     * @throws IllegalArgumentException if any input is invalid
     */

    public static Evaluation of( wres.statistics.generated.Evaluation evaluationDescription,
                                 BrokerConnectionFactory broker )
    {
        return new Builder().setBroker( broker )
                            .setEvaluationDescription( evaluationDescription )
                            .build();
    }

    /**
     * Opens an evaluation with a prescribed identifier.
     * 
     * @param evaluationDescription the evaluation description message
     * @param broker the broker
     * @param evaluationId the evaluation identifier
     * @return an open evaluation
     * @throws NullPointerException if any input is null
     * @throws EvaluationEventException if the evaluation could not be constructed
     * @throws IllegalArgumentException if any input is invalid
     */

    public static Evaluation of( wres.statistics.generated.Evaluation evaluationDescription,
                                 BrokerConnectionFactory broker,
                                 String evaluationId )
    {
        return new Builder().setBroker( broker )
                            .setEvaluationDescription( evaluationDescription )
                            .setEvaluationId( evaluationId )
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
     * @throws IllegalArgumentException if the group has already been marked complete
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
            this.messageGroups.putIfAbsent( groupId, new AtomicInteger() );
        }
    }

    /**
     * Publish an {@link wres.statistics.generated.Statistics} message for the current evaluation.
     * 
     * @param statistics the statistics message
     * @param groupId an optional group identifier to identify grouped status messages (required if group subscribers)
     * @throws NullPointerException if the message is null
     * @throws IllegalStateException if the publication of messages to this evaluation has been notified complete
     * @throws IllegalArgumentException if the group has already been marked complete
     */

    public void publish( Statistics statistics, String groupId )
    {
        Objects.requireNonNull( statistics );

        this.validateRequestToPublish();
        this.validateGroupId( groupId );

        // Acquire a publication lock when producer flow control is engaged
        if ( Objects.nonNull( groupId ) )
        {
            this.startFlowControl();
        }

        try
        {

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

            ByteBuffer status = ByteBuffer.wrap( ongoing.toByteArray() );
            this.internalPublish( status,
                                  this.evaluationStatusPublisher,
                                  Evaluation.EVALUATION_STATUS_QUEUE,
                                  groupId );
            this.statusMessageCount.getAndIncrement();

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
     * @throws IllegalStateException if publication was already marked complete
     * @see #stop(Exception)
     */

    public void markPublicationCompleteReportedSuccess()
    {
        this.completeAllMessageGroups();

        CompletionStatus status = CompletionStatus.PUBLICATION_COMPLETE_REPORTED_SUCCESS;

        EvaluationStatus complete = EvaluationStatus.newBuilder()
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
                              Evaluation.EVALUATION_STATUS_QUEUE,
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
     * If no statistics messages were published by this instance, then no tracking by group is needed and 
     * {@link #markPublicationCompleteReportedSuccess()} should be used instead.
     * 
     * @param groupId the group identifier
     * @see #markPublicationCompleteReportedSuccess()
     * @throws IllegalArgumentException if no messages were published for the prescribed group prior to completion
     * @throws NullPointerException if the group identifier is null
     */

    public void markGroupPublicationCompleteReportedSuccess( String groupId )
    {
        Objects.requireNonNull( groupId );

        if ( !this.messageGroups.containsKey( groupId ) )
        {
            throw new IllegalArgumentException( "Cannot close message group " + groupId
                                                + " because no statistics "
                                                + "messages were published to this group." );
        }

        CompletionStatus status = CompletionStatus.GROUP_PUBLICATION_COMPLETE;

        AtomicInteger count = this.messageGroups.get( groupId );

        EvaluationStatus complete = EvaluationStatus.newBuilder()
                                                    .setCompletionStatus( status )
                                                    .setGroupId( groupId )
                                                    .setMessageCount( count.get() )
                                                    .build();

        this.publish( complete, groupId );

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Publication of messages to group {} within evaluation {} has been marked complete. No "
                          + "further messages may be published to this group. Upon completion, {} statistics messages "
                          + "were published to this group.",
                          groupId,
                          this.getEvaluationId(),
                          count.get() );
        }

        // Make completion of this group transparent to publishers by assigning a negative message count
        count.set( -1 );
    }

    /**
     * Returns the message that describes the evaluation, provided on construction of this instance.
     * 
     * @return the evaluation description
     */

    public wres.statistics.generated.Evaluation getEvaluationDescription()
    {
        return this.evaluationDescription;
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
     * terminate gracefully and await all outstanding publication and consumption, use {@link #await()}.
     * 
     * <p>The provided exception is used to notify consumers of the failed evaluation, even when publication has been
     * marked complete for this instance.
     * 
     * <p>Calling this method multiple times has no effect (other than logging the additional attempts).
     * 
     * @param exception an optional exception instance to propagate to consumers
     * @throws IOException if the evaluation could not be stopped. 
     * @see #close()
     */

    public void stop( Exception exception ) throws IOException
    {
        LOGGER.debug( "Stopping evaluation {} on encountering an exception.", this.getEvaluationId() );

        CompletionStatus status = CompletionStatus.EVALUATION_COMPLETE_REPORTED_FAILURE;

        // Create a string representation of the exception stack
        StringWriter sw = new StringWriter();

        sw.append( EVALUATION_STRING + this.getEvaluationId() + " failed." );

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

        ByteBuffer body = ByteBuffer.wrap( complete.toByteArray() );

        try
        {
            // Internal publish in case publication has already been completed for this instance      
            this.internalPublish( body,
                                  this.evaluationStatusPublisher,
                                  Evaluation.EVALUATION_STATUS_QUEUE,
                                  null );
        }
        catch ( EvaluationEventException e )
        {
            LOGGER.debug( "Unable to notify consumers about an exception that stopped evalation {}.",
                          this.getEvaluationId() );
        }

        // Set a non-normal exit code
        this.exitCode.set( 1 );

        // Now do the actual close ordinarily
        this.close();
    }

    /**
     * <p>Closes the evaluation. After closure, the status of this evaluation can be acquired from {@link #getExitCode()}.
     * However, this evaluation will only publish its completion status when the completion status is exceptional, i.e.,
     * {@link CompletionStatus#EVALUATION_COMPLETE_REPORTED_FAILURE}. Indeed, a nominal closure only occurs after all
     * registered consumers have reported {@link CompletionStatus#CONSUMPTION_COMPLETE_REPORTED_SUCCESS}. Thus, any 
     * message about a nominal exit status would be unheard by any consumers.
     * 
     * <p>This method does not wait for publication or consumption to complete. To await completion, call 
     * {@link #await()} before calling {@link #close()}. An exception may be notified with {@link #stop(Exception)}.
     * 
     * <p>Calling this method multiple times has no effect (other than logging the additional attempts).
     */

    @Override
    public void close() throws IOException
    {
        LOGGER.debug( "Closing evaluation {}.", this.getEvaluationId() );

        if ( this.isClosed.getAndSet( true ) )
        {
            LOGGER.debug( "Evaluation {} has already been closed.", this.getEvaluationId() );

            return;
        }

        // Close the status tracker
        this.statusTracker.close();

        // Close the publishers gracefully
        this.closeGracefully( this.evaluationPublisher );
        this.closeGracefully( this.evaluationStatusPublisher );
        this.closeGracefully( this.statisticsPublisher );

        try
        {
            this.producerConnection.close();
        }
        catch ( JMSException e )
        {
            LOGGER.error( "Encountered an error while attempting to close a broker connection for the message "
                          + "publishers associated with evaluation {}: {}.",
                          this,
                          e.getMessage() );
        }

        try
        {
            this.consumerConnection.close();
        }
        catch ( JMSException e )
        {
            LOGGER.error( "Encountered an error while attempting to close a broker connection for the message "
                          + "subscribers associated with evaluation {}: {}.",
                          this,
                          e.getMessage() );
        }

        CompletionStatus onCompletion = CompletionStatus.EVALUATION_COMPLETE_REPORTED_SUCCESS;
        if ( this.getExitCode() != 0 )
        {
            onCompletion = CompletionStatus.EVALUATION_COMPLETE_REPORTED_FAILURE;
        }

        LOGGER.info( "Closed evaluation {} with status {}. This evaluation contained 1 evaluation description "
                     + "message, {} statistics messages, {} pairs messages and {} evaluation status messages. The "
                     + "exit code was {}.",
                     this.getEvaluationId(),
                     onCompletion,
                     this.getPublishedMessageCount() - 1,
                     this.getPublishedPairsMessageCount(),
                     this.getPublishedStatusMessageCount(),
                     this.getExitCode() );
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
        if ( this.isStopped() || this.isClosed() )
        {
            LOGGER.debug( "Evaluation {} has already been closed.", this.getEvaluationId() );

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
        catch ( IOException e )
        {
            LOGGER.debug( "Encountered an error while awaiting completion of evaluation {}, but failed to "
                          + "stop the evaluation.",
                          this.getEvaluationId() );
        }

        // Evaluation finished (but not yet closed)
        this.isStopped.set( true );

        return this.getExitCode();
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

        if ( ! ( this.isStopped() || this.isClosed() ) )
        {
            throw new IllegalStateException( "Cannot acquire the exit status of a running evaluation." );
        }

        return code;
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

        private wres.statistics.generated.Evaluation evaluationDescription;

        /**
         * Evaluation identifier.
         */

        private String evaluationId = null;

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
         * Sets the evaluation identifier. See {@link Evaluation#getUniqueId()}.
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
     * Returns a unique identifier for identifying a component of an evaluation, such as the evaluation itself or an
     * internal subscriber.
     * 
     * @return a unique identifier
     */

    public static String getUniqueId()
    {
        return Evaluation.ID_GENERATOR.generate();
    }

    /**
     * Small bag of evaluation state for sharing.
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
         * Return an instance.
         * 
         * @param evaluationId the evaluation identifier
         * @return an instance
         */

        static EvaluationInfo of( String evaluationId )
        {
            return new EvaluationInfo( evaluationId );
        }

        /**
         * Build an instance.
         * 
         * @param evaluationId the evaluation identifier
         * @throws NullPointerException if any input is null
         */

        private EvaluationInfo( String evaluationId )
        {
            Objects.requireNonNull( evaluationId );

            this.evaluationId = evaluationId;
            this.queuesConstructed = new AtomicInteger();
        }
    }

    /**
     * Starts producer flow control when consumption lags behind production.
     */

    void startFlowControl()
    {
        this.flowControlLock.lock();
    }

    /**
     * Stops producer flow control when consumption catches up with production. Should be stopped by the thread that 
     * started flow control.
     */

    void stopFlowControl()
    {
        if ( this.flowControlLock.isHeldByCurrentThread() )
        {
            this.flowControlLock.unlock();
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
            this.validateGroupId( groupId );

            if ( message.getMessageCount() == 0 )
            {
                throw new IllegalArgumentException( WHILE_PUBLISHING_TO_EVALUATION + this.getEvaluationId()
                                                    + DISCOVERED_AN_EVALUATION_MESSAGE_WITH_MISSING_INFORMATION
                                                    + "The expected message count is required when the completion "
                                                    + "status reports "
                                                    + CompletionStatus.GROUP_PUBLICATION_COMPLETE );
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
     * Looks for the presence of a group identifier and throws an exception when absent or when the group has already 
     * been completed.
     * 
     * @param groupId a group identifier
     * @throws NullPointerException if there are group subscriptions and the group identifier is null
     * @throws IllegalArgumentException if there are group subscriptions and the group has already been marked complete
     */

    private void validateGroupId( String groupId )
    {
        if ( Objects.nonNull( groupId ) )
        {
            AtomicInteger count = this.messageGroups.get( groupId );

            if ( Objects.nonNull( count ) && count.get() < 0 )
            {
                throw new IllegalArgumentException( "While attempting to publish a grouped message to evaluation "
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

    private Evaluation( Builder builder )
    {
        String internalId = builder.evaluationId;

        if ( Objects.isNull( internalId ) )
        {
            internalId = Evaluation.getUniqueId();
        }

        this.evaluationId = internalId;

        LOGGER.info( "Creating an evaluation with identifier {}.", this.evaluationId );

        // Copy then validate
        BrokerConnectionFactory broker = builder.broker;

        this.evaluationDescription = builder.evaluationDescription;

        // Get the formats that are required
        Set<Format> formatsRequired = this.getFormatsAwaited( this.evaluationDescription.getOutputs() );

        Objects.requireNonNull( broker, "Cannot create an evaluation without a broker connection." );
        Objects.requireNonNull( this.evaluationDescription,
                                "Cannot create an evaluation without an evaluation description message." );
        
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
        this.isClosed = new AtomicBoolean();

        // Register publishers and subscribers
        ConnectionFactory factory = broker.get();

        try
        {
            this.producerConnection = factory.createConnection();
            this.consumerConnection = factory.createConnection();

            // Create the status tracker first  so that subscribers can register
            String statusTrackerId = Evaluation.getUniqueId();
            this.statusTracker = new EvaluationStatusTracker( this,
                                                              broker,
                                                              formatsRequired,
                                                              statusTrackerId,
                                                              broker.getMaximumMessageRetries() );

            Topic status = (Topic) broker.getDestination( Evaluation.EVALUATION_STATUS_QUEUE );

            this.evaluationStatusPublisher = MessagePublisher.of( this.producerConnection, status );

            Topic evaluation = (Topic) broker.getDestination( Evaluation.EVALUATION_QUEUE );
            this.evaluationPublisher = MessagePublisher.of( this.producerConnection, evaluation );

            Topic statistics = (Topic) broker.getDestination( Evaluation.STATISTICS_QUEUE );
            this.statisticsPublisher = MessagePublisher.of( this.producerConnection, statistics );

            Topic pairs = (Topic) broker.getDestination( Evaluation.PAIRS_QUEUE );
            this.pairsPublisher = MessagePublisher.of( this.producerConnection, pairs );
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
        this.flowControlLock = new ReentrantLock();

        // Await all subscribers to be negotiated
        try
        {
            this.statusTracker.awaitNegotiatedSubscribers();
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();

            throw new EvaluationEventException( EVALUATION_STRING + this.evaluationId
                                                + " was Interrupted while waiting for "
                                                + "subscriptions to be negotiated.",
                                                e );
        }

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

        // Publish the evaluation description  and update the evaluation status
        this.internalPublish( this.evaluationDescription );

        LOGGER.info( "Finished creating evaluation {}, which negotiated these subscribers by output format type: {}.",
                     this.evaluationId,
                     this.statusTracker.getNegotiatedSubscribers() );
    }

    /**
     * Compares the formats requested for an evaluation with the formats delivered by existing subscribers. Returns the
     * set of formats that are still awaiting subscribers.
     * 
     * @param outputs the outputs requested for the evaluation, including the formats required
     * @return the formats to be delivered by negotiation
     */

    private Set<Format> getFormatsAwaited( Outputs outputs )
    {
        Set<Format> returnMe = new HashSet<>();

        if ( outputs.hasCsv() )
        {
            returnMe.add( Format.CSV );
        }

        if ( outputs.hasPng() )
        {
            returnMe.add( Format.PNG );
        }

        if ( outputs.hasSvg() )
        {
            returnMe.add( Format.SVG );
        }

        if ( outputs.hasNetcdf() )
        {
            returnMe.add( Format.NETCDF );
        }

        if ( outputs.hasProtobuf() )
        {
            returnMe.add( Format.PROTOBUF );
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Internal publish, do not expose.
     * 
     * @param body the message body
     * @param publisher the publisher
     * @param queue the queue name on the amq.topic
     * @param groupId the optional message group identifier
     */

    private void internalPublish( ByteBuffer body,
                                  MessagePublisher publisher,
                                  String queue,
                                  String groupId )
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
        catch ( JMSException e )
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

        ByteBuffer body = ByteBuffer.wrap( evaluation.toByteArray() );

        this.internalPublish( body, this.evaluationPublisher, Evaluation.EVALUATION_QUEUE, null );

        this.messageCount.getAndIncrement();
    }

    /**
     * Closes a publisher gracefully.
     * 
     * @param publisher the publisher to close
     */

    private void closeGracefully( MessagePublisher publisher )
    {
        try
        {
            publisher.close();
        }
        catch ( IOException e )
        {
            String message = "Encountered an error while attempting to close a publisher associated with evaluation "
                             + this.getEvaluationId();

            LOGGER.error( message, e );
        }
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

}
