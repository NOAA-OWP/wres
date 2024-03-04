package wres.events.subscribe;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import jakarta.jms.JMSException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

import wres.events.EvaluationEventException;
import wres.events.EvaluationEventUtilities;
import wres.events.TimedCountDownLatch;
import wres.events.publish.MessagePublisher;
import wres.events.publish.MessagePublisher.MessageProperty;
import wres.statistics.MessageFactory;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Consumer;
import wres.statistics.generated.Consumer.Format;
import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.Statistics;
import wres.statistics.generated.EvaluationStatus.CompletionStatus;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent;

/**
 * <p>Consumer of messages for one evaluation. Receives messages and forwards them to underlying format consumers, which 
 * serialize outputs. The serialization may happen per message or per message group. Where consumption happens per 
 * message group, the {@link EvaluationConsumer} manages the semantics associated with that, forwarding the messages to 
 * a caching consumer, which caches the statistics messages until all expected messages have arrived and then writes 
 * them. 
 *
 * <p>Also notifies all listening clients of various stages within the lifecycle of an evaluation or exposes methods 
 * that allow a subscriber to drive that notification. In particular, on closure, notifies all listening clients whether 
 * the evaluation succeeded or failed. Additionally notifies listening clients when the consumption of a message group 
 * has completed. This notification may be used by a client to trigger/release producer flow control by message group 
 * (i.e., allowing for the publication of another group of messages).
 *
 * @author James Brown
 */

@ThreadSafe
class EvaluationConsumer
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( EvaluationConsumer.class );

    /** Timeout period after an evaluation has started before the evaluation description message can be received. */
    private static final long CONSUMER_TIMEOUT = 600_000;

    /** Re-used string. */
    private static final String FAILED_TO_COMPLETE_A_CONSUMPTION_TASK_FOR_EVALUATION =
            " failed to complete a consumption task for evaluation ";

    /** Re-used string. */
    private static final String CONSUMER_STRING = "Consumer ";

    /** Evaluation identifier. */
    private final String evaluationId;

    /** Description of this consumer. */
    private final Consumer consumerDescription;

    /** Actual number of messages consumed. */
    private final AtomicInteger consumed;

    /** Expected number of messages. */
    private final AtomicInteger expected;

    /** Registered complete. */
    private final AtomicBoolean isComplete;

    /** To notify when consumption complete. */
    private final MessagePublisher evaluationStatusPublisher;

    /** Consumer creation lock. */
    private final Object consumerCreationLock = new Object();

    /** Consumer of individual messages. */
    private StatisticsConsumer consumer;

    /** An elementary group consumer for message groups that provides the template for the {@link #groupConsumers}. */
    private Function<Collection<Statistics>, Set<Path>> consumerForGroupedMessages;

    /** The time at which progress was last recorded. Used to timeout an evaluation on lack of progress (a well-behaving
     * publisher regularly publishes an {@link CompletionStatus#EVALUATION_ONGOING}). */
    private Instant timeSinceLastProgress;

    /** A map of group consumers by group identifier. */
    @GuardedBy( "groupConsumersLock" )
    private final Map<String, OneGroupConsumer<Statistics>> groupConsumers;

    /** A mutex lock for removing spent group consumers from the {@link #groupConsumers}. */
    private final ReentrantLock groupConsumersLock;

    /** Is <code>true</code> when the consumers are ready to consume. Until then, cache the statistics. */
    private final AtomicBoolean areConsumersReady;

    /** Is <code>true</code> if the evaluation has been closed, otherwise <code>false</code>. */
    private final AtomicBoolean isClosed;

    /** Is <code>true</code> if the evaluation description has arrived, otherwise <code>false</code>. */
    private final AtomicBoolean hasEvaluationDescriptionArrived;

    /** Thread pool to do writing work. */
    private final ExecutorService executorService;

    /** The state of the consumer, which is one of {@link CompletionStatus#READY_TO_CONSUME},
     * {@link CompletionStatus#CONSUMPTION_ONGOING}, 
     * {@link CompletionStatus#CONSUMPTION_COMPLETE_REPORTED_SUCCESS} or 
     * {@link CompletionStatus#CONSUMPTION_COMPLETE_REPORTED_FAILURE}. It is wrapped in an {@link AtomicReference} for
     * thread-safe mutation. */
    private final AtomicReference<CompletionStatus> completionStatus;

    /** Is <code>true</code> if the evaluation failure has been notified, otherwise <code>false</code>. */
    private final AtomicBoolean isNotified;

    /** A set of paths written. */
    private final Set<Path> pathsWritten;

    /** The factory that supplies consumers for evaluations. */
    private final ConsumerFactory consumerFactory;

    /** To await the arrival of an evaluation description in order to create consumers and then consume statistics. */
    private final TimedCountDownLatch consumersReady;

    /** Subscriber status. */
    private final SubscriberStatus subscriberStatus;

    /** Monitors the evaluation. */
    private final EvaluationConsumptionEvent monitor;

    /**
     * Builds a consumer.
     *
     * @param evaluationId the evaluation identifier
     * @param consumerDescription a description of the consumer
     * @param consumerFactory the consumer factory
     * @param evaluationStatusPublisher the evaluation status publisher
     * @param executorService the executor to do writing work (this instance is not responsible for closing)
     * @param subscriberStatus subscriber status to update (if consumption fails in this consumer)
     * @throws NullPointerException if any input is null
     */

    EvaluationConsumer( String evaluationId,
                        Consumer consumerDescription,
                        ConsumerFactory consumerFactory,
                        MessagePublisher evaluationStatusPublisher,
                        ExecutorService executorService,
                        SubscriberStatus subscriberStatus )
    {
        Objects.requireNonNull( evaluationId );
        Objects.requireNonNull( consumerDescription );
        Objects.requireNonNull( evaluationStatusPublisher );
        Objects.requireNonNull( subscriberStatus );

        this.evaluationId = evaluationId;
        this.evaluationStatusPublisher = evaluationStatusPublisher;
        this.consumed = new AtomicInteger();
        this.expected = new AtomicInteger();
        this.isComplete = new AtomicBoolean();
        this.hasEvaluationDescriptionArrived = new AtomicBoolean();
        this.areConsumersReady = new AtomicBoolean();
        this.isClosed = new AtomicBoolean();
        this.isNotified = new AtomicBoolean();
        this.groupConsumers = new ConcurrentHashMap<>();
        this.executorService = executorService;
        this.consumerDescription = consumerDescription;
        this.consumerFactory = consumerFactory;
        this.pathsWritten = new HashSet<>();
        this.consumersReady = new TimedCountDownLatch( 1 );
        this.subscriberStatus = subscriberStatus;
        this.groupConsumersLock = new ReentrantLock();
        this.completionStatus = new AtomicReference<>( CompletionStatus.READY_TO_CONSUME );
        this.registerProgress();
        this.monitor = EvaluationConsumptionEvent.of( evaluationId );
        this.getMonitor().begin(); // Begin monitoring

        LOGGER.info( "Consumer {} opened evaluation {}, which is ready to consume messages.",
                     this.getClientId(),
                     this.getEvaluationId() );
    }

    /**
     * Marks an evaluation as failed unrecoverably due to an error in this consumer or in the subscriber that wraps it
     * (i.e., with an internal cause), as distinct from an evaluation that was notified to this consumer as failed.
     *
     * @see #markEvaluationFailedOnProduction(EvaluationStatus)
     * @param exception an exception to notify in an evaluation status message
     * @throws JMSException if the failure cannot be notified
     */

    void markEvaluationFailedOnConsumption( Exception exception ) throws JMSException
    {
        // Notify
        try
        {
            this.notifyFailure( exception );
        }
        catch ( EvaluationEventException e )
        {
            String message = "While marking evaluation " + this.getEvaluationId()
                             + " as "
                             + CompletionStatus.CONSUMPTION_COMPLETE_REPORTED_FAILURE
                             + ": unable to publish the notification.";

            LOGGER.error( message, e );
        }
        finally
        {
            this.isComplete.set( true );
            this.completionStatus.set( CompletionStatus.CONSUMPTION_COMPLETE_REPORTED_FAILURE );
            this.subscriberStatus.registerEvaluationFailed( this.getEvaluationId() );

            // Close the consumer
            this.close();
        }
    }

    /**
     * Notifies and publishes success. It is the responsibility of a subscriber to trigger this notification because 
     * the subscriber controls the messaging semantics, such as ACKnowledging an incoming message and handling retries.
     *
     * @see #isComplete()
     * @throws EvaluationEventException if the notification fails for any reason
     */

    void markEvaluationSucceeded()
    {
        if ( !this.isNotified.getAndSet( true ) )
        {
            try
            {
                this.publishCompletionState( CompletionStatus.CONSUMPTION_COMPLETE_REPORTED_SUCCESS,
                                             null,
                                             List.of() );
            }
            finally
            {
                this.isComplete.set( true );
                this.completionStatus.set( CompletionStatus.CONSUMPTION_COMPLETE_REPORTED_SUCCESS );
                this.subscriberStatus.registerEvaluationCompleted( this.getEvaluationId() );

                // Close the consumer
                this.close();
            }
        }
    }

    /**
     * Accepts a statistics messages for consumption.
     * @param statistics the statistics
     * @param groupId a message group identifier, which only applies to grouped messages
     * @param messageId the message identifier to help with logging
     * @throws JMSException if the evaluation failed in a way that may be recoverable
     * @throws UnrecoverableEvaluationException if the evaluation fails in a way that should not be retried 
     * @throws UnrecoverableSubscriberException if the evaluation fails in a way that should not be retried and should 
     *            stop the subscriber
     */

    void acceptStatisticsMessage( Statistics statistics,
                                  String groupId,
                                  String messageId )
            throws JMSException
    {
        Objects.requireNonNull( statistics );
        Objects.requireNonNull( messageId );

        // Wait until the underlying consumers are built, which requires an evaluation description message
        try
        {
            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "While processing evaluation {} in evaluation consumer {}, accepted a statistics message "
                              + "with message identifier {} and group identifier {}. Awaiting an evaluation "
                              + "description message in order to build the format writers before the statistics "
                              + "message can be consumed.",
                              this.getEvaluationId(),
                              this.getClientId(),
                              messageId,
                              groupId );
            }

            this.consumersReady.await( EvaluationConsumer.CONSUMER_TIMEOUT, TimeUnit.MILLISECONDS );

            if ( this.consumersReady.timedOut() )
            {
                this.markEvaluationTimedOutAwaitingConsumers();
            }
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();

            throw new UnrecoverableEvaluationException( "Interrupted while waiting for an evaluation description "
                                                        + "message.",
                                                        e );
        }

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "EvaluationMessager consumer {} accepted a statistics message with message identifier {} and "
                          + "group identifier {}. The message was routed to a waiting format consumer.",
                          this.getClientId(),
                          messageId,
                          groupId );
        }

        // Accept inner
        this.acceptStatisticsMessageInner( statistics, groupId, messageId );

        this.registerProgress();
    }

    /**
     * Accepts an evaluation description message for consumption.
     * @param evaluationDescription the evaluation description message
     * @param messageId the message identifier to help with logging
     * @param jobId an optional job identifier
     * @throws JMSException if a consumer could not be created when required
     */

    void acceptEvaluationMessage( Evaluation evaluationDescription, String messageId, String jobId )
            throws JMSException
    {
        LOGGER.debug( "Accepting an evaluation message with messageId {} and jobId {}.", messageId, jobId );

        // This consumer should be retry friendly. Warn if an evaluation description has already arrived, but allow 
        // because this method may trigger the consumption of statistics messages, which could retry.
        if ( !this.hasEvaluationDescriptionArrived.getAndSet( true ) )
        {
            Objects.requireNonNull( evaluationDescription );

            this.createConsumers( evaluationDescription, this.getConsumerFactory(), jobId );
            LOGGER.debug( "Finished creating consumers for evaluation {}.", this.getEvaluationId() );

            LOGGER.debug( "Consumer {} received and consumed an evaluation description message with "
                          + "identifier {} for evaluation {}.",
                          this.getClientId(),
                          messageId,
                          this.getEvaluationId() );

            // Record consumption
            this.consumed.incrementAndGet();

            // Set the formats on the monitor, which is the intersection of the declared formats and the formats this
            // consumer can handle
            Set<Format> declaredFormats = MessageFactory.getDeclaredFormats( evaluationDescription.getOutputs() );
            Set<Format> formats = new TreeSet<>( declaredFormats );
            formats.retainAll( this.getConsumerDescription().getFormatsList() );
            this.getMonitor()
                .setFormats( Collections.unmodifiableSet( formats ) );
        }
        else if ( LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( "While processing evaluation {} in consumer {}, encountered two instances of an evaluation "
                         + "description message. This is unexpected behavior unless a retry is in progress.",
                         this.getEvaluationId(),
                         this.getClientId() );
        }

        this.registerProgress();

        // Update the consumer state
        this.completionStatus.set( CompletionStatus.CONSUMPTION_ONGOING );
    }

    /**
     * Accepts an evaluation status message for consumption.
     * @param status the evaluation status message
     * @param messageId the message identifier to help with logging
     */

    void acceptStatusMessage( EvaluationStatus status, String messageId )
    {
        Objects.requireNonNull( status );

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Consumer {} received and consumed an evaluation status message with identifier {} "
                          + "for evaluation {}.",
                          this.getClientId(),
                          messageId,
                          this.getEvaluationId() );
        }

        switch ( status.getCompletionStatus() )
        {
            case GROUP_PUBLICATION_COMPLETE -> this.setExpectedMessageCountForGroups( status );
            case PUBLICATION_COMPLETE_REPORTED_SUCCESS -> this.setExpectedMessageCount( status );
            case PUBLICATION_COMPLETE_REPORTED_FAILURE, EVALUATION_COMPLETE_REPORTED_FAILURE ->
                    this.markEvaluationFailedOnProduction( status );
            default ->
            {
                // Do nothing
            }
        }

        this.registerProgress();
    }

    /**
     * @return true if consumption is complete, otherwise false.
     */

    boolean isComplete()
    {
        // Already registered complete
        if ( this.isComplete.get() )
        {
            return true;
        }

        String append = "of an expected message count that is not yet known";
        if ( this.expected.get() > 0 )
        {
            append = "of an expected " + this.expected.get() + " messages";
        }

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "For evaluation {}, consumer {} has consumed {} messages {}.",
                          this.getEvaluationId(),
                          this.getClientId(),
                          this.consumed.get(),
                          append );
        }

        // An evaluation is not complete until all group consumption has succeeded. A group may have received all 
        // messages and not yet propagated them (because the expected count has not yet been received).
        boolean allGroupsComplete = this.groupConsumers.values()
                                                       .stream()
                                                       .allMatch( OneGroupConsumer::isComplete );

        this.isComplete.set( this.expected.get() > 0 && this.consumed.get() == this.expected.get()
                             && allGroupsComplete );

        return this.isComplete.get();
    }

    /**
     * @return true if the evaluation has been closed, otherwise false
     */
    boolean isClosed()
    {
        return this.isClosed.get();
    }

    /**
     * @return the completion status
     */
    CompletionStatus getCompletionStatus()
    {
        return this.completionStatus.get();
    }

    /**
     * @return the duration since progress was last recorded.
     */

    Duration getDurationSinceLastProgress()
    {
        return Duration.between( this.timeSinceLastProgress, Instant.now() );
    }

    /**
     * @return the evaluation identifier.
     */

    String getEvaluationId()
    {
        return this.evaluationId;
    }

    /**
     * Marks an evaluation as failed for reasons outside the control of this consumer, i.e., during production. In 
     * other words, the evaluation should be marked complete from the perspective of this consumer.
     *
     * @see #markEvaluationFailedOnConsumption(Exception)
     * @param status the completion status notified to this consumer
     */

    private void markEvaluationFailedOnProduction( EvaluationStatus status )
    {
        if ( !this.isClosed() )
        {
            LOGGER.debug( "Consumer {} has marked evaluation {} as failed unrecoverably. The failure was not caused by "
                          + "this consumer. The failure was notified to this consumer as {}.",
                          this.getClientId(),
                          this.getEvaluationId(),
                          status.getCompletionStatus() );

            this.completionStatus.set( CompletionStatus.CONSUMPTION_COMPLETE_REPORTED_FAILURE );
            this.isComplete.set( true );
            this.subscriberStatus.registerEvaluationFailed( this.getEvaluationId() );

            // Close the consumer
            this.close();
        }
    }

    /**
     * Notifies the failure of this evaluation by first notifying any incomplete groups as completed and then notifying
     * the overall consumption complete. Only notifies if the notification has not already happened.
     *
     * @param cause an optional exception to notify 
     * @throws EvaluationEventException if the notification fails for any reason
     */

    private void notifyFailure( Exception cause )
    {
        if ( !this.isNotified.getAndSet( true ) )
        {
            // Notify any incomplete groups
            for ( OneGroupConsumer<Statistics> next : this.groupConsumers.values() )
            {
                if ( !next.isComplete() )
                {
                    // Notify completion
                    this.publishCompletionState( CompletionStatus.GROUP_CONSUMPTION_COMPLETE,
                                                 next.getGroupId(),
                                                 List.of() );

                    LOGGER.debug( "Consumer {} registered consumption as forcibly completed (failed) for group {} "
                                  + "of evaluation {}.",
                                  this.consumerDescription.getConsumerId(),
                                  next.getGroupId(),
                                  this.getEvaluationId() );
                }
            }

            // Create the exception events to notify
            List<EvaluationStatusEvent> causeEvents = new ArrayList<>();
            if ( Objects.nonNull( cause ) )
            {
                EvaluationStatusEvent causeEvent = EvaluationEventUtilities.getStatusEventFromException( cause );
                causeEvents.add( causeEvent );
            }

            this.publishCompletionState( CompletionStatus.CONSUMPTION_COMPLETE_REPORTED_FAILURE,
                                         null,
                                         Collections.unmodifiableList( causeEvents ) );

            if ( LOGGER.isWarnEnabled() )
            {
                String message = EvaluationConsumer.CONSUMER_STRING + this.getClientId()
                                 + " has marked evaluation "
                                 + this.getEvaluationId()
                                 + " as failed unrecoverably.";

                LOGGER.warn( message, cause );
            }
        }
    }

    /**
     * Marks an evaluation timed-out on awaiting an evaluation description message and, therefore, failed. Adds no-op
     * consumers to consume any messages that arrive after the timeout.
     * @throws JMSException if the failure cannot be notified
     */

    private void markEvaluationTimedOutAwaitingConsumers() throws JMSException
    {
        SubscriberTimedOutException timeOut =
                new SubscriberTimedOutException( "EvaluationMessager " + this.getEvaluationId()
                                                 + " failed to receive the evaluation description message "
                                                 + "within the timeout period of "
                                                 + CONSUMER_TIMEOUT
                                                 + " milliseconds." );

        // Add no-op consumers
        this.consumer = StatisticsConsumer.getResourceFreeConsumer( statistics -> Set.of() );
        this.consumerForGroupedMessages = statistics -> Set.of();

        this.markEvaluationFailedOnConsumption( timeOut );
    }

    /**
     * Publishes the completion status of a message group or the overall consumer.
     *
     * @param completionStatus the completion status, not null
     * @param groupId the groupId, possibly null
     * @param events evaluation status events, not null
     * @throws NullPointerException if the input is null
     * @throws EvaluationEventException if the status could not be published
     */

    private void publishCompletionState( CompletionStatus completionStatus,
                                         String groupId,
                                         List<EvaluationStatusEvent> events )
    {
        Objects.requireNonNull( completionStatus );
        Objects.requireNonNull( events );

        // Create the status message to publish
        EvaluationStatus.Builder message = EvaluationStatus.newBuilder()
                                                           .setCompletionStatus( completionStatus )
                                                           .setClientId( this.getClientId() )
                                                           .setConsumer( this.getConsumerDescription() )
                                                           .addAllStatusEvents( events );

        // Add the paths, but only if the overall evaluation has completed (not if a group completed).
        if ( completionStatus == CompletionStatus.CONSUMPTION_COMPLETE_REPORTED_SUCCESS
             || completionStatus == CompletionStatus.CONSUMPTION_COMPLETE_REPORTED_FAILURE )
        {
            // Collect the paths written, if available
            List<String> addThesePaths = this.getPathsWritten()
                                             .stream()
                                             .map( Path::toString )
                                             .toList();

            message.addAllResourcesCreated( addThesePaths );
        }

        if ( Objects.nonNull( groupId ) )
        {
            message.setGroupId( groupId );
        }

        // Create the metadata
        String messageId = "ID:" + this.getClientId() + "-m" + EvaluationEventUtilities.getId();

        ByteBuffer buffer = ByteBuffer.wrap( message.build()
                                                    .toByteArray() );

        Map<MessageProperty, String> properties = new EnumMap<>( MessageProperty.class );
        properties.put( MessageProperty.JMS_MESSAGE_ID, messageId );
        properties.put( MessageProperty.JMS_CORRELATION_ID, this.getEvaluationId() );
        properties.put( MessageProperty.CONSUMER_ID, this.getClientId() );

        this.evaluationStatusPublisher.publish( buffer, Collections.unmodifiableMap( properties ) );

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Published the completion state of {} as {}.", this.getClientId(), completionStatus );
        }
    }

    /**
     * Closes the evaluation on completion.
     */
    private void close()
    {
        if ( !this.isClosed.getAndSet( true ) )
        {
            LOGGER.debug( "Consumer {} is closing evaluation {}.",
                          this.getClientId(),
                          this.getEvaluationId() );

            String append = "";

            try
            {
                // All groups complete?
                boolean groupsIncomplete = this.groupConsumers.values()
                                                              .stream()
                                                              .anyMatch( next -> !next.isComplete() );

                // Overall complete?
                boolean incomplete = !this.isComplete();

                // Paths written?
                String pathsMessage = this.getPathsMessage();

                // Marked failed?
                if ( this.getCompletionStatus() == CompletionStatus.CONSUMPTION_COMPLETE_REPORTED_FAILURE )
                {
                    this.notifyFailure( null );
                    append = ", which completed unsuccessfully. " + pathsMessage;
                }
                // Closed prematurely?
                else if ( groupsIncomplete || incomplete )
                {
                    UnrecoverableEvaluationException e =
                            new UnrecoverableEvaluationException( "Attempted to close evaluation "
                                                                  + this.getEvaluationId()
                                                                  + " before consumption had completed. Groups "
                                                                  + "marked complete: "
                                                                  + !groupsIncomplete
                                                                  + ". Overall consumption marked complete: "
                                                                  + !incomplete
                                                                  + "." );

                    // Propagate to other clients
                    this.notifyFailure( e );
                    append = ", which completed unsuccessfully. " + pathsMessage;

                    // Rethrow
                    throw e;
                }
                else
                {
                    append = ", which completed successfully. " + pathsMessage;
                }
            }
            finally
            {
                try
                {
                    this.consumer.close();
                }
                catch( IOException e )
                {
                    LOGGER.warn( "Unable to close consumer {} for evaluation {}.",
                                 this.consumerDescription.getConsumerId(),
                                 this.getEvaluationId() );
                }

                // Add monitoring attributes
                this.getMonitor()
                    .setResources( this.getPathsWritten() );
                this.getMonitor()
                    .complete(); // Copies incremented state to final state
                this.getMonitor()
                    .commit();

                // Log the paths in debug mode before they are squashed
                if ( LOGGER.isDebugEnabled() )
                {
                    LOGGER.debug( "Upon completing evaluation {}, consumer {} wrote the following paths: {}.",
                                  this.getEvaluationId(),
                                  this.getClientId(),
                                  this.getPathsWritten() );
                }

                // Now the evaluation is really over, minimize the state as it may hang around for a while to mop-up 
                // late arriving evaluation status messages. This removes paths written etc.
                this.squash();

                LOGGER.info( "Consumer {} closed evaluation {}{}.",
                             this.getClientId(),
                             this.getEvaluationId(),
                             append );
            }

            // This instance is not responsible for closing the executor service.
        }
    }

    /**
     * Returns a message about paths written.
     *
     * @return a message about paths written.
     */

    private String getPathsMessage()
    {
        Set<Path> parentPaths = this.pathsWritten.stream()
                                                 .map( Path::getParent )
                                                 .collect( Collectors.toSet() );

        int count = this.pathsWritten.size();
        String message = "Wrote " + count;

        if ( count == 1 )
        {
            message = message + " path";
        }
        else
        {
            message = message + " paths";
        }

        if ( count > 0 )
        {
            String paths;
            if ( parentPaths.size() > 1 )
            {
                paths = parentPaths.toString();
            }
            else
            {
                paths = parentPaths.iterator()
                                   .next()
                                   .toString();
            }
            message = message + " to " + paths;
        }

        return message;
    }

    /**
     * Minimizes expensive state, allowing the evaluation to hang around and mop-up late arriving messages.
     */

    private void squash()
    {
        this.pathsWritten.clear();
        this.groupConsumers.clear();
    }

    /**
     * @return the executor to do writing work.
     */

    private ExecutorService getExecutorService()
    {
        return this.executorService;
    }

    /**
     * Executes a writing task
     * @param task the task to execute
     * @throws ConsumerException if the task fails exceptionally, but potentially in a recoverable way
     * @throws UnrecoverableEvaluationException if the evaluation fails in a way that should not be retried
     * @throws UnrecoverableSubscriberException if the evaluation fails in a way that should not be retried and should 
     *            stop the subscriber
     */

    private void execute( Runnable task )
    {
        Future<?> future = this.getExecutorService()
                               .submit( task );

        // Get and propagate any exception
        try
        {
            future.get();
        }
        catch ( ExecutionException e )
        {
            // Consumer exceptions are propagated for retries
            if ( e.getCause() instanceof ConsumerException )
            {
                throw new ConsumerException( CONSUMER_STRING + this.getClientId()
                                             + FAILED_TO_COMPLETE_A_CONSUMPTION_TASK_FOR_EVALUATION
                                             + this.getEvaluationId()
                                             + ".",
                                             e );
            }
            // Add context to all other exceptions, which should stop the evaluation
            else if ( e.getCause() instanceof RuntimeException )
            {
                throw new UnrecoverableEvaluationException( CONSUMER_STRING + this.getClientId()
                                                            + FAILED_TO_COMPLETE_A_CONSUMPTION_TASK_FOR_EVALUATION
                                                            + this.getEvaluationId()
                                                            + ".",
                                                            e );
            }

            // All other instances should stop the subscriber
            throw new UnrecoverableSubscriberException( CONSUMER_STRING + this.getClientId()
                                                        + FAILED_TO_COMPLETE_A_CONSUMPTION_TASK_FOR_EVALUATION
                                                        + this.getEvaluationId()
                                                        + ".",
                                                        e );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();

            throw new ConsumerException( CONSUMER_STRING + this.getClientId()
                                         + FAILED_TO_COMPLETE_A_CONSUMPTION_TASK_FOR_EVALUATION
                                         + this.getEvaluationId()
                                         + ".",
                                         e );
        }
    }

    /**
     * Sets the expected message count.
     * @param status the evaluation status message with the expected message count
     * @throws EvaluationEventException if setting the count closes the consumer and notification of closure fails
     */

    private void setExpectedMessageCount( EvaluationStatus status )
    {
        Objects.requireNonNull( status );

        this.expected.set( status.getMessageCount() );

        LOGGER.debug( "Consumer {} received notification of publication complete for evaluation "
                      + "{}. The message indicated an expected message count of {}.",
                      this.getClientId(),
                      this.getEvaluationId(),
                      this.expected.get() );
    }

    /**
     * Sets the expected message count for message groups.
     *
     * @param status the evaluation status message
     * @throws EvaluationEventException if the completion state could not be published
     * @throws IllegalArgumentException if the status message does not have all expected fields
     */

    private void setExpectedMessageCountForGroups( EvaluationStatus status )
    {
        // Set the expected number of messages per group
        String groupId = status.getGroupId();

        Objects.requireNonNull( groupId );

        if ( status.getCompletionStatus() != CompletionStatus.GROUP_PUBLICATION_COMPLETE )
        {
            throw new IllegalArgumentException( "While registered the expected message count of group " + groupId
                                                + ", received an unexpected completion "
                                                + "status  "
                                                + status.getCompletionStatus()
                                                + ". Expected "
                                                + CompletionStatus.GROUP_PUBLICATION_COMPLETE );
        }

        if ( status.getMessageCount() == 0 )
        {
            LOGGER.warn( "While registering the expected message count of group {} in consumer {}, discovered a "
                         + "message count of zero, which may indicate an error, otherwise that no statistics were "
                         + "published for this group.",
                         groupId,
                         this.getClientId() );
        }

        // Set the expected count
        OneGroupConsumer<Statistics> groupCon = this.getGroupConsumer( groupId );

        // May trigger group completion and consumption
        this.execute( () -> groupCon.setExpectedMessageCount( status.getMessageCount() ) );

        this.closeGroupIfComplete( groupCon );
    }

    /**
     * @return the consumer factory.
     */

    private ConsumerFactory getConsumerFactory()
    {
        return this.consumerFactory;
    }

    /**
     * @return true when the consumers are ready to consume, false otherwise.
     */
    private boolean getAreConsumersReady()
    {
        return this.areConsumersReady.get();
    }

    /**
     * @return the incremental consumer.
     */
    private StatisticsConsumer getConsumer()
    {
        return this.consumer;
    }

    /**
     * @param groupId the group identifier
     * @return the group consumer.
     */
    private OneGroupConsumer<Statistics> getGroupConsumer( String groupId )
    {
        Objects.requireNonNull( groupId );

        // Await for the consumers to be created 
        try
        {
            this.consumersReady.await( EvaluationConsumer.CONSUMER_TIMEOUT, TimeUnit.MILLISECONDS );

            if ( this.consumersReady.timedOut() )
            {
                this.markEvaluationTimedOutAwaitingConsumers();
            }
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();

            throw new EvaluationEventException( "Interrupted while waiting for an evaluation description "
                                                + "message.",
                                                e );
        }
        catch ( JMSException e )
        {
            throw new EvaluationEventException( "While attempting to notify an evaluation as timed out.", e );
        }

        // Create the group consumer from the underlying consumer that should be called once per group
        OneGroupConsumer<Statistics> newGroupConsumer =
                OneGroupConsumer.of( this.consumerForGroupedMessages, groupId );
        OneGroupConsumer<Statistics> existingGroupConsumer = this.groupConsumers.putIfAbsent( groupId,
                                                                                              newGroupConsumer );

        if ( Objects.isNull( existingGroupConsumer ) )
        {
            return newGroupConsumer;
        }

        return existingGroupConsumer;
    }

    /**
     * Attempts to create the consumers.
     *
     * @param evaluationDescription a description of the evaluation
     * @param consumerFactory the consumer factory
     * @param jobId an optional job identifier
     */

    private void createConsumers( Evaluation evaluationDescription,
                                  ConsumerFactory consumerFactory,
                                  String jobId )
    {
        synchronized ( this.getConsumerCreationLock() )
        {
            if ( !this.getAreConsumersReady() )
            {
                LOGGER.debug( "Creating consumers for evaluation {}, which are attached to subscriber {}.",
                              this.getEvaluationId(),
                              this.getClientId() );

                // Create a path to write
                LOGGER.debug( "Attempting to acquire the path to which statistics formats should be written..." );

                Path path = this.getPathToWrite( this.getEvaluationId(),
                                                 this.getClientId(),
                                                 jobId );

                LOGGER.debug( "Acquired the path to which statistics formats should be written: {}.", path );

                LOGGER.debug( "Creating a format writer for statistics messages." );
                this.consumer = consumerFactory.getConsumer( evaluationDescription, path );
                LOGGER.debug( "Finished creating a format writer for statistics messages." );

                LOGGER.debug( "Creating a format writer for grouped statistics messages." );
                this.consumerForGroupedMessages = consumerFactory.getGroupedConsumer( evaluationDescription, path );
                LOGGER.debug( "Finished creating a format writer for grouped statistics messages." );

                LOGGER.debug( "Finished creating consumers for evaluation {}, which are attached to subscriber {}.",
                              this.getEvaluationId(),
                              this.getClientId() );

                // Flag that the consumers are ready
                this.areConsumersReady.set( true );
                this.consumersReady.countDown();
            }
        }
    }

    /**
     * Returns a path to write, creating a temporary directory for the outputs with the correct permissions, as needed. 
     *
     * @param evaluationId the evaluation identifier
     * @param consumerId the consumer identifier used to help with messaging
     * @param jobId an optional evaluation job identifier (see #84942)
     * @return the path to the temporary output directory
     * @throws ConsumerException if the temporary directory cannot be created
     * @throws NullPointerException if any input is null
     */

    private Path getPathToWrite( String evaluationId,
                                 String consumerId,
                                 String jobId )
    {
        Objects.requireNonNull( evaluationId );
        Objects.requireNonNull( consumerId );

        // Where outputs files will be written
        Path outputDirectory;
        String tempDir = System.getProperty( "java.io.tmpdir" );

        // Is this instance running in a context that uses a wres job identifier?
        // If so, create a directory corresponding to the job identifier
        if ( Objects.nonNull( jobId ) )
        {
            tempDir = tempDir + System.getProperty( "file.separator" ) + jobId;
        }

        try
        {
            Path namedPath = Paths.get( tempDir, "wres_evaluation_" + evaluationId );

            // POSIX-compliant    
            if ( FileSystems.getDefault()
                            .supportedFileAttributeViews()
                            .contains( "posix" ) )
            {
                Set<PosixFilePermission> permissions = EnumSet.of( PosixFilePermission.OWNER_READ,
                                                                   PosixFilePermission.OWNER_WRITE,
                                                                   PosixFilePermission.OWNER_EXECUTE,
                                                                   PosixFilePermission.GROUP_READ,
                                                                   PosixFilePermission.GROUP_WRITE,
                                                                   PosixFilePermission.GROUP_EXECUTE );

                FileAttribute<Set<PosixFilePermission>> fileAttribute =
                        PosixFilePermissions.asFileAttribute( permissions );

                // Create if not exists
                LOGGER.debug( "Creating or acquiring path '{}' with POSIX permissions: {}.", namedPath, permissions );
                outputDirectory = Files.createDirectories( namedPath, fileAttribute );
            }
            // Not POSIX-compliant
            else
            {
                LOGGER.debug( "Creating or acquiring path '{}'.", namedPath );
                outputDirectory = Files.createDirectories( namedPath );
            }

            LOGGER.debug( "Created or acquired path '{}'.", namedPath );
        }
        catch ( IOException e )
        {
            throw new ConsumerException( "Encountered an error in subscriber " + consumerId
                                         + " while attempting to create a temporary "
                                         + "directory for evaluation "
                                         + evaluationId
                                         + ".",
                                         e );
        }

        // Render absolute
        if ( !outputDirectory.isAbsolute() )
        {
            outputDirectory = outputDirectory.toAbsolutePath();
        }

        return outputDirectory;
    }

    /**
     * Inner method to accepts statistics. This method does not contain logic for caching statistics when consumers are 
     * not ready.
     * @param statistics the statistics
     * @param groupId a message group identifier, which only applies to grouped messages
     * @param messageId the message identifier to help with logging
     * @throws EvaluationEventException if the group completion could not be notified
     * @throws UnrecoverableEvaluationException if the evaluation fails in a way that should not be retried
     * @throws UnrecoverableSubscriberException if the evaluation fails in a way that should not be retried and should 
     *            stop the subscriber
     */

    private void acceptStatisticsMessageInner( Statistics statistics, String groupId, String messageId )
    {
        // Accept the incremental types
        this.execute( () -> this.addPathsWritten( this.getConsumer()
                                                      .apply( List.of( statistics ) ) ) );

        // Accept the grouped types
        if ( Objects.nonNull( groupId ) )
        {
            OneGroupConsumer<Statistics> groupCon = this.getGroupConsumer( groupId );

            // Get the statistics for grouped consumption
            Statistics groupedStatistics = this.getStatisticsForGroupedConsumption( statistics, groupId, messageId );

            // May trigger group completion and consumption
            this.execute( () -> groupCon.accept( messageId, groupedStatistics ) );

            this.closeGroupIfComplete( groupCon );
        }

        // Record consumption
        this.consumed.incrementAndGet();
        this.getMonitor().addStatistics( statistics );

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Consumer {} received and consumed a statistics message with identifier {} "
                          + "and group identifier {} for evaluation {}.",
                          this.getClientId(),
                          messageId,
                          groupId,
                          this.getEvaluationId() );
        }
    }

    /**
     * Provides the subset of statistics that are consumed by grouped consumers. The main reason to restrict this list
     * is to avoid caching unnecessary statistics. The {@link ConsumerFactory} can already decide how to route 
     * statistics messages to underlying consumers (e.g., to not route some statistics). However, unless these 
     * statistics are filtered on arrival in the messaging client (i.e., here), they are cached and the cached 
     * statistics are not then used by any underlying consumer. This ends up with unnecessary heap use for the box plot 
     * statistics by pair because they are currently not consumed in a grouped context under any circumstances. If this 
     * changes, then the grouped statistics should become part of the API of the {@link ConsumerFactory} by declaring a
     * nested interface for a grouped consumer whose statistics types are visible and can then be filtered by this 
     * method before they are forwarded to the grouped consumer. In the mean time, forward all statistics that are not 
     * box plots per pair.
     *
     * @param statistics the statistics of which some may not be for grouped consumption
     * @param groupId a message group identifier to help with logging
     * @param messageId the message identifier to help with logging
     * @return the statistics that are eligible for grouped consumption, in principle
     */

    private Statistics getStatisticsForGroupedConsumption( Statistics statistics, String groupId, String messageId )
    {
        Objects.requireNonNull( statistics );

        if ( LOGGER.isDebugEnabled() && statistics.getOneBoxPerPairCount() > 0 )
        {
            LOGGER.debug( "While routing statistics with message identifier {} to group {}, removed {} box plot per "
                          + "pair statistics because they are not eligible for grouped consumption.",
                          messageId,
                          groupId,
                          statistics.getOneBoxPerPairCount() );
        }

        // Clear the box plots per pair
        return statistics.toBuilder()
                         .clearOneBoxPerPair()
                         .build();
    }

    /**
     * Checks the group consumer for completion and, if complete, publishes a status message 
     * {@link CompletionStatus#GROUP_CONSUMPTION_COMPLETE} and updates the paths written.
     *
     * @param groupCon the group consumer
     * @throws EvaluationEventException if the completion state could not be published
     * @throws NullPointerException if the input is null
     */

    private void closeGroupIfComplete( OneGroupConsumer<Statistics> groupCon )
    {
        if ( groupCon.isComplete() )
        {
            // Add any paths created if the group has completed
            Set<Path> paths = groupCon.get();
            this.addPathsWritten( paths );

            // Notify completion
            this.publishCompletionState( CompletionStatus.GROUP_CONSUMPTION_COMPLETE,
                                         groupCon.getGroupId(),
                                         List.of() );

            LOGGER.debug( "Consumer {} registered consumption complete for group {} of evaluation {}.",
                          this.consumerDescription.getConsumerId(),
                          groupCon.getGroupId(),
                          this.getEvaluationId() );

            // Remove the spent group consumer 
            this.groupConsumersLock.lock();

            try
            {
                String groupId = groupCon.getGroupId();
                OneGroupConsumer<Statistics> removed = this.groupConsumers.remove( groupId );

                LOGGER.debug( "Upon completing message group {} in evaluation {}, retired the group consumer {}. "
                              + "There are {} message groups remaining.",
                              groupId,
                              this.getEvaluationId(),
                              removed,
                              this.groupConsumers.size() );
            }
            finally
            {
                this.groupConsumersLock.unlock();
            }
        }
    }

    /**
     * Registers progress.
     */

    private void registerProgress()
    {
        this.timeSinceLastProgress = Instant.now();
    }

    /**
     * @return the consumer creation lock
     */
    private Object getConsumerCreationLock()
    {
        return this.consumerCreationLock;
    }

    /**
     * Adds paths written to the cache of all paths written.
     *
     * @param paths the paths to append
     */

    private void addPathsWritten( Set<Path> paths )
    {
        this.pathsWritten.addAll( paths );
    }

    /**
     * @return an immutable view of the paths written so far.
     */

    private Set<Path> getPathsWritten()
    {
        return Collections.unmodifiableSet( this.pathsWritten );
    }

    /**
     * @return the consumer description
     */

    private Consumer getConsumerDescription()
    {
        return this.consumerDescription;
    }

    /**
     * @return the consumer identifier
     */

    private String getClientId()
    {
        return this.consumerDescription.getConsumerId();
    }

    /**
     * @return the monitor
     */

    private EvaluationConsumptionEvent getMonitor()
    {
        return this.monitor;
    }

}