package wres.events.subscribe;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.jms.JMSException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;
import wres.events.EvaluationEventException;
import wres.events.EvaluationEventUtilities;
import wres.events.TimedCountDownLatch;
import wres.events.publish.MessagePublisher;
import wres.events.publish.MessagePublisher.MessageProperty;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Consumer;
import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.Statistics;
import wres.statistics.generated.EvaluationStatus.CompletionStatus;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent;

/**
 * <p>Consumer of messages for one evaluation. Receives messages and forwards them to underlying format consumers, which 
 * serialize outputs. The serialization may happen per message or per message group. Where consumption happens per 
 * message group, the {@link EvaluationConsumer} manages the semantics associated with that, forwarding the messages to 
 * a caching consumer until the group has completed and then asking the consumer, finally, to serialize all messages 
 * from the completed group. 
 * 
 * <p>Also notifies all listening clients of various stages within the lifecycle of an evaluation or exposes methods 
 * that allow a subscriber to drive that notification. In particular, on closure, notifies all listening clients whether 
 * the evaluation succeeded or failed. Additionally notifies listening clients when the consumption of a message group 
 * has completed. This notification may be used by a client to trigger/release producer flow control by message group 
 * (i.e., allowing for the publication of another group of messages).
 * 
 * @author james.brown@hydrosolved.com
 */

@ThreadSafe
class EvaluationConsumer
{
    private static final String FAILED_TO_COMPLETE_A_CONSUMPTION_TASK_FOR_EVALUATION =
            " failed to complete a consumption task for evaluation ";

    private static final String CONSUMER_STRING = "Consumer ";

    private static final Logger LOGGER = LoggerFactory.getLogger( EvaluationConsumer.class );

    /**
     * Timeout period after an evaluation has started before the evaluation description message can be received.
     */

    private static final long CONSUMER_TIMEOUT = 60_000;

    /**
     * Evaluation identifier.
     */

    private final String evaluationId;

    /**
     * Description of this consumer.
     */

    private final Consumer consumerDescription;

    /**
     * Actual number of messages consumed.
     */

    private final AtomicInteger consumed;

    /**
     * Expected number of messages.
     */

    private final AtomicInteger expected;

    /**
     * Registered complete.
     */

    private final AtomicBoolean isComplete;

    /**
     * To notify when consumption complete.
     */

    private final MessagePublisher evaluationStatusPublisher;

    /**
     * Consumer creation lock.
     */

    private final Object consumerCreationLock = new Object();

    /**
     * Consumer of individual messages.
     */

    private Function<Statistics, Set<Path>> consumer;

    /**
     * An elementary group consumer for message groups that provides the template for the {@link #groupConsumers}. 
     */

    private Function<Collection<Statistics>, Set<Path>> groupConsumer;

    /**
     * The time at which progress was last recorded. Used to timeout an evaluation on lack of progress (a well-behaving
     * publisher regularly publishes an {@link CompletionStatus#EVALUATION_ONGOING}).
     */

    private Instant timeSinceLastProgress;

    /**
     * A map of group consumers by group identifier.
     */

    @GuardedBy( "groupConsumersLock" )
    private final Map<String, OneGroupConsumer<Statistics>> groupConsumers;

    /**
     * A mutex lock for removing spent group consumers from the {@link #groupConsumers}.
     */

    private final ReentrantLock groupConsumersLock;

    /**
     * Is <code>true</code> when the consumers are ready to consume. Until then, cache the statistics.
     */

    private final AtomicBoolean areConsumersReady;

    /**
     * Is <code>true</code> if the evaluation has been closed, otherwise <code>false</code>.
     */

    private final AtomicBoolean isClosed;

    /**
     * Is <code>true</code> if the evaluation description has arrived, otherwise <code>false</code>.
     */

    private final AtomicBoolean hasEvaluationDescriptionArrived;

    /**
     * Thread pool to do writing work.
     */

    private final ExecutorService executorService;

    /**
     * Is <code>true</code> if the evaluation has failed, otherwise <code>false</code>.
     */

    private final AtomicBoolean isFailed;

    /**
     * Is <code>true</code> if the evaluation failure has been notified, otherwise <code>false</code>.
     */

    private final AtomicBoolean isFailureNotified;

    /**
     * A set of paths written.
     */

    private final Set<Path> pathsWritten;

    /**
     * The factory that supplies consumers for evaluations.
     */

    private final ConsumerFactory consumerFactory;

    /**
     * To await the arrival of an evaluation description in order to create consumers and then consume statistics.
     */

    private final TimedCountDownLatch consumersReady;

    /**
     * Subscriber status.
     */

    private final SubscriberStatus subscriberStatus;

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
        this.isFailed = new AtomicBoolean();
        this.isFailureNotified = new AtomicBoolean();
        this.groupConsumers = new ConcurrentHashMap<>();
        this.executorService = executorService;
        this.consumerDescription = consumerDescription;
        this.consumerFactory = consumerFactory;
        this.pathsWritten = new HashSet<>();
        this.consumersReady = new TimedCountDownLatch( 1 );
        this.subscriberStatus = subscriberStatus;
        this.groupConsumersLock = new ReentrantLock();
        this.registerProgress();

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
            this.isFailed.set( true );
            this.isComplete.set( true );
            this.subscriberStatus.registerEvaluationFailed( this.getEvaluationId() );

            // Close the consumer
            this.close();
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
            LOGGER.debug( "Evaluation consumer {} accepted a statistics message with message identifier {} and "
                          + "group identifier {}. The message was routed to a waiting format consumer.",
                          this.getClientId(),
                          messageId,
                          groupId );
        }

        // Accept inner
        this.acceptStatisticsMessageInner( statistics, groupId, messageId );

        // If consumption is complete, then close the consumer
        this.closeConsumerIfComplete();

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
        // This consumer should be retry friendly. Warn if an evaluation description has already arrived, but allow 
        // because this method may trigger the consumption of statistics messages, which could retry.
        if ( !this.hasEvaluationDescriptionArrived.getAndSet( true ) )
        {
            Objects.requireNonNull( evaluationDescription );

            this.createConsumers( evaluationDescription, this.getConsumerFactory(), jobId );

            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "Consumer {} received and consumed an evaluation description message with "
                              + "identifier {} for evaluation {}.",
                              this.getClientId(),
                              messageId,
                              this.getEvaluationId() );
            }

            // Record consumption
            this.consumed.incrementAndGet();

            // If consumption is complete, then close the consumer
            this.closeConsumerIfComplete();
        }
        else if ( LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( "While processing evaluation {} in consumer {}, encountered two instances of an evaluation "
                         + "description message. This is unexpected behavior unless a retry is in progress.",
                         this.getEvaluationId(),
                         this.getClientId() );
        }

        this.registerProgress();
    }

    /**
     * Accepts an evaluation status message for consumption.
     * @param status the evaluation status message
     * @param groupId a message group identifier, which only applies to grouped messages
     * @param messageId the message identifier to help with logging
     * @throws JMSException if a group completion could not be notified
     */

    void acceptStatusMessage( EvaluationStatus status, String groupId, String messageId ) throws JMSException
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
            case GROUP_PUBLICATION_COMPLETE:
                this.setExpectedMessageCountForGroups( status );
                break;
            case PUBLICATION_COMPLETE_REPORTED_SUCCESS:
                this.setExpectedMessageCount( status );
                break;
            case PUBLICATION_COMPLETE_REPORTED_FAILURE:
            case EVALUATION_COMPLETE_REPORTED_FAILURE:
                this.markEvaluationFailedOnProduction( status );
                break;
            default:
                break;
        }

        // If consumption is complete, then close the consumer
        this.closeConsumerIfComplete();

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
     * @return true if the evaluation failed, otherwise false
     */
    boolean isFailed()
    {
        return this.isFailed.get();
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

            this.isFailed.set( true );
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
        if ( !this.isFailureNotified.getAndSet( true ) )
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

                    LOGGER.debug( "Consumer {} registered consumption as forcibly completed (failed) for group {} of "
                                  + "evaluation {}.",
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

            String message = EvaluationConsumer.CONSUMER_STRING + this.getClientId()
                             + " has marked evaluation "
                             + this.getEvaluationId()
                             + " as failed unrecoverably with cause:";

            LOGGER.warn( message, cause );
        }
    }

    /**
     * Notifies successful completion of the evaluation.
     * 
     * @throws EvaluationEventException if the notification fails for any reason
     */

    private void notifySuccess()
    {
        this.publishCompletionState( CompletionStatus.CONSUMPTION_COMPLETE_REPORTED_SUCCESS,
                                     null,
                                     List.of() );
    }

    /**
     * Marks an evaluation timed-out on awaiting an evaluation description message and, therefore, failed. Adds no-op
     * consumers to consume any messages that arrive after the timeout.
     * @throws JMSException if the failure cannot be notified
     */

    private void markEvaluationTimedOutAwaitingConsumers() throws JMSException
    {
        SubscriberTimedOutException timeOut =
                new SubscriberTimedOutException( "Evaluation " + this.getEvaluationId()
                                                 + " failed to receive the evaluation description message "
                                                 + "within the timeout period of "
                                                 + CONSUMER_TIMEOUT
                                                 + " milliseconds." );

        // Add no-op consumers
        this.consumer = statistics -> Set.of();
        this.groupConsumer = statistics -> Set.of();

        this.markEvaluationFailedOnConsumption( timeOut );
    }

    /**
     * Publishes the completion status of a message group or the overall consumer.
     * 
     * @param completionStatus the completion status
     * @param events evaluation status events
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
                                             .collect( Collectors.toUnmodifiableList() );

            message.addAllResourcesCreated( addThesePaths );
        }

        if ( Objects.nonNull( groupId ) )
        {
            message.setGroupId( groupId );
        }

        // Create the metadata
        String messageId = "ID:" + this.getClientId() + "-m" + EvaluationEventUtilities.getUniqueId();

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
     * Checks whether consumption is complete and, if so, closes the consumer.
     * @throws EvaluationEventException if the completion state could not be notified
     */

    private void closeConsumerIfComplete()
    {
        // Complete? Then close.
        if ( this.isComplete() )
        {
            this.close();
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

                // Marked failed?
                if ( this.isFailed() )
                {
                    this.notifyFailure( null );
                    append = ", which completed unsuccessfully";
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
                    append = ", which completed unsuccessfully";

                    // Rethrow
                    throw e;
                }
                else
                {
                    this.notifySuccess();
                    append = ", which completed successfully";
                }
            }
            finally
            {
                // Make the (potentially large) set of paths eligible for gc as the evaluation may hang around for a 
                // while.
                this.pathsWritten.clear();

                LOGGER.info( "Consumer {} closed evaluation {}{}.",
                             this.getClientId(),
                             this.getEvaluationId(),
                             append );
            }

            // This instance is not responsible for closing the executor service.
        }
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

        // If consumption is complete, then close the consumer
        this.closeConsumerIfComplete();
    }

    /**
     * Sets the expected message count for message groups.
     * 
     * @param status the evaluation status message
     * @throws JMSException if the group completion could not be notified
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
    private Function<Statistics, Set<Path>> getConsumer()
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

        OneGroupConsumer<Statistics> newGroupConsumer = OneGroupConsumer.of( this.groupConsumer::apply, groupId );
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
                Path path = ConsumerFactory.getPathToWrite( this.getEvaluationId(), this.getClientId(), jobId );

                this.consumer = consumerFactory.getConsumer( evaluationDescription, path );
                this.groupConsumer = consumerFactory.getGroupedConsumer( evaluationDescription, path );

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
                                                      .apply( statistics ) ) );

        // Accept the grouped types
        if ( Objects.nonNull( groupId ) )
        {
            OneGroupConsumer<Statistics> groupCon = this.getGroupConsumer( groupId );

            // May trigger group completion and consumption
            this.execute( () -> groupCon.accept( messageId, statistics ) );

            this.closeGroupIfComplete( groupCon );
        }

        // Record consumption
        this.consumed.incrementAndGet();

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Consumer {} received and consumed a statistics message with identifier {} "
                          + "for evaluation {}.",
                          this.getClientId(),
                          messageId,
                          this.getEvaluationId() );
        }
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

}