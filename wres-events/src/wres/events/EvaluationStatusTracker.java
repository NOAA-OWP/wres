package wres.events;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
import com.google.protobuf.ProtocolStringList;

import net.jcip.annotations.ThreadSafe;
import wres.statistics.generated.Consumer;
import wres.statistics.generated.Consumer.Format;
import wres.events.publish.MessagePublisher.MessageProperty;
import wres.events.subscribe.SubscriberApprover;
import wres.events.subscribe.SubscriberTimedOutException;
import wres.eventsbroker.BrokerConnectionFactory;
import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.EvaluationStatus.CompletionStatus;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent.StatusMessageType;

/**
 * A class that tracks the status of an evaluation via its {@link EvaluationStatus} messages and awaits its 
 * completion upon request ({@link #await()}).
 * 
 * @author james.brown@hydrosolved.com
 */

@ThreadSafe
class EvaluationStatusTracker implements Closeable
{

    private static final String WHILE_COMPLETING_EVALUATION = "While completing evaluation ";

    private static final String FOR_SUBSCRIPTION = "for subscription {}.";

    private static final String ENCOUNTERED_AN_ATTEMPT_TO_MARK_SUBSCRIBER =
            ", encountered an attempt to mark subscriber ";

    private static final String WHILE_PROCESSING_EVALUATION = "While processing evaluation ";

    private static final Logger LOGGER = LoggerFactory.getLogger( EvaluationStatusTracker.class );

    /**
     * The frequency between logging updates in milliseconds upon awaiting a subscriber. 
     */

    private static final long TIMEOUT_UPDATE_FREQUENCY = 10_000;

    /**
     * Minimum period to wait before reporting that a subscriber is inactive.
     */

    private static final Duration MINIMUM_PERIOD_BEFORE_REPORTING_INACTIVE_SUBSCRIBER = Duration.ofMillis( 200_000 );

    /**
     * Maximum number of retries allowed.
     */

    private final int maximumRetries;

    /**
     * Actual number of retries attempted.
     */

    private final AtomicInteger retriesAttempted;

    /**
     * Is <code>true</code> if the subscriber failed after all attempts to recover.
     */

    private final AtomicBoolean isFailedUnrecoverably;

    /**
     * A latch that records the completion of publication. There is no timeout on publication.
     */

    private final CountDownLatch publicationLatch = new CountDownLatch( 1 );

    /**
     * A latch that records the completion of the evaluation, which requires the receipt of an 
     * {@link EvaluationStatusMessage} with {@link CompletionStatus#EVALUATION_COMPLETE_REPORTED_SUCCESS} else a 
     * failure. For the latter, an evaluation may complete without receipt of a status message because the messaging 
     * infrastructure may be down.
     */

    private final CountDownLatch evaluationCompletionLatch = new CountDownLatch( 1 );

    /**
     * A latch for each top-level subscriber by subscriber identifier. A top-level subscriber collates one or more 
     * consumers. Once set, this latch is counted down for each subscriber that reports completion. Each latch is 
     * initialized with a count of one. Consumption is allowed to timeout when no progress is recorded within a 
     * prescribed duration.
     */

    private final Map<String, TimedCountDownLatch> negotiatedSubscriberLatches;

    /**
     * The evaluation.
     */

    private final Evaluation evaluation;

    /**
     * The connection.
     */

    private final Connection connection;

    /**
     * The session.
     */

    private final Session session;

    /**
     * A negotiated subscriber for each format type that is not delivered by the core evaluation client.
     */

    private final Map<Format, String> negotiatedSubscribers;

    /**
     * The set of formats required by the evaluation.
     */

    private final Set<Format> formatsRequired;

    /**
     * A set of subscribers that succeeded.
     */

    private final Set<String> success;

    /**
     * A set of subscribers that failed.
     */

    private final Set<String> failure;

    /**
     * The timeout for a subscriber during consumption (post-negotiation).
     */

    private final long timeoutDuringConsumption;

    /**
     * The units for a subscriber timeout during consumption (post-negotiation).
     */

    private final TimeUnit timeoutDuringConsumptionUnits;

    /**
     * The consumer identifier of the subscriber to which this instance is attached. Helps me ignore messages that 
     * were reported by the subscriber to which I am attached.
     */

    private final String trackerId;

    /**
     * The fully qualified name of the durable subscriber on the broker.
     */

    private final String subscriberName;

    /**
     * The exit code.
     */

    private final AtomicInteger exitCode = new AtomicInteger();

    /**
     * Strings that represents paths or URIs to resources written during the evaluation.
     */

    private final Set<String> resourcesWritten;

    /**
     * Producer flow control.
     */

    private final ProducerFlowController flowController;

    /**
     * Timer used to monitor subscriptions for inactivity.
     */

    private final Timer subscriberInactivityTimer;

    /**
     * Determines whether subscription offers from format writers are viable. 
     */

    private final SubscriberApprover subscriberApprover;

    /**
     * Negotiates format subscribers.
     */

    private final SubscriberNegotiator subscriberNegotiator;

    /**
     * Is true to use a durable subscriber, false for a temporary subscriber, which is auto-deleted.
     */

    private final boolean durableSubscriber;

    @Override
    public void close()
    {
        // Awaiting receipt of the final success message, else failure and then close the tracker
        try
        {
            this.awaitSuccessMessageOrFailure();
        }
        catch ( InterruptedException e )
        {
            String message = "Interrupted while awaiting the completion of evaluation " + this.getEvaluationId() + ".";
            LOGGER.warn( message, e );
            Thread.currentThread().interrupt();
        }
        
        // Unsubscribe any durable subscriber
        if ( this.isDurableSubscriber() )
        {
            try
            {
                this.session.unsubscribe( this.subscriberName );
            }
            catch ( JMSException e )
            {
                String message = "While attempting to close evaluation status subscriber " + this.getTrackerId()
                                 + " for evaluation "
                                 + this.getEvaluationId()
                                 + ", failed to remove the subscription from the session.";

                LOGGER.warn( message, e );
            }
        }

        // No need to close the session, only the connection
        try
        {
            this.connection.close();
            LOGGER.debug( "Closed connection {} in evaluation status tracker {}.", this.connection, this );
        }
        catch ( JMSException e )
        {
            String message = "While attempting to close evaluation status subscriber " + this.getTrackerId()
                             + " for evaluation "
                             + this.getEvaluationId()
                             + ", failed to close the connection.";

            LOGGER.warn( message, e );
        }

        // Stop the subscriber inactivity timer
        this.subscriberInactivityTimer.cancel();
    }

    /**
     * Returns the strings (paths or URIs) that represent resources written.
     * 
     * @return the resources written
     */

    Set<String> getResourcesWritten()
    {
        return Collections.unmodifiableSet( this.resourcesWritten );
    }

    /**
     * Wait until publication and consumption has completed.
     * 
     * @return the exit code. A non-zero exit code corresponds to failure
     * @throws InterruptedException if the evaluation was interrupted
     */

    int await() throws InterruptedException
    {
        // Await publication
        this.awaitPublication();

        // Await consumption
        this.awaitConsumption();

        return this.exitCode.get();
    }

    /**
     * Awaits the conclusion of negotiations for output formats that are delivered by subscribers. The negotiation is 
     * concluded when all formats have been negotiated or the timeout is reached.
     * 
     * @throws InterruptedException if the negotiation of subscriptions was interrupted
     */

    void awaitNegotiatedSubscribers() throws InterruptedException
    {
        // Negotiate the subscribers
        Map<Format, String> subscribers = this.subscriberNegotiator.negotiateSubscribers();
        this.negotiatedSubscribers.putAll( subscribers );

        // Add the subscriber latches that are freed when consumption completes
        this.negotiatedSubscribers.values()
                                  .forEach( nextSubscriber -> this.negotiatedSubscriberLatches.put( nextSubscriber,
                                                                                                    new TimedCountDownLatch( 1 ) ) );

        // Add the subscribers to the flow controller
        this.negotiatedSubscribers.values().forEach( this.flowController::addSubscriber );

        // Monitor the subscribers for inactivity
        this.monitorForInactiveSubscribers( this );
    }

    /**
     * @return the set of failed subscribers.
     */

    Set<String> getFailedSubscribers()
    {
        return Collections.unmodifiableSet( this.failure );
    }

    /**
     * @return true if there are failed subscribers.
     */

    boolean hasFailedSubscribers()
    {
        return !this.getFailedSubscribers().isEmpty();
    }

    /**
     * Returns <code>true</code> if this status tracker is in a failure state, otherwise <code>false</code>.
     * 
     * @return true if an unrecoverable exception occurred.
     */

    boolean isFailed()
    {
        return this.isFailedUnrecoverably.get();
    }

    /**
     * @return a set of negotiated subscribers by format.
     */

    Map<Format, String> getNegotiatedSubscribers()
    {
        return Collections.unmodifiableMap( this.negotiatedSubscribers );
    }

    /**
     * Awaits completion of publication to the evaluation being tracked by this instance.
     * @throws InterruptedException if publication is interrupted while waiting
     */

    private void awaitPublication() throws InterruptedException
    {
        LOGGER.debug( "While processing evaluation {}, awaiting confirmation that publication has completed.",
                      this.getEvaluationId() );

        this.publicationLatch.await();

        String evaluationId = this.getEvaluationId();

        LOGGER.debug( "While processing evaluation {}, received confirmation that publication has completed.",
                      evaluationId );
    }

    /**
     * Awaits completion of consumption for the evaluation being tracked by this instance. Consumption should not be
     * awaited until publication is complete.
     * @throws InterruptedException if consumption is interrupted while waiting
     */

    private void awaitConsumption() throws InterruptedException
    {
        String evaluationId = this.getEvaluationId();

        if ( this.publicationLatch.getCount() > 0 )
        {
            throw new IllegalStateException( EvaluationStatusTracker.WHILE_PROCESSING_EVALUATION + evaluationId
                                             + ", attempted to await "
                                             + "consumption while publication was ongoing, which is not allowed. "
                                             + "Mark publication complete before attempting to await consumption." );
        }

        // Iterate through the negotiated subscribers and await each one
        for ( Map.Entry<String, TimedCountDownLatch> nextEntry : this.negotiatedSubscriberLatches.entrySet() )
        {
            String consumerId = nextEntry.getKey();
            TimedCountDownLatch nextLatch = nextEntry.getValue();

            LOGGER.debug( "While processing evaluation {}, awaiting confirmation that consumption has completed "
                          + FOR_SUBSCRIPTION,
                          this.getEvaluationId(),
                          consumerId );

            // Wait for a fixed period unless there is progress
            nextLatch.await( this.timeoutDuringConsumption, this.timeoutDuringConsumptionUnits );

            LOGGER.debug( "While processing evaluation {}, received confirmation that consumption has completed "
                          + FOR_SUBSCRIPTION,
                          evaluationId,
                          consumerId );
        }

        LOGGER.debug( "While processing evaluation {}, received confirmation that consumption has stopped "
                      + "across all {} registered subscriptions.",
                      evaluationId,
                      this.negotiatedSubscriberLatches.size() );

        // Throw an exception if any consumers failed to complete consumption
        Set<String> identifiers = this.negotiatedSubscriberLatches.entrySet()
                                                                  .stream()
                                                                  .filter( next -> next.getValue().getCount() > 0 )
                                                                  .map( Map.Entry::getKey )
                                                                  .collect( Collectors.toUnmodifiableSet() );

        if ( !identifiers.isEmpty() )
        {
            throw new SubscriberTimedOutException( WHILE_PROCESSING_EVALUATION
                                                   + evaluationId
                                                   + ", the following subscribers failed to show progress within a "
                                                   + "timeout period of "
                                                   + this.timeoutDuringConsumption
                                                   + " "
                                                   + this.timeoutDuringConsumptionUnits
                                                   + ": "
                                                   + identifiers
                                                   + ". Subscribers should report their status regularly, in "
                                                   + "order to reset the timeout period. " );
        }
    }
    
    /**
     * Awaits the receipt of an {@link EvaluationStatus} message with 
     * {@link CompletionStatus#EVALUATION_COMPLETE_REPORTED_SUCCESS}, else the failure of the evaluation, regardless of 
     * whether a message was received. Receipt of a success message can only be awaited once {@link #await()} returns 
     * because an evaluation cannot message its success until all of its awaited stages have completed.
     * 
     * @throws InterruptedException if interrupted while waiting
     */

    private void awaitSuccessMessageOrFailure() throws InterruptedException
    {
        LOGGER.debug( "Awaiting  of evaluation status tracker {}.", this );
        this.evaluationCompletionLatch.await();
    }

    /**
     * Stops all tracking on failure.
     * @param failure the failure message.
     * @throws IllegalStateException if stopping a consumer that has previously been reported as failed or succeeded
     */

    private void stopOnFailure( EvaluationStatus failure )
    {
        // Mark failed
        this.isFailedUnrecoverably.set( true );

        // Non-zero exit code
        this.exitCode.set( 1 );

        // Now that the evaluation has stopped, check that the notification was not duplicated, as this would indicate
        // a subscriber notification failure: we expect one notification per subscriber
        String consumerId = failure.getConsumer()
                                   .getConsumerId();

        // Signal the failing consumer
        boolean consumerAlreadyFailed = false;
        if ( !consumerId.isBlank() )
        {
            consumerAlreadyFailed = !this.failure.add( consumerId );
        }

        // Add any paths written
        ProtocolStringList resources = failure.getResourcesCreatedList();
        this.resourcesWritten.addAll( resources );

        LOGGER.debug( "While processing evaluation {}, discovered {} resources that were created by subscribers.",
                      this.getEvaluationId(),
                      resources.size() );

        LOGGER.error( "While tracking evaluation {}, encountered an error that prevented completion. The failure "
                      + "message is {}",
                      this.getEvaluationId(),
                      failure );

        // Stop waiting
        this.publicationLatch.countDown();
        this.negotiatedSubscriberLatches.forEach( ( a, b ) -> b.countDown() );
        this.subscriberNegotiator.stopNegotiation();
        this.evaluationCompletionLatch.countDown();

        // Stop the evaluation, which also stops flow control
        LOGGER.debug( "The evaluation status tracker that is tracking evaluation {} encountered a request to stop "
                      + "on failure with signal {}. Stopping the evaluation.",
                      this.getEvaluationId(),
                      failure.getCompletionStatus() );

        this.evaluation.stop( null );

        // Now that the evaluation has stopped, check that the notification was not duplicated, as this would indicate
        // a subscriber notification failure: we expect one notification per subscriber. Do this last because it should 
        // not prevent the evaluation from closing and freeing resources.
        if ( consumerAlreadyFailed )
        {
            throw new IllegalStateException( WHILE_PROCESSING_EVALUATION
                                             + this.getEvaluationId()
                                             + ENCOUNTERED_AN_ATTEMPT_TO_MARK_SUBSCRIBER
                                             + consumerId
                                             + " as "
                                             + CompletionStatus.CONSUMPTION_COMPLETE_REPORTED_FAILURE
                                             + " when it has already been marked as "
                                             + CompletionStatus.CONSUMPTION_COMPLETE_REPORTED_FAILURE
                                             + ". This probably represents an error in the subscriber that should "
                                             + "be fixed." );
        }
        // Succeeded previously. Definitely a subscriber notification failure.
        if ( !consumerId.isBlank() && this.success.contains( consumerId ) )
        {
            throw new IllegalStateException( WHILE_PROCESSING_EVALUATION
                                             + this.getEvaluationId()
                                             + ENCOUNTERED_AN_ATTEMPT_TO_MARK_SUBSCRIBER
                                             + consumerId
                                             + " as "
                                             + CompletionStatus.CONSUMPTION_COMPLETE_REPORTED_FAILURE
                                             + " when it has previously been marked as "
                                             + CompletionStatus.CONSUMPTION_COMPLETE_REPORTED_SUCCESS
                                             + ". This is an error in the subscriber that should be fixed." );
        }
    }

    /**
     * Monitors for subscribers that are inactive and throws an exception if one is encountered. An inactive subscriber
     * fails to make progress or indicate that it is alive within a prescribed period. Evaluations are allowed to 
     * complete nominally when a subscriber continues to indicate that it is alive, regardless of the time required to 
     * complete the evaluation, but an evaluation cannot last indefinitely when a subscriber is inactive.
     */

    private void monitorForInactiveSubscribers( EvaluationStatusTracker statusTracker )
    {
        String evaluationId = this.getEvaluationId();

        for ( Map.Entry<String, TimedCountDownLatch> nextEntry : this.negotiatedSubscriberLatches.entrySet() )
        {
            String consumerId = nextEntry.getKey();
            TimedCountDownLatch nextLatch = nextEntry.getValue();

            LOGGER.debug( "While processing evaluation {}, awaiting confirmation that consumption has completed "
                          + FOR_SUBSCRIPTION,
                          this.getEvaluationId(),
                          consumerId );

            TimerTask updater = this.getInactiveSubscriberMonitorTask( statusTracker,
                                                                       evaluationId,
                                                                       consumerId,
                                                                       nextLatch );

            // Log progress
            this.subscriberInactivityTimer.schedule( updater, 0, EvaluationStatusTracker.TIMEOUT_UPDATE_FREQUENCY );
        }
    }

    /**
     * Returns a task for monitoring subscribers and stopping an evaluation if they become inactive beyond an 
     * prescribed period without a progress report.
     *  
     * @param statusTracker the status tracker
     * @param evaluationId the evaluation identifier
     * @param consumerId the consumer identifier
     * @param subscriberLatch the latch corresponding to the subscriber whose status is being tracked
     * @return the monitor task
     */

    private TimerTask getInactiveSubscriberMonitorTask( EvaluationStatusTracker statusTracker,
                                                        String evaluationId,
                                                        String consumerId,
                                                        TimedCountDownLatch subscriberLatch )
    {
        // Create a timer task to log progress while awaiting the subscriber
        AtomicReference<Instant> then = new AtomicReference<>( Instant.now() );
        Duration periodToWait = Duration.of( this.timeoutDuringConsumption,
                                             ChronoUnit.valueOf( this.timeoutDuringConsumptionUnits.name() ) );

        AtomicInteger resetCount = new AtomicInteger( subscriberLatch.getResetCount() );

        return new TimerTask()
        {
            @Override
            public void run()
            {
                // Only continue to monitor if the latch has not been counted down
                if ( subscriberLatch.getCount() > 0 )
                {
                    Duration timeElapsed = Duration.between( then.get(), Instant.now() );
                    Duration timeLeft = periodToWait.minus( timeElapsed );

                    // Latch was reset, so report the new time left
                    String append = "";
                    if ( subscriberLatch.getResetCount() > resetCount.get() )
                    {
                        append = " (the subscriber just registered an update, which reset the clock)";
                        timeLeft = periodToWait;
                        resetCount.incrementAndGet();

                        // Reset the reference to now
                        then.set( Instant.now() );
                    }

                    // Insufficient progress?
                    if ( timeLeft.isNegative() )
                    {
                        EvaluationStatus status =
                                statusTracker.getStatusMessageIndicatingConsumptionFailureOnInactivity( evaluationId,
                                                                                                        consumerId,
                                                                                                        periodToWait );

                        statusTracker.stopOnFailure( status );
                    }

                    // Report
                    if ( timeElapsed.compareTo( EvaluationStatusTracker.MINIMUM_PERIOD_BEFORE_REPORTING_INACTIVE_SUBSCRIBER ) > 0
                         && LOGGER.isInfoEnabled() )
                    {
                        LOGGER.info( "While completing evaluation {}, awaiting subscriber {}. There is {} remaining "
                                     + "before the subscriber timeout occurs, unless the subscriber provides an update "
                                     + "sooner{}.",
                                     evaluationId,
                                     consumerId,
                                     timeLeft,
                                     append );
                    }
                }
            }
        };
    }

    /**
     * Returns an {@link EvaluationStatus} message indicating that consumption has failed due to inactivity.
     * 
     * @param evaluationId the evaluation identifier
     * @param consumerId the consumer identifier
     * @param timeoutPeriod the timeout period
     * @return a status message indicating a subscriber timeout
     */

    private EvaluationStatus getStatusMessageIndicatingConsumptionFailureOnInactivity( String evaluationId,
                                                                                       String consumerId,
                                                                                       Duration timeoutPeriod )
    {
        String message = WHILE_COMPLETING_EVALUATION + evaluationId
                         + ", encountered an inactive subscriber with identifier "
                         + consumerId
                         + " that failed to report progress within the timeout "
                         + "period of "
                         + timeoutPeriod
                         + ".";

        Set<Format> formats = this.getFormatsOfferedBySubscriber( consumerId );

        return EvaluationStatus.newBuilder()
                               .setClientId( this.getClientId() )
                               .setConsumer( Consumer.newBuilder()
                                                     .setConsumerId( consumerId )
                                                     .addAllFormats( formats ) )
                               .setCompletionStatus( CompletionStatus.CONSUMPTION_COMPLETE_REPORTED_FAILURE )
                               .addStatusEvents( EvaluationStatusEvent.newBuilder()
                                                                      .setEventType( StatusMessageType.ERROR )
                                                                      .setEventMessage( message ) )
                               .build();
    }

    /**
     * @return the evaluation identifier of the evaluation whose status is being tracked
     */

    private String getEvaluationId()
    {
        return this.evaluation.getEvaluationId();
    }

    /**
     * Registers the publication of a message group complete.
     * 
     * TODO: delegate producer flow control to the broker. See #83536.
     * 
     * @param message the status message containing the status event
     * @throws NullPointerException if the message is null
     */

    private void registerGroupPublicationComplete( EvaluationStatus message )
    {
        Objects.requireNonNull( message );

        this.flowController.start();

        LOGGER.debug( "Started flow control for message group {}.", message.getGroupId() );
    }

    /**
     * Registers the publication of a message group complete.
     * 
     * TODO: delegate producer flow control to the broker. See #83536.
     * 
     * @param message the status message containing the status event
     * @throws NullPointerException if the message is null
     * @throws IllegalArgumentException if the consumer is absent
     */

    private void registerGroupConsumptionComplete( EvaluationStatus message )
    {
        Objects.requireNonNull( message );

        if ( message.hasConsumer() )
        {
            String consumerId = message.getConsumer()
                                       .getConsumerId();

            LOGGER.debug( "Registering group consumption complete for subscriber {}.", consumerId );

            this.flowController.stop( consumerId );

            LOGGER.debug( "Stopped flow control for message group {}.", message.getGroupId() );
        }
        else
        {
            // Only grouped messages control flow.
            LOGGER.debug( "Not attempting to stop flow control because the message received had no consumer set." );
        }
    }

    /**
     * Registers the completion of the evaluation with {@link CompletionStatus#EVALUATION_COMPLETE_REPORTED_SUCCESS}.
    
     * @param message the status message containing the status event
     * @throws NullPointerException if the message is null
     */

    private void registerSuccess( EvaluationStatus message )
    {
        Objects.requireNonNull( message );

        LOGGER.debug( "Registered the successful completion of evaluation {}.", this.getEvaluationId() );

        this.evaluationCompletionLatch.countDown();
    }

    /**
     * Attempts to negotiate a subscription for one of the formats required by this evaluation.
     * 
     * @param message the message with the subscription information
     */

    private void registerAnOfferToDeliverFormats( EvaluationStatus message )
    {
        this.subscriberNegotiator.registerAnOfferToDeliverFormats( message );
    }

    /**
     * Returns the formats offered by the subscriber.
     * @param subscriberId the subscriber identifier
     * @return the formats offered by the specified subscriber
     */

    private Set<Format> getFormatsOfferedBySubscriber( String subscriberId )
    {
        return this.negotiatedSubscribers.entrySet()
                                         .stream()
                                         .filter( next -> next.getValue().equals( subscriberId ) )
                                         .map( Map.Entry::getKey )
                                         .collect( Collectors.toSet() );
    }

    /**
     * Registers publication as completed successfully.
     * 
     * @param message the status message containing the evaluation event
     */

    private void registerPublicationCompleteReportedSuccess( EvaluationStatus message )
    {
        LOGGER.info( "Messaging client {} notified {} for evaluation {}.",
                     message.getClientId(),
                     message.getCompletionStatus(),
                     this.getEvaluationId() );

        this.publicationLatch.countDown();
    }

    /**
     * Registers consumption as completed successfully.
     * 
     * @param message the status message containing the subscriber event
     * @throws IllegalStateException if the consumer has already been marked as failed
     */

    private void registerConsumptionCompleteReportedSuccess( EvaluationStatus message )
    {
        String consumerId = message.getConsumer()
                                   .getConsumerId();

        // A real consumer
        if ( this.isValidConsumerId( consumerId ) )
        {
            if ( this.failure.contains( consumerId ) )
            {
                throw new IllegalStateException( WHILE_PROCESSING_EVALUATION + this.getEvaluationId()
                                                 + ENCOUNTERED_AN_ATTEMPT_TO_MARK_SUBSCRIBER
                                                 + consumerId
                                                 + " as "
                                                 + CompletionStatus.CONSUMPTION_COMPLETE_REPORTED_SUCCESS
                                                 + " when it has previously been marked as "
                                                 + CompletionStatus.CONSUMPTION_COMPLETE_REPORTED_FAILURE
                                                 + "." );
            }

            // Add any paths written
            ProtocolStringList resources = message.getResourcesCreatedList();
            this.resourcesWritten.addAll( resources );

            LOGGER.debug( "While processing evaluation {}, discovered {} resources that were created by subscribers.",
                          this.getEvaluationId(),
                          resources.size() );

            // Countdown the subscription as complete
            this.success.add( consumerId );
            TimedCountDownLatch subscriberLatch = this.getSubscriberLatch( consumerId );
            subscriberLatch.countDown();

            LOGGER.info( "Messaging client {} notified {} for evaluation {}.",
                         message.getClientId(),
                         message.getCompletionStatus(),
                         this.getEvaluationId() );
        }
    }

    /**
     * Registers consumption as ongoing for a given subscriber.
     * 
     * @param message the status message containing the subscriber event
     */

    private void registerConsumptionOngoing( EvaluationStatus message )
    {
        String consumerId = message.getConsumer()
                                   .getConsumerId();

        // A real consumer
        if ( this.isValidConsumerId( consumerId ) )
        {
            // Reset the countdown
            this.getSubscriberLatch( consumerId )
                .resetClock();

            LOGGER.debug( "Message subscriber {} for evaluation {} reports {}.",
                          consumerId,
                          this.getEvaluationId(),
                          message.getCompletionStatus() );
        }
    }

    /**
     * Validates the consumer identifier.
     * 
     * @param consumerId the consumer identifier
     * @return true if the consumer is not me and is being tracked by this tracker instance, false otherwise
     * @throws EvaluationEventException if the consumer identifier is blank
     */

    private boolean isValidConsumerId( String consumerId )
    {
        if ( consumerId.isBlank() )
        {
            throw new EvaluationEventException( "While awaiting consumption for evaluation "
                                                + this.getEvaluationId()
                                                + " received a message about a consumer event that did not "
                                                + "contain the consumerId, which is not allowed." );
        }

        // Not this consumer and a negotiated subscriber
        return !this.isThisConsumerMe( consumerId ) && this.isNegotiatedSubscriber( consumerId );
    }

    /**
     * Returns true if the identified consumer is a negotiated subscriber, otherwise false.
     * 
     * @return true if the identified consumer has been negotiated by this evaluation, otherwise false
     */

    private boolean isNegotiatedSubscriber( String consumerId )
    {
        return this.negotiatedSubscribers.values()
                                         .contains( consumerId );
    }

    /**
     * Returns true if the identified subscriber is this status tracker, false otherwise.
     * 
     * @param consumerId the subscriber identifier
     * @return true if it is me, false otherwise
     */

    private boolean isThisConsumerMe( String consumerId )
    {
        return Objects.equals( consumerId, this.trackerId );
    }

    /**
     * Returns a subscriber latch for a given subscriber identifier.
     * 
     * @param consumerId the subscriber identifier
     * @return the latch
     */

    private TimedCountDownLatch getSubscriberLatch( String consumerId )
    {
        this.isValidConsumerId( consumerId );

        return this.negotiatedSubscriberLatches.get( consumerId );
    }

    /**
     * Returns the unique identifier of the messaging client that created the evaluation being tracked.
     * 
     * @return the client identifier
     */

    private String getClientId()
    {
        return this.evaluation.getClientId();
    }

    /**
     * @return true if the subscriber created by this tracker is durable, false if it is temporary
     */

    private boolean isDurableSubscriber()
    {
        return this.durableSubscriber;
    }

    /**
     * Create an instance with an evaluation and an expected list of subscriber identifiers.
     * @param evaluation the evaluation
     * @param broker the broker connection factory
     * @param formatsRequired the formats to be delivered by subscribers yet to be negotiated
     * @param maximum retries the maximum number of retries on failing to consume a message
     * @param subscriberApprover determines whether subscription offers from format writers are viable
     * @param flowController producer flow controller
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the set of formats is empty or the maximum number of retries is < 0
     * @throws EvaluationEventException if the status tracker could not be created for any other reason
     */

    EvaluationStatusTracker( Evaluation evaluation,
                             BrokerConnectionFactory broker,
                             Set<Format> formatsRequired,
                             int maximumRetries,
                             SubscriberApprover subscriberApprover,
                             ProducerFlowController flowController )
    {
        Objects.requireNonNull( evaluation );
        Objects.requireNonNull( broker );
        Objects.requireNonNull( formatsRequired );
        Objects.requireNonNull( subscriberApprover );
        Objects.requireNonNull( flowController );

        if ( maximumRetries < 0 )
        {
            throw new IllegalArgumentException( "While building an evaluation status tracker, discovered a maximum "
                                                + "retry count that is less than zero, which is not allowed: "
                                                + maximumRetries );
        }

        if ( formatsRequired.isEmpty() )
        {
            throw new IllegalArgumentException( "Cannot build a status tracker for evaluation "
                                                + evaluation.getEvaluationId()
                                                + " because there are no formats to be delivered by format "
                                                + "subscribers." );
        }

        this.evaluation = evaluation;
        this.success = ConcurrentHashMap.newKeySet();
        this.failure = ConcurrentHashMap.newKeySet();
        this.negotiatedSubscribers = new EnumMap<>( Format.class );
        this.flowController = flowController;
        this.formatsRequired = formatsRequired;
        this.subscriberInactivityTimer = new Timer( "SubscriberInactivityTimer-1", true );
        this.subscriberApprover = subscriberApprover;
        this.subscriberNegotiator = new SubscriberNegotiator( this.evaluation,
                                                              this.formatsRequired,
                                                              this.subscriberApprover );

        // Non-durable subscribers until we can properly recover from broker/client failures to warrant durable ones
        this.durableSubscriber = false;
        this.logSubscriberPolicy( this.durableSubscriber );

        // Default timeout for consumption from an individual consumer unless progress is reported
        // In practice, this is extremely lenient. TODO: expose this to configuration.
        this.timeoutDuringConsumption = 120;
        this.timeoutDuringConsumptionUnits = TimeUnit.MINUTES;

        // Mutable because subscriptions are negotiated and hence latches added upon successful negotiation
        this.negotiatedSubscriberLatches = new ConcurrentHashMap<>();

        // Create a unique identifier for this tracker so that it can ignore messages related to itself
        this.trackerId = EvaluationEventUtilities.getUniqueId();
        this.resourcesWritten = new HashSet<>();
        this.retriesAttempted = new AtomicInteger();
        this.isFailedUnrecoverably = new AtomicBoolean();
        this.maximumRetries = maximumRetries;

        // Set the message consumer and listener
        try
        {
            // Get a connection
            this.connection = broker.get();
            LOGGER.debug( "Created connection {} in evaluation status tracker {}.", this.connection, this );

            this.connection.setExceptionListener( new ConnectionExceptionListener( this ) );
            this.session = connection.createSession( false, Session.CLIENT_ACKNOWLEDGE );
            Topic topic = (Topic) broker.getDestination( Evaluation.EVALUATION_STATUS_QUEUE );

            // Only consider messages that belong to this evaluation. Even when negotiating subscribers, offers should
            // refer to the specific evaluation that requested one or more format writers
            String selector = MessageProperty.JMS_CORRELATION_ID + "='" + this.getEvaluationId() + "'";
            this.subscriberName = Evaluation.EVALUATION_STATUS_QUEUE + "-HOUSEKEEPING-evaluation-status-tracker-"
                                  + this.getEvaluationId()
                                  + "-"
                                  + this.getTrackerId();

            MessageConsumer consumer = this.getMessageConsumer( topic,
                                                                this.subscriberName,
                                                                selector,
                                                                this.isDurableSubscriber() );

            this.registerListenerForConsumer( consumer,
                                              this.getEvaluationId() );

            // Start the connection
            this.connection.start();
        }
        catch ( JMSException | NamingException e )
        {
            throw new EvaluationEventException( "Unable to construct an evaluation.", e );
        }
    }

    /**
     * Returns a consumer.
     * 
     * @param topic the topic
     * @param name the name of the subscriber
     * @param selector a selector
     * @param durableSubscriber is true to use a durable subscriber, false for a temporary subscriber
     * @return a consumer
     * @throws JMSException if the consumer could not be created for any reason
     */

    private MessageConsumer getMessageConsumer( Topic topic, String name, String selector, boolean durableSubscriber )
            throws JMSException
    {
        if ( durableSubscriber )
        {
            return this.session.createDurableSubscriber( topic, name, selector, false );
        }
        else
        {
            return this.session.createConsumer( topic, selector, false );
        }
    }

    /**
     * Registers a listener to a consumer.
     * 
     * @param statusConsumer the status consumer
     * @param evaluationId the identifier of the evaluation being tracked
     * @throws JMSException if the listener could not be registered for any reason
     */

    private void registerListenerForConsumer( MessageConsumer statusConsumer,
                                              String evaluationId )
            throws JMSException
    {
        // Now listen for status messages when a group completes
        MessageListener listener = message -> {
            BytesMessage receivedBytes = (BytesMessage) message;
            String messageId = null;
            String correlationId = null;

            try
            {
                // Only consume if the subscriber is live
                if ( !this.isFailed() )
                {
                    messageId = message.getJMSMessageID();
                    correlationId = message.getJMSCorrelationID();

                    // Create the byte array to hold the message
                    int messageLength = (int) receivedBytes.getBodyLength();

                    byte[] messageContainer = new byte[messageLength];

                    receivedBytes.readBytes( messageContainer );

                    ByteBuffer bufferedMessage = ByteBuffer.wrap( messageContainer );

                    EvaluationStatus statusMessage = EvaluationStatus.parseFrom( bufferedMessage.array() );

                    // Order of receipt and ack requires special handling on final success message otherwise the
                    // connection and session may be closed before the ack
                    if ( statusMessage.getCompletionStatus() == CompletionStatus.EVALUATION_COMPLETE_REPORTED_SUCCESS )
                    {
                        // Acknowledge and accept the message
                        message.acknowledge();
                        this.acceptStatusMessage( statusMessage );
                    }
                    else
                    {
                        // Accept and acknowledge the message
                        this.acceptStatusMessage( statusMessage );
                        message.acknowledge();
                    }
                }
            }
            // Throwing exceptions on the MessageListener::onMessage is considered a bug, so need to track status and 
            // exit gracefully when problems occur. The message is either delivered, a checked exception is caught and
            // the session is recovering, an unchecked exception is caught and the subscriber is unrecoverable or an
            // unrecoverable error is thrown/unhandled.
            catch ( JMSException | InvalidProtocolBufferException e )
            {
                // Attempt to recover and flag recovery locally
                this.recover( messageId, correlationId, e );
            }
            catch ( RuntimeException e )
            {
                LOGGER.error( "While processing an evaluation status message, encountered an unrecoverable error that "
                              + "will stop the evaluation status tracker.",
                              e );

                EvaluationStatus errorStatusMessage = this.getStatusMessageFromException( e );

                this.stopOnFailure( errorStatusMessage );
            }
        };

        statusConsumer.setMessageListener( listener );

        LOGGER.debug( "Successfully registered an evaluation status tracker {} for the evaluation status messages "
                      + "associated with evaluation {}",
                      this,
                      evaluationId );
    }

    /**
     * Accepts and routes an evaluation status message.
     * @param message the message
     */
    private void acceptStatusMessage( EvaluationStatus message )
    {
        Objects.requireNonNull( message );
        CompletionStatus status = message.getCompletionStatus();
        switch ( status )
        {
            case PUBLICATION_COMPLETE_REPORTED_SUCCESS:
                this.registerPublicationCompleteReportedSuccess( message );
                break;
            case PUBLICATION_COMPLETE_REPORTED_FAILURE:
                this.stopOnFailure( message );
                break;
            case READY_TO_CONSUME:
                this.registerAnOfferToDeliverFormats( message );
                break;
            case CONSUMPTION_COMPLETE_REPORTED_SUCCESS:
                this.registerConsumptionCompleteReportedSuccess( message );
                break;
            case CONSUMPTION_COMPLETE_REPORTED_FAILURE:
                this.stopOnFailure( message );
                break;
            case CONSUMPTION_ONGOING:
                this.registerConsumptionOngoing( message );
                break;
            case GROUP_PUBLICATION_COMPLETE:
                this.registerGroupPublicationComplete( message );
                break;
            case GROUP_CONSUMPTION_COMPLETE:
                this.registerGroupConsumptionComplete( message );
                break;
            case EVALUATION_COMPLETE_REPORTED_FAILURE:
                this.stopOnFailure( message );
                break;
            case EVALUATION_COMPLETE_REPORTED_SUCCESS:
                this.registerSuccess( message );
                break;
            default:
                break;
        }
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

    private void recover( String messageId, String correlationId, Exception exception )
    {
        // Only try to recover if the tracker hasn't already failed        
        if ( !this.isFailed() )
        {
            int retryCount = this.getNumberOfRetriesAttempted()
                                 .incrementAndGet(); // Counter starts at zero

            try
            {
                // Exponential back-off, which includes a PT2S wait before the first attempt
                Thread.sleep( (long) Math.pow( 2, retryCount ) * 1000 );

                String errorMessage = "While attempting to consume a message with identifier " + messageId
                                      + " and correlation identifier "
                                      + correlationId
                                      + " in evaluation status tracker "
                                      + this.getClientId()
                                      + ", encountered an error. The session will now attempt to recover. This "
                                      + "is "
                                      + retryCount
                                      + " of "
                                      + this.getMaximumRetries()
                                      + " allowed consumption failures before the status tracker will notify an "
                                      + "unrecoverable failure for evaluation "
                                      + correlationId
                                      + ", unless the status tracker is otherwise marked "
                                      + "failed (in which case further retries may not occur).";

                LOGGER.error( errorMessage, exception );

                // Attempt recovery in order to cycle the delivery attempts. When the maximum is reached, poison
                // messages should hit the Dead Letter Queue (DLQ), assuming a DLQ is configured.
                this.session.recover();
            }
            catch ( JMSException f )
            {
                String message = "While attempting to recover a session for evaluation " + correlationId
                                 + " in evaluation status tracker "
                                 + this.getClientId()
                                 + ", encountered "
                                 + "an error that prevented recovery.";

                LOGGER.error( message, f );
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();

                String message = "Interrupted while waiting to recover a session in evaluation " + correlationId + ".";

                LOGGER.error( message, correlationId );
            }

            // Stop if the maximum number of retries has been reached
            if ( retryCount == this.getMaximumRetries() )
            {
                LOGGER.error( "Evaluation status tracker {} encountered a consumption failure for evaluation {}. "
                              + "Recovery failed after {} attempts.",
                              this.getTrackerId(),
                              correlationId,
                              this.getMaximumRetries() );

                EvaluationStatus errorStatusMessage = this.getStatusMessageFromException( exception );

                // Register the status tracker as failed
                this.stopOnFailure( errorStatusMessage );
            }
        }
    }

    /**
     * Returns a status message from an exception.
     * @param e the exception
     * @return the status message that wraps the exception
     */

    private EvaluationStatus getStatusMessageFromException( Exception e )
    {
        EvaluationStatusEvent errorEvent = EvaluationEventUtilities.getStatusEventFromException( e );
        return EvaluationStatus.newBuilder()
                               .setClientId( this.getClientId() )
                               .setCompletionStatus( CompletionStatus.EVALUATION_COMPLETE_REPORTED_FAILURE )
                               .addStatusEvents( errorEvent )
                               .build();
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
     * @return the identifier of this status tracker.
     */

    private String getTrackerId()
    {
        return this.trackerId;
    }

    /**
     * Listen for failures on a connection.
     */

    private static class ConnectionExceptionListener implements ExceptionListener
    {

        /**
         * The status tracker.
         */

        private final EvaluationStatusTracker statusTracker;

        @Override
        public void onException( JMSException exception )
        {
            String message = WHILE_COMPLETING_EVALUATION + this.statusTracker.getEvaluationId()
                             + ", encountered an error on a connection owned by the evaluation status tracker "
                             + "responsible for tracking this evaluation. If a failover policy was configured on the "
                             + "connection factory (e.g., connection retries), then that policy was exhausted before "
                             + "this error was thrown. As such, the error is not recoverable and the evaluation will "
                             + "now stop.";

            EvaluationStatus status = EvaluationStatus.newBuilder()
                                                      .setClientId( this.statusTracker.getClientId() )
                                                      .setCompletionStatus( CompletionStatus.EVALUATION_COMPLETE_REPORTED_FAILURE )
                                                      .addStatusEvents( EvaluationStatusEvent.newBuilder()
                                                                                             .setEventType( StatusMessageType.ERROR )
                                                                                             .setEventMessage( message ) )
                                                      .build();

            this.statusTracker.stopOnFailure( status );
        }

        /**
         * Creates an instance with a status tracker.
         * 
         * @param statusTracker the status tracker
         */

        ConnectionExceptionListener( EvaluationStatusTracker statusTracker )
        {
            Objects.requireNonNull( statusTracker );

            this.statusTracker = statusTracker;
        }

    }
}
