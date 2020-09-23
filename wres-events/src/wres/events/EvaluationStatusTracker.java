package wres.events;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ProtocolStringList;

import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.EvaluationStatus.CompletionStatus;

/**
 * A consumer that tracks the status of an evaluation via its {@link EvaluationStatus} messages and awaits its 
 * completion upon request ({@link #await()}).
 * 
 * @author james.brown@hydrosolved.com
 */

class EvaluationStatusTracker implements Consumer<EvaluationStatus>
{

    private static final String ENCOUNTERED_AN_ATTEMPT_TO_MARK_SUBSCRIBER = ", encountered an attempt to mark subscriber ";

    private static final String WHILE_PROCESSING_EVALUATION = "While processing evaluation ";

    private static final Logger LOGGER = LoggerFactory.getLogger( EvaluationStatusTracker.class );

    /**
     * The frequency between logging updates in milliseconds upon awaiting a subscriber. 
     */

    private static final long TIMEOUT_UPDATE_FREQUENCY = 10_000;

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
     * A set of subscribers that succeeded.
     */

    private final Set<String> success;

    /**
     * A set of subscribers that failed.
     */

    private final Set<String> failure;

    /**
     * The timeout duration.
     */

    private final long timeout;

    /**
     * The timeout units.
     */

    private final TimeUnit timeoutUnits;

    /**
     * The consumer identifier of the subscriber to which this instance is attached. Helps me ignore messages that 
     * were reported by the subscriber to which I am attached.
     */

    private final String myConsumerId;

    /**
     * The exit code.
     */

    private final AtomicInteger exitCode = new AtomicInteger();

    /**
     * Strings that represents paths or URIs to resources written during the evaluation.
     */

    private final Set<String> resourcesWritten;

    @Override
    public void accept( EvaluationStatus message )
    {
        Objects.requireNonNull( message );

        CompletionStatus status = message.getCompletionStatus();

        switch ( status )
        {
            case PUBLICATION_COMPLETE_REPORTED_SUCCESS:
                this.publicationLatch.countDown();
                break;
            case PUBLICATION_COMPLETE_REPORTED_FAILURE:
                this.stopOnFailure( message );
                break;
            case READY_TO_CONSUME:
                this.registerSubscriberReady( message );
                break;
            case CONSUMPTION_COMPLETE_REPORTED_SUCCESS:
                this.registerSubscriberCompleteReportedSuccess( message );
                break;
            case CONSUMPTION_COMPLETE_REPORTED_FAILURE:
                this.stopOnFailure( message );
                break;
            case CONSUMPTION_ONGOING:
                this.registerConsumptionOngoing( message );
                break;
            default:
                break;
        }
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
     * Stops all tracking on failure.
     * @param failure the failure message.
     * @throws IllegalStateException if stopping a consumer that has previously been reported as failed or succeeded
     */

    private void stopOnFailure( EvaluationStatus failure )
    {
        // Non-zero exit code
        this.exitCode.set( 1 );

        String consumerId = failure.getConsumerId();
        if ( !consumerId.isBlank() )
        {
            // Failed previously. Probably a subscriber notification error.
            if ( this.failure.contains( consumerId ) )
            {
                throw new IllegalStateException( WHILE_PROCESSING_EVALUATION
                                                 + this.evaluation.getEvaluationId()
                                                 + ENCOUNTERED_AN_ATTEMPT_TO_MARK_SUBSCRIBER
                                                 + consumerId
                                                 + " as "
                                                 + CompletionStatus.CONSUMPTION_COMPLETE_REPORTED_FAILURE
                                                 + " when it has already been marked as "
                                                 + CompletionStatus.CONSUMPTION_COMPLETE_REPORTED_FAILURE
                                                 + ". This probably represents an error in the subscriber that should "
                                                 + "be fixed." );
            }

            this.failure.add( consumerId );

            // Succeeded previously. Definitely a subscriber notification failure.
            if ( this.success.contains( consumerId ) )
            {
                throw new IllegalStateException( WHILE_PROCESSING_EVALUATION
                                                 + this.evaluation.getEvaluationId()
                                                 + ENCOUNTERED_AN_ATTEMPT_TO_MARK_SUBSCRIBER
                                                 + consumerId
                                                 + " as "
                                                 + CompletionStatus.CONSUMPTION_COMPLETE_REPORTED_FAILURE
                                                 + " when it has previously been marked as "
                                                 + CompletionStatus.CONSUMPTION_COMPLETE_REPORTED_SUCCESS
                                                 + ". This is an error in the subscriber that should be fixed." );
            }
        }

        // Add any paths written
        ProtocolStringList resources = failure.getResourcesCreatedList();
        this.resourcesWritten.addAll( resources );

        LOGGER.debug( "While processing evaluation {}, discovered {} resources that were created by subscribers.",
                      this.evaluation.getEvaluationId(),
                      resources.size() );

        LOGGER.error( "While tracking evaluation {}, encountered an error that prevented completion. The failure "
                      + "message is {}.",
                      this.evaluation.getEvaluationId(),
                      failure );

        // Stop waiting
        this.publicationLatch.countDown();
        this.subscriberLatches.forEach( ( a, b ) -> b.countDown() );
    }

    /**
     * Wait until the evaluation has completed.
     * 
     * @return the exit code. A non-zero exit code corresponds to failure
     * @throws InterruptedException if the evaluation was interrupted.
     */

    int await() throws InterruptedException
    {
        LOGGER.debug( "While processing evaluation {}, awaiting confirmation that publication has completed.",
                      this.evaluation.getEvaluationId() );

        this.publicationLatch.await();

        LOGGER.debug( "While processing evaluation {}, received confirmation that publication has completed.",
                      this.evaluation.getEvaluationId() );

        String evaluationId = this.evaluation.getEvaluationId();

        for ( Map.Entry<String, TimedCountDownLatch> nextEntry : this.subscriberLatches.entrySet() )
        {
            String consumerId = nextEntry.getKey();
            TimedCountDownLatch nextLatch = nextEntry.getValue();

            LOGGER.debug( "While processing evaluation {}, awaiting confirmation that consumption has completed "
                          + "for subscription {}.",
                          this.evaluation.getEvaluationId(),
                          consumerId );

            // Create a timer task to log progress while awaiting the subscriber
            Instant then = Instant.now();
            Duration periodToWait = Duration.of( this.timeout, ChronoUnit.valueOf( this.timeoutUnits.name() ) );
            AtomicInteger resetCount = new AtomicInteger();
            TimerTask updater = new TimerTask()
            {
                @Override
                public void run()
                {
                    Duration timeElapsed = Duration.between( then, Instant.now() );
                    Duration timeLeft = periodToWait.minus( timeElapsed );

                    // Latch was reset, so report the new time left
                    String append = "";
                    if ( nextLatch.getResetCount() > resetCount.get() )
                    {
                        append = " (the subscriber just registered an update, which reset the timeout)";
                        timeLeft = periodToWait;
                        resetCount.incrementAndGet();
                    }

                    LOGGER.info( "While completing evaluation {}, awaiting subscriber {}. There is {} remaining before "
                                 + "the subscriber timeout occurs, unless the subscriber provides an update sooner{}.",
                                 evaluationId,
                                 consumerId,
                                 timeLeft,
                                 append );
                }
            };

            // Log progress
            Timer timer = new Timer();
            timer.schedule( updater, 0, EvaluationStatusTracker.TIMEOUT_UPDATE_FREQUENCY );

            // Wait for a fixed period unless there is progress
            nextLatch.await( this.timeout, this.timeoutUnits );

            // Stop logging progress
            timer.cancel();

            LOGGER.debug( "While processing evaluation {}, received confirmation that consumption has completed "
                          + "for subscription {}.",
                          evaluationId,
                          consumerId );
        }

        LOGGER.debug( "While processing evaluation {}, received confirmation that consumption has stopped "
                      + "across all {} registered subscriptions.",
                      evaluationId,
                      this.expectedSubscribers.size() );

        // Throw an exception if any consumers failed to complete consumption
        Set<String> identifiers = this.subscriberLatches.entrySet()
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
                                                   + this.timeout
                                                   + " "
                                                   + this.timeoutUnits
                                                   + ": "
                                                   + identifiers
                                                   + ". Subscribers should report their status regularly, in "
                                                   + "order to reset the timeout period. " );
        }

        return this.exitCode.get();
    }

    /**
     * @return the set of failed susbcribers.
     */
    
    Set<String> getFailedSubscribers()
    {
        return Collections.unmodifiableSet( this.failure );
    }
    
    /**
     * Registers a new consumer as ready to consume.
     * 
     * @param message the status message containing the subscriber event
     */

    private void registerSubscriberReady( EvaluationStatus message )
    {
        String consumerId = message.getConsumerId();
        boolean isBeingTracked = this.validateConsumerId( consumerId );

        // A real consumer
        if ( isBeingTracked )
        {
            LOGGER.debug( "Registering a message subscriber {} for evaluation {} as {}.",
                          consumerId,
                          this.evaluation.getEvaluationId(),
                          message.getCompletionStatus() );

            this.subscribersReady.add( consumerId );

            // Reset the countdown
            TimedCountDownLatch latch = this.getSubscriberLatch( consumerId );

            latch.resetClock();

            LOGGER.info( "Registered a message subscriber {} for evaluation {} as {}.",
                         consumerId,
                         this.evaluation.getEvaluationId(),
                         message.getCompletionStatus() );
        }
    }

    /**
     * Registers a subscriber as completed successfully.
     * 
     * @param message the status message containing the subscriber event
     * @throws IllegalStateException if the consumer has already been marked as failed
     */

    private void registerSubscriberCompleteReportedSuccess( EvaluationStatus message )
    {
        String consumerId = message.getConsumerId();
        boolean isBeingTracked = this.validateConsumerId( consumerId );

        // A real consumer
        if ( isBeingTracked )
        {
            if ( this.failure.contains( consumerId ) )
            {
                throw new IllegalStateException( WHILE_PROCESSING_EVALUATION + this.evaluation.getEvaluationId()
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
                          this.evaluation.getEvaluationId(),
                          resources.size() );

            // Countdown the subscription as complete
            this.subscribersReady.remove( consumerId );
            this.success.add( consumerId );
            TimedCountDownLatch subscriberLatch = this.getSubscriberLatch( consumerId );
            subscriberLatch.countDown();

            LOGGER.debug( "Removed a message subscriber {} for evaluation {} as {}.",
                          consumerId,
                          this.evaluation.getEvaluationId(),
                          message.getCompletionStatus() );
        }
    }

    /**
     * Registers consumption as ongoing for a given subscriber.
     * 
     * @param message the status message containing the subscriber event
     */

    private void registerConsumptionOngoing( EvaluationStatus message )
    {
        String consumerId = message.getConsumerId();
        boolean isBeingTracked = this.validateConsumerId( consumerId );

        // A real consumer
        if ( isBeingTracked )
        {
            // Reset the countdown
            this.getSubscriberLatch( consumerId ).resetClock();

            LOGGER.debug( "Message subscriber {} for evaluation {} reports {}.",
                          consumerId,
                          this.evaluation.getEvaluationId(),
                          message.getCompletionStatus() );
        }
    }

    /**
     * Throws an exception if the consumer identifier is blank.
     * @param consumerId the consumer identifier
     * @throws EvaluationEventException if the consumer identifier is blank
     * @return true if the consumer is not me and is being tracked by this tracker instance, false otherwise
     */

    private boolean validateConsumerId( String consumerId )
    {
        if ( consumerId.isBlank() )
        {
            throw new EvaluationEventException( "While awaiting consumption for evaluation "
                                                + this.evaluation.getEvaluationId()
                                                + " received a message about a consumer event that did not "
                                                + "contain the consumerId, which is not allowed." );
        }

        return !this.isThisConsumerMe( consumerId ) && this.expectedSubscribers.contains( consumerId );
    }

    /**
     * Returns true if the identified subscriber is this status tracker, false otherwise.
     * 
     * @param consumerId the subscriber identifier
     * @return true if it is me, false otherwise
     */

    private boolean isThisConsumerMe( String consumerId )
    {
        return Objects.equals( consumerId, this.myConsumerId );
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
     * @param myConsumerId the consumer identifier of this instance, used to ignore messages related to the tracker
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the list of subscribers is empty
     */

    EvaluationStatusTracker( Evaluation evaluation, Set<String> expectedSubscribers, String myConsumerId )
    {
        Objects.requireNonNull( evaluation );
        Objects.requireNonNull( expectedSubscribers );
        Objects.requireNonNull( myConsumerId );

        if ( expectedSubscribers.isEmpty() )
        {
            throw new IllegalArgumentException( "Expected one or more subscribers when building evaluation "
                                                + evaluation.getEvaluationId()
                                                + " but found none." );
        }

        this.evaluation = evaluation;
        this.subscribersReady = new HashSet<>();
        this.success = ConcurrentHashMap.newKeySet();
        this.failure = ConcurrentHashMap.newKeySet();
        this.expectedSubscribers = expectedSubscribers;

        LOGGER.info( "Registering the following message subscribers for evaluation {}: {}.",
                     this.evaluation.getEvaluationId(),
                     this.expectedSubscribers );

        // Default timeout for consumption from an individual consumer unless progress is reported
        // In practice, this is extremely lenient
        this.timeout = 120;
        this.timeoutUnits = TimeUnit.MINUTES;

        // Create the latches
        Map<String, TimedCountDownLatch> internalLatches = new HashMap<>( this.expectedSubscribers.size() );
        this.expectedSubscribers.forEach( next -> internalLatches.put( next, new TimedCountDownLatch( 1 ) ) );
        this.subscriberLatches = Collections.unmodifiableMap( internalLatches );
        this.myConsumerId = myConsumerId;
        this.resourcesWritten = new HashSet<>();
    }
}
