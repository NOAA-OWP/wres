package wres.events;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger LOGGER = LoggerFactory.getLogger( EvaluationStatusTracker.class );

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

    /**
     * The consumer identifier of the subscriber to which this instance is attached. Helps me ignore messages that 
     * were reported by the subscriber to which I am attached.
     */

    private final String myConsumerId;

    /**
     * The exit code.
     */

    private final AtomicInteger exitCode = new AtomicInteger();

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
                this.stopOnFailureAndThrowException( message );
                break;
            case READY_TO_CONSUME:
                this.registerSubscriberReady( message );
                break;
            case CONSUMPTION_COMPLETE_REPORTED_SUCCESS:
                this.registerSubscriberComplete( message );
                break;
            case CONSUMPTION_COMPLETE_REPORTED_FAILURE:
                this.stopOnFailureAndThrowException( message );
                break;
            case CONSUMPTION_ONGOING:
                this.registerConsumptionOngoing( message );
                break;
            default:
                break;
        }
    }

    /**
     * Stops all tracking on failure.
     * @param failure the failure message.
     * @throws EvaluationFailedToCompleteException always
     */

    private void stopOnFailureAndThrowException( EvaluationStatus failure )
    {

        // Stop waiting
        this.publicationLatch.countDown();
        this.subscriberLatches.forEach( ( a, b ) -> b.countDown() );

        // Non-zero exit code
        this.exitCode.set( 1 );

        throw new EvaluationFailedToCompleteException( "While tracking evaluation " + this.evaluation.getEvaluationId()
                                                       + ", encountered an error that prevented completion. The "
                                                       + "failure message is: "
                                                       + failure );
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

        return this.exitCode.get();
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

        // A real consumer, not the status tracker a.k.a. me
        if ( !this.isThisConsumerMe( consumerId ) )
        {
            LOGGER.debug( "Registering a message subscriber {} for evaluation {} as {}.",
                          consumerId,
                          this.evaluation.getEvaluationId(),
                          message.getCompletionStatus() );

            this.subscribersReady.add( consumerId );

            // Reset the countdown
            TimedCountDownLatch latch = this.getSubscriberLatch( consumerId );

            latch.resetClock();

            LOGGER.debug( "Finished registered a message subscriber {} for evaluation {} as {}.",
                          consumerId,
                          this.evaluation.getEvaluationId(),
                          message.getCompletionStatus() );
        }
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

        // A real consumer, not the status tracker a.k.a. me
        if ( !this.isThisConsumerMe( consumerId ) && this.expectedSubscribers.contains( consumerId ) )
        {
            // Countdown the subscription as complete
            this.subscribersReady.remove( consumerId );
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
        this.validateConsumerId( consumerId );

        // A real consumer, not the status tracker a.k.a. me
        if ( !this.isThisConsumerMe( consumerId ) )
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
        this.expectedSubscribers = expectedSubscribers;

        LOGGER.info( "Registering the following messages subscribers for evaluation {}: {}.",
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
    }
}
