package wres.events;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ProtocolStringList;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;
import wres.statistics.generated.Consumer.Format;
import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.EvaluationStatus.CompletionStatus;

/**
 * A consumer that tracks the status of an evaluation via its {@link EvaluationStatus} messages and awaits its 
 * completion upon request ({@link #await()}).
 * 
 * @author james.brown@hydrosolved.com
 */

@ThreadSafe
class EvaluationStatusTracker implements Consumer<EvaluationStatus>
{

    private static final String ENCOUNTERED_AN_ATTEMPT_TO_MARK_SUBSCRIBER =
            ", encountered an attempt to mark subscriber ";

    private static final String WHILE_PROCESSING_EVALUATION = "While processing evaluation ";

    private static final Logger LOGGER = LoggerFactory.getLogger( EvaluationStatusTracker.class );

    /**
     * The frequency between logging updates in milliseconds upon awaiting a subscriber. 
     */

    private static final long TIMEOUT_UPDATE_FREQUENCY = 10_000;

    /**
     * The frequency between updates that the evaluation is ready to receive subscription offers from consumers that
     * deliver output formats.
     */

    private static final long READY_TO_RECEIVE_CONSUMERS_UPDATE_FREQUENCY = 5_000;

    /**
     * An RNG to randomly pick a subscriber among the valid offers, which guarantees that work is distributed evenly
     * among the client subscribers, asymptotically.
     */

    private static final Random SUBSCRIBER_RESOLVER = new Random();

    /**
     * The negotiation period after receipt of the first valid offer during which other valid offers to deliver formats 
     * will be considered. The period is in milliseconds and adds latency, so must be kept short. See #82939-27.
     */

    private static final int NEGOTIATION_PERIOD = 100;

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
     * A set of identifiers for subscribers that have already been negotiated. These subscribers are created and 
     * registered by the core evaluation client, as distinct from the {@link #negotiatedSubscribers}.
     */

    private final Set<String> expectedSubscribers;

    /**
     * A negotiated subscriber for each format type that is not delivered by the core evaluation client.
     */

    @GuardedBy( "negotiatedSubscribersLock" )
    private final Map<Format, String> negotiatedSubscribers;

    /**
     * A map of subscription offers (by subscriber identifier) for each format.
     */

    private final Map<Format, Set<String>> subscriptionOffers;

    /**
     * A mutation lock on the negotiated subscribers. If a subscriber succeeds for one format offered, it should 
     * succeed for all formats offered that do not currently have subscriptions. Locking the map of subscriptions
     * achieves this.
     */

    private final Object negotiatedSubscribersLock = new Object();

    /**
     * A latch with a count equal to the number of formats delivered by negotiated subscribers. Use this latch to 
     * await the completion of a negotiation for all formats. As each format is negotiated, the latch is counted down.
     */

    private final TimedCountDownLatch negotiatedSubscribersLatch;

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
     * The timeout for all subscribers to be negotiated.
     */

    private final long timeoutDuringNegotiation;

    /**
     * The units for the timeout associated with the negotiation of all subscriptions.
     */

    private final TimeUnit timeoutDuringNegotiationUnits;

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
            case GROUP_PUBLICATION_COMPLETE:
                this.registerGroupPublicationComplete( message );
                break;
            case GROUP_CONSUMPTION_COMPLETE:
                this.registerGroupConsumptionComplete( message );
                break;
            case EVALUATION_COMPLETE_REPORTED_FAILURE:
                this.stopOnFailure( message );
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
        
        String consumerId = failure.getConsumer()
                                   .getConsumerId();

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
        
        // Stop any flow control blocking
        this.evaluation.stopFlowControl();
    }

    /**
     * Wait until publication and consumption has completed.
     * 
     * @return the exit code. A non-zero exit code corresponds to failure
     * @throws InterruptedException if the evaluation was interrupted.
     */

    int await() throws InterruptedException
    {
        LOGGER.debug( "While processing evaluation {}, awaiting confirmation that publication has completed.",
                      this.evaluation.getEvaluationId() );

        this.publicationLatch.await();

        String evaluationId = this.evaluation.getEvaluationId();

        LOGGER.debug( "While processing evaluation {}, received confirmation that publication has completed.",
                      evaluationId );

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
            Duration periodToWait = Duration.of( this.timeoutDuringConsumption,
                                                 ChronoUnit.valueOf( this.timeoutDuringConsumptionUnits.name() ) );
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
            nextLatch.await( this.timeoutDuringConsumption, this.timeoutDuringConsumptionUnits );

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
                                                   + this.timeoutDuringConsumption
                                                   + " "
                                                   + this.timeoutDuringConsumptionUnits
                                                   + ": "
                                                   + identifiers
                                                   + ". Subscribers should report their status regularly, in "
                                                   + "order to reset the timeout period. " );
        }

        return this.exitCode.get();
    }

    /**
     * Awaits the conclusion of negotiations for output formats that are not delivered by the core client. The 
     * negotiation is concluded when all formats have been negotiated or the timeout is reached.
     * 
     * @throws InterruptedException if the evaluation was interrupted.
     */

    void awaitNegotiatedSubscribers() throws InterruptedException
    {
        // Nothing to await
        if ( this.negotiatedSubscribers.isEmpty() )
        {
            return;
        }

        String evaluationId = this.evaluation.getEvaluationId();

        LOGGER.info( "While processing evaluation {}, awaiting the negotiation of subscribers for output formats {}...",
                     evaluationId,
                     this.negotiatedSubscribers.keySet() );

        // Add a timer task that sends a notification that the evaluation is ready to publish
        EvaluationStatusTracker tracker = this;
        TimerTask updater = new TimerTask()
        {
            @Override
            public void run()
            {
                tracker.notifyConsumerRequired();
            }
        };

        Timer timer = new Timer();

        // Send with zero initial delay to avoid latency with awaiting subscriber
        timer.schedule( updater, 0, EvaluationStatusTracker.READY_TO_RECEIVE_CONSUMERS_UPDATE_FREQUENCY );

        // Wait for a fixed period unless there is progress
        this.negotiatedSubscribersLatch.await( this.timeoutDuringNegotiation, this.timeoutDuringNegotiationUnits );

        timer.cancel();

        // Throw an exception if some required subscriptions could not be negotiated
        if ( this.negotiatedSubscribersLatch.getCount() != 0 )
        {
            Duration timeout = Duration.of( this.timeoutDuringNegotiation,
                                            ChronoUnit.valueOf( this.timeoutDuringNegotiationUnits.name() ) );

            Set<Format> failedFormats = this.getFormatsAwaitingSubscribers();

            throw new SubscriberTimedOutException( "Failed to negotiate all subscriptions within the timeout period "
                                                   + "of "
                                                   + timeout
                                                   + ". Subscribers could not be negotiated for the following output "
                                                   + "formats: "
                                                   + failedFormats );
        }


        // At least one valid offer has been received, but wait a minimum period for alternative offers to accrue.
        // This small latency increases the probability of a fair competition between competing subscribers because 
        // subscribers that register with the topic exchange earlier may receive messages sooner, even though all 
        // competing subscribers should be treated equally. This should ensure work is distributed roughly evenly
        // across subscribers that offer within the short negotiation period. See #82939-27.
        Thread.sleep( EvaluationStatusTracker.NEGOTIATION_PERIOD );

        // Choose between competing offers
        this.chooseBetweenOffers( this.negotiatedSubscribers, this.subscriptionOffers );

        LOGGER.info( "While processing evaluation {}, received confirmation that all required subscriptions have "
                     + "been negotiated. The negotiated subscriptions are {}.",
                     evaluationId,
                     this.negotiatedSubscribers );
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
     * @return a set of negotiated subscribers by format.
     */

    Map<Format, String> getNegotiatedSubscribers()
    {
        return Collections.unmodifiableMap( this.negotiatedSubscribers );
    }

    /**
     * Registers a new consumer as ready to consume.
     * 
     * @param message the status message containing the subscriber event
     */

    private void registerSubscriberReady( EvaluationStatus message )
    {
        // Attempt to fulfil a contract with this subscriber
        this.attemptToNegotiateSubscriber( message );

        String consumerId = message.getConsumer()
                                   .getConsumerId();

        // Must be an expected subscriber, not a negotiated one because the negotiated subscribers are provisional until 
        // the negotiation period has expired.
        boolean isexpectedSubscriber = this.expectedSubscribers.contains( consumerId );

        // A real consumer that is not a subscriber just negotiated
        if ( isexpectedSubscriber )
        {
            LOGGER.debug( "Registering a pre-notified message subscriber {} for evaluation {} as {}.",
                          consumerId,
                          this.evaluation.getEvaluationId(),
                          message.getCompletionStatus() );

            // Reset the countdown
            TimedCountDownLatch latch = this.getSubscriberLatch( consumerId );

            latch.resetClock();

            LOGGER.debug( "Registered a pre-notified message subscriber {} for evaluation {} as {}.",
                          consumerId,
                          this.evaluation.getEvaluationId(),
                          message.getCompletionStatus() );
        }
    }

    /**
     * Notifies any listening subscribers that the evaluation is ready to receive offers of subscriptions for 
     * any of the prescribed formats required.
     */

    private void notifyConsumerRequired()
    {
        Set<Format> formatsRequired = this.getFormatsAwaitingSubscribers();

        if ( !formatsRequired.isEmpty() )
        {
            EvaluationStatus readyForSubs = EvaluationStatus.newBuilder()
                                                            .setCompletionStatus( CompletionStatus.CONSUMER_REQUIRED )
                                                            .addAllFormatsRequired( formatsRequired )
                                                            .build();

            this.evaluation.publish( readyForSubs );
        }
    }

    /**
     * Attempts to negotiate a subscription for one of the formats required by this evaluation.
     * 
     * @param message the message with the subscription information
     */

    private void attemptToNegotiateSubscriber( EvaluationStatus message )
    {
        String evaluationId = this.evaluation.getEvaluationId();

        // If the consumer details are not present, then warn and return
        if ( !message.hasConsumer() )
        {
            LOGGER.warn( "While negotiating evaluation {}, encountered an evaluation status message from a consumer "
                         + "with status {} but whose consumer description was missing. Cannot negotiate with this "
                         + "consumer, so ignoring it.",
                         evaluationId,
                         message.getCompletionStatus() );

            return;
        }

        // If the consumerId is already valid in this context, then warn and return
        String consumerId = message.getConsumer()
                                   .getConsumerId();

        if ( this.validateConsumerId( consumerId ) )
        {
            LOGGER.debug( "While negotiating evaluation {}, encountered an evaluation status message from consumer "
                          + "{} with status {}. The consumer has already been obligated for this evaluation and "
                          + "will not be considered further.",
                          evaluationId,
                          consumerId,
                          message.getCompletionStatus() );

            return;
        }

        // Lock acquired, so if it succeeds for one it succeeds for all open formats that are offered
        List<Format> formatsOffered = message.getConsumer()
                                             .getFormatsList();

        // If the consumer cannot satisfy any outstanding formats then log and return
        Set<Format> formatsRequired = this.negotiatedSubscribers.keySet();

        // Log the offer
        if ( !formatsRequired.isEmpty() )
        {
            Set<Format> formatsAwaited = this.getFormatsAwaitingSubscribers();
            Set<Format> formatsAwaitedThatAreOffered = new HashSet<>( formatsAwaited );
            Set<Format> formatsRequiredThatAreOffered = new HashSet<>( formatsRequired );
            formatsAwaitedThatAreOffered.retainAll( formatsOffered );
            formatsRequiredThatAreOffered.retainAll( formatsOffered );

            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "While negotiating evaluation {}, encountered an evaluation status message from consumer {} "
                              + "with status {} offering to deliver formats. Formats required: {}. Formats offered: {}. "
                              + "Formats awaited: {}",
                              evaluationId,
                              consumerId,
                              message.getCompletionStatus(),
                              formatsRequired,
                              formatsOffered,
                              formatsAwaited );
            }

            // Record the offer
            this.recordAnOffer( formatsAwaitedThatAreOffered, formatsRequiredThatAreOffered, consumerId );
        }
    }

    /**
     * Records a subscription offer for the prescribed formats.
     * @param formatsAwaitedThatAreOffered the formats required for which subscribers have not yet been identified
     * @param formatsRequiredThatAreOffered the formats required that are also offered by the consumer
     * @param consumerId the consumer identifier
     */

    private void recordAnOffer( Set<Format> formatsAwaitedThatAreOffered,
                                Set<Format> formatsRequiredThatAreOffered,
                                String consumerId )
    {
        // For each format supported by the consumer that is also one of the required formats, attempt to add
        // the subscription. If it succeeds, then report the subscription as having succeeded. If the subscriber
        // succeeds for one of the formats offered, it should succeed for all open formats offered by the 
        // subscriber, otherwise evaluations will be distributed across subscribers that do the same work. This is 
        // achieved by locking the whole map to mutation by one thread. An underlying assumption here is that each
        // competing subscriber has a symmetric offer. See #82939-8 and #82939-9. Success here is only provisional
        // because a window of time is allowed for other offers to accumulate, in order to ensure a fair 
        // competition. See #82939-27.
        Set<String> offers = new HashSet<>();
        offers.add( consumerId );
        for ( Format next : formatsRequiredThatAreOffered )
        {
            Set<String> added = this.subscriptionOffers.putIfAbsent( next, offers );
            // Already exists?
            if ( Objects.nonNull( added ) )
            {
                added.add( consumerId );
            }
        }

        // Determine whether an offer can meet the contract
        synchronized ( this.negotiatedSubscribersLock )
        {
            boolean won = false;
            for ( Format next : formatsAwaitedThatAreOffered )
            {
                won = this.negotiatedSubscribers.replace( next, StringUtils.EMPTY, consumerId );
            }

            // Success
            if ( won )
            {
                // Countdown the negotiation latch for each format offered and won, noting here that any win means
                // all win and a win cannot happen in another thread because this thread has the mutex lock
                formatsAwaitedThatAreOffered.forEach( next -> this.negotiatedSubscribersLatch.countDown() );

                LOGGER.debug( "While negotiating evaluation {}, subscriber {} was the first to offer formats {}.",
                              this.evaluation.getEvaluationId(),
                              consumerId,
                              formatsAwaitedThatAreOffered );
            }
        }
    }

    /**
     * Choose one of the viable subscription offers for each required format. If an offer has been provisionally 
     * accepted, allow for it to be replaced by one of the offered candidates in order to ensure a fair competition and,
     * thus, an even distribution of work across subscriber clients.
     * See #82939-27.
     * 
     * @param accepted the map of provisionally accepted subscribers
     * @param offered the offered subscribers
     */

    private void chooseBetweenOffers( Map<Format, String> accepted, Map<Format, Set<String>> offered )
    {
        // Iterate through the formats
        Set<Format> acceptedFormats = new HashSet<>( accepted.keySet() );
        Set<String> replaced = new HashSet<>();
        for ( Format nextAccepted : acceptedFormats )
        {
            List<String> consumers = new ArrayList<>( offered.get( nextAccepted ) );
            String existingConsumerId = accepted.get( nextAccepted );

            // Pick a consumer when there are some to pick from and the existing consumer wasn't already picked
            // for another format by this method (and thus used across all formats offered).
            if ( !consumers.isEmpty() && !replaced.contains( existingConsumerId ) )
            {
                // Find a new consumer id
                int size = consumers.size();
                int pick = SUBSCRIBER_RESOLVER.nextInt( size );
                String newConsumerId = consumers.get( pick );

                // Replace. Is the new consumer id different than the old one?
                existingConsumerId = accepted.replace( nextAccepted, newConsumerId );

                // Replace all instances of the old references with the new one if the winning subscriber offers 
                // multiple formats that are required.
                if ( Objects.nonNull( existingConsumerId ) )
                {
                    replaced.add( newConsumerId );

                    for ( Format next : acceptedFormats )
                    {
                        accepted.replace( next, existingConsumerId, newConsumerId );
                    }
                }

                // Add a consumption latch for the new consumer.
                this.subscriberLatches.put( newConsumerId, new TimedCountDownLatch( 1 ) );
            }
        }
    }

    /**
     * Returns the formats to be negotiated.
     * 
     * @return the formats awaiting subscribers
     */

    private Set<Format> getFormatsAwaitingSubscribers()
    {
        return this.negotiatedSubscribers.entrySet()
                                         .stream()
                                         .filter( next -> next.getValue().equals( StringUtils.EMPTY ) )
                                         .map( Map.Entry::getKey )
                                         .collect( Collectors.toUnmodifiableSet() );
    }

    /**
     * Registers a subscriber as completed successfully.
     * 
     * @param message the status message containing the subscriber event
     * @throws IllegalStateException if the consumer has already been marked as failed
     */

    private void registerSubscriberCompleteReportedSuccess( EvaluationStatus message )
    {
        String consumerId = message.getConsumer()
                                   .getConsumerId();
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
        String consumerId = message.getConsumer()
                                   .getConsumerId();
        boolean isBeingTracked = this.validateConsumerId( consumerId );

        // A real consumer
        if ( isBeingTracked )
        {
            // Reset the countdown
            this.getSubscriberLatch( consumerId )
                .resetClock();

            LOGGER.debug( "Message subscriber {} for evaluation {} reports {}.",
                          consumerId,
                          this.evaluation.getEvaluationId(),
                          message.getCompletionStatus() );
        }
    }

    /**
     * Registers the publication of a message group complete.
     * 
     * TODO: Add the group identity to the message and move the group flow control logic to the 
     * {@link GroupCompletionTracker}, which can be constructed with the {@link Evaluation} and passed to this 
     * instance and then called from here.
     * 
     * @param message the status message containing the status event
     */

    private void registerGroupPublicationComplete( EvaluationStatus message )
    {
        LOGGER.debug( "Evaluation {} reports {}. Engaging producer flow control until the same or a different message "
                      + "group acknowledges consumption complete.",
                      this.evaluation.getEvaluationId(),
                      message.getCompletionStatus() );

        this.evaluation.startFlowControl();
    }

    /**
     * Registers the publication of a message group complete.
     * 
     * TODO: Add the group identity to the message and move the group flow control logic to the 
     * {@link GroupCompletionTracker}, which can be constructed with the {@link Evaluation} and passed to this 
     * instance and then called from here.
     * 
     * @param message the status message containing the status event
     */

    private void registerGroupConsumptionComplete( EvaluationStatus message )
    {
        LOGGER.debug( "Evaluation {} reports {}. Disengaging producer flow control.",
                      this.evaluation.getEvaluationId(),
                      message.getCompletionStatus() );

        this.evaluation.stopFlowControl();
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

        // Not this consumer and either one of the expected subscribers or a negotiated subscriber
        return !this.isThisConsumerMe( consumerId )
               && ( this.expectedSubscribers.contains( consumerId ) || this.isNegotiatedSubscriber( consumerId ) );
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
     * @param expectedSubscribers the list of unique identifiers for subscribers that have already been negotiated
     * @param formatsAwaited the formats to be delivered by subscribers yet to be negotiated
     * @param myConsumerId the consumer identifier of this instance, used to ignore messages related to the tracker
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the list of subscribers is empty
     */

    EvaluationStatusTracker( Evaluation evaluation,
                             Set<String> expectedSubscribers,
                             Set<Format> formatsAwaited,
                             String myConsumerId )
    {
        Objects.requireNonNull( evaluation );
        Objects.requireNonNull( expectedSubscribers );
        Objects.requireNonNull( formatsAwaited );
        Objects.requireNonNull( myConsumerId );

        if ( expectedSubscribers.isEmpty() )
        {
            throw new IllegalArgumentException( "Expected one or more subscribers when building evaluation "
                                                + evaluation.getEvaluationId()
                                                + " but found none." );
        }

        this.evaluation = evaluation;
        this.success = ConcurrentHashMap.newKeySet();
        this.failure = ConcurrentHashMap.newKeySet();
        this.expectedSubscribers = expectedSubscribers;
        this.negotiatedSubscribers = new EnumMap<>( Format.class );
        this.subscriptionOffers = new ConcurrentHashMap<>();

        // Add a placeholder entry for each unknown subscriber
        formatsAwaited.forEach( nextFormat -> this.negotiatedSubscribers.put( nextFormat, StringUtils.EMPTY ) );

        LOGGER.info( "Registering the following message subscribers for evaluation {}: {}. The following output "
                     + "formats will be delivered by subscribers that are yet to be negotiated: {}.",
                     this.evaluation.getEvaluationId(),
                     this.expectedSubscribers,
                     formatsAwaited );

        // Default timeout for consumption from an individual consumer unless progress is reported
        // In practice, this is extremely lenient
        this.timeoutDuringConsumption = 120;
        this.timeoutDuringConsumptionUnits = TimeUnit.MINUTES;

        // Timeout period for the negotiation of all subscriptions. This is intentionally much shorter than the timeout
        // during consumption because it is much easier to resubmit an evaluation when subscribers are down for a short
        // period than to redo an evaluation whose production has been completed.
        this.timeoutDuringNegotiation = 5;
        this.timeoutDuringNegotiationUnits = TimeUnit.MINUTES;

        // Create the latches
        Map<String, TimedCountDownLatch> internalLatches = new ConcurrentHashMap<>( this.expectedSubscribers.size() );
        this.expectedSubscribers.forEach( next -> internalLatches.put( next, new TimedCountDownLatch( 1 ) ) );

        // Mutable because some subscriptions may be negotiated and hence latches added upon successful negotiation
        this.subscriberLatches = internalLatches;

        // A latch whose count equals the number of formats to be delivered by external subscribers
        this.negotiatedSubscribersLatch = new TimedCountDownLatch( this.negotiatedSubscribers.size() );
        this.myConsumerId = myConsumerId;
        this.resourcesWritten = new HashSet<>();
    }
}
