package wres.events;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jcip.annotations.ThreadSafe;
import wres.statistics.generated.Consumer.Format;
import wres.events.subscribe.SubscriberApprover;
import wres.events.subscribe.SubscriberTimedOutException;
import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.EvaluationStatus.CompletionStatus;

/**
 * A class that negotiates subscriptions that deliver formats for an evaluation.
 * 
 * @author james.brown@hydrosolved.com
 */

@ThreadSafe
class SubscriberNegotiator
{

    private static final String WHILE_COMPLETING_EVALUATION = "While completing evaluation ";

    private static final Logger LOGGER = LoggerFactory.getLogger( SubscriberNegotiator.class );

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
     * A map of subscription offers (by subscriber identifier) for each format.
     */

    private final Map<Format, Set<String>> subscriptionOffers;

    /**
     * The set of formats required by the evaluation.
     */

    private final Set<Format> formatsRequired;

    /**
     * One latch for each format required. The count of each latch is initially one. When a subscriber arrives that can
     * deliver the format, the corresponding latch is counted down.
     */

    private final Map<Format, TimedCountDownLatch> formatNegotiationLatches;

    /**
     * The timeout for all subscribers to be negotiated.
     */

    private final long timeoutDuringNegotiation;

    /**
     * The units for the timeout associated with the negotiation of all subscriptions.
     */

    private final TimeUnit timeoutDuringNegotiationUnits;

    /**
     * The evaluation.
     */

    private final Evaluation evaluation;

    /**
     * Determines whether subscription offers from format writers are viable. 
     */

    private final SubscriberApprover subscriberApprover;

    /**
     * Negotiates a subscriber for each required output format.
     * 
     * @return a map of negotiated subscribers by format
     * @throws InterruptedException if the negotiation of subscriptions was interrupted
     * @throws SubscriberTimedOutException if subscriptions could not be notified within the timeout period
     * @throws EvaluationEventException if the negotiation failed for any other reason
     */

    Map<Format, String> negotiateSubscribers() throws InterruptedException
    {
        LOGGER.info( "While processing evaluation {}, awaiting the negotiation of subscribers for output formats "
                     + "{}...",
                     this.getEvaluationId(),
                     this.getFormatsAwaitingSubscribers() );

        SubscriberNegotiator negotiator = this;

        AtomicReference<EvaluationEventException> negotiationFailed = new AtomicReference<>();
        TimerTask updater = new TimerTask()
        {
            @Override
            public void run()
            {
                try
                {
                    negotiator.notifyConsumerRequired();
                }
                catch ( EvaluationEventException e )
                {
                    // Set the exception and free the latches on subscribers so that the negotiation can fail in an 
                    // orderly way. This updater runs async in a timer task, so any exception might not be set until
                    // the format negotiation latches have been triggered, awaiting success or failure.
                    negotiationFailed.set( e );
                    negotiator.formatNegotiationLatches.forEach( ( a, b ) -> b.countDown() );
                }
            }
        };

        Timer timer = new Timer();

        // Send with zero initial delay to avoid latency with awaiting subscriber
        timer.schedule( updater, 0, SubscriberNegotiator.READY_TO_RECEIVE_CONSUMERS_UPDATE_FREQUENCY );

        // Wait for each format to receive an offer
        for ( Map.Entry<Format, TimedCountDownLatch> next : this.formatNegotiationLatches.entrySet() )
        {
            Format format = next.getKey();
            LOGGER.debug( "Awaiting a format writer for {}.", format );
            TimedCountDownLatch latch = next.getValue();
            latch.await( this.timeoutDuringNegotiation, this.timeoutDuringNegotiationUnits );

            // Only continue to wait for other formats if negotiation succeeded for the current format
            if ( latch.timedOut() )
            {
                LOGGER.debug( "Timed out awaiting format {}. The negotiation has failed.", format );
                break;
            }
        }

        timer.cancel();

        // Negotiation failed completely?
        if ( Objects.nonNull( negotiationFailed.get() ) )
        {
            throw new EvaluationEventException( WHILE_COMPLETING_EVALUATION + this.getEvaluationId()
                                                + ", failed to notify subscribers of the need for "
                                                + "format writers.",
                                                negotiationFailed.get() );
        }

        // Throw an exception if some formats have no offers
        if ( this.isAwaitingSubscribers() )
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
        Thread.sleep( SubscriberNegotiator.NEGOTIATION_PERIOD );

        // Choose between competing offers. If other offers arrive after this point, they are ignored
        Map<Format, Set<String>> offers = Collections.unmodifiableMap( this.subscriptionOffers );

        LOGGER.info( "While processing evaluation {}, received the following subscription offers by format within the "
                     + "subscription negotiation window: {}.",
                     this.getEvaluationId(),
                     offers );

        Map<Format, String> negotiatedSubscribers = this.chooseASubscriberForEachFormatRequired( offers );

        LOGGER.info( "While processing evaluation {}, received confirmation that all required subscriptions have "
                     + "been negotiated. The negotiated subscriptions are {}.",
                     this.getEvaluationId(),
                     negotiatedSubscribers );

        return negotiatedSubscribers;
    }

    /**
     * Registers an offer from a subscriber to deliver one or more formats required by the evaluation being negotiated.
     * 
     * @param message the message with the subscription information
     * @throws NullPointerException if the message is null
     * @throws EvaluationEventException if the status message containers a blank consumer identifier
     */

    void registerAnOfferToDeliverFormats( EvaluationStatus message )
    {
        Objects.requireNonNull( message );

        // If the completion status is unexpected, warn and return
        if ( message.getCompletionStatus() != CompletionStatus.READY_TO_CONSUME )
        {
            LOGGER.warn( "While negotiating evaluation {}, encountered an evaluation status message from a subscriber "
                         + "with status {}. Expected a completion status of {}. Cannot negotiate with this subscriber, "
                         + "so ignoring it.",
                         this.getEvaluationId(),
                         message.getCompletionStatus(),
                         CompletionStatus.READY_TO_CONSUME );

            return;
        }

        // If the consumer details are not present, then warn and return
        if ( !message.hasConsumer() )
        {
            LOGGER.warn( "While negotiating evaluation {}, encountered an evaluation status message from a subscriber "
                         + "with status {} but whose consumer description was missing. Cannot negotiate with this "
                         + "subscriber, so ignoring it.",
                         this.getEvaluationId(),
                         message.getCompletionStatus() );

            return;
        }

        // If the consumerId is already valid in this context, then return
        String consumerId = message.getConsumer()
                                   .getConsumerId();

        if ( consumerId.isBlank() )
        {
            throw new EvaluationEventException( "While awaiting consumption for evaluation "
                                                + this.getEvaluationId()
                                                + " received a message about a subscriber event that did not "
                                                + "contain the consumerId, which is not allowed." );
        }

        // Already received an offer from this subscriber?
        if ( this.subscriptionOffers.values()
                                    .stream()
                                    .flatMap( Set::stream )
                                    .anyMatch( consumerId::equals ) )
        {
            LOGGER.debug( "While negotiating evaluation {}, encountered an evaluation status message from subscriber "
                          + "{} with status {}. The subscriber has already been offered for this evaluation and "
                          + "will not be considered further.",
                          this.getEvaluationId(),
                          consumerId,
                          message.getCompletionStatus() );

            return;
        }

        // All formats offered by the consumer
        List<Format> formatsList = message.getConsumer()
                                          .getFormatsList();

        // Subscriber must be pre-approved for at least one format it offers: see #88262 and #88267.
        if ( formatsList.stream().noneMatch( next -> this.subscriberApprover.isApproved( next, consumerId ) ) )
        {
            LOGGER.debug( "While negotiating evaluation {}, encountered an evaluation status message from subscriber "
                          + "{} with status {}. The subscriber offered formats {}, but the offer was rejected because "
                          + "the subscriber was not approved to deliver any of these formats for this evaluation.",
                          this.getEvaluationId(),
                          consumerId,
                          message.getCompletionStatus(),
                          formatsList );

            return;
        }

        // The set of formats offered by the consumer that are also required by the evaluation
        Set<Format> formatsOfferedByConsumer = new HashSet<>( this.formatsRequired );
        formatsOfferedByConsumer.retainAll( formatsList );

        if ( LOGGER.isInfoEnabled() )
        {
            LOGGER.info( "While negotiating evaluation {}, encountered an evaluation status message from subscriber {} "
                         + "with status {} offering to deliver formats. Formats required: {}. Formats offered: {}. "
                         + "Formats offered that are required: {}",
                         this.getEvaluationId(),
                         consumerId,
                         message.getCompletionStatus(),
                         this.formatsRequired,
                         formatsList,
                         formatsOfferedByConsumer );
        }

        // Register the offer
        for ( Format next : formatsOfferedByConsumer )
        {
            // Approved subscriber for this format?
            // Yes. See #88262 and #88267.
            if ( this.subscriberApprover.isApproved( next, consumerId ) )
            {
                Set<String> offers = new HashSet<>();
                offers.add( consumerId );
                Set<String> added = this.subscriptionOffers.putIfAbsent( next, offers );

                // Already exists?
                if ( Objects.nonNull( added ) )
                {
                    added.add( consumerId );
                }

                TimedCountDownLatch latch = this.formatNegotiationLatches.get( next );
                latch.countDown();
            }
            // No.
            else if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "While negotiating evaluation {}, encountered a subscriber {} with an offer to deliver "
                              + "format {}. This offer was rejected because the subscriber was not approved to deliver "
                              + "this format for evaluation {}.",
                              this.getEvaluationId(),
                              consumerId,
                              next,
                              this.getEvaluationId() );
            }
        }

        // Report on formats still awaiting subscribers
        Set<Format> formatsAwaited = this.getFormatsAwaitingSubscribers();

        if ( !formatsAwaited.isEmpty() && LOGGER.isInfoEnabled() )
        {
            LOGGER.info( "While processing evaluation {}, awaiting the negotiation of subscribers for output "
                         + "formats {}...",
                         this.getEvaluationId(),
                         formatsAwaited );
        }
    }

    /**
     * Stops the negotiation by counting down all format negotiation latches.
     */

    void stopNegotiation()
    {
        this.formatNegotiationLatches.values()
                                     .forEach( TimedCountDownLatch::countDown );
    }

    /**
     * @return the evaluation identifier of the evaluation whose status is being tracked
     */

    private String getEvaluationId()
    {
        return this.evaluation.getEvaluationId();
    }

    /**
     * @return true if the evaluation is awaiting subscribers for one or more formats
     */

    private boolean isAwaitingSubscribers()
    {
        return !this.getFormatsAwaitingSubscribers()
                    .isEmpty();
    }

    /**
     * Choose one of the viable subscription offers for each required format. See #82939-27.
     * 
     * @param offered the offered subscribers
     */

    private Map<Format, String> chooseASubscriberForEachFormatRequired( Map<Format, Set<String>> offered )
    {
        Map<Format, String> formatsAgreed = new EnumMap<>( Format.class );

        // Iterate through the required formats and choose a subscriber for each one
        for ( Format nextFormat : this.formatsRequired )
        {
            // Not already agreed
            if ( !formatsAgreed.containsKey( nextFormat ) )
            {
                // Get the subscribers with equally good offers for this format
                Set<String> offerSet = this.getSubscribersWithBestOffer( nextFormat, offered );
                // Create a list to facilitate picking
                List<String> bestOffers = List.copyOf( offerSet );

                // Pick one randomly among the equally good offers. This ensures a fair competition among subscribers
                // that registered their offers within the subscription window. See #82939-27.
                int size = bestOffers.size();
                int pick = SubscriberNegotiator.SUBSCRIBER_RESOLVER.nextInt( size );
                String bestOffer = bestOffers.get( pick );

                // Get the formats delivered by this subscriber
                Set<Format> formats = this.getFormatsOfferedBySubscriber( bestOffer, offered );

                // Accept the best offer for each format it offers
                formats.forEach( next -> formatsAgreed.put( next, bestOffer ) );
            }
        }

        return Collections.unmodifiableMap( formatsAgreed );
    }

    /**
     * Returns a set of subscriber identifiers that provide an equally good offer for the specified format. An offer is
     * equally good if the subscriber delivers the largest number of required formats, including the specified format. 
     * In other words, this method embeds a particular definition of subscriber "goodness". An alternative definition 
     * would consider all valid offers equal; this would allow a broader distribution of work across offers, but would 
     * require more duplication of work, specifically message deserialization work (since every subscriber must 
     * deserialize messages before routing them to format writers). When N subscribers provide symmetric offers, then 
     * all of them are equally good.
     * 
     * @param format the format required
     * @param offered the subscribers with format offers
     * @return the subscriber identifiers of the equally good offers to deliver the required format
     * @throws IllegalArgumentException if the required format is not present among the offers
     */

    private Set<String> getSubscribersWithBestOffer( Format format, Map<Format, Set<String>> offered )
    {
        if ( !offered.containsKey( format ) )
        {
            throw new IllegalArgumentException( "The format " + format + " was not offered by any subscriber." );
        }

        // The possible subscribers 
        Set<String> possibilities = offered.get( format );

        Set<String> returnMe = new HashSet<>();

        // For each subscriber, find the number of formats it offers, including the required format, and then map the 
        // subscribers to their format counts
        Map<Long, Set<String>> map = offered.values()
                                            .stream()
                                            .flatMap( Set::stream )
                                            // Only consider subscribers that offer the required format
                                            .filter( possibilities::contains )
                                            // Find the total number of formats offered per subscriber. This will 
                                            // produce a map of (subscriberId,formatCount)
                                            .collect( Collectors.groupingBy( Function.identity(),
                                                                             Collectors.counting() ) )
                                            // Iterate the map
                                            .entrySet()
                                            .stream()
                                            // Group by the number of formats offered. This will produce a map of
                                            // (formatCount,{subscriberId_1,...,subscriberId_N})
                                            .collect( Collectors.groupingBy( Map.Entry::getValue,
                                                                             Collectors.mapping( Map.Entry::getKey,
                                                                                                 Collectors.toSet() ) ) );

        // Return the subscribers that offer the most formats. If there is more than one, then they are "equally good" 
        // and a fair competition will pick one at random
        Optional<Map.Entry<Long, Set<String>>> max = map.entrySet()
                                                        .stream()
                                                        .max( ( a, b ) -> a.getKey().compareTo( b.getKey() ) );

        // Must be at least one - see exception on entry - but guard nonetheless
        if ( max.isPresent() )
        {
            returnMe.addAll( max.get().getValue() );
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Returns the formats offered by the subscriber.
     * @param subscriberId the subscriber identifier
     * @param offered the formats offered by all subscribers
     * @return the formats offered by the specified subscriber
     */

    private Set<Format> getFormatsOfferedBySubscriber( String subscriberId, Map<Format, Set<String>> offered )
    {
        return offered.entrySet()
                      .stream()
                      .filter( next -> next.getValue().contains( subscriberId ) )
                      .map( Map.Entry::getKey )
                      .collect( Collectors.toSet() );
    }

    /**
     * Returns the formats to be negotiated.
     * 
     * @return the formats awaiting subscribers
     */

    private Set<Format> getFormatsAwaitingSubscribers()
    {
        return this.formatNegotiationLatches.entrySet()
                                            .stream()
                                            .filter( next -> next.getValue().getCount() > 0 )
                                            .map( Map.Entry::getKey )
                                            .collect( Collectors.toUnmodifiableSet() );
    }

    /**
     * Notifies any listening subscribers that the evaluation is ready to receive offers of subscriptions for 
     * any of the prescribed formats required.
     */

    private void notifyConsumerRequired()
    {
        Set<Format> formatsAwaited = this.getFormatsAwaitingSubscribers();

        if ( !formatsAwaited.isEmpty() )
        {
            EvaluationStatus readyForSubs = EvaluationStatus.newBuilder()
                                                            .setClientId( this.evaluation.getClientId() )
                                                            .setCompletionStatus( CompletionStatus.CONSUMER_REQUIRED )
                                                            .addAllFormatsRequired( formatsRequired )
                                                            .build();

            this.evaluation.publish( readyForSubs );
        }
    }

    /**
     * Create an instance with an evaluation and an expected list of subscriber identifiers.
     * @param evaluation the evaluation
     * @param formatsRequired the formats to be delivered by subscribers yet to be negotiated
     * @param subscriberApprover determines whether subscription offers from format writers are approved for negotiation
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the set of formats is empty or the maximum number of retries is < 0
     */

    SubscriberNegotiator( Evaluation evaluation,
                          Set<Format> formatsRequired,
                          SubscriberApprover subscriberApprover )
    {
        Objects.requireNonNull( evaluation );
        Objects.requireNonNull( formatsRequired );
        Objects.requireNonNull( subscriberApprover );

        if ( formatsRequired.isEmpty() )
        {
            throw new IllegalArgumentException( "Cannot build a subscriber negotiator for evaluation "
                                                + evaluation.getEvaluationId()
                                                + " because there are no formats to be delivered by format "
                                                + "subscribers." );
        }

        this.subscriptionOffers = new ConcurrentHashMap<>();
        this.formatsRequired = formatsRequired;
        this.subscriberApprover = subscriberApprover;
        this.evaluation = evaluation;

        LOGGER.info( "The following output formats for evaluation {} will be delivered by subscribers that are yet to "
                     + "be negotiated: {}.",
                     this.getEvaluationId(),
                     formatsRequired );

        // Timeout period for the negotiation of all subscriptions. This is intentionally much shorter than the timeout
        // during consumption because it is much easier to resubmit an evaluation when subscribers are down for a short
        // period than to redo an evaluation whose production has been completed. TODO: expose this to configuration.
        this.timeoutDuringNegotiation = 5;
        this.timeoutDuringNegotiationUnits = TimeUnit.MINUTES;

        // A latch for each format to be negotiated
        this.formatNegotiationLatches = new EnumMap<>( Format.class );
        this.formatsRequired.forEach( next -> this.formatNegotiationLatches.put( next, new TimedCountDownLatch( 1 ) ) );
    }

}
