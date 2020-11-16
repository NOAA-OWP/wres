package wres.events;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.Topic;
import javax.naming.NamingException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ProtocolStringList;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;
import wres.statistics.generated.Consumer.Format;
import wres.events.publish.MessagePublisher.MessageProperty;
import wres.events.subscribe.SubscriberTimedOutException;
import wres.eventsbroker.BrokerConnectionFactory;
import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.EvaluationStatus.CompletionStatus;

/**
 * A consumer that tracks the status of an evaluation via its {@link EvaluationStatus} messages and awaits its 
 * completion upon request ({@link #await()}).
 * 
 * @author james.brown@hydrosolved.com
 */

@ThreadSafe
class EvaluationStatusTracker implements Closeable
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
     * Maximum number of retries allowed.
     */

    private final int maximumRetries;

    /**
     * Actual number of retries attempted.
     */

    private final AtomicInteger retriesAttempted;

    /**
     * A message that could not be consumed by this status tracker, null if no failure.
     */

    private Message trackerFailedOn = null;

    /**
     * Is <code>true</code> if the subscriber failed after all attempts to recover.
     */

    private final AtomicBoolean isFailedUnrecoverably;

    /**
     * A latch that records the completion of publication. There is no timeout on publication.
     */

    private final CountDownLatch publicationLatch = new CountDownLatch( 1 );

    /**
     * A latch for each top-level subscriber by subscriber identifier. A top-level subscriber collates one or more 
     * consumers. Once set, this latch is counted down for each subscriber that reports completion. Each latch is 
     * initialized with a count of one. Consumption is allowed to timeout when no progress is recorded within a 
     * prescribed duration.
     */

    private final Map<String, TimedCountDownLatch> subscriberLatches;

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

    private final String identifier;

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

    @Override
    public void close()
    {
        // Unsubscribe
        try
        {
            this.session.unsubscribe( this.subscriberName );
        }
        catch ( JMSException e )
        {
            String message = "While attempting to close evaluation status subscriber " + this.getIdentifier()
                             + " for evaluation "
                             + this.evaluation.getEvaluationId()
                             + ", failed to remove the subscription from the session.";

            LOGGER.error( message, e );
        }

        // No need to close the session, only the connection
        try
        {
            this.connection.close();
        }
        catch ( JMSException e )
        {
            String message = "While attempting to close evaluation status subscriber " + this.getIdentifier()
                             + " for evaluation "
                             + this.evaluation.getEvaluationId()
                             + ", failed to close the connection.";

            LOGGER.error( message, e );
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
                      this.subscriberLatches.size() );

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
     * Awaits the conclusion of negotiations for output formats that are delivered by subscribers. The negotiation is 
     * concluded when all formats have been negotiated or the timeout is reached.
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

                if ( tracker.failed() )
                {
                    tracker.close();
                }
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
     * Returns <code>true</code> if this status tracker is in a failure state, otherwise <code>false</code> . 
     * Note that {@link #getFirstFailure()} may contain an exception that was recovered.
     * 
     * @return true if an unrecoverable exception occurred.
     */

    boolean failed()
    {
        return this.isFailedUnrecoverably.get();
    }

    /**
     * Returns a message on which consumption failed, <code>null</code> if no failure occurred.
     * 
     * @return a message that was not consumed
     */

    Message getFirstFailure()
    {
        return this.trackerFailedOn;
    }

    /**
     * @return a set of negotiated subscribers by format.
     */

    Map<Format, String> getNegotiatedSubscribers()
    {
        return Collections.unmodifiableMap( this.negotiatedSubscribers );
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
        }
        else
        {
            // Only grouped messages control flow.
            LOGGER.debug( "Not attempting to stop flow control because the message received had no consumer set." );
        }
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
        Set<Format> formatsRequired = new HashSet<>( this.negotiatedSubscribers.keySet() );

        // Record the offer
        if ( !formatsRequired.isEmpty() )
        {
            Set<Format> formatsAwaited = this.getFormatsAwaitingSubscribers();
            Set<Format> formatsWithoutConsumers = new HashSet<>( formatsAwaited );
            Set<Format> formatsOfferedByConsumer = new HashSet<>( formatsRequired );
            formatsWithoutConsumers.retainAll( formatsOffered );
            formatsOfferedByConsumer.retainAll( formatsOffered );

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
            this.recordAnOffer( formatsWithoutConsumers, formatsOfferedByConsumer, consumerId );
        }
    }

    /**
     * Records a subscription offer for the prescribed formats.
     * @param formatsWithoutConsumers the formats required for which consumers have not yet been identified
     * @param formatsOfferedByConsumer the formats required that are also offered by the consumer
     * @param consumerId the consumer identifier of the consumer with an offer
     */

    private void recordAnOffer( Set<Format> formatsWithoutConsumers,
                                Set<Format> formatsOfferedByConsumer,
                                String consumerId )
    {
        // For each format supported by the consumer that is also one of the required formats, attempt to add
        // the subscription. If it succeeds, then report the subscription as having succeeded. If the subscriber
        // succeeds for one of the formats offered, it should succeed for all open formats offered by the 
        // subscriber, otherwise evaluations will be distributed across subscribers that do the same work. This is 
        // achieved by locking the whole map to mutation by one thread. An underlying assumption here is that each
        // competing subscriber has a symmetric offer, i.e., there are no two subscribers that overlap partially in the
        // formats they offer, rather not at all or completely. See #82939-8 and #82939-9. Success here is only 
        // provisional because a window of time is allowed for other offers to accumulate, in order to ensure a fair 
        // competition. See #82939-27.
        Set<String> offers = new HashSet<>();
        offers.add( consumerId );
        for ( Format next : formatsOfferedByConsumer )
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
            for ( Format next : formatsWithoutConsumers )
            {
                won = this.negotiatedSubscribers.replace( next, StringUtils.EMPTY, consumerId );
            }

            // Provisionally awarded these formats to a subscriber. However, the final award may be to a different
            // subscriber if other offers arrive within the negotiation period. See chooseBetweenOffers.
            if ( won )
            {
                // Countdown the negotiation latch for each format offered and won, noting here that any win means
                // all win and a win cannot happen in another thread because this thread has the mutex lock
                formatsWithoutConsumers.forEach( next -> this.negotiatedSubscribersLatch.countDown() );

                LOGGER.debug( "While negotiating evaluation {}, subscriber {} was the first to offer formats {}.",
                              this.evaluation.getEvaluationId(),
                              consumerId,
                              formatsWithoutConsumers );
            }
        }
    }

    /**
     * Choose one of the viable subscription offers for each required format. If an offer has been provisionally 
     * accepted, allow for it to be replaced by one of the offered candidates in order to ensure a fair competition and,
     * thus, an even distribution of work across subscriber clients.
     * See #82939-27.
     * 
     * @param accepted the map of provisionally accepted consumers
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
                int pick = EvaluationStatusTracker.SUBSCRIBER_RESOLVER.nextInt( size );
                String newConsumerId = consumers.get( pick );

                // Replace, which returns the existing value if one exists
                existingConsumerId = accepted.replace( nextAccepted, newConsumerId );

                // Replace all instances of the old references with the new one if the winning subscriber offers 
                // multiple formats that are required. Don't check if the existing and new are the same.
                if ( Objects.nonNull( existingConsumerId ) && !existingConsumerId.equals( newConsumerId ) )
                {
                    replaced.add( newConsumerId );

                    this.replaceExistingConsumerWithNewConsumerForAllOfferedFormats( existingConsumerId,
                                                                                     newConsumerId,
                                                                                     acceptedFormats,
                                                                                     accepted,
                                                                                     offered );
                }

                // Add a consumption latch for the new consumer.
                this.subscriberLatches.put( newConsumerId, new TimedCountDownLatch( 1 ) );

                // Flow control the consumer
                this.flowController.addSubscriber( newConsumerId );
            }
        }
    }

    /**
     * For each format offered by the new consumer, replace the existing consumer that was provisionally accepted for 
     * that format. This ensures a minimum number of consumers across all formats (since, in general, it is more 
     * efficient for a consumer to deliver all of the required formats that it offers, rather than distributing formats
     * across consumers).
     * 
     * @param existingConsumerId the existing consumer id to replace
     * @param newConsumerId the new consumer id
     * @param acceptedFormats the formats for which consumers have been accepted
     * @param accepted the map of provisionally accepted consumers
     * @param offered the map of consumers that can satisfy each format
     */

    private void replaceExistingConsumerWithNewConsumerForAllOfferedFormats( String existingConsumerId,
                                                                             String newConsumerId,
                                                                             Set<Format> acceptedFormats,
                                                                             Map<Format, String> accepted,
                                                                             Map<Format, Set<String>> offered )
    {
        // Iterate through the other formats 
        for ( Format next : acceptedFormats )
        {
            // Is the format offered by the chosen consumer? If so, use the chosen consumer for this
            // format too.
            if ( offered.get( next ).contains( newConsumerId ) )
            {
                accepted.replace( next, existingConsumerId, newConsumerId );
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
        return Objects.equals( consumerId, this.identifier );
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
     * @param broker the broker connection factory
     * @param formatsAwaited the formats to be delivered by subscribers yet to be negotiated
     * @param identifier the consumer identifier of this instance, used to ignore messages related to the tracker
     * @param maximum retries the maximum number of retries on failing to consume a message
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the list of subscribers is empty or the maximum number of retries is < 0
     * @throws EvaluationEventException if the status tracker could not be created for any other reason
     */

    EvaluationStatusTracker( Evaluation evaluation,
                             BrokerConnectionFactory broker,
                             Set<Format> formatsAwaited,
                             String identifier,
                             int maximumRetries )
    {
        Objects.requireNonNull( evaluation );
        Objects.requireNonNull( broker );
        Objects.requireNonNull( formatsAwaited );
        Objects.requireNonNull( identifier );

        if ( maximumRetries < 0 )
        {
            throw new IllegalArgumentException( "While building an evaluation status tracker, discovered a maximum "
                                                + "retry count that is less than zero, which is not allowed: "
                                                + maximumRetries );
        }

        this.evaluation = evaluation;
        this.success = ConcurrentHashMap.newKeySet();
        this.failure = ConcurrentHashMap.newKeySet();
        this.negotiatedSubscribers = new EnumMap<>( Format.class );
        this.subscriptionOffers = new ConcurrentHashMap<>();
        this.flowController = new ProducerFlowController( evaluation );

        // Add a placeholder entry for each unknown subscriber
        formatsAwaited.forEach( nextFormat -> this.negotiatedSubscribers.put( nextFormat, StringUtils.EMPTY ) );

        LOGGER.info( "The following output formats for evaluation {} will be delivered by subscribers that are yet to "
                     + "be negotiated: {}.",
                     this.evaluation.getEvaluationId(),
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

        // Mutable because some subscriptions may be negotiated and hence latches added upon successful negotiation
        this.subscriberLatches = new ConcurrentHashMap<>();

        // A latch whose count equals the number of formats to be delivered by external subscribers
        this.negotiatedSubscribersLatch = new TimedCountDownLatch( this.negotiatedSubscribers.size() );
        this.identifier = identifier;
        this.resourcesWritten = new HashSet<>();
        this.retriesAttempted = new AtomicInteger();
        this.isFailedUnrecoverably = new AtomicBoolean();
        this.maximumRetries = maximumRetries;

        // Set the message consumer and listener
        try
        {
            this.connection = broker.get()
                                    .createConnection();
            this.connection.setExceptionListener( new ConnectionExceptionListener( this.getIdentifier() ) );
            this.session = this.connection.createSession( false, Session.CLIENT_ACKNOWLEDGE );
            Topic topic = (Topic) broker.getDestination( Evaluation.EVALUATION_STATUS_QUEUE );

            String selector = MessageProperty.JMS_CORRELATION_ID + "='" + this.evaluation.getEvaluationId() + "'";
            this.subscriberName = Evaluation.EVALUATION_STATUS_QUEUE + "-HOUSEKEEPING-evaluation-status-tracker-"
                                  + this.evaluation.getEvaluationId()
                                  + "-"
                                  + this.getIdentifier();

            this.registerListenerForConsumer( this.getMessageConsumer( topic, this.subscriberName, selector ),
                                              this.evaluation.getEvaluationId() );

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
     * @return a consumer
     * @throws JMSException if the consumer could not be created for any reason
     */

    private MessageConsumer getMessageConsumer( Topic topic, String name, String selector )
            throws JMSException
    {
        return this.session.createDurableSubscriber( topic, name, selector, false );
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

            // Throwing exceptions on the MessageListener::onMessage is considered a bug, so need to track status and 
            // exit gracefully when problems occur. The message is either delivered, the session is recovering or the
            // subscriber is unrecoverable.
            AtomicBoolean consumeSucceeded = new AtomicBoolean();
            AtomicBoolean recovering = new AtomicBoolean();

            BytesMessage receivedBytes = (BytesMessage) message;
            String messageId = "unknown";
            String correlationId = "unknown";

            try
            {
                // Only consume if the subscriber is live
                if ( !this.failed() )
                {
                    messageId = message.getJMSMessageID();
                    correlationId = message.getJMSCorrelationID();

                    // Create the byte array to hold the message
                    int messageLength = (int) receivedBytes.getBodyLength();

                    byte[] messageContainer = new byte[messageLength];

                    receivedBytes.readBytes( messageContainer );

                    ByteBuffer bufferedMessage = ByteBuffer.wrap( messageContainer );

                    EvaluationStatus statusMessage = EvaluationStatus.parseFrom( bufferedMessage.array() );

                    // Accept the message
                    this.acceptStatusMessage( statusMessage );

                    consumeSucceeded.set( true );
                }

                // Acknowledge
                message.acknowledge();
            }
            catch ( JMSException | InvalidProtocolBufferException | RuntimeException e )
            {
                // Messages are on the DLQ, but signal locally too
                if ( Objects.isNull( this.trackerFailedOn ) )
                {
                    this.trackerFailedOn = receivedBytes;
                }

                // Attempt to recover and flag recovery locally
                this.recover( messageId, correlationId, e );
                recovering.set( true );
            }
            finally
            {
                // A consumption failed and a recovery is not underway. Unrecoverable.
                if ( !consumeSucceeded.get() && !recovering.get() )
                {
                    this.markSubscriberFailed();
                }
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

    private void recover( String messageId, String correlationId, Exception e )
    {
        // Only try to recover if an evaluation hasn't already failed
        if ( !this.failed() )
        {
            try ( StringWriter sw = new StringWriter();
                  PrintWriter pw = new PrintWriter( sw ); )
            {
                // Create a stack trace to log
                e.printStackTrace( pw );
                String message = sw.toString();

                // Attempt recovery in order to cycle the delivery attempts. When the maximum is reached, poison
                // messages should hit the dead letter queue/DLQ
                LOGGER.error( "While attempting to consume a message with identifier {} and correlation identifier "
                              + "{} in subscriber {}, encountered an error. This is {} of {} allowed consumption "
                              + "failures before the subscriber will notify an unrecoverable failure for "
                              + "evaluation {}. The error is: {}",
                              messageId,
                              correlationId,
                              this.getIdentifier(),
                              this.getNumberOfRetriesAttempted().get() + 1, // Counter starts at zero
                              this.getMaximumRetries(),
                              correlationId,
                              message );

                this.session.recover();
            }
            catch ( JMSException f )
            {
                LOGGER.error( "While attempting to recover a session for evaluation {} in evaluation status tracker {}, "
                              + "encountered an error that prevented recovery: ",
                              correlationId,
                              this.getIdentifier(),
                              f.getMessage() );
            }
            catch ( IOException g )
            {
                LOGGER.error( "While attempting recovery in evaluation status tracker {}, failed to close an exception "
                              + "writer.",
                              this.getIdentifier() );
            }
        }

        // Stop if the maximum number of retries has been reached
        if ( this.getNumberOfRetriesAttempted()
                 .incrementAndGet() == this.getMaximumRetries() )
        {

            LOGGER.error( "Evaluation status tracker {} encountered a consumption failure for evaluation {}. Recovery "
                          + "failed after {} attempts.",
                          this.getIdentifier(),
                          correlationId,
                          this.getMaximumRetries() );

            // Register the subscriber as failed
            this.markSubscriberFailed();
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
     * Marks the subscriber as failed after all recovery attempts.
     */

    private void markSubscriberFailed()
    {
        this.isFailedUnrecoverably.set( true );
    }

    /**
     * @return the identifier of this status tracker.
     */

    private String getIdentifier()
    {
        return this.identifier;
    }

    /**
     * Listen for failures on a connection.
     */

    private static class ConnectionExceptionListener implements ExceptionListener
    {
        private static final Logger LOGGER = LoggerFactory.getLogger( ConnectionExceptionListener.class );

        /**
         * The client that encountered the exception.
         */

        private final String clientId;

        @Override
        public void onException( JMSException exception )
        {
            LOGGER.warn( "An exception listener uncovered an error in client {}. {}",
                         this.clientId,
                         exception.getMessage() );
        }

        /**
         * Creates an instance with an evaluation identifier and a message client identifier.
         * 
         * @param evaluationId the evaluation identifier
         * @param clientId the client identifier
         */

        ConnectionExceptionListener( String clientId )
        {
            Objects.requireNonNull( clientId );

            this.clientId = clientId;
        }

    }
}
