package wres.events.subscribe;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jcip.annotations.ThreadSafe;
import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.Consumer.Format;
import wres.statistics.generated.EvaluationStatus.CompletionStatus;

/**
 * A class that offers a subscription and, optionally, allows for a subscriber to be "booked" upon offering. When a 
 * subscriber is booked, it cannot offer again until it is "unbooked". This allows for a subscriber to deliver precisely
 * one (or N) evaluations at one time, thereby allowing a more predictable resource footprint for the client that wraps
 * it. 
 * 
 * @author james.brown@hydrosolved.com
 */

@ThreadSafe
class SubscriberOfferer
{

    /** 
     * Logger. 
     */
    private static final Logger LOGGER = LoggerFactory.getLogger( SubscriberOfferer.class );

    /** 
     * Is true if the subscriber adopts a booking strategy, allowing one evaluation per subscriber at any one time, 
     * false to allow unlimited evaluations per subscriber at once.
     * 
     * @see #book(String)
     * @see #unbook(String)
     */
    private static final boolean BOOKING_ENABLED = true;

    /** 
     * An atomic reference to an evaluation that has booked this subscriber or null if the subscriber is unbooked. 
     */
    private final AtomicReference<String> booked;

    /** 
     * The identifier of the evaluation served by this offerer, if the offerer can only serve one evaluation. 
     */
    private final String evaluationToServe;

    /** 
     * Formats offered by this offerer.
     */
    private final Set<Format> formatsOffered;

    /** 
     * Identifier of the client that manages this offerer. 
     */
    private final String clientId;

    /**
     * Consumes an evaluation identifier and makes an offer for that evaluation.
     */
    private final Consumer<String> offerer;

    /**
     * Offers services.
     * 
     * @param status the evaluation status message with the consumer request
     * @param evaluationId the identifier of the evaluation for which services should be offered
     * @throws IllegalArgumentException of the status message is not a request for formats
     */

    void offerServices( EvaluationStatus status, String evaluationId )
    {
        CompletionStatus completionStatus = status.getCompletionStatus();

        // Request for a consumer?
        if ( completionStatus != CompletionStatus.CONSUMER_REQUIRED )
        {
            throw new IllegalArgumentException( "Cannot offer services without a request for services. "
                                                + "Expected "
                                                + CompletionStatus.CONSUMER_REQUIRED
                                                + " but found "
                                                + completionStatus
                                                + "." );
        }

        // Was the subscriber built for a single evaluation? If so, do not offer
        // TODO: remove this logic when all subscribers are in long-running processes, serving many evaluations
        if ( Objects.nonNull( this.evaluationToServe ) && !evaluationId.equals( this.evaluationToServe ) )
        {
            LOGGER.debug( "Subscriber {} was notified of {} for evaluation {} but will not offer services for this "
                          + "evaluation because the subscriber was built for a different evaluation, {}.",
                          this.getClientId(),
                          CompletionStatus.CONSUMER_REQUIRED,
                          evaluationId,
                          this.evaluationToServe );

            return;
        }

        // Only offer formats if the status message is non-specific or some required formats intersect the formats
        // offered, i.e., if the subscriber can potentially fulfill the request
        Set<Format> formats = this.getFormatsOffered();
        boolean formatsIntersect = status.getFormatsRequiredList().isEmpty()
                                   || status.getFormatsRequiredList().stream().anyMatch( formats::contains );

        // Some formats to write and succeeded in booking the subscriber if booking is enabled
        if ( formatsIntersect && this.book( evaluationId ) )
        {
            LOGGER.debug( "Subscriber {} is offering services for formats {}.", this.getClientId(), formats );

            // Make the offer
            this.getOfferer()
                .accept( evaluationId );
        }
        else
        {
            LOGGER.debug( "Received a request from evaluation {} for a consumer, but subscriber {} could not fulfill "
                          + "the contract.",
                          evaluationId,
                          this.getClientId() );
        }
    }

    /**
     * @return true if the subscriber is booked, otherwise false
     */

    boolean isBooked()
    {
        return SubscriberOfferer.BOOKING_ENABLED && Objects.isNull( this.booked.get() );
    }

    /**
     * @return true if the subscriber is booked with the prescribed evaluation, otherwise false
     */

    boolean isBookedWith( String evaluationId )
    {
        return Objects.equals( evaluationId, this.booked.get() );
    }

    /**
     * Attempts to "book" the subscriber and thereby prevent further offers for work. The subscriber is booked if there
     * is no evaluation underway. Always returns true if {@link EvaluationSubscriber.BOOKING_ENABLED} is false.
     * 
     * @see #unbook(String)
     * @param evaluationId the evaluation identifier to use when booking the subscriber
     * @return true if the subscriber was booked or booking is disabled, false otherwise
     */

    boolean book( String evaluationId )
    {
        if ( SubscriberOfferer.BOOKING_ENABLED )
        {
            // Set atomically
            // Note that reference equality is used here and null == null
            boolean returnMe = this.booked.compareAndSet( null, evaluationId );

            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "Attempted to book subscriber {} with evaluation {}. Success?: {}",
                              this.getClientId(),
                              evaluationId,
                              returnMe );
            }

            return returnMe;
        }

        // Booking always succeeds if disabled
        return true;
    }

    /**
     * <p>Unbooks the subscriber if the specified evaluation identifier matches the identifier that was used to book the
     * subscriber. Unbooking allows the subscriber to re-offer services. This method should be called when:
     * 
     * <ol>
     * <li>The booked evaluation succeeds;</li>
     * <li>The booked evaluation fails for any reason (e.g., timed out); or</li>
     * <li>The offered evaluation is lost to another subscriber</li>
     * </ol>.
     * 
     * <p>The reason to provide the evaluation identifier is that unbooking should only happen once for the evaluation
     * that booked the subscriber, just as booking should only happen once.
     * 
     * @see #book(String)
     * @param evaluationId the evaluationId
     */

    void unbook( String evaluationId )
    {
        // Atomic reference compareAndSet uses identity equals, but content equals is needed
        String existingReference = this.booked.get();
        String referenceEquality = evaluationId;
        if ( evaluationId.equals( existingReference ) )
        {
            referenceEquality = existingReference;
        }

        // Set atomically
        boolean unbooked = this.booked.compareAndSet( referenceEquality, null );

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Attempted to unbook subscriber {} with evaluation {}. Upon entry, the subscriber was booked "
                          + "with evaluation {}. Success?: {}",
                          this.getClientId(),
                          referenceEquality,
                          existingReference,
                          unbooked );
        }
    }

    /**
     * @return the identifier of the client that manages this offerer
     */

    private String getClientId()
    {
        return this.clientId;
    }

    /**
     * @return the formats offered.
     */

    private Set<Format> getFormatsOffered()
    {
        return this.formatsOffered;
    }

    /**
     * @return the consumer that makes the service offer.
     */

    private Consumer<String> getOfferer()
    {
        return this.offerer;
    }

    /**
     * Constructor.
     * 
     * @param evaluationToServe the evaluation to serve, possibly null
     * @param offerer the runnable that makes an offer
     * @param formatsOffered the formats offered
     * @param clientId the client identifier
     * @throws NullPointerException if any required input is null
     */
    SubscriberOfferer( String evaluationToServe, Consumer<String> offerer, Set<Format> formatsOffered, String clientId )
    {
        Objects.requireNonNull( offerer );
        Objects.requireNonNull( formatsOffered );
        Objects.requireNonNull( clientId );

        this.booked = new AtomicReference<>();
        this.evaluationToServe = evaluationToServe;
        this.offerer = offerer;
        this.formatsOffered = formatsOffered;
        this.clientId = clientId;
    }
}
