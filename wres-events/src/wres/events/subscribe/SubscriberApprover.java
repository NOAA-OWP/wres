package wres.events.subscribe;

import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import net.jcip.annotations.Immutable;
import wres.statistics.generated.Consumer.Format;

/**
 * A class that contains approved subscriptions for each of several format types. When negotiating subscriptions, use
 * this class to determine whether the subscriber is approved to deliver the format required. The default behavior is
 * permissive; in other words, if no subscriber is explicitly set for a given format, then every subscriber is approved.
 * 
 * Currently, some subscribers are attached to short-running wres-core clients and others are attached to dedicated 
 * format writers in long-running processes. Subscriptions from short-running clients should not be accepted unless the 
 * short-running client contains the evaluation being negotiated. See #88262 and #88267. This class satisfies the 
 * requirement that some subscribers are not considered equal and are not, therefore, approved for negotiation. However,
 * it is desirable that all subscribers are considered equal and hence that all format writers should be placed in 
 * long-running processes eventually.
 * 
 * @author james.brown@hydrosolved.com
 */

@Immutable
public class SubscriberApprover
{
    /**
     * A map of the unique identifiers of approved subscribers by format type. The default status is permissive; if 
     * empty, all subscribers are approved for all formats. If a format contains one or more entries, then only those 
     * subscribers are approved.
     */

    private final Map<Format, Set<String>> approvedSubscribers;

    /**
     * Returns {@code true} if the subscriber is approved for the designated format, otherwise {@code false}.
     * 
     * @param format the format
     * @param subscriberId the subscriber identifier
     * @return true if the subscriber is approved
     * @throws NullPointerException if either input is null
     */

    public boolean isApproved( Format format, String subscriberId )
    {
        Objects.requireNonNull( format );
        Objects.requireNonNull( subscriberId );

        // Permissive by default
        if ( this.approvedSubscribers.isEmpty() || !this.approvedSubscribers.containsKey( format ) )
        {
            return true;
        }

        return this.approvedSubscribers.get( format )
                                       .contains( subscriberId );
    }

    /**
     * Builds a {@link SubscriberApprover}.
     * 
     * @author james.brown@hydrosolved.com
     */

    public static class Builder
    {
        /**
         * A map of approved subscribers.
         */

        private final Map<Format, Set<String>> approvedSubscribers = new EnumMap<>( Format.class );

        /**
         * Adds an approved subscriber
         * @param format the format
         * @param subscriberId the subscriber identifier
         * @return the builder
         * @throws NullPointerException if either input is null
         */

        public Builder addApprovedSubscriber( Format format, String subscriberId )
        {
            Objects.requireNonNull( format );
            Objects.requireNonNull( subscriberId );

            Set<String> approved = this.approvedSubscribers.get( format );

            if ( Objects.isNull( approved ) )
            {
                approved = new HashSet<>();
                this.approvedSubscribers.put( format, approved );
            }

            approved.add( subscriberId );

            return this;
        }

        /**
         * Adds an approved subscriber for each of several formats.
         * @param formats the formats
         * @param subscriberId the subscriber identifier
         * @return the builder
         * @throws NullPointerException if either input is null
         */

        public Builder addApprovedSubscriber( Set<Format> formats, String subscriberId )
        {
            Objects.requireNonNull( formats );
            Objects.requireNonNull( subscriberId );

            formats.forEach( format -> this.addApprovedSubscriber( format, subscriberId ) );

            return this;
        }

        /**
         * Builds an instance of a {@link SubscriberApprover}.
         * @return a {@link SubscriberApprover}.
         */

        public SubscriberApprover build()
        {
            return new SubscriberApprover( this );
        }
    }

    /**
     * Hidden constructor.
     * @param builder the builder.
     */
    private SubscriberApprover( Builder builder )
    {
        this.approvedSubscribers = new EnumMap<>( Format.class );
        this.approvedSubscribers.putAll( builder.approvedSubscribers );
    }
}
