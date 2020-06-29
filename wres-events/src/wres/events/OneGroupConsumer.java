package wres.events;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

/**
 * <p>An abstraction for grouped consumption of messages. This consumer will cache messages until the expected group has 
 * been assembled and will then propagate the grouped messages to the inner consumer on request.
 * 
 * <p>This consumer is intended to consume one group only. Upon attempting to re-use the consumer, after calling 
 * {@link #acceptGroup()}, an exception will be thrown.
 * 
 * @param <T> the type of message to be consumed
 * @author james.brown@hydrosolved.com
 */

@ThreadSafe
class OneGroupConsumer<T> implements Consumer<T>
{

    private static final Logger LOGGER = LoggerFactory.getLogger( OneGroupConsumer.class );

    /**
     * Error message.
     */

    private static final String ATTEMPTED_TO_REUSE_A_ONE_USE_CONSUMER_WHICH_IS_NOT_ALLOWED =
            "Attempted to reuse a one-use consumer, which is not allowed.";

    /**
     * Inner consumer to consume the messages upon flushing the cache.
     */

    private final Consumer<List<T>> innerConsumer;

    /**
     * Cache lock for mutation of the cache.
     */

    private final Object cacheLock = new Object();

    /**
     * Group identifier.
     */

    private final String groupId;

    /**
     * Cache of statistics.
     */
    @GuardedBy( "cacheLock" )
    private final List<T> cache;

    /**
     * Is <code>true</code> if this consumer has been used once.
     */

    private final AtomicBoolean hasBeenUsed;

    /**
     * Returns an instance for grouped consumption of messages.
     * 
     * @param <T> the message type
     * @param innerConsumer the inner consumer
     * @param groupId the message groupId
     * @return an instance
     * @throws NullPointerException if any input is null
     */

    static <T> OneGroupConsumer<T> of( Consumer<List<T>> innerConsumer,
                                       String groupId )
    {
        return new OneGroupConsumer<>( innerConsumer, groupId );
    }

    /**
     * Returns the number of messages in the cache.
     * 
     * @return the number of messages
     */

    int size()
    {
        return this.cache.size();
    }

    /**
     * Gets the inner consumer.
     * 
     * @return the inner consumer
     */

    Consumer<List<T>> getInnerConsumer()
    {
        return this.innerConsumer;
    }

    /**
     * Accept a message.
     * 
     * @param message the message to accept
     * @throws NullPointerException if the input is null
     * @throws IllegalStateException if an attempt is made to re-use this consumer
     */

    @Override
    public void accept( T message )
    {
        Objects.requireNonNull( message );

        if ( this.hasBeenUsed() )
        {
            throw new IllegalStateException( ATTEMPTED_TO_REUSE_A_ONE_USE_CONSUMER_WHICH_IS_NOT_ALLOWED );
        }

        synchronized ( this.cacheLock )
        {
            this.cache.add( message );

            LOGGER.trace( "Grouped consumer {} accepted a new message, {}.", this, message );
        }
    }

    /**
     * Flushes the cache of statistics to the inner consumer.
     */

    void acceptGroup()
    {
        if ( this.hasBeenUsed.getAndSet( true ) )
        {
            throw new IllegalStateException( ATTEMPTED_TO_REUSE_A_ONE_USE_CONSUMER_WHICH_IS_NOT_ALLOWED );
        }

        synchronized ( this.cacheLock )
        {
            LOGGER.trace( "Grouped consumer {} consumed a new group of {} message.", this, this.cache.size() );

            // Propagate
            this.innerConsumer.accept( this.cache );

            // Clear the cache
            this.cache.clear();
        }
    }

    /**
     * Gets the group identifier.
     * 
     * @return the groupId
     */

    String getGroupId()
    {
        return this.groupId;
    }

    /**
     * Returns <code>true</code> if the consumer has been used already, otherwise <code>false</code>.
     * 
     * @return true if the consumer has consumed
     */

    boolean hasBeenUsed()
    {
        return this.hasBeenUsed.get();
    }

    /**
     * Hidden constructor.
     * 
     * @param innerConsumer the inner consumer
     * @param groupId the message groupId
     * @throws NullPointerException if any required input is null
     */

    private OneGroupConsumer( Consumer<List<T>> innerConsumer, String groupId )
    {
        Objects.requireNonNull( groupId );
        Objects.requireNonNull( innerConsumer );

        this.innerConsumer = innerConsumer;
        this.cache = new ArrayList<>();
        this.hasBeenUsed = new AtomicBoolean();
        this.groupId = groupId;
    }

}
