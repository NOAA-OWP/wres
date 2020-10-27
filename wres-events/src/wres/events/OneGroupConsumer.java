package wres.events;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jcip.annotations.ThreadSafe;

/**
 * <p>An abstraction for grouped consumption of messages. This consumer will cache messages until the expected group has 
 * been assembled and will then propagate the grouped messages to the inner consumer on request.
 * 
 * <p>This consumer is intended to consume one group only. Upon attempting to re-use the consumer, after calling 
 * {@link #acceptGroup()}, an exception will be thrown.
 * 
 * <p>Consumes messages by unique message identifier. A message with a given identifier is mapped. As such, this 
 * consumer is "retry friendly". Retries will replace an already mapped message, but will not duplicate messages by
 * identifier.
 * 
 * @param <T> the type of message to be consumed
 * @author james.brown@hydrosolved.com
 */

@ThreadSafe
public class OneGroupConsumer<T> implements BiConsumer<String, T>
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

    private final Function<Collection<T>, Set<Path>> innerConsumer;

    /**
     * Group identifier.
     */

    private final String groupId;

    /**
     * Cache of statistics.
     */
    private final Map<String, T> cache;

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

    public static <T> OneGroupConsumer<T> of( Function<Collection<T>, Set<Path>> innerConsumer,
                                              String groupId )
    {
        return new OneGroupConsumer<>( innerConsumer, groupId );
    }

    /**
     * Returns the number of messages in the cache.
     * 
     * @return the number of messages
     */

    public int size()
    {
        return this.cache.size();
    }

    /**
     * Gets the inner consumer.
     * 
     * @return the inner consumer
     */

    Function<Collection<T>, Set<Path>> getInnerConsumer()
    {
        return this.innerConsumer;
    }

    /**
     * Accept a message.
     * 
     * @param messageId the message identifier
     * @param message the message to accept
     * @throws NullPointerException if either input is null
     * @throws IllegalStateException if an attempt is made to re-use this consumer
     */

    @Override
    public void accept( String messageId, T message )
    {
        Objects.requireNonNull( messageId );
        Objects.requireNonNull( message );

        if ( this.hasBeenUsed() )
        {
            throw new IllegalStateException( ATTEMPTED_TO_REUSE_A_ONE_USE_CONSUMER_WHICH_IS_NOT_ALLOWED );
        }

        // Do atomic put if absent and, if present, then do atomic replace.
        T cachedMessage = this.cache.putIfAbsent( messageId, message );

        if ( Objects.nonNull( cachedMessage ) )
        {
            this.cache.replace( messageId, message );

            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "Group consumer {} replaced an existing message with identifier {}.", this, messageId );
            }
        }
        else if ( LOGGER.isTraceEnabled() )
        {
            LOGGER.trace( "Group consumer {} accepted a new message, {}.", this, message );
        }
    }

    /**
     * Flushes the cache of statistics to the inner consumer.
     * @return a set of paths mutated
     */

    public Set<Path> acceptGroup()
    {
        // Flag this immediately because the state is visible to other threads via hasBeenUsed()
        if ( this.hasBeenUsed.getAndSet( true ) )
        {
            throw new IllegalStateException( ATTEMPTED_TO_REUSE_A_ONE_USE_CONSUMER_WHICH_IS_NOT_ALLOWED );
        }

        // Propagate, but make the acceptance of the group "retry friendly". In other words, if the consumption fails, 
        // then return the consumer to unused.
        try
        {
            Set<Path> paths = this.innerConsumer.apply( this.cache.values() );

            // Clear the cache
            this.cache.clear();

            LOGGER.trace( "Group consumer {} consumed a new group of {} message.", this, this.size() );

            return paths;
        }
        catch ( RuntimeException e )
        {
            this.hasBeenUsed.set( false );

            throw e;
        }
    }

    /**
     * Gets the group identifier.
     * 
     * @return the groupId
     */

    public String getGroupId()
    {
        return this.groupId;
    }

    /**
     * Returns <code>true</code> if the consumer has been used already, otherwise <code>false</code>.
     * 
     * @return true if the consumer has consumed
     */

    public boolean hasBeenUsed()
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

    private OneGroupConsumer( Function<Collection<T>, Set<Path>> innerConsumer, String groupId )
    {
        Objects.requireNonNull( groupId );
        Objects.requireNonNull( innerConsumer );

        this.innerConsumer = innerConsumer;
        this.cache = new ConcurrentHashMap<>();
        this.hasBeenUsed = new AtomicBoolean();
        this.groupId = groupId;
    }

}
