package wres.events.subscribe;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

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
class OneGroupConsumer<T> implements BiConsumer<String, T>, Supplier<Set<Path>>
{

    private static final Logger LOGGER = LoggerFactory.getLogger( OneGroupConsumer.class );

    /**
     * Error message.
     */

    private static final String ATTEMPTED_TO_REUSE_A_ONE_USE_CONSUMER_WHICH_IS_NOT_ALLOWED =
            "Attempted to reuse a one-use consumer, which is not allowed.";

    /**
     * Inner consumer to consume the messages from the cache.
     */

    private final Function<Collection<T>, Set<Path>> innerConsumer;

    /**
     * Group identifier.
     */

    private final String groupId;

    /**
     * Cache of statistics by statistics message identifier.
     */
    private final Map<String, T> cache;

    /**
     * Is <code>true</code> if this consumer has been used once.
     */

    private final AtomicBoolean isComplete;

    /**
     * Expected number of messages in the group.
     */

    private final AtomicInteger expectedMessageCount;

    /**
     * Actual number of messages received.
     */

    private final AtomicInteger actualMessageCount;

    /**
     * A set of paths written by the consumer.
     */

    private final Set<Path> pathsWritten;

    /**
     * Mutex lock that protects completion of a group.
     */

    private final ReentrantLock groupCompletionLock = new ReentrantLock();

    /**
     * Returns an instance for grouped consumption of messages.
     * 
     * @param <T> the message type
     * @param innerConsumer the inner consumer
     * @param groupId the message groupId
     * @return an instance
     * @throws NullPointerException if any input is null
     */

    static <T> OneGroupConsumer<T> of( Function<Collection<T>, Set<Path>> innerConsumer,
                                       String groupId )
    {
        return new OneGroupConsumer<>( innerConsumer, groupId );
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

        if ( this.isComplete() )
        {
            throw new IllegalStateException( ATTEMPTED_TO_REUSE_A_ONE_USE_CONSUMER_WHICH_IS_NOT_ALLOWED );
        }

        // Do atomic put-if-absent and, if present, then do atomic replace.
        T cachedMessage = this.cache.putIfAbsent( messageId, message );

        if ( Objects.nonNull( cachedMessage ) )
        {
            this.cache.replace( messageId, message );

            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "Group consumer {} replaced an existing message with identifier {}.", this, messageId );
            }
        }
        // Only increment the actual message count if this message is not replacing an existing one (e.g., on retry)
        else
        {
            this.actualMessageCount.incrementAndGet();
        }

        if ( LOGGER.isTraceEnabled() )
        {
            LOGGER.trace( "Group consumer {} accepted a new message, {}.", this, message );
        }

        // Try to accept the group.
        this.acceptGroup();
    }

    @Override
    public Set<Path> get()
    {
        return Collections.unmodifiableSet( this.pathsWritten );
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
      * Sets the number of messages expected in the group.
      * 
      * @param expectedMessageCount the expected message count
      * @throws IllegalArgumentException if the expected count is less than or equal to zero
      */

    void setExpectedMessageCount( int expectedMessageCount )
    {
        // Flag as used
        if ( this.isComplete() )
        {
            throw new IllegalStateException( "Cannot reset the expected message count for message group "
                                             + this.getGroupId()
                                             + " after the group has been consumed." );
        }

        if ( expectedMessageCount <= 0 )
        {
            throw new IllegalArgumentException( "While setting the expected message count for group "
                                                + this.getGroupId()
                                                + "discovered an expected count of less than or equal to zero, which "
                                                + "is not allowed: "
                                                + expectedMessageCount
                                                + "." );
        }

        this.expectedMessageCount.set( expectedMessageCount );

        // Try to accept the group, which completes the consumer.
        this.acceptGroup();

        // Log status
        if ( LOGGER.isDebugEnabled() && this.isComplete() )
        {
            LOGGER.debug( "Received notification of publication complete for group {}. The message indicated an "
                          + "expected message count of {} and the group was completed, as all of these messages have "
                          + "been received.",
                          this.getGroupId(),
                          this.expectedMessageCount.get() );
        }
        else if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Received notification of publication complete for group {}. The expected number of messages "
                          + "within the group is {} but {} of these messages are outstanding. Grouped consumption will "
                          + "happen when all of the outstanding messages have been received.",
                          this.getGroupId(),
                          this.expectedMessageCount.get(),
                          this.expectedMessageCount.get() - this.actualMessageCount.get() );
        }
    }

    /**
     * Returns <code>true</code> if the consumer has been used already, otherwise <code>false</code>.
     * 
     * @return true if the consumer has consumed
     */

    boolean isComplete()
    {
        return this.isComplete.get();
    }

    /**
     * Flushes the cache of statistics to the inner consumer. Calls to this method should be guarded by the 
     * {@link #groupCompletionLock}.
     */

    private void acceptGroup()
    {
        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Consumer {} is attempting to accept message group {}. The expected message count is {}. "
                          + "The actual message count is {}. The completion status is {}.",
                          this,
                          this.getGroupId(),
                          this.expectedMessageCount.get(),
                          this.actualMessageCount.get(),
                          this.isComplete.get() );
        }

        // Lock available and consumer ready to complete.
        // If this method is called by a second thread while a first thread succeeds, the second thread will fail to
        // acquire the lock and the group will complete gracefully (either successfully or exceptionally). This is 
        // necessary because group acceptance can happen in two places within this class: 1) when the expected message
        // count is supplied; and 2) when the last message is supplied.
        if ( this.isReadyToComplete() && this.groupCompletionLock.tryLock() )
        {
            LOGGER.debug( "Consumer {} is continuing to complete message group {}.", this, this.getGroupId() );

            try
            {
                // Propagate
                Collection<T> statistics = Collections.unmodifiableCollection( this.cache.values() );
                Set<Path> paths = this.innerConsumer.apply( statistics );

                // Clear the cache
                this.cache.clear();

                LOGGER.debug( "Group consumer {} completed message group {}, which contained {} messages.",
                              this,
                              this.getGroupId(),
                              this.actualMessageCount.get() );

                this.pathsWritten.addAll( paths );

                // Consumption happened without exception (or immediate exception), flag complete
                this.isComplete.set( true );
            }
            finally
            {
                this.groupCompletionLock.unlock();
            }
        }
    }

    /**
     * @return true if the consumer is ready to complete, otherwise false
     */

    private boolean isReadyToComplete()
    {
        return this.expectedMessageCount.get() > 0
               && this.expectedMessageCount.get() == this.actualMessageCount.get()
               && !this.isComplete();
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
        this.isComplete = new AtomicBoolean();
        this.expectedMessageCount = new AtomicInteger();
        this.actualMessageCount = new AtomicInteger();
        this.groupId = groupId;
        this.pathsWritten = new TreeSet<>();
    }

}
