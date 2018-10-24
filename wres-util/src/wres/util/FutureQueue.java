package wres.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
* Collection object used to loop through and complete a collection of future tasks
**/
public class FutureQueue<V>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( FutureQueue.class );

    /**
     * Protects operations on the queue itself
     */
    private final ReentrantLock queueLock;

    /**
     * Contains the objects to loop through
     */
    private final Queue<Future<V>> queue;

    /**
     * The number of TimeUnits to wait before moving onto another
     * task when processing
     */
    private final int timeout;

    /**
     * The unit detailing the duration of time to wait for a future to
     * complete before moving to the next
     */
    private final TimeUnit timeoutUnit;

    /**
     * Creates the queue with the default timeout of 500 milliseconds
     */
    public FutureQueue()
    {
        this.queue = new LinkedList<>(  );
        this.queueLock = new ReentrantLock(  );
        this.timeout = 500;
        this.timeoutUnit = TimeUnit.MILLISECONDS;
    }

    /**
     * Creates the queue with the given timeout between task completion attempts
     * @param timeout The number of timeout units to wait before moving on to another future
     * @param timeoutUnit The unit of time to wait before moving on to another future
     */
    public FutureQueue(final int timeout, final TimeUnit timeoutUnit)
    {
        this.queue = new LinkedList<>(  );
        this.queueLock = new ReentrantLock(  );
        this.timeout = timeout;
        this.timeoutUnit = timeoutUnit;
    }

    /**
     * Adds a future to the list of tasks to process
     * @param future An asynchronous task to process
     */
    public void add(final Future<V> future)
    {
        try
        {
            this.queueLock.lock();
            this.queue.add(future );
        }
        finally
        {
            if (this.queueLock.isHeldByCurrentThread())
            {
                this.queueLock.unlock();
            }
        }
    }

    public int size()
    {
        try
        {
            this.queueLock.lock();
            return this.queue.size();
        }
        finally
        {
            if (this.queueLock.isHeldByCurrentThread())
            {
                this.queueLock.unlock();
            }
        }
    }

    /**
     * Loops through each collected asynchronous task and completes them
     * <br><br>
     * The result of each task is gathered within the configured timeout. If a task takes too long,
     * it is added to the end of the queue and the next is processed.
     * @return A collection of all the values that came from the tasks in the queue
     * @throws ExecutionException Thrown if one of the tasks throws an exception
     */
    public Collection<V> loop() throws ExecutionException
    {
        Future<V> future = null;
        List<V> results = new ArrayList<>(  );
        try
        {
            this.queueLock.lock();
            while ( !this.queue.isEmpty() )
            {
                try
                {
                    future = this.queue.remove();

                    if ( this.queue.isEmpty() )
                    {
                        results.add(future.get());
                    }
                    else
                    {
                        results.add(future.get( this.timeout, this.timeoutUnit ));
                    }
                }
                catch ( InterruptedException e )
                {
                    LOGGER.error("Future processing has been interrupted.", e);
                    Thread.currentThread().interrupt();
                }
                catch ( ExecutionException e )
                {
                    int cancelCount = 0;
                    for (Future futureTask : this.queue)
                    {
                        try
                        {
                            futureTask.cancel( true );
                            cancelCount++;
                        }
                        catch ( Exception ce )
                        {
                            LOGGER.debug( "Failed to cancel a task.", ce );
                        }
                    }

                    if (cancelCount > 0)
                    {
                        LOGGER.debug( "Canceled {} tasks.", cancelCount );
                    }

                    throw e;
                }
                catch ( TimeoutException e )
                {
                    LOGGER.trace( "An asynchronous task timed out; adding back to the queue to try again later." );
                    this.queue.add( future );
                }
            }
        }
        finally
        {
            if (this.queueLock.isHeldByCurrentThread())
            {
                this.queueLock.unlock();
            }
        }

        return results;
    }
}
