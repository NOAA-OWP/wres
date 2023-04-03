package wres.io.ingesting.database;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
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
*/
class FutureQueue
{
    private static final Logger LOGGER = LoggerFactory.getLogger( FutureQueue.class );

    /**
     * Protects operations on the queue itself
     */
    private final ReentrantLock queueLock;

    /**
     * Contains the objects to loop through
     */
    private final Queue<Future<?>> queue;

    /**
     * Contains the results of each task that had to be processed early
     */
    private final List<Object> earlyResults = new ArrayList<>();

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
        this.queue = new LinkedList<>();
        this.queueLock = new ReentrantLock();
        this.timeout = 500;
        this.timeoutUnit = TimeUnit.MILLISECONDS;
    }

    /**
     * Adds a future to the list of tasks to process.
     * @param future An asynchronous task to process
     * @throws ExecutionException Thrown if an asynchronous task had to be complete prior to
     * adding a new task threw an exception
     */
    public void add( final Future<?> future ) throws ExecutionException
    {
        try
        {
            this.queueLock.lock();
            this.queue.add( future );
        }
        finally
        {
            if ( this.queueLock.isHeldByCurrentThread() )
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
            if ( this.queueLock.isHeldByCurrentThread() )
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
     * @return A collection of all the values that came from the tasks in the queue, combined
     * with any results of any task that had to be completed early
     * @throws ExecutionException Thrown if one of the tasks throws an exception
     */
    public Collection<Object> loop() throws ExecutionException
    {
        List<Object> results = new ArrayList<>();
        try
        {
            this.queueLock.lock();
            while ( !this.queue.isEmpty() )
            {
                Object result = this.processTask();
                results.add( result );
            }

            results.addAll( this.earlyResults );
            this.earlyResults.clear();
        }
        finally
        {
            if ( this.queueLock.isHeldByCurrentThread() )
            {
                this.queueLock.unlock();
            }
        }

        return results;
    }

    /**
     * Completes a single task from the queue
     * <br><br>
     * The result of a task is gathered within the configured timeout. If a task takes too long,
     * it is added to the end of the queue and the next is processed.
     * @return The resulting value from the completed task
     * @throws ExecutionException Thrown if the task encounters an exception of some sort
     */
    private Object processTask() throws ExecutionException
    {
        Object result;

        while ( !this.queue.isEmpty() )
        {
            result = this.processOneTask();

            if ( Objects.nonNull( result ) )
            {
                return result;
            }
        }

        return null;
    }

    /**
     * Completes a single task from the queue
     * <br><br>
     * The result of a task is gathered within the configured timeout. If a task takes too long,
     * it is added to the end of the queue and the next is processed.
     * @return The resulting value from the completed task
     * @throws ExecutionException Thrown if the task encounters an exception of some sort
     */
    private Object processOneTask() throws ExecutionException
    {
        Object result = null;

        Future<?> future = null;

        try
        {
            future = this.queue.remove();

            if ( this.queue.isEmpty() )
            {
                result = future.get();
            }
            else
            {
                result = future.get( this.timeout, this.timeoutUnit );
            }
        }
        catch ( InterruptedException e )
        {
            LOGGER.warn( "Future processing has been interrupted.", e );

            int cancelCount = 0;
            for ( Future<?> futureTask : this.queue )
            {
                try
                {
                    futureTask.cancel( true );
                    cancelCount++;
                }
                catch ( RuntimeException ce )
                {
                    LOGGER.warn( "Failed to cancel a task.", ce );
                }
            }

            if ( cancelCount > 0 )
            {
                LOGGER.debug( "Canceled {} tasks.", cancelCount );
            }

            Thread.currentThread().interrupt();
        }
        catch ( TimeoutException e )
        {
            LOGGER.trace( "An asynchronous task timed out; adding back to the queue to try again later." );
            this.queue.add( future );
        }

        return result;
    }
}
