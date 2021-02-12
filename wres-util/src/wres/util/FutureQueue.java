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
     * Contains the results of each task that had to be processed early
     */
    private final List<V> earlyResults = new ArrayList<>(  );

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
     * The maximum number of tasks that may live in the queue without
     * being forced to process early
     */
    private int maximumTasks = Integer.MAX_VALUE;

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

    public FutureQueue(final int size)
    {
        this.queue = new LinkedList<>(  );
        this.queueLock = new ReentrantLock(  );
        this.timeout = 500;
        this.timeoutUnit = TimeUnit.MILLISECONDS;
        this.maximumTasks = size;
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
     * <br><br>
     * If adding a task will grow the queue beyond the limit, one is completed
     * prior to adding the new one. The resulting value is both returned and stored so
     * it is included in the final list of results.
     * @param future An asynchronous task to process
     * @return The possible result of a task that had to be completed before a new one
     * could be added. All non-null results are also added to a collection that will be
     * included in any final processing.
     * @throws ExecutionException Thrown if an asynchronous task had to be complete prior to
     * adding a new task threw an exception
     */
    public V add(final Future<V> future) throws ExecutionException
    {
        V earlyResult = null;

        try
        {
            this.queueLock.lock();

            if (this.queue.size() > this.maximumTasks)
            {
                try
                {
                    earlyResult = this.processTask();

                    if (earlyResult != null)
                    {
                        this.earlyResults.add( earlyResult );
                    }
                }
                catch ( ExecutionException e )
                {
                    throw new ExecutionException( "An asynchronous task needed to be completed prior "
                                                  + "to adding a new one, but failed during execution.", e );
                }
            }

            this.queue.add(future );
        }
        finally
        {
            if (this.queueLock.isHeldByCurrentThread())
            {
                this.queueLock.unlock();
            }
        }

        return earlyResult;
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
     * Sets the maximum number of allowable tasks in the queue
     * <br><br>
     * If the new maximum number is greater than the number of tasks
     * currently in the queue, tasks are completed and their results are stored
     * so that the new queue size fits within the limit
     * @param maximumTasks The maximum number of asynchronous tasks that may be held at once
     * @throws ExecutionException Thrown if tasks had to be completed to force the queue
     * to be the correct size but one threw an exception.
     */
    public void setMaximumTasks(final int maximumTasks) throws ExecutionException
    {
        try
        {
            this.queueLock.lock();

            this.maximumTasks = maximumTasks;

            while (this.size() > this.maximumTasks)
            {
                V earlyResult = processTask();

                if (earlyResult != null)
                {
                    this.earlyResults.add(earlyResult);
                }
            }
        }
        catch ( ExecutionException e )
        {
            throw new ExecutionException( "Tasks had to be completed to comply with the new "
                                          + "limit for the maximum number of tasks, but a "
                                          + "task encountered an exception.", e );
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
     * @return A collection of all the values that came from the tasks in the queue, combined
     * with any results of any task that had to be completed early
     * @throws ExecutionException Thrown if one of the tasks throws an exception
     */
    public Collection<V> loop() throws ExecutionException
    {
        List<V> results = new ArrayList<>(  );
        try
        {
            this.queueLock.lock();
            while ( !this.queue.isEmpty() )
            {
                V result = this.processTask();
                results.add(result);
            }

            results.addAll( this.earlyResults );
            this.earlyResults.clear();
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

    /**
     * Completes a single task from the queue
     * <br><br>
     * The result of a task is gathered within the configured timeout. If a task takes too long,
     * it is added to the end of the queue and the next is processed.
     * @return The resulting value from the completed task
     * @throws ExecutionException Thrown if the task encounters an exception of some sort
     */
    private V processTask() throws ExecutionException
    {
        V result = null;

        Future<V> future = null;

        while ( !this.queue.isEmpty() )
        {
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

                break;
            }
            catch ( InterruptedException e )
            {
                LOGGER.warn( "Future processing has been interrupted.", e );

                int cancelCount = 0;
                for (Future futureTask : this.queue)
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

                if (cancelCount > 0)
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
        }

        return result;
    }
}
