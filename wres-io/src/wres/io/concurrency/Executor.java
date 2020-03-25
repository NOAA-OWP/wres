package wres.io.concurrency;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.system.SystemSettings;

/**
 * The static thread executor 
 * 
 * @author Christopher Tubbs
 */
public class Executor {

    private static final Logger LOGGER = LoggerFactory.getLogger( Executor.class );

    private final SystemSettings systemSettings;

	// The underlying thread executor
    private final ThreadPoolExecutor service;
    private final ThreadPoolExecutor highPriorityService;

    public Executor( SystemSettings systemSettings)
    {
        this.systemSettings = systemSettings;
        this.service = createService();
        this.highPriorityService = createHighPriorityService();
    }


	/**
	 * Creates a new thread executor
	 * @return A new thread executor that may run the maximum number of configured threads
	 */
    private ThreadPoolExecutor createService()
	{
		ThreadFactory factory = runnable -> new Thread(runnable, "Executor Thread");
		ThreadPoolExecutor executor = new ThreadPoolExecutor(
                systemSettings.maximumThreadCount(),
                systemSettings.maximumThreadCount(),
                systemSettings.poolObjectLifespan(),
				TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>( systemSettings.maximumThreadCount() * 5 ),
				factory
		);

		executor.setRejectedExecutionHandler( new ThreadPoolExecutor.CallerRunsPolicy() );
		return executor;
	}

	private static ThreadPoolExecutor createHighPriorityService()
	{
		ThreadFactory factory = runnable -> new Thread(runnable, "High Priority Database Thread");
		return (ThreadPoolExecutor) Executors.newFixedThreadPool(10, factory);
	}

    public <V> Future<V> submitHighPriorityTask(Callable<V> task)
	{
        return highPriorityService.submit( task );
	}
	
	/**
	 * Submits the passed in callable thread for execution
	 * @param <U> the result type
	 * @param task The thread whose task to call
	 * @return An object containing the value returned in the future
	 */
    public <U> Future<U> submit(Callable<U> task)
	{
        return service.submit( task);
	}
	
	/**
	 * Submits the passed in runnable task for execution
	 * @param task The thread whose task to execute
	 * @return An object containing an empty value generated at the end of thread execution
	 */
    public Future execute(Runnable task)
	{
        return service.submit( task);
	}
	
	/**
	 * Waits until all passed in jobs have executed.
	 */
    public void complete()
	{
        if (!service.isShutdown())
		{
            service.shutdown();
            while (!service.isTerminated());
		}

        if (!highPriorityService.isShutdown())
		{
            highPriorityService.shutdown();
            while (!highPriorityService.isTerminated());
		}
	}


    /**
     * Shuts down the executors using a timeout. The caller is giving a holistic
     * timeout.
     * @param timeOut the desired maximum wait, measured in timeUnit
     * @param timeUnit the unit of the maximum wait
     * @return the list of abandoned tasks as a result of forced shutdown
     */

    public List<Runnable> forceShutdown( long timeOut,
                                         TimeUnit timeUnit )
    {
        long halfTheTimeout = timeOut / 2;
        List<Runnable> abandonedTasks = new ArrayList<>();

        service.shutdown();
        try
        {
            service.awaitTermination( halfTheTimeout, timeUnit );
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Executor 1 shutdown interrupted.", ie );
            List<Runnable> abandoned = service.shutdownNow();
            abandonedTasks.addAll( abandoned );
            Thread.currentThread().interrupt();
        }

        List<Runnable> abandonedOne = service.shutdownNow();
        abandonedTasks.addAll( abandonedOne );

        highPriorityService.shutdown();
        try
        {
            highPriorityService.awaitTermination( halfTheTimeout, timeUnit );
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Executor 2 shutdown interrupted.", ie );
            List<Runnable> abandoned = highPriorityService.shutdownNow();
            abandonedTasks.addAll( abandoned );
            Thread.currentThread().interrupt();
        }

        List<Runnable> abandonedTwo = highPriorityService.shutdownNow();
        abandonedTasks.addAll( abandonedTwo );

        return abandonedTasks;
    }


	/**
	 * For system-level monitoring information, return the number of tasks in
	 * the io executor queue.
	 * @return the count of tasks waiting to be performed by the workers.
	 */

    public int getIoExecutorQueueTaskCount()
	{
        if ( this.service != null
             && this.service.getQueue() != null )
        {
            return this.service.getQueue().size();
        }

		return 0;
	}


    /**
     * For system-level monitoring information, return the number of tasks in
     * the high priority tasks queue.
     * @return the count of tasks waiting to be performed by the hi pri workers.
     */

    public int getHiPriIoExecutorQueueTaskCount()
    {
        if ( this.highPriorityService.getQueue() != null )
        {
            return this.highPriorityService.getQueue().size();
        }

        return 0;
    }
}
