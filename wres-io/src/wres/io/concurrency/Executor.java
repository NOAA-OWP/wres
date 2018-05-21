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

import wres.io.config.SystemSettings;

/**
 * The static thread executor 
 * 
 * @author Christopher Tubbs
 */
public final class Executor {

	// The underlying thread executor
	private static final ThreadPoolExecutor SERVICE = createService();

	private static final ThreadPoolExecutor HIGH_PRIORITY_TASKS = createHighPriorityService();

    private static final Logger LOGGER = LoggerFactory.getLogger( Executor.class );

    private Executor()
    {
        // prevent direct construction
    }

	/**
	 * Creates a new thread executor
	 * @return A new thread executor that may run the maximum number of configured threads
	 */
	private static ThreadPoolExecutor createService()
	{
		ThreadFactory factory = runnable -> new Thread(runnable, "Executor Thread");
		ThreadPoolExecutor executor = new ThreadPoolExecutor(
				SystemSettings.maximumThreadCount(),
				SystemSettings.maximumThreadCount(),
				SystemSettings.poolObjectLifespan(),
				TimeUnit.MILLISECONDS,
				new ArrayBlockingQueue<>( SystemSettings.maximumThreadCount() * 5 ),
				factory
		);

		executor.setRejectedExecutionHandler( new ThreadPoolExecutor.CallerRunsPolicy() );
		return executor;
	}

	private static ThreadPoolExecutor createHighPriorityService()
	{
		if (HIGH_PRIORITY_TASKS != null)
		{
			HIGH_PRIORITY_TASKS.shutdown();
			while (!HIGH_PRIORITY_TASKS.isTerminated());
		}

		ThreadFactory factory = runnable -> new Thread(runnable, "High Priority Database Thread");
		return (ThreadPoolExecutor) Executors.newFixedThreadPool(10, factory);
	}

	public static <V> Future<V> submitHighPriorityTask(Callable<V> task)
	{
		return HIGH_PRIORITY_TASKS.submit( task );
	}
	
	/**
	 * Submits the passed in callable thread for execution
	 * @param <U> the result type
	 * @param task The thread whose task to call
	 * @return An object containing the value returned in the future
	 */
	public static <U> Future<U> submit(Callable<U> task)
	{
		return SERVICE.submit(task);
	}
	
	/**
	 * Submits the passed in runnable task for execution
	 * @param task The thread whose task to execute
	 * @return An object containing an empty value generated at the end of thread execution
	 */
	public static Future execute(Runnable task)
	{
		return SERVICE.submit(task);
	}
	
	/**
	 * Waits until all passed in jobs have executed.
	 */
	public static void complete()
	{
		if (!SERVICE.isShutdown())
		{
			SERVICE.shutdown();
			while (!SERVICE.isTerminated());
		}

		if (!HIGH_PRIORITY_TASKS.isShutdown())
		{
			HIGH_PRIORITY_TASKS.shutdown();
			while (!HIGH_PRIORITY_TASKS.isTerminated());
		}
	}


    /**
     * Shuts down the executors using a timeout. The caller is giving a holistic
     * timeout.
     * @param timeOut the desired maximum wait, measured in timeUnit
     * @param timeUnit the unit of the maximum wait
     * @return the list of abandoned tasks as a result of forced shutdown
     */

    public static List<Runnable> forceShutdown( long timeOut,
                                                TimeUnit timeUnit )
    {
        long halfTheTimeout = timeOut / 2;
        List<Runnable> abandonedTasks = new ArrayList<>();

        SERVICE.shutdown();
        try
        {
            SERVICE.awaitTermination( halfTheTimeout, timeUnit );
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Executor 1 shutdown interrupted." );
            List<Runnable> abandoned = SERVICE.shutdownNow();
            abandonedTasks.addAll( abandoned );
            Thread.currentThread().interrupt();
        }

        List<Runnable> abandonedOne = SERVICE.shutdownNow();
        abandonedTasks.addAll( abandonedOne );

        HIGH_PRIORITY_TASKS.shutdown();
        try
        {
            HIGH_PRIORITY_TASKS.awaitTermination( halfTheTimeout, timeUnit );
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Executor 2 shutdown interrupted." );
            List<Runnable> abandoned = HIGH_PRIORITY_TASKS.shutdownNow();
            abandonedTasks.addAll( abandoned );
            Thread.currentThread().interrupt();
        }

        List<Runnable> abandonedTwo = HIGH_PRIORITY_TASKS.shutdownNow();
        abandonedTasks.addAll( abandonedTwo );

        return abandonedTasks;
    }


	/**
	 * For system-level monitoring information, return the number of tasks in
	 * the io executor queue.
	 * @return the count of tasks waiting to be performed by the workers.
	 */

	public static int getIoExecutorQueueTaskCount()
	{
		if ( Executor.SERVICE != null
             && Executor.SERVICE.getQueue() != null )
        {
            return Executor.SERVICE.getQueue().size();
        }

		return 0;
	}


    /**
     * For system-level monitoring information, return the number of tasks in
     * the high priority tasks queue.
     * @return the count of tasks waiting to be performed by the hi pri workers.
     */

    public static int getHiPriIoExecutorQueueTaskCount()
    {
        if ( Executor.HIGH_PRIORITY_TASKS != null
             && Executor.HIGH_PRIORITY_TASKS.getQueue() != null )
        {
            return Executor.HIGH_PRIORITY_TASKS.getQueue().size();
        }

        return 0;
    }
}
