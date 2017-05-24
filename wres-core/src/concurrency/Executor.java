/**
 * 
 */
package concurrency;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

import config.SystemConfig;

/**
 * The static thread executor 
 * 
 * @author Christopher Tubbs
 */
public final class Executor {
	// The underlying thread executor
	private static ExecutorService service = createService();
	private static final AtomicLong submittedCount = new AtomicLong();

    private Executor()
    {
        // prevent direct construction
    }

	/**
	 * Creates a new thread executor
	 * @return A new thread executor that may run the maximum number of configured threads
	 */
	private static final ExecutorService createService() {
		if (service != null)
		{
			service.shutdown();
		}
        submittedCount.set(0);
		return Executors.newFixedThreadPool(SystemConfig.maximumThreadCount());
	}
	
	/**
	 * Submits the passed in callable thread for execution
	 * @param task The thread whose task to call
	 * @return An object containing the value returned in the future
	 */
	public static <U> Future<U> submit(Callable<U> task)
	{
		if (service == null || service.isShutdown())
		{
			service = createService();
		}

		submittedCount.incrementAndGet();
		return service.submit(task);
	}
	
	/**
	 * Submits the passed in runnable task for execution
	 * @param task The thread whose task to execute
	 * @return An object containing an empty value generated at the end of thread execution
	 */
	public static Future execute(Runnable task)
	{
		if (service == null || service.isShutdown())
		{
			service = createService();
		}

        submittedCount.incrementAndGet();
		return service.submit(task);
	}
	
	/**
	 * Waits until all passed in jobs have executed.
	 */
	public static void complete()
	{
		if (!service.isShutdown())
		{
			service.shutdown();
			while (!service.isTerminated());
		}
	}

	/**
	 * Get the number of tasks submitted through this static class.
	 * @return the number of tasks ever submitted to the current executor
	 */
    public static long getSubmittedCount()
    {
        return submittedCount.get();
    }

    /**
     * Attempt to get the number of tasks completed, if possible.
     * Otherwise, return -1
     *
     * @return the number of tasks completed by current executor, otherwise -1
     */
    public static long getCompletedCount()
    {
        if (service instanceof ThreadPoolExecutor)
        {
            return ((ThreadPoolExecutor) service).getCompletedTaskCount();
        }
        return -1;
    }
}
