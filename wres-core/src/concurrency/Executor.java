/**
 * 
 */
package concurrency;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import config.SystemConfig;

/**
 * The static thread executor 
 */
public final class Executor 
{
	// The underlying thread executor
	private static ExecutorService service = createService();
	
	/**
	 * Creates a new thread executor
	 * @return A new thread executor that may run the maximum number of configured threads
	 */
	private static final ExecutorService createService()
	{
		if (service != null)
		{
			service.shutdown();
		}
		return Executors.newFixedThreadPool(SystemConfig.instance().get_maximum_thread_count());
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
		//print_status();
		return service.submit(task);
	}
	
	/**
	 * Submits the passed in runnable task for execution
	 * @param task The thread whose task to execute
	 * @return An object containing an empty value generated at the end of thread execution
	 */
	public static Future<?> execute(Runnable task)
	{
		if (service == null || service.isShutdown())
		{
			service = createService();
		}
		//print_status();
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
}
