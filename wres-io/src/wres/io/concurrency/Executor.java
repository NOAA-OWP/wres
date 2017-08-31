package wres.io.concurrency;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

import wres.io.config.SystemSettings;
import wres.util.Internal;

/**
 * The static thread executor 
 * 
 * @author Christopher Tubbs
 */
@Internal(exclusivePackage = "wres.io")
public final class Executor {

	// The underlying thread executor
	private static ThreadPoolExecutor SERVICE = createService();

    private Executor()
    {
        // prevent direct construction
    }

	/**
	 * Creates a new thread executor
	 * @return A new thread executor that may run the maximum number of configured threads
	 */
	private static ThreadPoolExecutor createService() {
		if (SERVICE != null)
		{
			SERVICE.shutdown();
		}

		ThreadFactory factory = runnable -> new Thread(runnable, "Executor Thread");
		return (ThreadPoolExecutor)Executors.newFixedThreadPool(SystemSettings.maximumThreadCount(), factory);
	}
	
	/**
	 * Submits the passed in callable thread for execution
	 * @param <U> the result type
	 * @param task The thread whose task to call
	 * @return An object containing the value returned in the future
	 */
	public static <U> Future<U> submit(Callable<U> task)
	{
		if (SERVICE == null || SERVICE.isShutdown())
		{
			SERVICE = createService();
		}

		return SERVICE.submit(task);
	}
	
	/**
	 * Submits the passed in runnable task for execution
	 * @param task The thread whose task to execute
	 * @return An object containing an empty value generated at the end of thread execution
	 */
	public static Future execute(Runnable task)
	{
		if (SERVICE == null || SERVICE.isShutdown())
		{
			SERVICE = createService();
		}

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
	}
}
