package wres.io.concurrency;

import wres.io.config.SystemSettings;

import java.util.concurrent.*;

/**
 * The static thread executor 
 * 
 * @author Christopher Tubbs
 */
public final class Executor {

	// The underlying thread executor
	private static ExecutorService service = createService();

    private Executor()
    {
        // prevent direct construction
    }

    public static float getLoad()
	{
		return ((ThreadPoolExecutor)service).getActiveCount() / SystemSettings.maximumThreadCount();
	}

	/**
	 * Creates a new thread executor
	 * @return A new thread executor that may run the maximum number of configured threads
	 */
	private static ExecutorService createService() {
		if (service != null)
		{
			service.shutdown();
		}
		/*if (submittedCount != null)
		{
		    submittedCount.set(0);
		}
		ThreadPoolExecutor executor = new ThreadPoolExecutor(SystemSettings.maximumThreadCount(),
				SystemSettings.maximumThreadCount(),
				SystemSettings.poolObjectLifespan(),
				TimeUnit.MILLISECONDS,
				new LinkedBlockingQueue<>(SystemSettings.maximumThreadCount() * 10)
		);

		executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());*/
		return /*executor;*/ Executors.newFixedThreadPool(SystemSettings.maximumThreadCount());
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

		//submittedCount.incrementAndGet();
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

       // submittedCount.incrementAndGet();
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

	public static void kill()
	{
		if (!service.isShutdown())
		{
			service.shutdownNow();
			while (!service.isTerminated());
		}
	}
}
