/**
 * 
 */
package wres.concurrency;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author ctubbs
 *
 */
public class Executor {
	
	private static int MAX_THREADS = 10; 
	
	private static ExecutorService service = createService();
	
	private static final ExecutorService createService()
	{
		//int max_threads = Runtime.getRuntime().availableProcessors() * 5;
		return Executors.newFixedThreadPool(MAX_THREADS);
	}
	/**
	 * @param <U>
	 * 
	 */
	public static <U> Future<U> submit(Callable<U> task)
	{
		if (service == null || service.isShutdown())
		{
			service = createService();
		}
		return service.submit(task);
	}
	
	public static void execute(Runnable task)
	{
		if (service == null || service.isShutdown())
		{
			service = createService();
		}
		service.execute(task);
	}
	
	public static void shutdown()
	{
		if (!service.isShutdown())
		{
			service.shutdown();
			while (!service.isTerminated());
		}
	}

}
