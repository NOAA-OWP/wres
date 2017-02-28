/**
 * 
 */
package wres.util;

/**
 * @author ctubbs
 *
 */
public class Stopwatch {

	/**
	 * 
	 */
	public Stopwatch() {
		// TODO Auto-generated constructor stub
		start_time = 0;
		stop_time = 0;
	}
	
	public void start()
	{
		start_time = System.nanoTime();
		stop_time = 0;
		running = true;
	}
	
	public void stop()
	{
		stop_time = System.nanoTime();
		running = false;
	}
	
	public long get_duration()
	{
		long dur = 0;
		
		if (running)
		{
			dur = System.nanoTime() - start_time;
		}
		else
		{
			dur = stop_time - start_time; 
		}
		
		return dur / 1000000;
	}
	
	public String get_formatted_duration()
	{
		double duration = get_duration();
		String time_units = " milliseconds";
		
		if (duration > 60000)
		{
			duration = duration / 60000.0;
			time_units = " minutes";
		}
		else if (duration > 1000)
		{
			duration = duration / 1000;
			time_units = " seconds";
		}
		
		return String.valueOf(duration) + time_units;
	}

	private long start_time = 0;
	private long stop_time = 0;

	private boolean running = false;
}
