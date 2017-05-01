/**
 * 
 */
package util;

/**
 * @author Christopher Tubbs
 * Basic structure used to evaluate time between two points
 */
public class Stopwatch {

	/**
	 * Constructor
	 */
	public Stopwatch() {
		start_time = 0;
		stop_time = 0;
	}
	
	/**
	 * Resets the time keeping and stores the current time in nanoseconds
	 */
	public void start() {
		start_time = System.nanoTime();
		stop_time = 0;
		running = true;
	}
	
	/**
	 * Halts operations so that a consistent duration may be retrieved
	 */
	public void stop() {
		stop_time = System.nanoTime();
		running = false;
	}
	
	/**
	 * @return The number of milliseconds between the start and end time. If the
	 * time is still running, the current time is used in place of the end time
	 */
	public long getDuration() {
		long dur = 0;
		
		if (running) {
			dur = System.nanoTime() - start_time;
		}
		else {
			dur = stop_time - start_time; 
		}
		
		return dur / 1000000;
	}
	
	/**
	 * @return The current duration between start and stop formatted for human readability
	 */
	public String getFormattedDuration() {
	    long milliseconds = getDuration();
	    String duration = "";
		
		int minutes = 0;
		double seconds = 0.0;
		
		
		if (milliseconds > 60000) {
		    minutes = Utilities.minutesFromMilliseconds(milliseconds);
		    milliseconds = milliseconds - (60000 * minutes); 
		}
		
		if (milliseconds > 1000) {
		    seconds = milliseconds / 1000.0;
		}
		
		if (minutes > 0) {
		    duration = minutes + "m ";		    
		}
		
		if (seconds > 0) {
		    duration += seconds + "s";
		}
		
		if (duration.isEmpty()) {
		    duration = milliseconds + " ms";
		}
		
		return duration;
	}

	/**
	 * The system time when the timer started
	 */
	private long start_time = 0;
	
	/**
	 * The system time when the timer stopped
	 */
	private long stop_time = 0;

	/**
	 * Whether or not the timer is still running
	 */
	private boolean running = false;
}
