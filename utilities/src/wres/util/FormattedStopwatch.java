package wres.util;

import java.time.OffsetDateTime;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

/**
 * @author Christopher Tubbs
 * Basic structure used to evaluate time between two points
 */
public class FormattedStopwatch {

	/**
	 * Constructor
	 */
	public FormattedStopwatch() {
		this.innerWatch = Stopwatch.createUnstarted();
	}
	
	/**
	 * Resets the time keeping and stores the current time in nanoseconds
	 */
	public void start()
	{
		this.innerWatch.reset();
	    this.innerWatch.start();
	    this.startTime = OffsetDateTime.now();
	}
	
	/**
	 * Halts operations so that a consistent duration may be retrieved
	 */
	public void stop()
	{
	    this.innerWatch.stop();
	    this.stopTime = OffsetDateTime.now();
	}
	
	/**
	 * @return The number of milliseconds between the start and end time. If the
	 * time is still running, the current time is used in place of the end time
	 */
	public long getDuration()
	{
		return this.innerWatch.elapsed(TimeUnit.MILLISECONDS);
	}
	
	/**
	 * @return The current duration between start and stop formatted for human readability
	 */
	public String getFormattedDuration()
	{
	    long milliseconds = getDuration();
	    String duration = "";
		
		int minutes = 0;
		int hours = 0;
		double seconds = 0.0;
		
		if (milliseconds >= 3600000)
		{
			hours = Time.hoursFromMilliseconds( milliseconds );
			milliseconds = milliseconds - (3600000 * hours);
		}

		if (milliseconds > 60000)
		{
		    minutes = Time.minutesFromMilliseconds(milliseconds);
		    milliseconds = milliseconds - (60000 * minutes); 
		}
		
		if (milliseconds > 1000)
		{
		    seconds = milliseconds / 1000.0;
		}

		if (hours > 0)
		{
			duration = hours + "hr ";
		}

		if (minutes > 0) {
		    duration += minutes + "m ";
		}
		
		if (seconds > 0) {
		    duration += seconds + "s";
		}
		
		if (duration.isEmpty())
		{
		    duration = milliseconds + " ms";
		}
		
		return duration;
	}

	public String getStartTime()
	{
		String start = "";

		if (this.startTime != null) {
			start =  Time.convertDateToString(this.startTime);
		}

		return start;
	}

	public String getStopTime()
	{
		String stop = "";

		if (this.stopTime != null)
		{
			stop = Time.convertDateToString(this.stopTime);
		}

		return stop;
	}
	
	private Stopwatch innerWatch;
	private OffsetDateTime stopTime;
	private OffsetDateTime startTime;
}
