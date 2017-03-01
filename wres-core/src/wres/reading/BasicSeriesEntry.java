/**
 * 
 */
package wres.reading;

import java.util.Date;
import wres.util.RealCollection;

/**
 * @author ctubbs
 *
 * Represents a collection of values at a specific point in time
 * 
 */
public abstract class BasicSeriesEntry implements Comparable<BasicSeriesEntry> {
	
	/**
	 * A collection of real numbered values used to demonstrate all readings for either a forecast or observation at
	 * this point in time
	 */
	public RealCollection values = new RealCollection();
	
	/**
	 * The Date in which the forecast occurred
	 */
	public Date date;
	
	/**
	 * The amount of hours the transpired between the basis time and the time of this forecast
	 */
	public double lead_time = 0.0;	
	
	/**
	 * Prints information about the forecast entry to the standard output
	 */
	public void print()
	{
		System.out.println(toString());
	}
	
	/**
	 * Formats the data entry into a string for human readability
	 * @return A formatted representation of the data entry
	 */
	public String toString()
	{
		String message = "";
		if (date != null)
		{
			message = date.toString();
			message += " ";
		}
		
		message += String.valueOf(lead_time);
		
		for (Double value : values)
		{
			message += " ";
			message += String.valueOf(value);
		}
		
		return message;
	}
	
	public void add_value(int value)
	{
		values.add((double)value);
	}
	
	public void add_value(float value)
	{
		values.add((double)value);
	}
	
	public void add_value(byte value)
	{
		values.add((double)value);
	}
	
	public void add_value(short value)
	{
		values.add((double)value);
	}
	
	public void add_value(long value)
	{
		values.add((double)value);
	}
	
	public void add_value(double value)
	{
		values.add(value);
	}
	
	public void add_value(String value)
	{
		values.add(Double.valueOf(value.trim()));
	}

	@Override
	public int compareTo(BasicSeriesEntry o) {
		// TODO Auto-generated method stub
		return 0;
	}
	
}
